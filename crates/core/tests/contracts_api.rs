//! API-level coverage for every public contract type: constructors, builder
//! methods, validation error paths and serialization shape.
//!
//! `contracts_json.rs` covers the documented wire examples; this file covers
//! the surface around them so no public behaviour is untested.

use ictus_core::execution::ObservationSummary;
use ictus_core::{
    Capability, ContractError, DecisionKind, DecisionProposal, EvidenceRef, ExecutionIntent,
    ExecutionObservation, ExecutionResult, ExecutionStatus, Fact, ObservationCategory,
    PolicyDecision, PolicyDecisionKind, ProviderMetadata, ProviderType, RequestedBy, RiskClass,
    RouteConstraints, StateSnapshot, Subject, DECISION_SCHEMA_VERSION,
    LEGACY_DECISION_SCHEMA_VERSION, SCHEMA_VERSION,
};

fn subject() -> Subject {
    Subject::new("task", "t1")
}

fn make_subject() -> Subject {
    subject()
}

fn make_snapshot() -> StateSnapshot {
    StateSnapshot::new("s1", "2026-10-01T00:00:00Z", "software", subject())
}

fn make_proposal(decision: DecisionKind) -> DecisionProposal {
    DecisionProposal::new(
        "p1",
        decision,
        subject(),
        ProviderMetadata::rules("rules.v1"),
        "2026-10-01T00:00:00Z",
    )
}

// -- version ----------------------------------------------------------------

#[test]
fn schema_version_constant_is_one() {
    assert_eq!(SCHEMA_VERSION, 1);
}

#[test]
fn decision_contract_is_v2_with_v1_legacy_support() {
    assert_eq!(DECISION_SCHEMA_VERSION, 2);
    assert_eq!(LEGACY_DECISION_SCHEMA_VERSION, 1);
    // New proposals default to the current decision contract version.
    assert_eq!(make_proposal(DecisionKind::Abort).schema_version, 2);
}

#[test]
fn version_and_field_helpers_behave() {
    assert!(ContractError::check_version(1).is_ok());
    assert!(ContractError::check_version(2).is_err());
    assert!(ContractError::require_non_empty("x", "value").is_ok());
    assert!(ContractError::require_non_empty("x", "  ").is_err());
}

// -- subject / fact / snapshot ---------------------------------------------

#[test]
fn subject_serializes_type_not_kind() {
    let value = serde_json::to_value(subject()).unwrap();
    assert_eq!(value["type"], "task");
    assert_eq!(value["id"], "t1");
    assert!(value.get("kind").is_none());
}

#[test]
fn evidence_ref_builders_and_optional_fields() {
    let bare = EvidenceRef::new("log", "file:out.log");
    let value = serde_json::to_value(&bare).unwrap();
    assert!(value.get("sha256").is_none());
    assert!(value.get("note").is_none());

    let full = EvidenceRef::new("log", "file:out.log")
        .with_sha256("abc123")
        .with_note("synthetic");
    let value = serde_json::to_value(&full).unwrap();
    assert_eq!(value["sha256"], "abc123");
    assert_eq!(value["note"], "synthetic");
}

#[test]
fn fact_helpers_round_trip() {
    assert_eq!(Fact::string("k", "v").value, serde_json::json!("v"));
    assert_eq!(Fact::integer("k", 3).value, serde_json::json!(3));
    assert_eq!(Fact::new("k", serde_json::json!(true)).key, "k");
}

#[test]
fn snapshot_builders_and_accessors() {
    let snapshot = make_snapshot()
        .with_fact(Fact::string("observation.category", "SUCCESS"))
        .with_fact(Fact::integer("retry.attempt", 1))
        .with_capability("demo.verify")
        .with_constraint("approval_required");

    assert_eq!(snapshot.fact_str("observation.category"), Some("SUCCESS"));
    assert_eq!(snapshot.fact_i64("retry.attempt"), Some(1));
    assert_eq!(snapshot.fact_i64("missing"), None);
    assert_eq!(snapshot.fact("missing"), None);
    assert_eq!(snapshot.fact_str("retry.attempt"), None); // not a string
    assert_eq!(snapshot.capabilities, vec!["demo.verify"]);
    assert_eq!(snapshot.constraints, vec!["approval_required"]);
    snapshot.validate().unwrap();
}

#[test]
fn snapshot_validation_rejects_blank_required_fields() {
    let mut snapshot = make_snapshot();
    snapshot.snapshot_id = "  ".to_string();
    assert!(matches!(
        snapshot.validate(),
        Err(ContractError::MissingField("snapshot_id"))
    ));

    let mut snapshot = make_snapshot();
    snapshot.domain = String::new();
    assert!(snapshot.validate().is_err());

    let mut snapshot = make_snapshot();
    snapshot.subject.kind = String::new();
    assert!(snapshot.validate().is_err());

    let mut snapshot = make_snapshot();
    snapshot.subject.id = String::new();
    assert!(snapshot.validate().is_err());
}

// -- capability -------------------------------------------------------------

#[test]
fn capability_builders_and_serialization() {
    let capability = Capability::new("software.promote", "2", RiskClass::High)
        .with_side_effects(true)
        .with_idempotent(false)
        .with_required_approval("human")
        .with_required_approval("security")
        .with_timeout_class("long")
        .with_backend("dagster");

    assert_eq!(capability.required_approvals, vec!["human", "security"]);
    assert_eq!(capability.timeout_class, "long");
    assert!(capability.side_effects);
    assert!(!capability.idempotent);
    assert_eq!(capability.execution_backend.as_deref(), Some("dagster"));
    capability.validate().unwrap();

    let value = serde_json::to_value(&capability).unwrap();
    assert_eq!(value["risk_class"], "HIGH"); // SCREAMING_SNAKE_CASE
    assert_eq!(value["required_approvals"][0], "human");
}

#[test]
fn capability_validation_rejects_blanks() {
    assert!(Capability::new("", "1", RiskClass::Low).validate().is_err());
    assert!(Capability::new("a", "", RiskClass::Low).validate().is_err());
    assert!(Capability::new("a", "1", RiskClass::Low)
        .with_timeout_class("")
        .validate()
        .is_err());
}

#[test]
fn capability_to_json_array_round_trips() {
    let capabilities = vec![Capability::new("demo.verify", "1", RiskClass::Low)];
    let json = ictus_core::capability::to_json_array(&capabilities).unwrap();
    let decoded: Vec<Capability> = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, capabilities);
}

// -- decision ---------------------------------------------------------------

#[test]
fn provider_metadata_and_proposal_builders() {
    let metadata = ProviderMetadata::rules("rules.v1").with_version("0.1.0");
    assert_eq!(metadata.provider_type, ProviderType::Rules);
    assert_eq!(metadata.provider_version.as_deref(), Some("0.1.0"));

    let proposal = make_proposal(DecisionKind::ExecuteCapability)
        .with_capability("demo.verify")
        .with_argument("k", serde_json::json!(1))
        .with_reason("because");
    assert!(proposal.requires_capability());
    assert_eq!(proposal.reason.as_deref(), Some("because"));
    proposal.validate().unwrap();

    assert!(!make_proposal(DecisionKind::Abort).requires_capability());
    assert!(!make_proposal(DecisionKind::Escalate).requires_capability());
}

#[test]
fn proposal_validation_rejects_bad_shapes() {
    // capability-requiring decision without a capability
    assert!(make_proposal(DecisionKind::Reexecute).validate().is_err());
    // empty capability string
    assert!(make_proposal(DecisionKind::Reexecute)
        .with_capability("  ")
        .validate()
        .is_err());
    // empty provider id
    let mut bad = make_proposal(DecisionKind::Abort);
    bad.provider.provider_id = String::new();
    assert!(bad.validate().is_err());
    // confidence out of range
    let mut bad = make_proposal(DecisionKind::Abort);
    bad.provider.confidence = Some(1.5);
    assert!(bad.validate().is_err());
}

#[test]
fn decision_kinds_serialize_screaming_snake() {
    assert_eq!(
        serde_json::to_value(DecisionKind::ExecuteCapability).unwrap(),
        "EXECUTE_CAPABILITY"
    );
    assert_eq!(
        serde_json::to_value(DecisionKind::Reexecute).unwrap(),
        "REEXECUTE"
    );
    assert_eq!(serde_json::to_value(DecisionKind::Route).unwrap(), "ROUTE");
    assert_eq!(
        serde_json::to_value(DecisionKind::Decompose).unwrap(),
        "DECOMPOSE"
    );
    assert_eq!(serde_json::to_value(ProviderType::Human).unwrap(), "HUMAN");
}

#[test]
fn v1_retry_token_is_normalized_to_reexecute() {
    let proposal: DecisionProposal = serde_json::from_str(
        r#"{
            "schema_version": 1,
            "proposal_id": "p1",
            "decision": "RETRY",
            "subject": { "type": "task", "id": "t1" },
            "capability": "demo.verify",
            "provider": { "provider_type": "RULES", "provider_id": "rules.v1" },
            "proposed_at": "2026-10-01T00:00:00Z"
        }"#,
    )
    .unwrap();
    assert_eq!(proposal.decision, DecisionKind::Reexecute);
    proposal.validate().unwrap();
    // v1 legacy proposals are still readable...
    assert_eq!(proposal.schema_version, 1);
}

#[test]
fn v1_payload_rejects_v2_only_decisions() {
    for decision in [DecisionKind::Route, DecisionKind::Decompose] {
        let mut proposal = make_proposal(decision);
        proposal.schema_version = LEGACY_DECISION_SCHEMA_VERSION;
        assert!(
            proposal.validate().is_err(),
            "{decision:?} must not be valid under v1"
        );
    }
}

#[test]
fn unknown_proposal_version_is_rejected() {
    let mut proposal = make_proposal(DecisionKind::Abort);
    proposal.schema_version = DECISION_SCHEMA_VERSION + 1;
    assert!(matches!(
        proposal.validate(),
        Err(ContractError::UnsupportedSchemaVersion { .. })
    ));
}

#[test]
fn capability_requirements_are_exhaustive() {
    for decision in [DecisionKind::Reexecute, DecisionKind::Route] {
        assert!(make_proposal(decision).requires_capability());
    }
    for decision in [
        DecisionKind::Abort,
        DecisionKind::Escalate,
        DecisionKind::Decompose,
    ] {
        assert!(!make_proposal(decision).requires_capability());
    }
}

#[test]
fn route_constraints_are_generic_and_round_trip() {
    let route = RouteConstraints::new()
        .with_exclude_backend("backend.a")
        .with_preferred_backend("backend.b")
        .with_required_provider("provider.c")
        .with_required_runtime("wasm");
    assert!(!route.is_empty());
    let proposal = make_proposal(DecisionKind::Route)
        .with_capability("demo.verify")
        .with_route(route);
    proposal.validate().unwrap();
    let value = serde_json::to_value(&proposal).unwrap();
    assert_eq!(value["route"]["exclude_backend"], "backend.a");
    assert_eq!(value["route"]["required_runtime"], "wasm");
    let decoded: DecisionProposal = serde_json::from_value(value).unwrap();
    assert_eq!(decoded, proposal);
}

#[test]
fn route_constraints_on_a_non_route_decision_are_rejected() {
    let proposal = make_proposal(DecisionKind::ExecuteCapability)
        .with_capability("demo.verify")
        .with_route(RouteConstraints::new().with_preferred_backend("backend.b"));
    assert!(proposal.validate().is_err());
}

#[test]
fn blank_route_constraint_is_rejected() {
    let proposal = make_proposal(DecisionKind::Route)
        .with_capability("demo.verify")
        .with_route(RouteConstraints::new().with_required_provider("  "));
    assert!(proposal.validate().is_err());
    assert!(RouteConstraints::new().is_empty());
}

// -- observation ------------------------------------------------------------

#[test]
fn observation_new_and_validate() {
    let observation = ExecutionObservation::new(
        "o1",
        "e1",
        "i1",
        ObservationCategory::WorkerTimeout,
        "2026-10-01T00:00:00Z",
    );
    observation.validate().unwrap();
    assert_eq!(
        serde_json::to_value(ObservationCategory::WorkerTimeout).unwrap(),
        "WORKER_TIMEOUT"
    );

    let mut bad = observation.clone();
    bad.intent_id = String::new();
    assert!(bad.validate().is_err());
}

// -- execution --------------------------------------------------------------

#[test]
fn intent_builders_and_validation() {
    let intent = ExecutionIntent::new(
        "i1",
        "demo.verify",
        subject(),
        RequestedBy::new("rules", "p1"),
    )
    .with_argument("fail_until_attempt", serde_json::json!(2))
    .with_policy_context("domain", serde_json::json!("software"));
    intent.validate().unwrap();
    assert_eq!(intent.arguments["fail_until_attempt"], serde_json::json!(2));
    assert_eq!(
        intent.policy_context["domain"],
        serde_json::json!("software")
    );

    let mut bad = intent.clone();
    bad.capability = "  ".to_string();
    assert!(bad.validate().is_err());

    let mut bad = intent.clone();
    bad.target.id = String::new();
    assert!(bad.validate().is_err());

    let mut bad = intent.clone();
    bad.requested_by.decision_id = String::new();
    assert!(bad.validate().is_err());
}

#[test]
fn result_builders_status_and_observation_mapping() {
    let mut result = ExecutionResult::new(
        "e1",
        "i1",
        ExecutionStatus::Succeeded,
        ObservationCategory::Success,
    )
    .with_evidence(EvidenceRef::new("dagster_run", "dagster://runs/e1"))
    .with_times("2026-10-01T00:00:00Z", "2026-10-01T00:00:01Z");

    assert!(result.is_success());
    result.validate().unwrap();
    assert_eq!(result.schema_version, SCHEMA_VERSION);
    assert_eq!(result.started_at.as_deref(), Some("2026-10-01T00:00:00Z"));
    assert_eq!(result.finished_at.as_deref(), Some("2026-10-01T00:00:01Z"));

    // A failed result is not a success.
    result.status = ExecutionStatus::Failed;
    result.observation = ObservationSummary {
        category: ObservationCategory::ProcessCrash,
    };
    assert!(!result.is_success());

    // Result -> observation fact mapping.
    let observation = result.to_observation("o2", "2026-10-01T00:00:02Z");
    observation.validate().unwrap();
    assert_eq!(observation.execution_id, "e1");
    assert_eq!(observation.intent_id, "i1");
    assert_eq!(observation.category, ObservationCategory::ProcessCrash);
    assert_eq!(observation.evidence.len(), 1);
}

#[test]
fn execution_status_serializes_snake_case() {
    for (status, token) in [
        (ExecutionStatus::Succeeded, "succeeded"),
        (ExecutionStatus::Failed, "failed"),
        (ExecutionStatus::TimedOut, "timed_out"),
        (ExecutionStatus::Cancelled, "cancelled"),
    ] {
        assert_eq!(serde_json::to_value(status).unwrap(), token);
    }
}

// -- policy -----------------------------------------------------------------

#[test]
fn policy_decision_builders_and_executability() {
    let denied = PolicyDecision::new("pd1", "p1", PolicyDecisionKind::Deny, "t").with_reason("no");
    assert!(!denied.is_executable());
    denied.validate().unwrap();
    assert_eq!(denied.reasons, vec!["no"]);

    let intent = ExecutionIntent::new(
        "i1",
        "demo.verify",
        subject(),
        RequestedBy::new("rules", "p1"),
    );
    let allowed =
        PolicyDecision::new("pd2", "p1", PolicyDecisionKind::Allow, "t").with_intent(intent);
    assert!(allowed.is_executable());
    allowed.validate().unwrap();

    // REQUIRE_APPROVAL is not executable even with an intent attached.
    let pending = PolicyDecision::new("pd3", "p1", PolicyDecisionKind::RequireApproval, "t")
        .with_intent(ExecutionIntent::new(
            "i1",
            "demo.verify",
            subject(),
            RequestedBy::new("rules", "p1"),
        ));
    assert!(!pending.is_executable());
}

#[test]
fn policy_decision_deny_with_intent_is_invalid() {
    let mut denied = PolicyDecision::new("pd", "p", PolicyDecisionKind::Deny, "t");
    denied.modified_intent = Some(ExecutionIntent::new(
        "i",
        "demo.verify",
        subject(),
        RequestedBy::new("rules", "p"),
    ));
    assert!(matches!(
        denied.validate(),
        Err(ContractError::InvalidValue {
            field: "modified_intent",
            ..
        })
    ));
}

#[test]
fn policy_decision_kind_serializes_screaming_snake() {
    assert_eq!(
        serde_json::to_value(PolicyDecisionKind::RequireApproval).unwrap(),
        "REQUIRE_APPROVAL"
    );
}

#[test]
fn observation_validate_rejects_empty_ids() {
    let mut observation = ExecutionObservation::new(
        "o1",
        "e1",
        "i1",
        ObservationCategory::Success,
        "2026-10-01T00:00:00Z",
    );
    observation.observation_id = String::new();
    assert!(observation.validate().is_err());

    let mut observation = ExecutionObservation::new(
        "o1",
        "e1",
        "i1",
        ObservationCategory::Success,
        "2026-10-01T00:00:00Z",
    );
    observation.execution_id = String::new();
    assert!(observation.validate().is_err());
}

#[test]
fn intent_validate_rejects_empty_ids_and_provenance() {
    let base = || {
        ExecutionIntent::new(
            "i1",
            "demo.verify",
            make_subject(),
            RequestedBy::new("rules", "p1"),
        )
    };

    let mut intent = base();
    intent.intent_id = String::new();
    assert!(intent.validate().is_err());

    let mut intent = base();
    intent.requested_by.provider = String::new();
    assert!(intent.validate().is_err());
}

#[test]
fn result_validate_rejects_empty_ids() {
    let mut result = ExecutionResult::new(
        "e1",
        "i1",
        ExecutionStatus::Succeeded,
        ObservationCategory::Success,
    );
    result.execution_id = String::new();
    assert!(result.validate().is_err());

    let mut result = ExecutionResult::new(
        "e1",
        "i1",
        ExecutionStatus::Succeeded,
        ObservationCategory::Success,
    );
    result.intent_id = String::new();
    assert!(result.validate().is_err());
}

#[test]
fn proposal_validate_rejects_empty_proposal_and_subject() {
    let mut proposal = make_proposal(DecisionKind::Abort);
    proposal.proposal_id = String::new();
    assert!(proposal.validate().is_err());

    let mut proposal = make_proposal(DecisionKind::Abort);
    proposal.subject.id = String::new();
    assert!(proposal.validate().is_err());
}

#[test]
fn policy_decision_validate_rejects_empty_ids() {
    let mut decision = PolicyDecision::new("pd", "p", PolicyDecisionKind::Allow, "t");
    decision.policy_decision_id = String::new();
    assert!(decision.validate().is_err());

    let mut decision = PolicyDecision::new("pd", "p", PolicyDecisionKind::Allow, "t");
    decision.proposal_id = String::new();
    assert!(decision.validate().is_err());
}
