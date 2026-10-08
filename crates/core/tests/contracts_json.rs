//! Contract compatibility tests.
//!
//! These assert that the Rust types round-trip the exact payload shapes
//! documented in `docs/CONTRACTS_AND_BOUNDARIES.md` and the JSON schemas in
//! `contracts/`. If a change breaks one of these, it is a breaking contract
//! change and requires a new `schema_version`.

use ictus_core::{
    AttemptBudget, ContextKind, ContractError, DecisionContext, DecisionEnvelope, DecisionKind,
    EnvelopeOutcome, ExecutionIntent, ExecutionObservation, ExecutionResult, ExecutionStatus, Fact,
    ObservationCategory, PolicyDecision, PolicyDecisionKind, SnapshotRef, StateSnapshot,
    SCHEMA_VERSION,
};

#[test]
fn state_snapshot_matches_documented_example() {
    let raw = r#"{
        "schema_version": 1,
        "snapshot_id": "5e2f1c2a-0000-4000-8000-000000000001",
        "timestamp": "2026-10-01T18:00:00Z",
        "domain": "software",
        "subject": { "type": "task", "id": "task-123" },
        "facts": [ { "key": "retry.attempt", "value": 0 } ],
        "capabilities": ["demo.verify"],
        "constraints": ["approval_required"]
    }"#;
    let snapshot: StateSnapshot = serde_json::from_str(raw).unwrap();
    snapshot.validate().unwrap();
    assert_eq!(snapshot.subject.kind, "task");
    assert_eq!(snapshot.fact_i64("retry.attempt"), Some(0));

    let value = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(value["subject"]["type"], "task");
    assert_eq!(value["schema_version"], SCHEMA_VERSION);
}

#[test]
fn execution_intent_matches_documented_example() {
    let raw = r#"{
        "schema_version": 1,
        "intent_id": "11111111-0000-4000-8000-000000000001",
        "capability": "software.verify",
        "target": { "type": "task", "id": "task-123" },
        "arguments": {},
        "policy_context": {},
        "requested_by": { "provider": "rules", "decision_id": "22222222-0000-4000-8000-000000000002" }
    }"#;
    let intent: ExecutionIntent = serde_json::from_str(raw).unwrap();
    intent.validate().unwrap();
    assert_eq!(intent.capability, "software.verify");

    let value = serde_json::to_value(&intent).unwrap();
    assert_eq!(value["target"]["type"], "task");
    assert_eq!(value["schema_version"], SCHEMA_VERSION);
}

#[test]
fn execution_result_matches_documented_example() {
    let raw = r#"{
        "schema_version": 1,
        "execution_id": "33333333-0000-4000-8000-000000000003",
        "intent_id": "11111111-0000-4000-8000-000000000001",
        "status": "failed",
        "observation": { "category": "VERIFICATION_FAILURE" },
        "evidence": [],
        "started_at": "2026-10-01T18:00:00Z",
        "finished_at": "2026-10-01T18:00:05Z"
    }"#;
    let result: ExecutionResult = serde_json::from_str(raw).unwrap();
    result.validate().unwrap();
    assert_eq!(result.status, ExecutionStatus::Failed);
    assert_eq!(
        result.observation.category,
        ObservationCategory::VerificationFailure
    );
    assert!(!result.is_success());

    let value = serde_json::to_value(&result).unwrap();
    assert_eq!(value["status"], "failed");
    assert_eq!(value["observation"]["category"], "VERIFICATION_FAILURE");
}

#[test]
fn all_serialized_contracts_carry_schema_version() {
    let snapshot = StateSnapshot::new(
        "s1",
        "2026-10-01T00:00:00Z",
        "software",
        ictus_core::Subject::new("task", "t1"),
    );
    let observation = ExecutionObservation::new(
        "o1",
        "e1",
        "i1",
        ObservationCategory::Success,
        "2026-10-01T00:00:00Z",
    );
    let intent = ExecutionIntent::new(
        "i1",
        "demo.verify",
        ictus_core::Subject::new("task", "t1"),
        ictus_core::RequestedBy::new("rules", "p1"),
    );
    let result = ExecutionResult::new(
        "e1",
        "i1",
        ExecutionStatus::Succeeded,
        ObservationCategory::Success,
    );
    let policy_decision = PolicyDecision::new(
        "pd1",
        "p1",
        PolicyDecisionKind::Allow,
        "2026-10-01T00:00:00Z",
    );

    for value in [
        serde_json::to_value(&snapshot).unwrap(),
        serde_json::to_value(&observation).unwrap(),
        serde_json::to_value(&intent).unwrap(),
        serde_json::to_value(&result).unwrap(),
        serde_json::to_value(&policy_decision).unwrap(),
    ] {
        assert_eq!(value["schema_version"], SCHEMA_VERSION);
    }
}

#[test]
fn unknown_schema_version_is_rejected() {
    let raw = r#"{
        "schema_version": 99,
        "intent_id": "i1",
        "capability": "demo.verify",
        "target": { "type": "task", "id": "t1" },
        "requested_by": { "provider": "rules", "decision_id": "p1" }
    }"#;
    let intent: ExecutionIntent = serde_json::from_str(raw).unwrap();
    assert!(matches!(
        intent.validate(),
        Err(ContractError::UnsupportedSchemaVersion { found: 99, .. })
    ));
}

#[test]
fn deny_decision_cannot_carry_an_intent() {
    let mut policy_decision = PolicyDecision::new(
        "pd1",
        "p1",
        PolicyDecisionKind::Deny,
        "2026-10-01T00:00:00Z",
    );
    policy_decision.modified_intent = Some(ExecutionIntent::new(
        "i1",
        "demo.verify",
        ictus_core::Subject::new("task", "t1"),
        ictus_core::RequestedBy::new("rules", "p1"),
    ));
    assert!(policy_decision.validate().is_err());
}

// ---------------------------------------------------------------------------
// Initial/recovery context and validated decision envelope.
//
// These parse the exact example files the Python suite validates against the
// JSON schemas, so the Rust and Python fixtures cannot drift apart.
// ---------------------------------------------------------------------------

const INITIAL_CONTEXT_JSON: &str = include_str!("../../../examples/decision-context.initial.json");
const RECOVERY_CONTEXT_JSON: &str =
    include_str!("../../../examples/decision-context.recovery.json");
const EXECUTABLE_ENVELOPE_JSON: &str =
    include_str!("../../../examples/decision-envelope.executable.json");
const DENIED_ENVELOPE_JSON: &str = include_str!("../../../examples/decision-envelope.denied.json");
const PENDING_ENVELOPE_JSON: &str =
    include_str!("../../../examples/decision-envelope.pending.json");

#[test]
fn initial_context_fixture_has_no_observation_and_validates() {
    let context: DecisionContext = serde_json::from_str(INITIAL_CONTEXT_JSON).unwrap();
    context.validate().unwrap();
    assert_eq!(context.kind, ContextKind::Initial);
    assert!(context.is_initial());
    assert!(context.recovery_binding().is_none());
    assert_eq!(context.attempts.semantic_attempts, 0);
    assert_eq!(context.attempts.max_semantic_attempts, 2);
    assert_eq!(context.attempts.step_retries, 0);

    let value = serde_json::to_value(&context).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert!(value.get("recovery").is_none());
}

#[test]
fn recovery_context_fixture_binds_the_failed_attempt_and_snapshot() {
    let context: DecisionContext = serde_json::from_str(RECOVERY_CONTEXT_JSON).unwrap();
    context.validate().unwrap();
    assert_eq!(context.kind, ContextKind::Recovery);
    let binding = context.recovery_binding().unwrap();
    assert_eq!(binding.failed_attempt_id, "attempt-0001");
    assert_eq!(binding.failed_intent_id, "intent:proposal-0001");
    assert_eq!(binding.failed_snapshot.snapshot_id, "snap-0001");
    assert_eq!(
        binding.observation.category,
        ObservationCategory::WorkerTimeout
    );
    // The accepted semantic count includes the failed attempt; step retries are
    // a separate execution-backend counter.
    assert_eq!(context.attempts.semantic_attempts, 1);
    assert_eq!(context.attempts.max_semantic_attempts, 2);
    assert_eq!(context.attempts.step_retries, 3);
}

#[test]
fn executable_envelope_fixture_binds_every_required_fact() {
    let envelope: DecisionEnvelope = serde_json::from_str(EXECUTABLE_ENVELOPE_JSON).unwrap();
    envelope.validate().unwrap();
    assert!(envelope.is_executable());
    assert_eq!(envelope.outcome, EnvelopeOutcome::Executable);
    assert_eq!(envelope.snapshot.digest.len(), 64);
    assert_eq!(envelope.proposal.decision, DecisionKind::Reexecute);
    assert_eq!(envelope.proposal.schema_version, 2);
    assert_eq!(envelope.policy.verdict, PolicyDecisionKind::Allow);
    assert!(envelope.capability_validation.admitted && envelope.capability_validation.permitted);
    let intent = envelope.intent.as_ref().unwrap();
    assert_eq!(intent.capability, envelope.capability_validation.capability);
    assert_eq!(intent.target, envelope.subject);
}

#[test]
fn non_executable_envelope_fixtures_carry_no_intent() {
    for raw in [DENIED_ENVELOPE_JSON, PENDING_ENVELOPE_JSON] {
        let envelope: DecisionEnvelope = serde_json::from_str(raw).unwrap();
        envelope.validate().unwrap();
        assert!(!envelope.is_executable());
        assert!(envelope.intent.is_none());
    }
}

#[test]
fn a_boolean_attempt_count_fails_closed() {
    let mut value: serde_json::Value = serde_json::from_str(INITIAL_CONTEXT_JSON).unwrap();
    value["attempts"]["semantic_attempts"] = serde_json::Value::Bool(true);
    assert!(serde_json::from_value::<DecisionContext>(value).is_err());
}

#[test]
fn a_recovery_context_without_a_binding_fails_closed() {
    let mut value: serde_json::Value = serde_json::from_str(RECOVERY_CONTEXT_JSON).unwrap();
    value.as_object_mut().unwrap().remove("recovery");
    let context: DecisionContext = serde_json::from_value(value).unwrap();
    assert!(matches!(
        context.validate(),
        Err(ContractError::MissingField("recovery"))
    ));
}

#[test]
fn accepted_attempts_may_not_exceed_the_total_bound() {
    let mut context: DecisionContext = serde_json::from_str(RECOVERY_CONTEXT_JSON).unwrap();
    context.attempts.semantic_attempts = 3;
    assert!(context.validate().is_err());
}

#[test]
fn an_initial_context_rejects_a_fabricated_observation() {
    let mut context: DecisionContext = serde_json::from_str(INITIAL_CONTEXT_JSON).unwrap();
    context
        .snapshot
        .facts
        .push(Fact::string("observation.category", "WORKER_TIMEOUT"));
    assert!(context.validate().is_err());
}

#[test]
fn a_denied_envelope_cannot_carry_an_intent() {
    let executable: DecisionEnvelope = serde_json::from_str(EXECUTABLE_ENVELOPE_JSON).unwrap();
    let mut denied: DecisionEnvelope = serde_json::from_str(DENIED_ENVELOPE_JSON).unwrap();
    denied.intent = executable.intent;
    assert!(denied.validate().is_err());
}

#[test]
fn an_unknown_context_kind_is_rejected() {
    let mut value: serde_json::Value = serde_json::from_str(INITIAL_CONTEXT_JSON).unwrap();
    value["kind"] = serde_json::Value::String("FRESH".to_string());
    assert!(serde_json::from_value::<DecisionContext>(value).is_err());
}

#[test]
fn snapshot_ref_and_attempt_budget_are_public_contract_types() {
    let reference = SnapshotRef::new("snap-1", 1, "abc");
    reference.validate().unwrap();
    let budget = AttemptBudget::with_attempts(1, 2, 0);
    budget.validate().unwrap();
    assert_eq!(budget.remaining(), 1);
}
