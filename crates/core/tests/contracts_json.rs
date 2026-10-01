//! Contract compatibility tests.
//!
//! These assert that the Rust types round-trip the exact payload shapes
//! documented in `docs/CONTRACTS_AND_BOUNDARIES.md` and the JSON schemas in
//! `contracts/`. If a change breaks one of these, it is a breaking contract
//! change and requires a new `schema_version`.

use agentic_core::{
    ContractError, ExecutionIntent, ExecutionObservation, ExecutionResult, ExecutionStatus,
    ObservationCategory, PolicyDecision, PolicyDecisionKind, StateSnapshot, SCHEMA_VERSION,
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
        agentic_core::Subject::new("task", "t1"),
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
        agentic_core::Subject::new("task", "t1"),
        agentic_core::RequestedBy::new("rules", "p1"),
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
        agentic_core::Subject::new("task", "t1"),
        agentic_core::RequestedBy::new("rules", "p1"),
    ));
    assert!(policy_decision.validate().is_err());
}
