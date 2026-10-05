//! End-to-end decision flow (M2 acceptance).
//!
//! A synthesized observation produces a typed proposal, a deterministic policy
//! decision and an execution intent — with no Dagster and no domain-system knowledge.

use ictus_core::{
    DecisionKind, Fact, ObservationCategory, PolicyDecisionKind, StateSnapshot, Subject,
};
use ictus_policy::{
    AlwaysApproved, DefaultPolicyEvaluator, InMemoryCapabilityRegistry, NeverApproved,
    RuleDecisionProvider,
};
use ictus_ports::{DecisionProvider, PolicyEvaluator};

fn recovery_snapshot(category: &str, attempt: i64, budget: i64) -> StateSnapshot {
    StateSnapshot::new(
        "snap-1",
        "2026-10-01T00:00:00Z",
        "software",
        Subject::new("task", "task-123"),
    )
    .with_fact(Fact::string("observation.category", category))
    .with_fact(Fact::integer("retry.attempt", attempt))
    .with_fact(Fact::integer("retry.budget", budget))
    .with_fact(Fact::string("capability.id", "demo.verify"))
    .with_capability("demo.verify")
}

fn registry() -> InMemoryCapabilityRegistry {
    use ictus_core::{Capability, RiskClass};
    InMemoryCapabilityRegistry::new()
        .with(Capability::new("demo.verify", "1", RiskClass::Low).with_idempotent(true))
}

#[test]
fn full_recovery_flow_produces_a_typed_intent() {
    let snapshot = recovery_snapshot("WORKER_TIMEOUT", 0, 2);
    let proposal = RuleDecisionProvider::new().propose(&snapshot).unwrap();
    assert_eq!(proposal.decision, DecisionKind::Reexecute);

    let policy_decision = DefaultPolicyEvaluator::new()
        .evaluate(&snapshot, &proposal, &registry(), &AlwaysApproved)
        .unwrap();
    assert_eq!(policy_decision.decision, PolicyDecisionKind::Allow);
    assert!(policy_decision.is_executable());

    let intent = policy_decision.modified_intent.unwrap();
    assert_eq!(intent.capability, "demo.verify");
    assert_eq!(intent.target.id, "task-123");
    assert_eq!(intent.requested_by.decision_id, proposal.proposal_id);
    intent.validate().unwrap();
}

#[test]
fn observation_to_contract_category_mapping_is_stable() {
    // The rules provider parses the same SCREAMING_SNAKE_CASE tokens the
    // contracts and JSON schemas use.
    for (token, expected) in [
        ("SUCCESS", ObservationCategory::Success),
        ("WORKER_TIMEOUT", ObservationCategory::WorkerTimeout),
        ("PROCESS_CRASH", ObservationCategory::ProcessCrash),
        (
            "VERIFICATION_FAILURE",
            ObservationCategory::VerificationFailure,
        ),
        (
            "INTEGRATION_CONFLICT",
            ObservationCategory::IntegrationConflict,
        ),
        ("RESOURCE_EXHAUSTED", ObservationCategory::ResourceExhausted),
        (
            "PROVIDER_UNAVAILABLE",
            ObservationCategory::ProviderUnavailable,
        ),
    ] {
        let serialized = serde_json::to_value(expected).unwrap();
        assert_eq!(serialized, token);
    }
}

#[test]
fn exhausted_budget_escalates_and_never_executes() {
    let snapshot = recovery_snapshot("WORKER_TIMEOUT", 2, 2);
    let proposal = RuleDecisionProvider::new().propose(&snapshot).unwrap();
    assert_eq!(proposal.decision, DecisionKind::Escalate);
    let policy_decision = DefaultPolicyEvaluator::new()
        .evaluate(&snapshot, &proposal, &registry(), &AlwaysApproved)
        .unwrap();
    assert_eq!(policy_decision.decision, PolicyDecisionKind::Allow);
    assert!(!policy_decision.is_executable());
    assert!(policy_decision.modified_intent.is_none());
}

#[test]
fn approval_required_path_is_not_executable_until_approved() {
    use ictus_core::{Capability, RiskClass};
    let snapshot = recovery_snapshot("WORKER_TIMEOUT", 0, 2);
    let proposal = RuleDecisionProvider::new().propose(&snapshot).unwrap();
    let registry = InMemoryCapabilityRegistry::new()
        .with(Capability::new("demo.verify", "1", RiskClass::High).with_required_approval("human"));
    let pending = DefaultPolicyEvaluator::new()
        .evaluate(&snapshot, &proposal, &registry, &NeverApproved)
        .unwrap();
    assert_eq!(pending.decision, PolicyDecisionKind::RequireApproval);
    assert!(!pending.is_executable());

    let approved = DefaultPolicyEvaluator::new()
        .evaluate(&snapshot, &proposal, &registry, &AlwaysApproved)
        .unwrap();
    assert_eq!(approved.decision, PolicyDecisionKind::Allow);
    assert!(approved.is_executable());
}
