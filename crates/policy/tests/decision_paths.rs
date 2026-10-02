//! Branch coverage for the deterministic rule provider and the policy evaluator.
//!
//! `decision_flow.rs` covers the happy paths; this file exercises every
//! observation category, the no-capability path, provenance capture and the
//! deny paths of the evaluator.

use ictus_core::{
    Capability, DecisionKind, DecisionProposal, Fact, PolicyDecisionKind, ProviderMetadata,
    RiskClass, StateSnapshot, Subject,
};
use ictus_policy::{
    AlwaysApproved, DefaultPolicyEvaluator, InMemoryCapabilityRegistry, RuleDecisionProvider,
};
use ictus_ports::{DecisionProvider, PolicyEvaluator};

fn snapshot(category: &str, attempt: i64, budget: i64, capability: Option<&str>) -> StateSnapshot {
    let mut snapshot = StateSnapshot::new(
        "s1",
        "2026-10-01T00:00:00Z",
        "software",
        Subject::new("task", "t1"),
    )
    .with_fact(Fact::string("observation.category", category))
    .with_fact(Fact::integer("retry.attempt", attempt))
    .with_fact(Fact::integer("retry.budget", budget));
    if let Some(capability) = capability {
        snapshot = snapshot.with_fact(Fact::string("capability.id", capability));
    }
    snapshot
}

fn registry() -> InMemoryCapabilityRegistry {
    InMemoryCapabilityRegistry::new()
        .with(Capability::new("demo.verify", "1", RiskClass::Low).with_idempotent(true))
}

fn propose(snapshot: &StateSnapshot) -> DecisionProposal {
    RuleDecisionProvider::new().propose(snapshot).unwrap()
}

fn evaluate(snapshot: &StateSnapshot, proposal: &DecisionProposal) -> ictus_core::PolicyDecision {
    DefaultPolicyEvaluator::new()
        .evaluate(snapshot, proposal, &registry(), &AlwaysApproved)
        .unwrap()
}

// -- rule provider branches -------------------------------------------------

#[test]
fn timeout_without_a_capability_escalates() {
    let proposal = propose(&snapshot("WORKER_TIMEOUT", 0, 2, None));
    assert_eq!(proposal.decision, DecisionKind::Escalate);
    assert!(proposal.capability.is_none());
}

#[test]
fn process_crash_within_budget_retries() {
    let proposal = propose(&snapshot("PROCESS_CRASH", 1, 3, Some("demo.verify")));
    assert_eq!(proposal.decision, DecisionKind::Retry);
}

#[test]
fn provider_unavailable_with_exhausted_budget_escalates() {
    let proposal = propose(&snapshot("PROVIDER_UNAVAILABLE", 3, 3, Some("demo.verify")));
    assert_eq!(proposal.decision, DecisionKind::Escalate);
}

#[test]
fn resource_exhausted_within_budget_retries() {
    let proposal = propose(&snapshot("RESOURCE_EXHAUSTED", 0, 1, Some("demo.verify")));
    assert_eq!(proposal.decision, DecisionKind::Retry);
}

#[test]
fn integration_conflict_escalates() {
    let proposal = propose(&snapshot("INTEGRATION_CONFLICT", 0, 9, Some("demo.verify")));
    assert_eq!(proposal.decision, DecisionKind::Escalate);
}

#[test]
fn unknown_category_escalates() {
    let proposal = propose(&snapshot("SOMETHING_NEW", 0, 9, Some("demo.verify")));
    assert_eq!(proposal.decision, DecisionKind::Escalate);
}

#[test]
fn observation_message_is_captured_as_an_argument() {
    let snapshot = snapshot("PROCESS_CRASH", 0, 2, Some("demo.verify"))
        .with_fact(Fact::string("observation.message", "worker exited 137"));
    let proposal = propose(&snapshot);
    assert_eq!(
        proposal.arguments["observation_message"],
        serde_json::json!("worker exited 137")
    );
}

// -- evaluator deny branches ------------------------------------------------

#[test]
fn structurally_invalid_proposal_is_denied() {
    let mut proposal = DecisionProposal::new(
        "p1",
        DecisionKind::Abort,
        Subject::new("task", "t1"),
        ProviderMetadata::rules("rules"),
        "2026-10-01T00:00:00Z",
    );
    proposal.provider.provider_id = String::new();
    let decision = evaluate(&snapshot("SUCCESS", 0, 0, None), &proposal);
    assert_eq!(decision.decision, PolicyDecisionKind::Deny);
}

#[test]
fn unsupported_snapshot_version_is_denied() {
    let mut bad_snapshot = snapshot("WORKER_TIMEOUT", 0, 2, Some("demo.verify"));
    bad_snapshot.schema_version = 99;
    let proposal = DecisionProposal::new(
        "p1",
        DecisionKind::ExecuteCapability,
        Subject::new("task", "t1"),
        ProviderMetadata::rules("rules"),
        "2026-10-01T00:00:00Z",
    )
    .with_capability("demo.verify");
    let decision = evaluate(&bad_snapshot, &proposal);
    assert_eq!(decision.decision, PolicyDecisionKind::Deny);
}

#[test]
fn invalid_registered_capability_is_denied() {
    let registry =
        InMemoryCapabilityRegistry::new().with(Capability::new("bad.cap", "", RiskClass::Low)); // empty version
    let proposal = DecisionProposal::new(
        "p1",
        DecisionKind::ExecuteCapability,
        Subject::new("task", "t1"),
        ProviderMetadata::rules("rules"),
        "2026-10-01T00:00:00Z",
    )
    .with_capability("bad.cap");
    let decision = DefaultPolicyEvaluator::new()
        .evaluate(
            &snapshot("SUCCESS", 0, 0, Some("bad.cap")),
            &proposal,
            &registry,
            &AlwaysApproved,
        )
        .unwrap();
    assert_eq!(decision.decision, PolicyDecisionKind::Deny);
}

#[test]
fn retry_within_budget_is_allowed() {
    let snapshot = snapshot("WORKER_TIMEOUT", 0, 2, Some("demo.verify"));
    let decision = evaluate(&snapshot, &propose(&snapshot));
    assert_eq!(decision.decision, PolicyDecisionKind::Allow);
    assert!(decision.is_executable());
}
