//! Production grant validation at the trusted local composition boundary.

use ictus_core::{
    ApprovalGrant, AttemptBudget, Capability, DecisionContext, DecisionKind, DecisionProposal,
    EvidenceRef, PolicyDecisionKind, ProviderMetadata, RiskClass, SnapshotRef, StateSnapshot,
    Subject,
};
use ictus_policy::{
    revalidate_modified_intent, InMemoryCapabilityRegistry, LocalPolicyComposition,
    ValidationRequest, DEFAULT_POLICY_VERSION,
};

fn context() -> DecisionContext {
    DecisionContext::initial(
        "ctx-1",
        "tactus.generic.v1",
        StateSnapshot::new(
            "snap-1",
            "2026-10-01T00:00:00Z",
            "software",
            Subject::new("task", "t1"),
        ),
        7,
        AttemptBudget::new(2),
    )
}

fn proposal() -> DecisionProposal {
    DecisionProposal::new(
        "proposal-1",
        DecisionKind::ExecuteCapability,
        Subject::new("task", "t1"),
        ProviderMetadata::rules("rules.v1"),
        "2026-10-01T00:00:00Z",
    )
    .with_capability("software.promote")
}

fn registry() -> InMemoryCapabilityRegistry {
    InMemoryCapabilityRegistry::new().with(
        Capability::new("software.promote", "3", RiskClass::High)
            .with_required_approval("human.promote"),
    )
}

fn grant() -> ApprovalGrant {
    ApprovalGrant::new(
        "grant-1",
        "human.promote",
        Subject::new("task", "t1"),
        7,
        "software.promote",
        DEFAULT_POLICY_VERSION,
        "2026-09-30T00:00:00Z",
        "2026-10-02T00:00:00Z",
    )
    .with_evidence(EvidenceRef::new(
        "approval_record",
        "tactus://approvals/grant-1",
    ))
}

fn validate(grants: &[ApprovalGrant]) -> ictus_policy::TrustedDecision {
    let context = context();
    let proposal = proposal();
    let registry = registry();
    LocalPolicyComposition::default()
        .validate(ValidationRequest {
            context: &context,
            snapshot: SnapshotRef::new("snap-1", 7, "digest-1"),
            proposal: &proposal,
            capabilities: &registry,
            grants,
            expiry: "2026-10-01T01:00:00Z",
        })
        .unwrap()
}

#[test]
fn exact_active_bound_grant_produces_a_serializable_trusted_intent() {
    let trusted = validate(&[grant()]);
    let envelope = trusted.envelope();
    assert!(envelope.is_executable());
    assert_eq!(envelope.policy.verdict, PolicyDecisionKind::Allow);
    assert_eq!(envelope.grants.len(), 1);
    assert_eq!(envelope.approvals[0].name, "human.promote");
    assert!(envelope.approvals[0].satisfied);
    assert_eq!(
        trusted.executable_intent().unwrap().policy_context["proposal_digest"],
        envelope.validation.proposal_digest
    );
    let wire = trusted.to_json().unwrap();
    assert!(wire.contains("grant-1"));
    assert!(wire.contains("proposal_digest"));
}

#[test]
fn insufficient_expired_revoked_and_mismatched_grants_never_emit_an_intent() {
    let cases = [
        Vec::new(),
        {
            let mut value = grant();
            value.expires_at = "2026-10-01T00:00:00Z".to_string();
            vec![value]
        },
        vec![grant().revoked()],
        {
            let mut value = grant();
            value.subject = Subject::new("task", "other");
            vec![value]
        },
        {
            let mut value = grant();
            value.revision = 8;
            vec![value]
        },
    ];
    for grants in cases {
        let trusted = validate(&grants);
        assert!(!trusted.envelope().is_executable());
        assert!(trusted.executable_intent().is_none());
        assert_eq!(
            trusted.envelope().policy.verdict,
            PolicyDecisionKind::RequireApproval
        );
    }
}

#[test]
fn wildcard_demo_token_is_not_a_domain_grant_for_high_risk_work() {
    let mut wildcard = grant();
    wildcard.approval = "*".to_string();
    let trusted = validate(&[wildcard]);
    assert!(!trusted.envelope().is_executable());
    assert_eq!(
        trusted.envelope().policy.verdict,
        PolicyDecisionKind::RequireApproval
    );
    assert!(trusted
        .envelope()
        .capability_validation
        .reason
        .contains("demo-only"));
}

#[test]
fn changed_payload_has_a_different_binding_and_requires_fresh_validation() {
    let first = validate(&[grant()]);
    let context = context();
    let changed = proposal().with_argument("target", serde_json::json!("changed"));
    let registry = registry();
    let second = LocalPolicyComposition::default()
        .validate(ValidationRequest {
            context: &context,
            snapshot: SnapshotRef::new("snap-1", 7, "digest-1"),
            proposal: &changed,
            capabilities: &registry,
            grants: &[grant()],
            expiry: "2026-10-01T01:00:00Z",
        })
        .unwrap();
    assert_ne!(
        first.envelope().validation.proposal_digest,
        second.envelope().validation.proposal_digest
    );
    assert_eq!(
        second.executable_intent().unwrap().policy_context["proposal_digest"],
        second.envelope().validation.proposal_digest
    );
    let first_intent = first.executable_intent().unwrap().clone();
    assert!(revalidate_modified_intent(&proposal(), &first_intent, &registry).is_ok());
    assert!(revalidate_modified_intent(&changed, &first_intent, &registry).is_err());
}

#[test]
fn unknown_capability_and_policy_fail_closed() {
    let context = context();
    let proposal = proposal();
    let unknown = LocalPolicyComposition::default()
        .validate(ValidationRequest {
            context: &context,
            snapshot: SnapshotRef::new("snap-1", 7, "digest-1"),
            proposal: &proposal,
            capabilities: &InMemoryCapabilityRegistry::new(),
            grants: &[grant()],
            expiry: "2026-10-01T01:00:00Z",
        })
        .unwrap();
    assert!(!unknown.envelope().is_executable());
    assert_eq!(unknown.envelope().policy.verdict, PolicyDecisionKind::Deny);

    let rejected = LocalPolicyComposition::new("unknown.policy.v9")
        .validate(ValidationRequest {
            context: &context,
            snapshot: SnapshotRef::new("snap-1", 7, "digest-1"),
            proposal: &proposal,
            capabilities: &registry(),
            grants: &[grant()],
            expiry: "2026-10-01T01:00:00Z",
        })
        .unwrap();
    assert!(!rejected.envelope().is_executable());
    assert_eq!(rejected.envelope().policy.verdict, PolicyDecisionKind::Deny);
}
