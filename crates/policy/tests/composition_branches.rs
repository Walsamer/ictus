//! Branch and boundary coverage for the trusted local policy composition.
//!
//! These tests exist to pin the exact conditional structure of
//! [`LocalPolicyComposition`] (context/snapshot/proposal binding checks, expiry
//! validation, grant timestamp windows and modified-intent revalidation), so a
//! weakened boolean operator cannot silently widen acceptance.

use ictus_core::{
    ApprovalGrant, AttemptBudget, Capability, DecisionContext, DecisionKind, DecisionProposal,
    EnvelopeOutcome, EvidenceRef, PolicyDecisionKind, ProviderMetadata, RiskClass, SnapshotRef,
    StateSnapshot, Subject,
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

fn compose_with(
    context: &DecisionContext,
    snapshot: SnapshotRef,
    proposal: &DecisionProposal,
    capabilities: &InMemoryCapabilityRegistry,
    grants: &[ApprovalGrant],
    expiry: &str,
) -> ictus_policy::TrustedDecision {
    LocalPolicyComposition::default()
        .validate(ValidationRequest {
            context,
            snapshot,
            proposal,
            capabilities,
            grants,
            expiry,
        })
        .unwrap()
}

fn snapshot() -> SnapshotRef {
    SnapshotRef::new("snap-1", 7, "digest-1")
}

fn baseline(grants: &[ApprovalGrant]) -> ictus_policy::TrustedDecision {
    let context = context();
    let proposal = proposal();
    let registry = registry();
    compose_with(
        &context,
        snapshot(),
        &proposal,
        &registry,
        grants,
        "2026-10-01T01:00:00Z",
    )
}

#[test]
fn policy_version_is_preserved_and_defaulted() {
    assert_eq!(
        LocalPolicyComposition::default().policy_version(),
        DEFAULT_POLICY_VERSION
    );
    assert_eq!(
        LocalPolicyComposition::new("custom.policy.v1").policy_version(),
        "custom.policy.v1"
    );
}

#[test]
fn snapshot_id_revision_and_subject_mismatches_are_independent() {
    let context = context();
    let proposal = proposal();
    let registry = registry();
    let grant = grant();

    // Baseline is executable.
    let ok = compose_with(
        &context,
        snapshot(),
        &proposal,
        &registry,
        std::slice::from_ref(&grant),
        "2026-10-01T01:00:00Z",
    );
    assert!(ok.envelope().is_executable());

    // Only the snapshot id differs.
    let id_only = compose_with(
        &context,
        SnapshotRef::new("snap-OTHER", 7, "digest-1"),
        &proposal,
        &registry,
        std::slice::from_ref(&grant),
        "2026-10-01T01:00:00Z",
    );
    assert_eq!(id_only.envelope().outcome, EnvelopeOutcome::Invalid);

    // Only the revision differs.
    let revision_only = compose_with(
        &context,
        SnapshotRef::new("snap-1", 8, "digest-1"),
        &proposal,
        &registry,
        std::slice::from_ref(&grant),
        "2026-10-01T01:00:00Z",
    );
    assert_eq!(revision_only.envelope().outcome, EnvelopeOutcome::Invalid);

    // Only the proposal subject differs from the context snapshot subject.
    let other_subject = DecisionProposal::new(
        "proposal-1",
        DecisionKind::ExecuteCapability,
        Subject::new("task", "other"),
        ProviderMetadata::rules("rules.v1"),
        "2026-10-01T00:00:00Z",
    )
    .with_capability("software.promote");
    let subject_only = compose_with(
        &context,
        snapshot(),
        &other_subject,
        &registry,
        std::slice::from_ref(&grant),
        "2026-10-01T01:00:00Z",
    );
    assert_eq!(subject_only.envelope().outcome, EnvelopeOutcome::Invalid);
}

#[test]
fn expiry_must_be_canonical_and_strictly_later_than_validation_time() {
    let context = context();
    let proposal = proposal();
    let registry = registry();
    let grant = grant();

    // Non-canonical but lexically later than the validation time.
    let non_canonical = compose_with(
        &context,
        snapshot(),
        &proposal,
        &registry,
        std::slice::from_ref(&grant),
        "2026-10-01T01:00:00+00:00",
    );
    assert_eq!(non_canonical.envelope().outcome, EnvelopeOutcome::Invalid);

    // Canonical but not later than the validation time.
    let not_later = compose_with(
        &context,
        snapshot(),
        &proposal,
        &registry,
        &[grant],
        "2026-10-01T00:00:00Z",
    );
    assert_eq!(not_later.envelope().outcome, EnvelopeOutcome::Invalid);
}

#[test]
fn high_risk_capability_without_declared_approvals_requires_named_human_approval() {
    let context = context();
    let proposal = proposal();
    let registry = InMemoryCapabilityRegistry::new().with(Capability::new(
        "software.promote",
        "3",
        RiskClass::High,
    ));
    let trusted = compose_with(
        &context,
        snapshot(),
        &proposal,
        &registry,
        &[],
        "2026-10-01T01:00:00Z",
    );
    let names: Vec<&str> = trusted
        .envelope()
        .approvals
        .iter()
        .map(|approval| approval.name.as_str())
        .collect();
    assert_eq!(names, vec!["human"]);
}

#[test]
fn modified_intent_revalidation_binds_capability_and_subject() {
    let trusted = baseline(&[grant()]);
    let intent = trusted.executable_intent().unwrap().clone();

    let proposal = proposal();
    let registry = registry();
    assert!(revalidate_modified_intent(&proposal, &intent, &registry).is_ok());

    let mut wrong_capability = intent.clone();
    wrong_capability.capability = "software.other".to_string();
    assert!(revalidate_modified_intent(&proposal, &wrong_capability, &registry).is_err());

    let mut wrong_target = intent;
    wrong_target.target = Subject::new("task", "other");
    assert!(revalidate_modified_intent(&proposal, &wrong_target, &registry).is_err());
}

#[test]
fn grant_issued_at_must_not_be_after_the_validation_time() {
    // issued_at == validated_at is still valid (strictly "after" is rejected).
    let mut equal = grant();
    equal.issued_at = "2026-10-01T00:00:00Z".to_string();
    assert!(baseline(&[equal]).envelope().is_executable());

    // issued_at strictly after the validation time is rejected.
    let mut after = grant();
    after.issued_at = "2026-10-01T00:30:00Z".to_string();
    let trusted = baseline(&[after]);
    assert!(!trusted.envelope().is_executable());
    assert!(trusted
        .envelope()
        .capability_validation
        .reason
        .contains("issued after validation time"));
}

#[test]
fn grant_expiry_must_be_later_than_the_validation_time() {
    // expires_at == validated_at is rejected (not later).
    let mut equal = grant();
    equal.expires_at = "2026-10-01T00:00:00Z".to_string();
    let trusted = baseline(&[equal]);
    assert!(!trusted.envelope().is_executable());
    assert!(trusted
        .envelope()
        .capability_validation
        .reason
        .contains("expired"));
}

#[test]
fn one_non_canonical_grant_timestamp_rejects_the_grant() {
    // issued_at canonical, expires_at not: the disjunction must still reject.
    let mut grant = grant();
    grant.expires_at = "2026-10-02T00:00:00+00:00".to_string();
    let trusted = baseline(&[grant]);
    assert!(!trusted.envelope().is_executable());
    assert!(trusted
        .envelope()
        .capability_validation
        .reason
        .contains("canonical UTC"));
}

#[test]
fn non_baseline_policy_version_denies_before_other_checks() {
    let context = context();
    let proposal = proposal();
    let registry = registry();
    let trusted = LocalPolicyComposition::new("unknown.policy.v9")
        .validate(ValidationRequest {
            context: &context,
            snapshot: snapshot(),
            proposal: &proposal,
            capabilities: &registry,
            grants: &[],
            expiry: "2026-10-01T01:00:00Z",
        })
        .unwrap();
    assert_eq!(trusted.envelope().outcome, EnvelopeOutcome::Denied);
    assert_eq!(trusted.envelope().policy.verdict, PolicyDecisionKind::Deny);
}
