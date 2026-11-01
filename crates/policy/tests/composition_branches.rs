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

fn capability_reason(envelope: &ictus_core::DecisionEnvelope) -> &str {
    &envelope
        .capability_validation
        .as_ref()
        .expect("a capability-bearing envelope carries capability_validation")
        .reason
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
    assert!(capability_reason(trusted.envelope()).contains("issued after validation time"));
}

#[test]
fn grant_expiry_must_be_later_than_the_validation_time() {
    // expires_at == validated_at is rejected (not later).
    let mut equal = grant();
    equal.expires_at = "2026-10-01T00:00:00Z".to_string();
    let trusted = baseline(&[equal]);
    assert!(!trusted.envelope().is_executable());
    assert!(capability_reason(trusted.envelope()).contains("expired"));
}

#[test]
fn one_non_canonical_grant_timestamp_rejects_the_grant() {
    // issued_at canonical, expires_at not: the disjunction must still reject.
    let mut grant = grant();
    grant.expires_at = "2026-10-02T00:00:00+00:00".to_string();
    let trusted = baseline(&[grant]);
    assert!(!trusted.envelope().is_executable());
    assert!(capability_reason(trusted.envelope()).contains("canonical UTC"));
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

// -- decisions that carry no capability --------------------------------------
//
// `ESCALATE`, `ABORT` and `DECOMPOSE` are the decisions where a human takes
// over, the work stops or is split. They carry no capability, so their envelope
// is v2: the same binding and the same digest chain as an execution envelope,
// with no `capability_validation`, no approvals and no intent.

fn no_capability_proposal(decision: DecisionKind) -> DecisionProposal {
    DecisionProposal::new(
        "proposal-2",
        decision,
        Subject::new("task", "t1"),
        ProviderMetadata::rules("rules.v1"),
        "2026-10-01T00:00:00Z",
    )
}

fn compose_no_capability(proposal: &DecisionProposal) -> ictus_policy::TrustedDecision {
    let context = context();
    let registry = registry();
    // The snapshot binding is built exactly as the bridge builds it: Ictus's own
    // digest over the snapshot as received.
    let snapshot = SnapshotRef::new(
        &context.snapshot.snapshot_id,
        context.revision,
        context.snapshot.content_digest(),
    );
    compose_with(
        &context,
        snapshot,
        proposal,
        &registry,
        &[],
        "2026-10-01T01:00:00Z",
    )
}

fn proposal_digest(proposal: &DecisionProposal) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(proposal).unwrap());
    format!("{:x}", hasher.finalize())
}

#[test]
fn an_escalation_envelope_is_v2_without_a_capability() {
    let trusted = compose_no_capability(&no_capability_proposal(DecisionKind::Escalate));
    let envelope = trusted.envelope();
    envelope.validate().unwrap();
    assert_eq!(envelope.schema_version, ictus_core::ENVELOPE_SCHEMA_VERSION);
    assert_eq!(envelope.schema_version, 2);
    assert_eq!(envelope.proposal.decision, DecisionKind::Escalate);
    assert_eq!(envelope.outcome, EnvelopeOutcome::NoExecution);
    assert!(!envelope.outcome.is_executable());
    assert!(envelope.capability_validation.is_none());
    assert!(envelope.intent.is_none());
    assert!(envelope.approvals.is_empty());
    assert!(envelope.grants.is_empty());
    // Escalation is a valid, permitted policy result (an escalation never
    // reaches the execution backend), never a denial or an invalid result.
    assert_eq!(envelope.policy.verdict, PolicyDecisionKind::Allow);
    assert_eq!(envelope.policy.policy_version, DEFAULT_POLICY_VERSION);
    assert!(trusted.executable_intent().is_none());
    // The serialized form omits the capability claim entirely.
    let json = serde_json::to_value(envelope).unwrap();
    assert!(json.get("capability_validation").is_none(), "{json}");
    assert!(json.get("intent").is_none(), "{json}");
}

#[test]
fn an_abort_or_decompose_envelope_is_v2_and_no_execution() {
    let abort = compose_no_capability(&no_capability_proposal(DecisionKind::Abort));
    abort.envelope().validate().unwrap();
    assert_eq!(abort.envelope().outcome, EnvelopeOutcome::NoExecution);
    assert!(abort.envelope().capability_validation.is_none());

    let decompose = no_capability_proposal(DecisionKind::Decompose).with_argument(
        "decomposition_plan",
        serde_json::json!({
            "plan_id": "plan-1",
            "children": [{ "subject_id": "t1-a", "capability": "software.promote" }]
        }),
    );
    let decompose = compose_no_capability(&decompose);
    decompose.envelope().validate().unwrap();
    assert_eq!(decompose.envelope().outcome, EnvelopeOutcome::NoExecution);
    assert!(decompose.envelope().capability_validation.is_none());
    assert_eq!(
        decompose.envelope().proposal.decision,
        DecisionKind::Decompose
    );
}

#[test]
fn an_escalation_envelope_binds_the_same_digests_as_an_execution_envelope() {
    let proposal = no_capability_proposal(DecisionKind::Escalate);
    let context = context();
    let trusted = compose_no_capability(&proposal);
    let envelope = trusted.envelope();
    envelope.validate().unwrap();

    // The proposal payload is bound by the digest of its canonical bytes, exactly
    // as for an execution envelope.
    assert_eq!(
        envelope.validation.proposal_digest,
        proposal_digest(&proposal)
    );
    // The snapshot is bound by Ictus's own digest over the snapshot, its id and
    // its revision.
    assert_eq!(
        envelope.snapshot.digest,
        context.snapshot.content_digest(),
        "the escalation envelope must bind the snapshot exactly as received"
    );
    assert_eq!(envelope.snapshot.snapshot_id, "snap-1");
    assert_eq!(envelope.snapshot.revision, context.revision);
    // The remaining binding fields are the same set an execution envelope
    // carries, and none of them is a capability.
    assert_eq!(envelope.validation.context_id, context.context_id);
    assert_eq!(envelope.validation.validated_at, proposal.proposed_at);
    assert_eq!(
        envelope.validation.capability_version,
        ictus_policy::NO_CAPABILITY_VERSION
    );
    assert_eq!(envelope.subject, context.snapshot.subject);
    assert_eq!(envelope.proposal.proposal_id, proposal.proposal_id);
    assert_eq!(envelope.proposal.schema_version, proposal.schema_version);
    assert_eq!(envelope.expiry, "2026-10-01T01:00:00Z");
    assert_eq!(
        envelope.policy.policy_decision_id,
        format!("policy:{}", proposal.proposal_id)
    );

    // The same binding rejects a different proposal payload: changing one
    // argument moves the digest, so a stale envelope cannot be replayed.
    let mut changed = no_capability_proposal(DecisionKind::Escalate);
    changed.reason = Some("different rationale".to_string());
    assert_ne!(
        envelope.validation.proposal_digest,
        proposal_digest(&changed)
    );
}

#[test]
fn a_capability_decision_keeps_the_v1_envelope_shape() {
    let trusted = baseline(&[grant()]);
    let envelope = trusted.envelope();
    assert_eq!(envelope.schema_version, 1);
    assert!(envelope.capability_validation.is_some());
    assert_eq!(envelope.outcome, EnvelopeOutcome::Executable);
    // v1 keeps every field a consumer already reads, in the same shape.
    let json = serde_json::to_value(envelope).unwrap();
    assert!(json.get("capability_validation").is_some(), "{json}");
}

#[test]
fn a_refused_decision_without_a_capability_is_denied_not_no_execution() {
    // A structurally invalid proposal is denied by policy; the envelope must
    // record the refusal rather than claim the decision stands.
    let mut proposal = no_capability_proposal(DecisionKind::Escalate);
    proposal.provider.provider_id = String::new();
    let trusted = compose_no_capability(&proposal);
    let envelope = trusted.envelope();
    envelope.validate().unwrap();
    assert_eq!(envelope.outcome, EnvelopeOutcome::Denied);
    assert_eq!(envelope.policy.verdict, PolicyDecisionKind::Deny);
    assert!(envelope.capability_validation.is_none());
    assert_eq!(envelope.schema_version, 2);
}
