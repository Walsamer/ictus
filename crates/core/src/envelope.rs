//! Versioned validated decision envelope.
//!
//! The [`DecisionEnvelope`] is the **output** side of the generic interchange a
//! consumer such as Tactus uses. It is the validated result of applying policy
//! and capability validation to an untrusted [`crate::DecisionProposal`], and
//! it binds every fact a consumer needs to trust or refuse the decision:
//! subject, snapshot digest/revision, proposal and policy identity/version,
//! capability validation, route constraints, approvals, the resolved effect and
//! an expiry.
//!
//! Only the [`EnvelopeOutcome::Executable`] effect may carry an
//! [`crate::ExecutionIntent`]. `DENY`, `REQUIRE_APPROVAL` (`Pending`), no-route
//! and invalid outcomes carry no executable intent. An executable envelope's
//! intent is revalidated as part of [`DecisionEnvelope::validate`], so a
//! modified proposal is revalidated before execution.
//!
//! The envelope is versioned. `capability_validation` exists exactly when the
//! decision carries a capability: `EXECUTE_CAPABILITY`, `REEXECUTE` and `ROUTE`
//! are emitted as v1 (byte-identical to before), while `ESCALATE`, `ABORT` and
//! `DECOMPOSE` — the decisions where a human takes over, the work stops or is
//! split — are emitted as v2 with no capability, no approvals and no intent, and
//! resolve to [`EnvelopeOutcome::NoExecution`]. The binding (subject, snapshot
//! digest and revision, proposal and policy identity/version, the validated
//! proposal digest and the expiry) is identical in both versions, so an
//! escalation is as verifiable as an execution.

use serde::{Deserialize, Serialize};

use crate::context::SnapshotRef;
use crate::decision::{DecisionKind, RouteConstraints};
use crate::execution::ExecutionIntent;
use crate::grant::ApprovalGrant;
use crate::policy::PolicyDecisionKind;
use crate::snapshot::Subject;
use crate::version::{
    check_envelope_version, ContractError, ENVELOPE_SCHEMA_VERSION, LEGACY_ENVELOPE_SCHEMA_VERSION,
};

/// The resolved effect of a validated decision.
///
/// This is deliberately *not* a durable workflow state machine: it says what
/// the decision layer concluded, once. Whether to act on it is the consumer's
/// bounded concern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EnvelopeOutcome {
    /// Validated and authorized to execute; may carry an `ExecutionIntent`.
    Executable,
    /// Policy denied the proposal; never carries an intent.
    Denied,
    /// A required approval is not yet satisfied; never carries an intent.
    Pending,
    /// No route could be selected for an otherwise-authorized capability.
    NoRoute,
    /// The proposal or context was invalid; never carries an intent.
    Invalid,
    /// The decision was validated and permitted, and it reaches no execution
    /// backend: `ESCALATE`, `ABORT` and `DECOMPOSE`.
    ///
    /// This is not a refusal. It says the decision stands and carries no
    /// capability to execute, so the consumer acts on `proposal.decision` (hand
    /// to a human, stop, decompose) rather than on an intent. It is only
    /// emitted for a decision that carries no capability, and it never carries
    /// an intent.
    NoExecution,
}

impl EnvelopeOutcome {
    /// True only for the single outcome that may carry an executable intent.
    pub fn is_executable(self) -> bool {
        matches!(self, EnvelopeOutcome::Executable)
    }

    /// Map a policy verdict to the envelope effect. A missing route downgrades
    /// an authorized decision to [`EnvelopeOutcome::NoRoute`].
    pub fn from_verdict(verdict: PolicyDecisionKind, route_found: bool) -> Self {
        match verdict {
            PolicyDecisionKind::Allow | PolicyDecisionKind::Modify => {
                if route_found {
                    EnvelopeOutcome::Executable
                } else {
                    EnvelopeOutcome::NoRoute
                }
            }
            PolicyDecisionKind::Deny => EnvelopeOutcome::Denied,
            PolicyDecisionKind::RequireApproval => EnvelopeOutcome::Pending,
        }
    }
}

/// Identity and version of the proposal a decision was made about.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProposalBinding {
    pub proposal_id: String,
    /// The `DecisionProposal` contract version (`1` or `2`).
    pub schema_version: u32,
    pub decision: DecisionKind,
}

impl ProposalBinding {
    pub fn new(
        proposal_id: impl Into<String>,
        schema_version: u32,
        decision: DecisionKind,
    ) -> Self {
        Self {
            proposal_id: proposal_id.into(),
            schema_version,
            decision,
        }
    }
}

/// Identity, version and verdict of the policy decision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyBinding {
    pub policy_decision_id: String,
    pub policy_version: String,
    pub verdict: PolicyDecisionKind,
}

impl PolicyBinding {
    pub fn new(
        policy_decision_id: impl Into<String>,
        policy_version: impl Into<String>,
        verdict: PolicyDecisionKind,
    ) -> Self {
        Self {
            policy_decision_id: policy_decision_id.into(),
            policy_version: policy_version.into(),
            verdict,
        }
    }
}

/// The capability validation result bound into the envelope.
///
/// Present exactly when the decision carries a capability. A decision that
/// carries none has nothing to admit or permit, so its envelope must not claim
/// a validation for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityValidation {
    pub capability: String,
    /// The capability exists in the registry.
    pub admitted: bool,
    /// The policy allows the capability for this subject.
    pub permitted: bool,
    pub reason: String,
}

impl CapabilityValidation {
    pub fn new(
        capability: impl Into<String>,
        admitted: bool,
        permitted: bool,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            capability: capability.into(),
            admitted,
            permitted,
            reason: reason.into(),
        }
    }
}

/// One approval requirement and whether it is satisfied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalBinding {
    pub name: String,
    pub satisfied: bool,
}

/// Evidence that binds an envelope to the exact input evaluated locally.
///
/// The proposal digest is calculated from the canonical serialized proposal.
/// It prevents a consumer from treating an intent for a changed payload as the
/// result of the earlier validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationBinding {
    pub context_id: String,
    pub proposal_digest: String,
    pub capability_version: String,
    pub validated_at: String,
}

impl ValidationBinding {
    pub fn new(
        context_id: impl Into<String>,
        proposal_digest: impl Into<String>,
        capability_version: impl Into<String>,
        validated_at: impl Into<String>,
    ) -> Self {
        Self {
            context_id: context_id.into(),
            proposal_digest: proposal_digest.into(),
            capability_version: capability_version.into(),
            validated_at: validated_at.into(),
        }
    }

    fn validate(&self) -> Result<(), ContractError> {
        for (field, value) in [
            ("validation.context_id", &self.context_id),
            ("validation.proposal_digest", &self.proposal_digest),
            ("validation.capability_version", &self.capability_version),
            ("validation.validated_at", &self.validated_at),
        ] {
            ContractError::require_non_empty(field, value)?;
        }
        if self.proposal_digest.len() != 64
            || !self
                .proposal_digest
                .chars()
                .all(|character| character.is_ascii_hexdigit())
        {
            return Err(ContractError::InvalidValue {
                field: "validation.proposal_digest",
                reason: "must be a SHA-256 hex digest".to_string(),
            });
        }
        Ok(())
    }
}

impl ApprovalBinding {
    pub fn new(name: impl Into<String>, satisfied: bool) -> Self {
        Self {
            name: name.into(),
            satisfied,
        }
    }
}

/// A validated decision, bound to its subject, snapshot, proposal, policy
/// verdict, capability validation (only when the decision carries a capability),
/// route, approvals, effect and expiry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionEnvelope {
    /// `1` for a capability-bearing decision, `2` for one that carries none.
    pub schema_version: u32,
    pub envelope_id: String,
    pub subject: Subject,
    pub snapshot: SnapshotRef,
    pub proposal: ProposalBinding,
    pub policy: PolicyBinding,
    /// Present exactly when `proposal.decision` carries a capability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability_validation: Option<CapabilityValidation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<RouteConstraints>,
    #[serde(default)]
    pub approvals: Vec<ApprovalBinding>,
    /// Domain grant records that satisfied approval requirements. These are
    /// evidence, not an approval database owned by Ictus.
    #[serde(default)]
    pub grants: Vec<ApprovalGrant>,
    /// Binding evidence emitted by the trusted local composition boundary.
    pub validation: ValidationBinding,
    pub outcome: EnvelopeOutcome,
    /// RFC 3339 timestamp after which the envelope is no longer valid.
    pub expiry: String,
    /// Only present for [`EnvelopeOutcome::Executable`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<ExecutionIntent>,
}

impl DecisionEnvelope {
    /// The `schema_version` a decision of `decision` is emitted under.
    ///
    /// A capability-bearing decision keeps the frozen v1 shape, so its output is
    /// byte-identical to what consumers already read. A decision that carries no
    /// capability needs the conditional v2 shape, which omits
    /// `capability_validation`; v1 cannot express it.
    pub fn schema_version_for(decision: DecisionKind) -> u32 {
        if decision.requires_capability() {
            LEGACY_ENVELOPE_SCHEMA_VERSION
        } else {
            ENVELOPE_SCHEMA_VERSION
        }
    }

    pub fn with_route(mut self, route: RouteConstraints) -> Self {
        self.route = Some(route);
        self
    }

    pub fn with_approval(mut self, approval: ApprovalBinding) -> Self {
        self.approvals.push(approval);
        self
    }

    pub fn with_grant(mut self, grant: ApprovalGrant) -> Self {
        self.grants.push(grant);
        self
    }

    pub fn with_intent(mut self, intent: ExecutionIntent) -> Self {
        self.intent = Some(intent);
        self
    }

    /// True only when the effect is executable and an intent is bound.
    pub fn is_executable(&self) -> bool {
        self.outcome.is_executable() && self.intent.is_some()
    }

    /// Validate the envelope and revalidate its executable intent.
    ///
    /// Fails closed when a non-executable outcome carries an intent, when an
    /// executable outcome is missing an intent, when the capability is not
    /// admitted and permitted, when the intent does not bind the same subject
    /// and capability, and when a decision's capability claim and its envelope
    /// disagree: `capability_validation` present for a decision that carries no
    /// capability, absent for one that does, or a v1 envelope about a decision
    /// that carries none.
    pub fn validate(&self) -> Result<(), ContractError> {
        check_envelope_version(self.schema_version)?;
        ContractError::require_non_empty("envelope_id", &self.envelope_id)?;
        ContractError::require_non_empty("subject.kind", &self.subject.kind)?;
        ContractError::require_non_empty("subject.id", &self.subject.id)?;
        self.snapshot.validate()?;
        ContractError::require_non_empty("proposal.proposal_id", &self.proposal.proposal_id)?;
        ContractError::require_non_empty(
            "policy.policy_decision_id",
            &self.policy.policy_decision_id,
        )?;
        ContractError::require_non_empty("policy.policy_version", &self.policy.policy_version)?;
        ContractError::require_non_empty("expiry", &self.expiry)?;

        // The capability claim is decided by the decision kind alone. v1 is the
        // frozen shape that always carries `capability_validation`, so it can
        // only describe a capability-bearing decision.
        let requires_capability = self.proposal.decision.requires_capability();
        if self.schema_version == LEGACY_ENVELOPE_SCHEMA_VERSION && !requires_capability {
            return Err(ContractError::InvalidValue {
                field: "schema_version",
                reason: format!(
                    "a {:?} decision carries no capability and requires schema_version {}; \
                     schema_version 1 always carries capability_validation",
                    self.proposal.decision, ENVELOPE_SCHEMA_VERSION
                ),
            });
        }
        match &self.capability_validation {
            Some(validation) if !requires_capability => {
                return Err(ContractError::InvalidValue {
                    field: "capability_validation",
                    reason: format!(
                        "a {:?} decision carries no capability, so its envelope must not \
                         claim capability validation",
                        self.proposal.decision
                    ),
                });
            }
            Some(validation) => {
                ContractError::require_non_empty(
                    "capability_validation.capability",
                    &validation.capability,
                )?;
                ContractError::require_non_empty(
                    "capability_validation.reason",
                    &validation.reason,
                )?;
            }
            None if requires_capability => {
                return Err(ContractError::MissingField("capability_validation"));
            }
            None => {
                // Nothing to gate, route or execute: an approval binding or a
                // route here would describe a decision this envelope is not.
                if !self.approvals.is_empty() || !self.grants.is_empty() {
                    return Err(ContractError::InvalidValue {
                        field: "approvals",
                        reason: format!(
                            "a {:?} decision has no capability, so no approval or grant binds \
                             to it",
                            self.proposal.decision
                        ),
                    });
                }
                if self.route.is_some() {
                    return Err(ContractError::InvalidValue {
                        field: "route",
                        reason: format!(
                            "a {:?} decision has no capability, so there is no route to \
                             constrain",
                            self.proposal.decision
                        ),
                    });
                }
            }
        }
        // NO_EXECUTION says "validated, and nothing executes", which only a
        // decision without a capability can mean.
        if self.outcome == EnvelopeOutcome::NoExecution && requires_capability {
            return Err(ContractError::InvalidValue {
                field: "outcome",
                reason: "a capability-bearing decision either executes or is refused, so it \
                         cannot resolve to NO_EXECUTION"
                    .to_string(),
            });
        }
        // A decision with no capability has no approval gate to wait on and no
        // route to select, so only three outcomes are meaningful: the decision
        // stands, policy refused it, or it could not be validated.
        if !requires_capability
            && matches!(
                self.outcome,
                EnvelopeOutcome::Pending | EnvelopeOutcome::NoRoute
            )
        {
            return Err(ContractError::InvalidValue {
                field: "outcome",
                reason: format!(
                    "a {:?} decision has no capability, so it has no approval gate (PENDING) \
                     and no route (NO_ROUTE)",
                    self.proposal.decision
                ),
            });
        }
        // NO_EXECUTION claims the decision was permitted; a refusal verdict with
        // it is a contradiction, not a shape to tolerate.
        if self.outcome == EnvelopeOutcome::NoExecution
            && !matches!(
                self.policy.verdict,
                PolicyDecisionKind::Allow | PolicyDecisionKind::Modify
            )
        {
            return Err(ContractError::InvalidValue {
                field: "policy.verdict",
                reason: "NO_EXECUTION says the decision was permitted, so it cannot carry a \
                         refusing verdict"
                    .to_string(),
            });
        }

        if let Some(route) = &self.route {
            route.validate()?;
        }
        for approval in &self.approvals {
            ContractError::require_non_empty("approvals.name", &approval.name)?;
        }
        for grant in &self.grants {
            grant.validate()?;
        }
        self.validation.validate()?;

        match self.outcome {
            EnvelopeOutcome::Executable => {
                let capability_validation =
                    self.capability_validation.as_ref().ok_or_else(|| {
                        ContractError::InvalidValue {
                            field: "outcome",
                            reason: "an EXECUTABLE envelope requires a capability-bearing decision"
                                .to_string(),
                        }
                    })?;
                if !capability_validation.admitted || !capability_validation.permitted {
                    return Err(ContractError::InvalidValue {
                        field: "capability_validation",
                        reason:
                            "an executable envelope requires an admitted and permitted capability"
                                .to_string(),
                    });
                }
                let intent = self
                    .intent
                    .as_ref()
                    .ok_or_else(|| ContractError::InvalidValue {
                        field: "intent",
                        reason: "an EXECUTABLE envelope must carry an intent".to_string(),
                    })?;
                // Revalidated before execution: a modified proposal is not trusted.
                intent.validate()?;
                if intent.capability != capability_validation.capability {
                    return Err(ContractError::InvalidValue {
                        field: "intent.capability",
                        reason: "intent capability does not match capability_validation"
                            .to_string(),
                    });
                }
                if intent.target != self.subject {
                    return Err(ContractError::InvalidValue {
                        field: "intent.target",
                        reason: "intent target does not bind the envelope subject".to_string(),
                    });
                }
                let bound_digest = intent
                    .policy_context
                    .get("proposal_digest")
                    .and_then(serde_json::Value::as_str);
                if bound_digest != Some(self.validation.proposal_digest.as_str()) {
                    return Err(ContractError::InvalidValue {
                        field: "intent.policy_context.proposal_digest",
                        reason: "intent does not bind the validated proposal payload".to_string(),
                    });
                }
                for approval in self.approvals.iter().filter(|approval| approval.satisfied) {
                    let has_bound_grant = self.grants.iter().any(|grant| {
                        !grant.revoked
                            && grant.approval == approval.name
                            && grant.subject == self.subject
                            && grant.revision == self.snapshot.revision
                            && grant.capability == capability_validation.capability
                            && grant.policy_version == self.policy.policy_version
                    });
                    if !has_bound_grant {
                        return Err(ContractError::InvalidValue {
                            field: "approvals",
                            reason: format!(
                                "satisfied approval '{}' has no matching bound grant",
                                approval.name
                            ),
                        });
                    }
                }
            }
            EnvelopeOutcome::Denied
            | EnvelopeOutcome::Pending
            | EnvelopeOutcome::NoRoute
            | EnvelopeOutcome::Invalid
            | EnvelopeOutcome::NoExecution => {
                if self.intent.is_some() {
                    return Err(ContractError::InvalidValue {
                        field: "intent",
                        reason: "only an EXECUTABLE envelope may carry an intent".to_string(),
                    });
                }
            }
        }
        Ok(())
    }
}
