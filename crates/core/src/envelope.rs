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

use serde::{Deserialize, Serialize};

use crate::context::SnapshotRef;
use crate::decision::{DecisionKind, RouteConstraints};
use crate::execution::ExecutionIntent;
use crate::policy::PolicyDecisionKind;
use crate::snapshot::Subject;
use crate::version::{check_contract_version, ContractError, ENVELOPE_SCHEMA_VERSION};

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

impl ApprovalBinding {
    pub fn new(name: impl Into<String>, satisfied: bool) -> Self {
        Self {
            name: name.into(),
            satisfied,
        }
    }
}

/// A validated decision, bound to its subject, snapshot, proposal, policy
/// verdict, capability validation, route, approvals, effect and expiry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionEnvelope {
    pub schema_version: u32,
    pub envelope_id: String,
    pub subject: Subject,
    pub snapshot: SnapshotRef,
    pub proposal: ProposalBinding,
    pub policy: PolicyBinding,
    pub capability_validation: CapabilityValidation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<RouteConstraints>,
    #[serde(default)]
    pub approvals: Vec<ApprovalBinding>,
    pub outcome: EnvelopeOutcome,
    /// RFC 3339 timestamp after which the envelope is no longer valid.
    pub expiry: String,
    /// Only present for [`EnvelopeOutcome::Executable`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<ExecutionIntent>,
}

impl DecisionEnvelope {
    pub fn with_route(mut self, route: RouteConstraints) -> Self {
        self.route = Some(route);
        self
    }

    pub fn with_approval(mut self, approval: ApprovalBinding) -> Self {
        self.approvals.push(approval);
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
    /// admitted and permitted, or when the intent does not bind the same
    /// subject and capability.
    pub fn validate(&self) -> Result<(), ContractError> {
        check_contract_version(self.schema_version, ENVELOPE_SCHEMA_VERSION)?;
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
        ContractError::require_non_empty(
            "capability_validation.capability",
            &self.capability_validation.capability,
        )?;
        ContractError::require_non_empty(
            "capability_validation.reason",
            &self.capability_validation.reason,
        )?;
        ContractError::require_non_empty("expiry", &self.expiry)?;
        if let Some(route) = &self.route {
            route.validate()?;
        }
        for approval in &self.approvals {
            ContractError::require_non_empty("approvals.name", &approval.name)?;
        }

        match self.outcome {
            EnvelopeOutcome::Executable => {
                if !self.capability_validation.admitted || !self.capability_validation.permitted {
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
                if intent.capability != self.capability_validation.capability {
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
            }
            EnvelopeOutcome::Denied
            | EnvelopeOutcome::Pending
            | EnvelopeOutcome::NoRoute
            | EnvelopeOutcome::Invalid => {
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
