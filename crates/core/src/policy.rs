//! Policy decisions.
//!
//! The policy layer is the trust boundary. A denied proposal must never reach
//! an execution backend.

use serde::{Deserialize, Serialize};

use crate::execution::ExecutionIntent;
use crate::version::{ContractError, SCHEMA_VERSION};

/// Outcome of policy/capability validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyDecisionKind {
    Allow,
    Deny,
    RequireApproval,
    Modify,
}

/// Result of validating a proposal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub schema_version: u32,
    pub policy_decision_id: String,
    pub proposal_id: String,
    pub decision: PolicyDecisionKind,
    #[serde(default)]
    pub reasons: Vec<String>,
    /// Present when the decision authorizes execution (Allow/RequireApproval
    /// once satisfied, or Modify). A Deny never carries an intent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_intent: Option<ExecutionIntent>,
    pub decided_at: String,
}

impl PolicyDecision {
    pub fn new(
        policy_decision_id: impl Into<String>,
        proposal_id: impl Into<String>,
        decision: PolicyDecisionKind,
        decided_at: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            policy_decision_id: policy_decision_id.into(),
            proposal_id: proposal_id.into(),
            decision,
            reasons: Vec::new(),
            modified_intent: None,
            decided_at: decided_at.into(),
        }
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reasons.push(reason.into());
        self
    }

    pub fn with_intent(mut self, intent: ExecutionIntent) -> Self {
        self.modified_intent = Some(intent);
        self
    }

    /// Only an `ALLOW` (or a `MODIFY` that carries an intent) is executable.
    /// A `DENY` is never executable, and a `REQUIRE_APPROVAL` must be
    /// re-evaluated after the approval is satisfied before it may execute.
    pub fn is_executable(&self) -> bool {
        matches!(
            self.decision,
            PolicyDecisionKind::Allow | PolicyDecisionKind::Modify
        ) && self.modified_intent.is_some()
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::check_version(self.schema_version)?;
        ContractError::require_non_empty("policy_decision_id", &self.policy_decision_id)?;
        ContractError::require_non_empty("proposal_id", &self.proposal_id)?;
        if matches!(self.decision, PolicyDecisionKind::Deny) && self.modified_intent.is_some() {
            return Err(ContractError::InvalidValue {
                field: "modified_intent",
                reason: "a DENY decision must not carry an execution intent".to_string(),
            });
        }
        Ok(())
    }
}
