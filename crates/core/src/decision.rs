//! Decision proposals.
//!
//! A proposal is untrusted input. It may be produced by deterministic rules, a
//! specialized model, an LLM or a human. It must pass validation before it can
//! become an `ExecutionIntent`.

use serde::{Deserialize, Serialize};

use crate::evidence::EvidenceRef;
use crate::snapshot::Subject;
use crate::version::{ContractError, SCHEMA_VERSION};

/// The bounded decision vocabulary. Deliberately small for the first version.
///
/// `Decompose` is intentionally *not* part of the initial vocabulary; it is
/// introduced only once the simpler decisions are proven.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DecisionKind {
    /// Re-execute the same bounded capability.
    Retry,
    /// Stop; no further action.
    Abort,
    /// Hand to a human.
    Escalate,
    /// Execute a named capability.
    ExecuteCapability,
}

/// Who produced a proposal. The core treats all providers uniformly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderType {
    Rules,
    Model,
    Llm,
    Human,
}

/// Provenance for a proposal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderMetadata {
    pub provider_type: ProviderType,
    pub provider_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
}

impl ProviderMetadata {
    pub fn rules(provider_id: impl Into<String>) -> Self {
        Self {
            provider_type: ProviderType::Rules,
            provider_id: provider_id.into(),
            provider_version: None,
            confidence: None,
        }
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.provider_version = Some(version.into());
        self
    }
}

/// A proposed bounded action, before validation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionProposal {
    pub schema_version: u32,
    pub proposal_id: String,
    pub decision: DecisionKind,
    pub subject: Subject,
    /// Required for `ExecuteCapability` and `Retry`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub arguments: serde_json::Map<String, serde_json::Value>,
    pub provider: ProviderMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default)]
    pub evidence: Vec<EvidenceRef>,
    pub proposed_at: String,
}

impl DecisionProposal {
    pub fn new(
        proposal_id: impl Into<String>,
        decision: DecisionKind,
        subject: Subject,
        provider: ProviderMetadata,
        proposed_at: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            proposal_id: proposal_id.into(),
            decision,
            subject,
            capability: None,
            arguments: serde_json::Map::new(),
            provider,
            reason: None,
            evidence: Vec::new(),
            proposed_at: proposed_at.into(),
        }
    }

    pub fn with_capability(mut self, capability: impl Into<String>) -> Self {
        self.capability = Some(capability.into());
        self
    }

    pub fn with_argument(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.arguments.insert(key.into(), value);
        self
    }

    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// True when the decision requires a capability id.
    pub fn requires_capability(&self) -> bool {
        matches!(
            self.decision,
            DecisionKind::Retry | DecisionKind::ExecuteCapability
        )
    }

    /// Structural validation. Semantic/policy checks live in `agentic-policy`.
    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::check_version(self.schema_version)?;
        ContractError::require_non_empty("proposal_id", &self.proposal_id)?;
        ContractError::require_non_empty("subject.kind", &self.subject.kind)?;
        ContractError::require_non_empty("subject.id", &self.subject.id)?;
        ContractError::require_non_empty("provider.provider_id", &self.provider.provider_id)?;
        if self.requires_capability() {
            match self.capability.as_deref() {
                Some(capability) if !capability.trim().is_empty() => {}
                _ => {
                    return Err(ContractError::InvalidValue {
                        field: "capability",
                        reason: format!(
                            "decision {:?} requires a non-empty capability id",
                            self.decision
                        ),
                    })
                }
            }
        }
        if let Some(confidence) = self.provider.confidence {
            if !(0.0..=1.0).contains(&confidence) {
                return Err(ContractError::InvalidValue {
                    field: "provider.confidence",
                    reason: "must be within [0.0, 1.0]".to_string(),
                });
            }
        }
        Ok(())
    }
}
