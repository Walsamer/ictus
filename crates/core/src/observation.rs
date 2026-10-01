//! Execution observations.
//!
//! An observation is a *fact* produced by execution. It is never a decision:
//! deciding whether a failure means retry, decompose or escalate is the
//! decision/policy layer's job, not the execution backend's.

use serde::{Deserialize, Serialize};

use crate::evidence::EvidenceRef;
use crate::version::{ContractError, SCHEMA_VERSION};

/// A domain-neutral category of execution outcome.
///
/// Deliberately small. Domain-specific failure taxonomies map onto these in an
/// adapter, never in the core.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObservationCategory {
    Success,
    WorkerTimeout,
    ProcessCrash,
    VerificationFailure,
    IntegrationConflict,
    ResourceExhausted,
    ProviderUnavailable,
    Unknown,
}

/// A fact produced by execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionObservation {
    pub schema_version: u32,
    pub observation_id: String,
    pub execution_id: String,
    pub intent_id: String,
    pub category: ObservationCategory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default)]
    pub evidence: Vec<EvidenceRef>,
    pub observed_at: String,
    /// The backend's own advisory hint. The core never treats this as policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
}

impl ExecutionObservation {
    pub fn new(
        observation_id: impl Into<String>,
        execution_id: impl Into<String>,
        intent_id: impl Into<String>,
        category: ObservationCategory,
        observed_at: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            observation_id: observation_id.into(),
            execution_id: execution_id.into(),
            intent_id: intent_id.into(),
            category,
            message: None,
            evidence: Vec::new(),
            observed_at: observed_at.into(),
            retryable: None,
        }
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::check_version(self.schema_version)?;
        ContractError::require_non_empty("observation_id", &self.observation_id)?;
        ContractError::require_non_empty("execution_id", &self.execution_id)?;
        ContractError::require_non_empty("intent_id", &self.intent_id)?;
        Ok(())
    }
}
