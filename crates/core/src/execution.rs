//! Execution contracts.
//!
//! `ExecutionIntent` is the **only** object allowed to cross into an execution
//! backend. `ExecutionResult` is the bounded result that comes back. Neither
//! carries durable workflow state: re-execution, retries and run persistence
//! are the backend's responsibility.

use serde::{Deserialize, Serialize};

use crate::evidence::EvidenceRef;
use crate::observation::ObservationCategory;
use crate::routing::SelectedRoute;
use crate::snapshot::Subject;
use crate::version::{ContractError, SCHEMA_VERSION};

/// Provenance of an intent: which provider and decision produced it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestedBy {
    pub provider: String,
    pub decision_id: String,
}

impl RequestedBy {
    pub fn new(provider: impl Into<String>, decision_id: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            decision_id: decision_id.into(),
        }
    }
}

/// A validated, bounded action for the execution backend.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionIntent {
    pub schema_version: u32,
    pub intent_id: String,
    pub capability: String,
    pub target: Subject,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub arguments: serde_json::Map<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub policy_context: serde_json::Map<String, serde_json::Value>,
    /// The single route selected by Ictus, with the source fact revisions used
    /// for admission. Execution adapters must reject stale facts rather than
    /// silently substituting another backend, provider, runtime, or model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_route: Option<SelectedRoute>,
    pub requested_by: RequestedBy,
}

impl ExecutionIntent {
    pub fn new(
        intent_id: impl Into<String>,
        capability: impl Into<String>,
        target: Subject,
        requested_by: RequestedBy,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            intent_id: intent_id.into(),
            capability: capability.into(),
            target,
            arguments: serde_json::Map::new(),
            policy_context: serde_json::Map::new(),
            selected_route: None,
            requested_by,
        }
    }

    pub fn with_argument(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.arguments.insert(key.into(), value);
        self
    }

    pub fn with_policy_context(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.policy_context.insert(key.into(), value);
        self
    }

    pub fn with_selected_route(mut self, route: SelectedRoute) -> Self {
        self.selected_route = Some(route);
        self
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::check_version(self.schema_version)?;
        ContractError::require_non_empty("intent_id", &self.intent_id)?;
        ContractError::require_non_empty("capability", &self.capability)?;
        ContractError::require_non_empty("target.kind", &self.target.kind)?;
        ContractError::require_non_empty("target.id", &self.target.id)?;
        ContractError::require_non_empty("requested_by.provider", &self.requested_by.provider)?;
        ContractError::require_non_empty(
            "requested_by.decision_id",
            &self.requested_by.decision_id,
        )?;
        if let Some(route) = &self.selected_route {
            route.validate()?;
        }
        Ok(())
    }
}

/// Terminal status of one bounded execution.
///
/// This is a *result-facing* status only. It is not a durable workflow state
/// machine; Dagster owns step/run lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
}

/// Bounded observation summary carried by a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservationSummary {
    pub category: ObservationCategory,
}

/// The bounded result returned by an execution backend.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub schema_version: u32,
    pub execution_id: String,
    pub intent_id: String,
    pub status: ExecutionStatus,
    pub observation: ObservationSummary,
    #[serde(default)]
    pub evidence: Vec<EvidenceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<String>,
}

impl ExecutionResult {
    pub fn new(
        execution_id: impl Into<String>,
        intent_id: impl Into<String>,
        status: ExecutionStatus,
        category: ObservationCategory,
    ) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            execution_id: execution_id.into(),
            intent_id: intent_id.into(),
            status,
            observation: ObservationSummary { category },
            evidence: Vec::new(),
            started_at: None,
            finished_at: None,
        }
    }

    pub fn with_evidence(mut self, evidence: EvidenceRef) -> Self {
        self.evidence.push(evidence);
        self
    }

    pub fn with_times(
        mut self,
        started_at: impl Into<String>,
        finished_at: impl Into<String>,
    ) -> Self {
        self.started_at = Some(started_at.into());
        self.finished_at = Some(finished_at.into());
        self
    }

    /// True when execution reported success.
    pub fn is_success(&self) -> bool {
        self.status == ExecutionStatus::Succeeded
    }

    /// Convert the result into a fact observation. This is how the decision
    /// layer learns about execution outcomes without the backend deciding.
    pub fn to_observation(
        &self,
        observation_id: impl Into<String>,
        observed_at: impl Into<String>,
    ) -> crate::observation::ExecutionObservation {
        let mut observation = crate::observation::ExecutionObservation::new(
            observation_id,
            self.execution_id.clone(),
            self.intent_id.clone(),
            self.observation.category,
            observed_at,
        );
        observation.evidence = self.evidence.clone();
        observation
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::check_version(self.schema_version)?;
        ContractError::require_non_empty("execution_id", &self.execution_id)?;
        ContractError::require_non_empty("intent_id", &self.intent_id)?;
        Ok(())
    }
}
