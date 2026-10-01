//! Capability metadata.
//!
//! The core reasons about capabilities, never about implementation details such
//! as a script path. A capability advertises what it needs and what it does.

use serde::{Deserialize, Serialize};

use crate::version::ContractError;

/// Coarse risk classification used by policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskClass {
    Low,
    Medium,
    High,
}

/// An executable capability advertised to the decision layer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Capability {
    pub id: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<String>,
    pub risk_class: RiskClass,
    /// Whether the capability can change external state.
    #[serde(default)]
    pub side_effects: bool,
    /// Whether re-execution is safe.
    #[serde(default)]
    pub idempotent: bool,
    /// Named approvals the capability requires before execution.
    #[serde(default)]
    pub required_approvals: Vec<String>,
    /// Opaque timeout class, resolved by the execution backend.
    pub timeout_class: String,
    /// Backend that is expected to execute this capability (advisory metadata).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_backend: Option<String>,
}

impl Capability {
    pub fn new(id: impl Into<String>, version: impl Into<String>, risk_class: RiskClass) -> Self {
        Self {
            id: id.into(),
            version: version.into(),
            input_schema: None,
            output_schema: None,
            risk_class,
            side_effects: false,
            idempotent: false,
            required_approvals: Vec::new(),
            timeout_class: "default".to_string(),
            execution_backend: None,
        }
    }

    pub fn with_side_effects(mut self, side_effects: bool) -> Self {
        self.side_effects = side_effects;
        self
    }

    pub fn with_idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent = idempotent;
        self
    }

    pub fn with_required_approval(mut self, approval: impl Into<String>) -> Self {
        self.required_approvals.push(approval.into());
        self
    }

    pub fn with_timeout_class(mut self, timeout_class: impl Into<String>) -> Self {
        self.timeout_class = timeout_class.into();
        self
    }

    pub fn with_backend(mut self, backend: impl Into<String>) -> Self {
        self.execution_backend = Some(backend.into());
        self
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::require_non_empty("capability.id", &self.id)?;
        ContractError::require_non_empty("capability.version", &self.version)?;
        ContractError::require_non_empty("capability.timeout_class", &self.timeout_class)?;
        Ok(())
    }
}

/// Serialize a capability list to a JSON array (used by `capabilities` files).
pub fn to_json_array(capabilities: &[Capability]) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(capabilities)
}
