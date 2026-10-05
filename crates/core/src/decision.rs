//! Decision proposals.
//!
//! A proposal is untrusted input. It may be produced by deterministic rules, a
//! specialized model, an LLM or a human. It must pass validation before it can
//! become an `ExecutionIntent`.

use serde::{Deserialize, Serialize};

use crate::evidence::EvidenceRef;
use crate::snapshot::Subject;
use crate::version::{ContractError, DECISION_SCHEMA_VERSION, LEGACY_DECISION_SCHEMA_VERSION};

/// The frozen generic semantic decision vocabulary.
///
/// Names are frozen by `docs/adr/0001-generic-decision-vocabulary.md`. The
/// vocabulary is intentionally domain-neutral: it contains no Tactus lifecycle
/// nouns (`REQUEUE_READY`, `BLOCK`, `RETIRE`) and no durable-execution retry
/// concept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DecisionKind {
    /// Request another **semantic** execution attempt of the same capability.
    ///
    /// This yields a new `ExecutionIntent`. It is explicitly *not* a Dagster
    /// step retry; the execution backend owns its own step retries.
    ///
    /// The legacy v1 token `RETRY` deserializes to this variant and is
    /// normalized on re-serialization.
    #[serde(alias = "RETRY")]
    Reexecute,
    /// Stop; no further action.
    Abort,
    /// Hand to a human.
    Escalate,
    /// Execute a named capability.
    ExecuteCapability,
    /// Execute a capability subject to generic route constraints.
    Route,
    /// Decide that the subject should be decomposed into smaller units. Ictus
    /// itself creates no child work items; translation is an adapter concern.
    Decompose,
}

impl DecisionKind {
    /// True when the decision produces an `ExecutionIntent`, and therefore when
    /// a non-empty capability id is required. Exhaustive by design.
    pub fn requires_capability(self) -> bool {
        match self {
            DecisionKind::Reexecute | DecisionKind::ExecuteCapability | DecisionKind::Route => true,
            DecisionKind::Abort | DecisionKind::Escalate | DecisionKind::Decompose => false,
        }
    }

    /// True when the token exists only in v2 of the `DecisionProposal`
    /// contract. Legacy v1 payloads must not use these. Exhaustive by design.
    pub fn is_v2_only(self) -> bool {
        match self {
            DecisionKind::Route | DecisionKind::Decompose => true,
            DecisionKind::Reexecute
            | DecisionKind::Abort
            | DecisionKind::Escalate
            | DecisionKind::ExecuteCapability => false,
        }
    }
}

/// Generic route constraints for a [`DecisionKind::Route`] decision.
///
/// These name *roles* (backend, provider, runtime), never concrete vendors or
/// domain nouns, so the constraint stays domain-neutral. A `None` field means
/// "no constraint".
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RouteConstraints {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_runtime: Option<String>,
}

impl RouteConstraints {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_exclude_backend(mut self, backend: impl Into<String>) -> Self {
        self.exclude_backend = Some(backend.into());
        self
    }

    pub fn with_preferred_backend(mut self, backend: impl Into<String>) -> Self {
        self.preferred_backend = Some(backend.into());
        self
    }

    pub fn with_required_provider(mut self, provider: impl Into<String>) -> Self {
        self.required_provider = Some(provider.into());
        self
    }

    pub fn with_required_runtime(mut self, runtime: impl Into<String>) -> Self {
        self.required_runtime = Some(runtime.into());
        self
    }

    /// True when no constraint is set. A route with no constraints is still a
    /// valid `ROUTE`, it simply expresses no preference.
    pub fn is_empty(&self) -> bool {
        self.exclude_backend.is_none()
            && self.preferred_backend.is_none()
            && self.required_provider.is_none()
            && self.required_runtime.is_none()
    }

    fn validate(&self) -> Result<(), ContractError> {
        for (field, value) in [
            ("route.exclude_backend", &self.exclude_backend),
            ("route.preferred_backend", &self.preferred_backend),
            ("route.required_provider", &self.required_provider),
            ("route.required_runtime", &self.required_runtime),
        ] {
            if let Some(value) = value {
                if value.trim().is_empty() {
                    return Err(ContractError::InvalidValue {
                        field,
                        reason: "route constraints must be non-empty when present".to_string(),
                    });
                }
            }
        }
        Ok(())
    }
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
    /// Required for capability-requiring decisions (`Reexecute`,
    /// `ExecuteCapability`, `Route`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
    /// Present only for a `Route` decision; ignored (and rejected) otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<RouteConstraints>,
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
            schema_version: DECISION_SCHEMA_VERSION,
            proposal_id: proposal_id.into(),
            decision,
            subject,
            capability: None,
            route: None,
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

    pub fn with_route(mut self, route: RouteConstraints) -> Self {
        self.route = Some(route);
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
        self.decision.requires_capability()
    }

    /// Structural validation. Semantic/policy checks live in `ictus-policy`.
    pub fn validate(&self) -> Result<(), ContractError> {
        match self.schema_version {
            DECISION_SCHEMA_VERSION => {}
            LEGACY_DECISION_SCHEMA_VERSION => {
                // Deliberate v1 compatibility: the legacy vocabulary is accepted,
                // but v2-only decisions must not be smuggled in under v1.
                if self.decision.is_v2_only() {
                    return Err(ContractError::InvalidValue {
                        field: "decision",
                        reason: format!(
                            "decision {:?} is not part of the v1 vocabulary",
                            self.decision
                        ),
                    });
                }
            }
            found => {
                return Err(ContractError::UnsupportedSchemaVersion {
                    found,
                    supported: DECISION_SCHEMA_VERSION,
                })
            }
        }
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
        if let Some(route) = &self.route {
            if self.decision != DecisionKind::Route {
                return Err(ContractError::InvalidValue {
                    field: "route",
                    reason: format!(
                        "route constraints are only valid for ROUTE, not {:?}",
                        self.decision
                    ),
                });
            }
            route.validate()?;
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
