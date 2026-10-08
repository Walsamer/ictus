//! Generic facts and contracts for backend/provider/runtime/model routing.
//!
//! A route is selected from observed facts, not from a runtime adapter.  The
//! selected route records the identity and revision of every fact used for the
//! admission so a consumer can reject a stale admission without re-ranking.

use serde::{Deserialize, Serialize};

use crate::{decision::RouteConstraints, evidence::EvidenceRef, version::ContractError};

/// Stable identity of one executable backend/provider/runtime/model combination.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RouteIdentity {
    pub backend: String,
    pub provider: String,
    pub runtime: String,
    pub model: String,
}

impl RouteIdentity {
    pub fn new(
        backend: impl Into<String>,
        provider: impl Into<String>,
        runtime: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        Self {
            backend: backend.into(),
            provider: provider.into(),
            runtime: runtime.into(),
            model: model.into(),
        }
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        for (field, value) in [
            ("route.backend", &self.backend),
            ("route.provider", &self.provider),
            ("route.runtime", &self.runtime),
            ("route.model", &self.model),
        ] {
            ContractError::require_non_empty(field, value)?;
        }
        Ok(())
    }
}

/// Identity and monotonic revision of one source fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteFactRef {
    pub fact_id: String,
    pub revision: u64,
}

impl RouteFactRef {
    pub fn new(fact_id: impl Into<String>, revision: u64) -> Self {
        Self {
            fact_id: fact_id.into(),
            revision,
        }
    }

    pub fn validate(&self, field: &'static str) -> Result<(), ContractError> {
        ContractError::require_non_empty(field, &self.fact_id)
    }
}

/// Explicit quality of an observed health, quota, or disablement fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObservationState {
    Available,
    Unavailable,
    Unknown,
    Stale,
}

/// A versioned descriptor advertised by a backend integration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteDescriptor {
    pub schema_version: u32,
    pub fact: RouteFactRef,
    pub route: RouteIdentity,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default = "enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub evidence: Vec<EvidenceRef>,
}

fn enabled() -> bool {
    true
}

impl RouteDescriptor {
    pub fn new(fact: RouteFactRef, route: RouteIdentity) -> Self {
        Self {
            schema_version: 1,
            fact,
            route,
            capabilities: Vec::new(),
            enabled: true,
            evidence: Vec::new(),
        }
    }

    pub fn with_capability(mut self, capability: impl Into<String>) -> Self {
        self.capabilities.push(capability.into());
        self
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        crate::version::ContractError::check_version(self.schema_version)?;
        self.fact.validate("descriptor.fact_id")?;
        self.route.validate()?;
        for capability in &self.capabilities {
            ContractError::require_non_empty("descriptor.capabilities", capability)?;
        }
        Ok(())
    }
}

/// A versioned operational fact for a specific route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteObservation {
    pub schema_version: u32,
    pub fact: RouteFactRef,
    /// The exact route this operational fact describes. Facts for another
    /// route cannot be combined into this candidate.
    pub route: RouteIdentity,
    pub state: ObservationState,
    pub observed_at: String,
    #[serde(default)]
    pub evidence: Vec<EvidenceRef>,
}

impl RouteObservation {
    pub fn new(
        fact: RouteFactRef,
        route: RouteIdentity,
        state: ObservationState,
        observed_at: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: 1,
            fact,
            route,
            state,
            observed_at: observed_at.into(),
            evidence: Vec::new(),
        }
    }

    pub fn validate(&self, name: &'static str) -> Result<(), ContractError> {
        ContractError::check_version(self.schema_version)?;
        self.fact.validate(name)?;
        self.route.validate()?;
        ContractError::require_non_empty("route observation.observed_at", &self.observed_at)
    }
}

/// Raw route facts supplied by an external integration. This is deliberately a
/// data-only adapter boundary: Ictus applies the compatibility policy after it
/// has normalized these facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteFacts {
    pub descriptor: RouteDescriptor,
    pub health: RouteObservation,
    pub quota: RouteObservation,
    pub disablement: RouteObservation,
}

/// A normalized candidate produced from one route's versioned raw facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteCandidate {
    pub descriptor: RouteDescriptor,
    pub health: RouteObservation,
    pub quota: RouteObservation,
    pub disablement: RouteObservation,
}

impl TryFrom<RouteFacts> for RouteCandidate {
    type Error = ContractError;

    fn try_from(value: RouteFacts) -> Result<Self, Self::Error> {
        let candidate = Self {
            descriptor: value.descriptor,
            health: value.health,
            quota: value.quota,
            disablement: value.disablement,
        };
        candidate.validate()?;
        Ok(candidate)
    }
}

impl RouteCandidate {
    pub fn identity(&self) -> &RouteIdentity {
        &self.descriptor.route
    }

    pub fn supports(&self, capability: &str) -> bool {
        self.descriptor
            .capabilities
            .iter()
            .any(|item| item == capability)
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        self.descriptor.validate()?;
        self.health.validate("health.fact_id")?;
        self.quota.validate("quota.fact_id")?;
        self.disablement.validate("disablement.fact_id")?;
        if self.health.route != self.descriptor.route
            || self.quota.route != self.descriptor.route
            || self.disablement.route != self.descriptor.route
        {
            return Err(ContractError::InvalidValue {
                field: "route observations",
                reason: "health, quota, and disablement facts must bind the descriptor route"
                    .to_string(),
            });
        }
        Ok(())
    }
}

/// Constraints passed to the selector. They are generic roles only; no
/// adapter is permitted to reinterpret or loosen them.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RouteRequirements {
    pub capability: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_runtime: Option<String>,
}

impl RouteRequirements {
    pub fn for_capability(capability: impl Into<String>) -> Self {
        Self {
            capability: capability.into(),
            ..Self::default()
        }
    }

    pub fn from_constraints(
        capability: impl Into<String>,
        constraints: Option<&RouteConstraints>,
    ) -> Self {
        let mut requirements = Self::for_capability(capability);
        if let Some(constraints) = constraints {
            requirements.exclude_backend = constraints.exclude_backend.clone();
            requirements.preferred_backend = constraints.preferred_backend.clone();
            requirements.required_provider = constraints.required_provider.clone();
            requirements.required_runtime = constraints.required_runtime.clone();
        }
        requirements
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        ContractError::require_non_empty("route_requirements.capability", &self.capability)?;
        for (field, value) in [
            ("route_requirements.exclude_backend", &self.exclude_backend),
            (
                "route_requirements.preferred_backend",
                &self.preferred_backend,
            ),
            (
                "route_requirements.required_provider",
                &self.required_provider,
            ),
            (
                "route_requirements.required_runtime",
                &self.required_runtime,
            ),
        ] {
            if let Some(value) = value {
                ContractError::require_non_empty(field, value)?;
            }
        }
        Ok(())
    }
}

/// A route which must not be retried for this selection attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteExclusion {
    pub route: RouteIdentity,
}

impl RouteExclusion {
    pub fn new(route: RouteIdentity) -> Self {
        Self { route }
    }
}

/// Complete selected route plus each exact fact identity/revision that admitted
/// it. Consumers can compare these values to current facts and reject stale
/// admission without asking Ictus to rank again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectedRoute {
    pub route: RouteIdentity,
    pub descriptor: RouteFactRef,
    pub health: RouteFactRef,
    pub quota: RouteFactRef,
    pub disablement: RouteFactRef,
}

impl SelectedRoute {
    pub fn from_candidate(candidate: &RouteCandidate) -> Self {
        Self {
            route: candidate.descriptor.route.clone(),
            descriptor: candidate.descriptor.fact.clone(),
            health: candidate.health.fact.clone(),
            quota: candidate.quota.fact.clone(),
            disablement: candidate.disablement.fact.clone(),
        }
    }

    pub fn validate(&self) -> Result<(), ContractError> {
        self.route.validate()?;
        self.descriptor
            .validate("selected_route.descriptor.fact_id")?;
        self.health.validate("selected_route.health.fact_id")?;
        self.quota.validate("selected_route.quota.fact_id")?;
        self.disablement
            .validate("selected_route.disablement.fact_id")
    }
}
