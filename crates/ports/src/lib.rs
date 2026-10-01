//! Abstract ports implemented by adapters.
//!
//! This crate depends only on `agentic-core`. It contains **no** concrete
//! adapter, transport, provider or domain logic. Adapters (local processes,
//! Dagster, HTTP, a rules engine, an LLM) implement these traits; the core and
//! policy layers depend on the traits, never on a concrete adapter.

use agentic_core::{
    Capability, DecisionProposal, ExecutionIntent, ExecutionResult, PolicyDecision, StateSnapshot,
    Subject,
};

/// Errors raised across a port boundary.
#[derive(Debug, thiserror::Error)]
pub enum PortError {
    #[error("no state provider is available: {0}")]
    StateUnavailable(String),
    #[error("decision provider failed: {0}")]
    DecisionFailed(String),
    #[error("policy evaluation failed: {0}")]
    PolicyFailed(String),
    #[error("capability registry failed: {0}")]
    CapabilityRegistry(String),
    #[error("execution backend failed: {0}")]
    ExecutionFailed(String),
    #[error("approval provider failed: {0}")]
    ApprovalFailed(String),
    #[error("evidence store failed: {0}")]
    EvidenceFailed(String),
    #[error("contract error: {0}")]
    Contract(#[from] agentic_core::ContractError),
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// Produces a normalized snapshot from a domain system.
pub trait StateProvider {
    fn snapshot(&self, subject: &Subject) -> Result<StateSnapshot, PortError>;
}

/// Produces an untrusted proposal from a snapshot.
///
/// Implementations may be deterministic rules, a specialized model, an LLM or a
/// human. The core treats them uniformly.
pub trait DecisionProvider {
    fn propose(&self, snapshot: &StateSnapshot) -> Result<DecisionProposal, PortError>;
}

/// Answers capability questions: does it exist, what does it require?
pub trait CapabilityRegistry {
    fn get(&self, id: &str) -> Option<Capability>;

    fn contains(&self, id: &str) -> bool {
        self.get(id).is_some()
    }
}

/// Answers whether the approvals a proposal needs are satisfied.
pub trait ApprovalProvider {
    /// True when every approval the proposal/capability requires is present.
    fn is_approved(&self, proposal: &DecisionProposal) -> bool;

    /// The approval tokens currently satisfied (for audit reasons).
    fn satisfied_approvals(&self) -> Vec<String>;
}

/// Validates a proposal against policy and capabilities.
///
/// Must be deterministic wherever possible.
pub trait PolicyEvaluator {
    fn evaluate(
        &self,
        snapshot: &StateSnapshot,
        proposal: &DecisionProposal,
        capabilities: &dyn CapabilityRegistry,
        approvals: &dyn ApprovalProvider,
    ) -> Result<PolicyDecision, PortError>;
}

/// Executes a validated intent durably and returns a bounded result.
///
/// The backend must never mutate decision/policy state; it reports facts only.
pub trait ExecutionBackend {
    fn execute(&self, intent: &ExecutionIntent) -> Result<ExecutionResult, PortError>;
}

/// Stores/records evidence references.
pub trait EvidenceStore {
    fn record(&self, evidence: &agentic_core::EvidenceRef) -> Result<(), PortError>;
}
