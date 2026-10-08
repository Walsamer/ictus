//! Abstract ports implemented by adapters.
//!
//! This crate depends only on `ictus-core`. It contains **no** concrete
//! adapter, transport, provider or domain logic. Adapters (local processes,
//! Dagster, HTTP, a rules engine, an LLM) implement these traits; the core and
//! policy layers depend on the traits, never on a concrete adapter.

pub use ictus_core::ExecutionReceipt;
use ictus_core::{
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
    #[error("execution submission conflicts with an existing receipt: {0}")]
    SubmissionConflict(String),
    #[error("execution receipt is unknown: {0}")]
    UnknownReceipt(String),
    #[error("approval provider failed: {0}")]
    ApprovalFailed(String),
    #[error("evidence store failed: {0}")]
    EvidenceFailed(String),
    #[error("contract error: {0}")]
    Contract(#[from] ictus_core::ContractError),
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

/// Answers whether the approvals a capability requires are satisfied.
pub trait ApprovalProvider {
    /// True when every token in `required` is satisfied (vacuously true when
    /// nothing is required). The required set comes from the *capability*, not
    /// from the proposal.
    fn is_approved(&self, required: &[String]) -> bool;

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

/// State returned when querying a durable execution receipt.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionQuery {
    Pending,
    Terminal(ExecutionResult),
}

/// Asynchronous execution boundary for production adapters.
///
/// This intentionally does not expose queues, launchers, retries or workflow
/// state.  Those remain owned by Dagster.  The older [`ExecutionBackend`]
/// remains useful for small synchronous examples.
pub trait DurableExecutionBackend {
    fn submit(
        &self,
        intent: &ExecutionIntent,
        intent_digest: &str,
    ) -> Result<ExecutionReceipt, PortError>;

    fn query(&self, receipt: &ExecutionReceipt) -> Result<ExecutionQuery, PortError>;

    fn cancel(&self, receipt: &ExecutionReceipt) -> Result<(), PortError>;
}

/// Stores/records evidence references.
pub trait EvidenceStore {
    fn record(&self, evidence: &ictus_core::EvidenceRef) -> Result<(), PortError>;
}
