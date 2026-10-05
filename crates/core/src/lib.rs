//! Typed, versioned domain contracts for the `ictus` layer.
//!
//! This crate is the *generic* core. It must never contain domain-specific,
//! model-specific, provider-specific, transport-specific or infrastructure
//! specific concepts. It owns typed decisions, policy outcomes, capability
//! metadata and the execution contracts that cross into a durable execution
//! backend such as Dagster.
//!
//! Responsibility boundary (binding):
//!
//! - This crate owns typed contracts, validation shapes and decision types.
//! - It does **not** own durable workflow state, retries, step sequencing,
//!   queues, scheduling or crash recovery. Those belong to the execution
//!   backend (Dagster). See `docs/ARCHITECTURE.md` and
//!   `docs/CONTRACTS_AND_BOUNDARIES.md`.

pub mod capability;
pub mod decision;
pub mod evidence;
pub mod execution;
pub mod observation;
pub mod policy;
pub mod snapshot;
pub mod version;

pub use capability::{Capability, RiskClass};
pub use decision::{
    DecisionKind, DecisionProposal, ProviderMetadata, ProviderType, RouteConstraints,
};
pub use evidence::EvidenceRef;
pub use execution::{ExecutionIntent, ExecutionResult, ExecutionStatus, RequestedBy};
pub use observation::{ExecutionObservation, ObservationCategory};
pub use policy::{PolicyDecision, PolicyDecisionKind};
pub use snapshot::{Fact, StateSnapshot, Subject};
pub use version::{
    ContractError, DECISION_SCHEMA_VERSION, LEGACY_DECISION_SCHEMA_VERSION, SCHEMA_VERSION,
};
