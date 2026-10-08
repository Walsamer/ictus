//! Deterministic policy, capability validation and rule-based decisions.
//!
//! This crate implements the abstract ports from `ictus-ports` with
//! deterministic, model-agnostic behaviour. It has no dependency on Dagster,
//! any domain system, a transport, an LLM or any infrastructure.
//!
//! Flow:
//!
//! ```text
//! StateSnapshot -> DecisionProvider -> DecisionProposal
//!               -> PolicyEvaluator  -> PolicyDecision (+ ExecutionIntent)
//! ```

pub mod approval;
pub mod composition;
pub mod evaluator;
pub mod intent;
pub mod registry;
pub mod rules;
pub mod validation;

pub use approval::{AlwaysApproved, NeverApproved, TokenApprovalProvider};
pub use composition::{
    revalidate_modified_intent, LocalPolicyComposition, TrustedDecision, ValidationRequest,
    DEFAULT_POLICY_VERSION,
};
pub use evaluator::DefaultPolicyEvaluator;
pub use intent::build_execution_intent;
pub use registry::InMemoryCapabilityRegistry;
pub use rules::RuleDecisionProvider;
pub use validation::{validate_capability, validate_proposal};
