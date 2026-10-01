//! Deterministic policy, capability validation and rule-based decisions.
//!
//! This crate implements the abstract ports from `agentic-ports` with
//! deterministic, model-agnostic behaviour. It has no dependency on Dagster,
//! the testbed, a transport, an LLM or any infrastructure.
//!
//! Flow:
//!
//! ```text
//! StateSnapshot -> DecisionProvider -> DecisionProposal
//!               -> PolicyEvaluator  -> PolicyDecision (+ ExecutionIntent)
//! ```

pub mod approval;
pub mod evaluator;
pub mod intent;
pub mod registry;
pub mod rules;
pub mod validation;

pub use approval::{AlwaysApproved, NeverApproved, TokenApprovalProvider};
pub use evaluator::DefaultPolicyEvaluator;
pub use intent::build_execution_intent;
pub use registry::InMemoryCapabilityRegistry;
pub use rules::RuleDecisionProvider;
pub use validation::{validate_capability, validate_proposal};
