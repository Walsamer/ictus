//! Approval providers.
//!
//! Approvals are the human gate in the trust boundary. The core never assumes
//! approval; adapters answer whether the required approvals are satisfied.

use ictus_core::DecisionProposal;
use ictus_ports::ApprovalProvider;

/// Denies every proposal approval. Safe default.
#[derive(Debug, Clone, Copy, Default)]
pub struct NeverApproved;

impl ApprovalProvider for NeverApproved {
    fn is_approved(&self, _proposal: &DecisionProposal) -> bool {
        false
    }

    fn satisfied_approvals(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Approves every proposal. For deterministic tests and fully-trusted paths.
#[derive(Debug, Clone, Copy, Default)]
pub struct AlwaysApproved;

impl ApprovalProvider for AlwaysApproved {
    fn is_approved(&self, _proposal: &DecisionProposal) -> bool {
        true
    }

    fn satisfied_approvals(&self) -> Vec<String> {
        vec!["*".to_string()]
    }
}

/// Approves only when every required token is present.
#[derive(Debug, Clone, Default)]
pub struct TokenApprovalProvider {
    satisfied: Vec<String>,
}

impl TokenApprovalProvider {
    pub fn new(satisfied: Vec<String>) -> Self {
        Self { satisfied }
    }

    pub fn with(mut self, token: impl Into<String>) -> Self {
        self.satisfied.push(token.into());
        self
    }
}

impl ApprovalProvider for TokenApprovalProvider {
    fn is_approved(&self, proposal: &DecisionProposal) -> bool {
        // A wildcard approval satisfies everything.
        if self.satisfied.iter().any(|token| token == "*") {
            return true;
        }
        proposal
            .arguments
            .get("required_approvals")
            .and_then(|value| value.as_array())
            .map(|required| {
                required.iter().all(|token| {
                    token
                        .as_str()
                        .map(|token| self.satisfied.iter().any(|s| s == token))
                        .unwrap_or(false)
                })
            })
            .unwrap_or(true)
    }

    fn satisfied_approvals(&self) -> Vec<String> {
        self.satisfied.clone()
    }
}
