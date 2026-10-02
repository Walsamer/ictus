//! Approval providers.
//!
//! Approvals are the human gate in the trust boundary. The *required* approval
//! set comes from the capability; a provider answers whether its satisfied
//! tokens cover that set. The core never assumes approval.

use ictus_ports::ApprovalProvider;

/// Denies every non-empty approval requirement. Safe default.
#[derive(Debug, Clone, Copy, Default)]
pub struct NeverApproved;

impl ApprovalProvider for NeverApproved {
    fn is_approved(&self, required: &[String]) -> bool {
        required.is_empty()
    }

    fn satisfied_approvals(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Approves every approval requirement. For deterministic tests and
/// fully-trusted paths.
#[derive(Debug, Clone, Copy, Default)]
pub struct AlwaysApproved;

impl ApprovalProvider for AlwaysApproved {
    fn is_approved(&self, _required: &[String]) -> bool {
        true
    }

    fn satisfied_approvals(&self) -> Vec<String> {
        vec!["*".to_string()]
    }
}

/// Approves only when every required token is present.
///
/// The wildcard token `*` satisfies everything.
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
    fn is_approved(&self, required: &[String]) -> bool {
        if required.is_empty() {
            return true;
        }
        if self.satisfied.iter().any(|token| token == "*") {
            return true;
        }
        required
            .iter()
            .all(|token| self.satisfied.iter().any(|satisfied| satisfied == token))
    }

    fn satisfied_approvals(&self) -> Vec<String> {
        self.satisfied.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_and_always_providers() {
        assert!(NeverApproved.is_approved(&[]));
        assert!(!NeverApproved.is_approved(&["human".to_string()]));
        assert!(AlwaysApproved.is_approved(&["human".to_string()]));
        assert!(NeverApproved.satisfied_approvals().is_empty());
        assert_eq!(AlwaysApproved.satisfied_approvals(), vec!["*".to_string()]);
    }

    #[test]
    fn empty_requirement_is_always_approved() {
        assert!(TokenApprovalProvider::new(vec![]).is_approved(&[]));
    }

    #[test]
    fn token_provider_requires_every_token() {
        let provider = TokenApprovalProvider::new(vec![]).with("human");
        assert!(provider.is_approved(&["human".to_string()]));
        assert!(!provider.is_approved(&["human".to_string(), "security".to_string()]));
        assert_eq!(provider.satisfied_approvals(), vec!["human".to_string()]);
    }

    #[test]
    fn an_empty_token_provider_denies_a_non_empty_requirement() {
        // Regression: previously an empty provider silently approved everything
        // because it read the (absent) requirement from the proposal.
        assert!(!TokenApprovalProvider::new(vec![]).is_approved(&["human".to_string()]));
    }

    #[test]
    fn wildcard_approval_satisfies_everything() {
        let provider = TokenApprovalProvider::new(vec![]).with("*");
        assert!(provider.is_approved(&["human".to_string(), "security".to_string()]));
    }
}
