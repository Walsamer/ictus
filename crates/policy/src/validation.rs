//! Structural validation.
//!
//! Structural validation is separate from policy evaluation. It answers
//! "is this shape well-formed and versioned?" — never "is this authorized?".

use ictus_core::{Capability, ContractError, DecisionProposal};

/// Validate a proposal's structure and version.
pub fn validate_proposal(proposal: &DecisionProposal) -> Result<(), ContractError> {
    proposal.validate()
}

/// Validate a capability record's structure and version.
pub fn validate_capability(capability: &Capability) -> Result<(), ContractError> {
    capability.validate()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ictus_core::{DecisionKind, ProviderMetadata, RiskClass, Subject, SCHEMA_VERSION};

    fn provider() -> ProviderMetadata {
        ProviderMetadata::rules("test-rules")
    }

    #[test]
    fn well_formed_proposal_is_accepted() {
        let proposal = DecisionProposal::new(
            "p1",
            DecisionKind::ExecuteCapability,
            Subject::new("task", "t1"),
            provider(),
            "2026-10-01T00:00:00Z",
        )
        .with_capability("demo.verify");
        assert!(validate_proposal(&proposal).is_ok());
    }

    #[test]
    fn capability_requiring_decision_without_capability_is_rejected() {
        let proposal = DecisionProposal::new(
            "p1",
            DecisionKind::Retry,
            Subject::new("task", "t1"),
            provider(),
            "2026-10-01T00:00:00Z",
        );
        assert!(validate_proposal(&proposal).is_err());
    }

    #[test]
    fn wrong_schema_version_is_rejected() {
        let mut proposal = DecisionProposal::new(
            "p1",
            DecisionKind::Abort,
            Subject::new("task", "t1"),
            provider(),
            "2026-10-01T00:00:00Z",
        );
        proposal.schema_version = SCHEMA_VERSION + 1;
        assert!(matches!(
            validate_proposal(&proposal),
            Err(ContractError::UnsupportedSchemaVersion { .. })
        ));
    }

    #[test]
    fn capability_without_version_is_rejected() {
        let capability = Capability::new("demo.verify", "", RiskClass::Low);
        assert!(validate_capability(&capability).is_err());
    }
}
