//! Contract versioning.
//!
//! Every externally serialized contract carries a `schema_version`. Breaking
//! changes require a new version; adapters may support multiple versions during
//! migration.

/// The current schema version for the base, unchanged externally serialized
/// payloads (`StateSnapshot`, `ExecutionObservation`, `ExecutionIntent`,
/// `ExecutionResult`, `PolicyDecision`).
pub const SCHEMA_VERSION: u32 = 1;

/// The current schema version of the `DecisionProposal` contract.
///
/// Bumped to `2` when the generic semantic decision vocabulary was frozen (see
/// `docs/adr/0001-generic-decision-vocabulary.md`): `RETRY` was renamed to
/// `REEXECUTE` and `ROUTE` / `DECOMPOSE` were added. `schema_version` is
/// per-contract, so the base contracts stay at [`SCHEMA_VERSION`].
pub const DECISION_SCHEMA_VERSION: u32 = 2;

/// The original `DecisionProposal` schema version, still accepted on input for
/// deliberate backward compatibility. A v1 proposal is normalized on read
/// (`RETRY` -> `REEXECUTE`) and may not use the v2-only `ROUTE` / `DECOMPOSE`.
pub const LEGACY_DECISION_SCHEMA_VERSION: u32 = 1;

/// The current schema version of the versioned initial/recovery
/// `DecisionContext` contract.
///
/// `schema_version` is per-contract: this new wire contract starts at `1` and
/// is versioned independently of [`SCHEMA_VERSION`] and
/// [`DECISION_SCHEMA_VERSION`].
pub const CONTEXT_SCHEMA_VERSION: u32 = 1;

/// The current schema version of the versioned validated
/// `DecisionEnvelope` contract. Per-contract, independently versioned.
pub const ENVELOPE_SCHEMA_VERSION: u32 = 1;

/// Reject any version other than the supported one for a specific contract.
///
/// `schema_version` is per-contract, so a version that is valid for one
/// contract may be unknown for another. Unknown versions fail closed.
pub fn check_contract_version(found: u32, supported: u32) -> Result<(), ContractError> {
    if found == supported {
        Ok(())
    } else {
        Err(ContractError::UnsupportedSchemaVersion { found, supported })
    }
}

/// Errors produced while validating a contract.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ContractError {
    #[error("unsupported schema_version {found}; supported version is {supported}")]
    UnsupportedSchemaVersion { found: u32, supported: u32 },
    #[error("missing required field: {0}")]
    MissingField(&'static str),
    #[error("invalid value for {field}: {reason}")]
    InvalidValue { field: &'static str, reason: String },
}

impl ContractError {
    /// Reject any version other than the supported one. Adapters may add their
    /// own multi-version handling on top of this, but the generic core fails
    /// closed on unknown versions.
    pub fn check_version(found: u32) -> Result<(), ContractError> {
        check_contract_version(found, SCHEMA_VERSION)
    }

    /// Validate that a required string field is non-empty and trimmed.
    pub fn require_non_empty(field: &'static str, value: &str) -> Result<(), ContractError> {
        if value.trim().is_empty() {
            Err(ContractError::MissingField(field))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_version_is_accepted() {
        assert!(ContractError::check_version(SCHEMA_VERSION).is_ok());
    }

    #[test]
    fn unknown_version_is_rejected() {
        let err = ContractError::check_version(SCHEMA_VERSION + 1).unwrap_err();
        assert_eq!(
            err,
            ContractError::UnsupportedSchemaVersion {
                found: SCHEMA_VERSION + 1,
                supported: SCHEMA_VERSION,
            }
        );
    }

    #[test]
    fn per_contract_version_check_fails_closed() {
        assert!(check_contract_version(1, 1).is_ok());
        assert_eq!(
            check_contract_version(2, 1).unwrap_err(),
            ContractError::UnsupportedSchemaVersion {
                found: 2,
                supported: 1,
            }
        );
    }

    #[test]
    fn empty_required_field_is_rejected() {
        assert_eq!(
            ContractError::require_non_empty("id", "   ").unwrap_err(),
            ContractError::MissingField("id")
        );
    }
}
