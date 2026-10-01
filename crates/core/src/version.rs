//! Contract versioning.
//!
//! Every externally serialized contract carries a `schema_version`. Breaking
//! changes require a new version; adapters may support multiple versions during
//! migration.

/// The current schema version for every externally serialized payload.
pub const SCHEMA_VERSION: u32 = 1;

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
        if found == SCHEMA_VERSION {
            Ok(())
        } else {
            Err(ContractError::UnsupportedSchemaVersion {
                found,
                supported: SCHEMA_VERSION,
            })
        }
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
    fn empty_required_field_is_rejected() {
        assert_eq!(
            ContractError::require_non_empty("id", "   ").unwrap_err(),
            ContractError::MissingField("id")
        );
    }
}
