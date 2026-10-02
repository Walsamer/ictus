//! File-backed approval provider.
//!
//! Loads satisfied approval tokens from a JSON file, so approvals can be
//! granted persistently and out-of-band instead of via a CLI flag. Fail closed:
//! a missing or malformed file is an error, never "approved".

use std::path::Path;

use ictus_ports::ApprovalProvider;

use crate::error::AdapterError;

/// An [`ApprovalProvider`] whose satisfied tokens come from a JSON file.
///
/// Accepted file shapes:
///
/// ```json
/// ["human", "security"]
/// ```
///
/// ```json
/// { "satisfied": ["human"], "wildcard": false }
/// ```
#[derive(Debug, Clone, Default)]
pub struct JsonFileApprovalProvider {
    satisfied: Vec<String>,
}

impl JsonFileApprovalProvider {
    /// Build from an explicit token list.
    pub fn from_tokens(satisfied: Vec<String>) -> Self {
        Self { satisfied }
    }

    /// Load satisfied tokens from a JSON file. Fails closed on any problem.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, AdapterError> {
        let path = path.as_ref();
        let display = || path.display().to_string();
        let raw = std::fs::read_to_string(path).map_err(|source| AdapterError::Read {
            path: display(),
            source,
        })?;
        let value: serde_json::Value =
            serde_json::from_str(&raw).map_err(|source| AdapterError::Json {
                path: display(),
                source,
            })?;

        let mut satisfied = Vec::new();
        let mut wildcard = false;
        match value {
            serde_json::Value::Array(items) => {
                satisfied = tokens_from(items, &display())?;
            }
            serde_json::Value::Object(map) => {
                if let Some(satisfied_value) = map.get("satisfied") {
                    let items = satisfied_value
                        .as_array()
                        .ok_or_else(|| AdapterError::Shape {
                            path: display(),
                            expected: "an array at 'satisfied'",
                        })?;
                    satisfied = tokens_from(items.clone(), &display())?;
                }
                wildcard = map
                    .get("wildcard")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
            }
            _ => {
                return Err(AdapterError::Shape {
                    path: display(),
                    expected: "an array of tokens or an object with 'satisfied'",
                })
            }
        }
        if wildcard {
            satisfied.push("*".to_string());
        }
        Ok(Self { satisfied })
    }

    /// The satisfied tokens.
    pub fn tokens(&self) -> &[String] {
        &self.satisfied
    }
}

fn tokens_from(items: Vec<serde_json::Value>, path: &str) -> Result<Vec<String>, AdapterError> {
    items
        .into_iter()
        .map(|item| {
            item.as_str()
                .map(str::to_string)
                .ok_or_else(|| AdapterError::Shape {
                    path: path.to_string(),
                    expected: "every token to be a string",
                })
        })
        .collect()
}

impl ApprovalProvider for JsonFileApprovalProvider {
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
