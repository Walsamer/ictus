//! Adapter errors.

/// Errors raised by the generic adapters. All are fail-closed: a missing or
/// malformed file never silently becomes "approved" or "recorded".
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("could not read {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid JSON in {path}: {source}")]
    Json {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("unexpected shape in {path}: expected {expected}")]
    Shape {
        path: String,
        expected: &'static str,
    },
}
