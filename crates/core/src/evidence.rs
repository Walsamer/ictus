//! Evidence references.
//!
//! Evidence is an opaque, transport-independent pointer at a fact. The generic
//! core never dereferences it; it records provenance so a decision can be
//! audited without storing secret or domain-specific payloads.

use serde::{Deserialize, Serialize};

/// A reference to evidence supporting an observation, proposal or result.
///
/// `uri` is intentionally scheme-agnostic (`file:`, `sha256:`, `https:`, a
/// domain record id, ...). The core never resolves it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceRef {
    /// Coarse kind of evidence, e.g. `command_output`, `test_report`, `log`.
    pub kind: String,
    /// Opaque locator for the evidence.
    pub uri: String,
    /// Optional content hash for immutability checks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// Optional human-readable note (must never contain secrets).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl EvidenceRef {
    /// Construct a minimal evidence reference.
    pub fn new(kind: impl Into<String>, uri: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            uri: uri.into(),
            sha256: None,
            note: None,
        }
    }

    /// Attach an optional content hash.
    pub fn with_sha256(mut self, sha256: impl Into<String>) -> Self {
        self.sha256 = Some(sha256.into());
        self
    }

    /// Attach an optional non-secret note.
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }
}
