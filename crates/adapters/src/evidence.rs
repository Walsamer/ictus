//! File-backed evidence store.

use std::io::Write;
use std::path::{Path, PathBuf};

use ictus_core::EvidenceRef;
use ictus_ports::{EvidenceStore, PortError};

/// Appends one JSON object per line to a file (JSONL).
///
/// The store is intentionally simple and deterministic: `record` appends the
/// serialized [`EvidenceRef`] plus a newline, creating parent directories as
/// needed. It never rewrites or truncates existing evidence.
#[derive(Debug, Clone)]
pub struct JsonlEvidenceStore {
    path: PathBuf,
}

impl JsonlEvidenceStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read every recorded line back (for inspection and tests).
    pub fn read_all(&self) -> Result<Vec<EvidenceRef>, PortError> {
        let raw = std::fs::read_to_string(&self.path)
            .map_err(|error| PortError::EvidenceFailed(error.to_string()))?;
        raw.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| {
                serde_json::from_str(line)
                    .map_err(|error| PortError::EvidenceFailed(error.to_string()))
            })
            .collect()
    }
}

impl EvidenceStore for JsonlEvidenceStore {
    fn record(&self, evidence: &EvidenceRef) -> Result<(), PortError> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|error| PortError::EvidenceFailed(error.to_string()))?;
            }
        }
        let line = serde_json::to_string(evidence)
            .map_err(|error| PortError::EvidenceFailed(error.to_string()))?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| PortError::EvidenceFailed(error.to_string()))?;
        file.write_all(format!("{line}\n").as_bytes())
            .map_err(|error| PortError::EvidenceFailed(error.to_string()))
    }
}
