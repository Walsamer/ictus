//! Versioned JSON-over-stdio bridge to a durable execution backend.
//!
//! ## Why JSON over stdin/stdout (initial boundary)
//!
//! This is the simplest maintainable boundary for the first local version:
//!
//! - no listening socket, port allocation, auth, or lifecycle to manage;
//! - the payload is a pure, independently testable text contract
//!   (`contracts/execution-intent.schema.json`,
//!   `contracts/execution-result.schema.json`);
//! - the transport is swappable: the *contracts* do not change if this becomes
//!   HTTP or gRPC later, because `ExecutionIntent`/`ExecutionResult` are
//!   transport-independent.
//!
//! One process invocation handles exactly one intent, so the boundary carries
//! no durable state. Durability, retries, step state and run persistence belong
//! to the backend (Dagster); see `docs/ARCHITECTURE.md`.

use std::io::Write;
use std::process::{Command, Stdio};

use agentic_core::{ExecutionIntent, ExecutionResult};
use agentic_ports::{ExecutionBackend, PortError};

/// An `ExecutionBackend` that sends one `ExecutionIntent` as JSON on the child
/// process's stdin and reads one `ExecutionResult` as JSON from its stdout.
#[derive(Debug, Clone)]
pub struct JsonStdioBackend {
    program: String,
    args: Vec<String>,
}

impl JsonStdioBackend {
    pub fn new(program: impl Into<String>, args: Vec<String>) -> Self {
        Self {
            program: program.into(),
            args,
        }
    }

    /// Resolve the backend command from `AGENTIC_DAGSTER_BRIDGE_CMD`, falling
    /// back to `uv run --frozen python -m agentic_dagster.bridge`.
    ///
    /// The command is intentionally overridable so the same Rust boundary can
    /// drive a different (e.g. enterprise) adapter without a code change.
    pub fn from_env() -> Self {
        let raw = std::env::var("AGENTIC_DAGSTER_BRIDGE_CMD")
            .unwrap_or_else(|_| "uv run --frozen python -m agentic_dagster.bridge".to_string());
        let mut parts = raw.split_whitespace();
        let program = parts.next().unwrap_or("uv").to_string();
        let args = parts.map(str::to_string).collect();
        Self { program, args }
    }
}

impl ExecutionBackend for JsonStdioBackend {
    fn execute(&self, intent: &ExecutionIntent) -> Result<ExecutionResult, PortError> {
        intent.validate()?;
        let payload =
            serde_json::to_vec(intent).map_err(|e| PortError::Serialization(e.to_string()))?;

        let mut child = Command::new(&self.program)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| {
                PortError::ExecutionFailed(format!(
                    "could not spawn execution backend '{}': {e}",
                    self.program
                ))
            })?;

        child
            .stdin
            .take()
            .ok_or_else(|| PortError::ExecutionFailed("backend stdin unavailable".into()))?
            .write_all(&payload)
            .map_err(|e| PortError::ExecutionFailed(format!("writing intent failed: {e}")))?;

        let output = child
            .wait_with_output()
            .map_err(|e| PortError::ExecutionFailed(format!("backend wait failed: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let tail: String = stderr.chars().rev().take(2000).collect::<String>();
            let tail: String = tail.chars().rev().collect();
            return Err(PortError::ExecutionFailed(format!(
                "backend exited with {}: {}",
                output.status,
                tail.trim()
            )));
        }

        let result: ExecutionResult = serde_json::from_slice(&output.stdout).map_err(|e| {
            let stdout = String::from_utf8_lossy(&output.stdout);
            PortError::Serialization(format!(
                "backend returned invalid ExecutionResult JSON: {e} (stdout: {})",
                stdout.trim()
            ))
        })?;
        result.validate()?;
        Ok(result)
    }
}
