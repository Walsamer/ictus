//! Boundary tests for the JSON-over-stdio execution backend.
//!
//! These use deterministic fake backends, so they prove the Rust side of the
//! contract without requiring Dagster. The real Dagster round trip is proven by
//! `tests/python/test_bridge_end_to_end.py` and `scripts/e2e_rust_dagster.sh`.

use std::path::PathBuf;

use ictus_bridge::JsonStdioBackend;
use ictus_core::{ExecutionIntent, ExecutionStatus, ObservationCategory, RequestedBy, Subject};
use ictus_ports::ExecutionBackend;

fn fixture(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

fn intent() -> ExecutionIntent {
    ExecutionIntent::new(
        "intent:p1",
        "demo.verify",
        Subject::new("task", "t1"),
        RequestedBy::new("rules.v1", "proposal:p1"),
    )
}

#[test]
fn valid_intent_round_trips_to_a_typed_result() {
    let backend = JsonStdioBackend::new("sh", vec![fixture("fake_bridge_success.sh")]);
    let result = backend.execute(&intent()).unwrap();
    result.validate().unwrap();
    assert_eq!(result.status, ExecutionStatus::Succeeded);
    assert_eq!(result.observation.category, ObservationCategory::Success);
    assert_eq!(result.intent_id, "intent:p1");
    assert!(result.is_success());
}

#[test]
fn non_zero_backend_exit_fails_closed() {
    let backend = JsonStdioBackend::new("sh", vec![fixture("fake_bridge_failure.sh")]);
    let error = backend.execute(&intent()).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("backend exited"), "unexpected: {message}");
    assert!(message.contains("simulated backend crash"));
}

#[test]
fn invalid_backend_json_is_rejected() {
    let backend = JsonStdioBackend::new("sh", vec![fixture("fake_bridge_invalid.sh")]);
    let error = backend.execute(&intent()).unwrap_err();
    assert!(
        error.to_string().contains("invalid ExecutionResult JSON"),
        "unexpected: {error}"
    );
}

#[test]
fn unknown_backend_program_fails_closed() {
    let backend = JsonStdioBackend::new("definitely-not-a-real-program-xyz", vec![]);
    let error = backend.execute(&intent()).unwrap_err();
    assert!(
        error.to_string().contains("could not spawn"),
        "unexpected: {error}"
    );
}

#[test]
fn result_maps_to_an_observation_fact() {
    let backend = JsonStdioBackend::new("sh", vec![fixture("fake_bridge_success.sh")]);
    let result = backend.execute(&intent()).unwrap();
    let observation = result.to_observation("obs-1", "2026-10-01T00:00:02Z");
    assert_eq!(observation.intent_id, "intent:p1");
    assert_eq!(observation.category, ObservationCategory::Success);
    observation.validate().unwrap();
}

#[test]
fn command_string_splits_program_and_args() {
    let backend =
        JsonStdioBackend::from_command_str("uv run --frozen python -m ictus_dagster.bridge");
    assert_eq!(backend.program(), "uv");
    assert_eq!(
        backend.args(),
        &["run", "--frozen", "python", "-m", "ictus_dagster.bridge"]
    );
}

#[test]
fn single_token_and_empty_command_strings() {
    let single = JsonStdioBackend::from_command_str("my-backend");
    assert_eq!(single.program(), "my-backend");
    assert!(single.args().is_empty());

    let empty = JsonStdioBackend::from_command_str("   ");
    assert_eq!(empty.program(), "uv");
    assert!(empty.args().is_empty());
}

#[test]
fn default_bridge_command_is_stable() {
    assert_eq!(
        JsonStdioBackend::DEFAULT_BRIDGE_COMMAND,
        "uv run --frozen python -m ictus_dagster.bridge"
    );
}

#[test]
fn from_env_prefers_override_then_default() {
    let previous = std::env::var("ICTUS_DAGSTER_BRIDGE_CMD").ok();

    std::env::set_var("ICTUS_DAGSTER_BRIDGE_CMD", "python3 -m fake.bridge");
    let overridden = JsonStdioBackend::from_env();
    assert_eq!(overridden.program(), "python3");
    assert_eq!(overridden.args(), &["-m", "fake.bridge"]);

    std::env::remove_var("ICTUS_DAGSTER_BRIDGE_CMD");
    let defaulted = JsonStdioBackend::from_env();
    assert_eq!(defaulted.program(), "uv");

    if let Some(value) = previous {
        std::env::set_var("ICTUS_DAGSTER_BRIDGE_CMD", value);
    }
}
