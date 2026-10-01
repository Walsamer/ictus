//! Boundary tests for the JSON-over-stdio execution backend.
//!
//! These use deterministic fake backends, so they prove the Rust side of the
//! contract without requiring Dagster. The real Dagster round trip is proven by
//! `tests/python/test_bridge_end_to_end.py` and `scripts/e2e_rust_dagster.sh`.

use std::path::PathBuf;

use agentic_bridge::JsonStdioBackend;
use agentic_core::{ExecutionIntent, ExecutionStatus, ObservationCategory, RequestedBy, Subject};
use agentic_ports::ExecutionBackend;

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
