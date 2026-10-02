//! CLI integration tests for the `ictus` binary.
//!
//! The binary previously had no tests. These drive it as a real process, with a
//! deterministic fake backend, so subcommand dispatch, flag parsing, stdin
//! handling, exit codes and stdout/stderr separation are all covered.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

fn fixture(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

fn run(args: &[&str], stdin: &str, env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ictus"));
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in env {
        command.env(key, value);
    }
    let mut child = command.spawn().expect("spawn ictus");
    // A subcommand may exit before reading stdin (e.g. `--help`, an unknown
    // argument). Writing then fails with BrokenPipe on Linux; that is expected
    // and not a test failure, so the write error is intentionally ignored.
    if let Some(mut pipe) = child.stdin.take() {
        let _ = pipe.write_all(stdin.as_bytes());
    }
    child.wait_with_output().unwrap()
}

fn snapshot(category: &str, attempt: i64, budget: i64, capability: &str) -> String {
    serde_json::json!({
        "schema_version": 1,
        "snapshot_id": "s-cli",
        "timestamp": "2026-10-01T00:00:00Z",
        "domain": "software",
        "subject": { "type": "task", "id": "t1" },
        "facts": [
            { "key": "observation.category", "value": category },
            { "key": "retry.attempt", "value": attempt },
            { "key": "retry.budget", "value": budget },
            { "key": "capability.id", "value": capability }
        ],
        "capabilities": [],
        "constraints": []
    })
    .to_string()
}

fn stdout_json(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).expect("stdout is valid JSON")
}

// -- decide -----------------------------------------------------------------

#[test]
fn decide_retry_within_budget_is_executable() {
    let out = run(
        &["decide", "--approve"],
        &snapshot("WORKER_TIMEOUT", 0, 2, "demo.verify"),
        &[],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let trace = stdout_json(&out);
    assert_eq!(trace["proposal"]["decision"], "RETRY");
    assert_eq!(trace["policy_decision"]["decision"], "ALLOW");
    assert_eq!(trace["execution_intent"]["capability"], "demo.verify");
}

#[test]
fn decide_exhausted_budget_is_not_executable() {
    let out = run(
        &["decide", "--approve"],
        &snapshot("WORKER_TIMEOUT", 2, 2, "demo.verify"),
        &[],
    );
    assert_eq!(out.status.code(), Some(3));
    let trace = stdout_json(&out);
    assert_eq!(trace["proposal"]["decision"], "ESCALATE");
    assert!(trace["execution_intent"].is_null());
}

#[test]
fn decide_unknown_capability_is_denied() {
    let out = run(
        &["decide", "--approve"],
        &snapshot("WORKER_TIMEOUT", 0, 5, "not.registered"),
        &[],
    );
    assert_eq!(out.status.code(), Some(3));
    let trace = stdout_json(&out);
    assert_eq!(trace["policy_decision"]["decision"], "DENY");
    assert!(trace["execution_intent"].is_null());
}

#[test]
fn decide_high_risk_requires_approval_then_allows() {
    let input = snapshot("WORKER_TIMEOUT", 0, 5, "software.promote");
    let pending = run(&["decide"], &input, &[]);
    assert_eq!(pending.status.code(), Some(3));
    assert_eq!(
        stdout_json(&pending)["policy_decision"]["decision"],
        "REQUIRE_APPROVAL"
    );

    let approved = run(&["decide", "--approve"], &input, &[]);
    assert_eq!(approved.status.code(), Some(0));
    assert_eq!(
        stdout_json(&approved)["policy_decision"]["decision"],
        "ALLOW"
    );
}

#[test]
fn decide_uses_a_capabilities_file() {
    let out = run(
        &[
            "decide",
            "--approve",
            "--capabilities",
            &fixture("capabilities.json"),
        ],
        &snapshot("WORKER_TIMEOUT", 0, 5, "custom.cap"),
        &[],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        stdout_json(&out)["execution_intent"]["capability"],
        "custom.cap"
    );
}

#[test]
fn decide_rejects_missing_capabilities_file() {
    let out = run(
        &["decide", "--capabilities", "/nonexistent/caps.json"],
        &snapshot("WORKER_TIMEOUT", 0, 1, "demo.verify"),
        &[],
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("cannot read capabilities file"));
}

#[test]
fn decide_rejects_unknown_flag() {
    let out = run(
        &["decide", "--nope"],
        &snapshot("WORKER_TIMEOUT", 0, 1, "demo.verify"),
        &[],
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("unknown argument"));
}

#[test]
fn decide_rejects_malformed_snapshot() {
    let out = run(&["decide"], "{not json", &[]);
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
}

#[test]
fn decide_data_quality_capability_is_allowed() {
    // The second (non-software) domain goes through the same core.
    let out = run(
        &["decide", "--approve"],
        &snapshot("WORKER_TIMEOUT", 0, 2, "data.quality_check"),
        &[],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let trace = stdout_json(&out);
    assert_eq!(trace["proposal"]["decision"], "RETRY");
    assert_eq!(
        trace["execution_intent"]["capability"],
        "data.quality_check"
    );
    assert_eq!(trace["execution_intent"]["target"]["type"], "task");
}

#[test]
fn decide_system_diagnose_capability_is_allowed() {
    // The third (system diagnostics) domain goes through the same core.
    let out = run(
        &["decide", "--approve"],
        &snapshot("PROCESS_CRASH", 0, 2, "system.diagnose"),
        &[],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let trace = stdout_json(&out);
    assert_eq!(trace["proposal"]["decision"], "RETRY");
    assert_eq!(trace["execution_intent"]["capability"], "system.diagnose");
}

// -- execute ----------------------------------------------------------------

#[test]
fn execute_round_trips_a_successful_result() {
    let intent = serde_json::json!({
        "schema_version": 1,
        "intent_id": "intent:cli",
        "capability": "demo.verify",
        "target": { "type": "task", "id": "t1" },
        "requested_by": { "provider": "rules.v1", "decision_id": "p1" }
    })
    .to_string();
    let out = run(
        &["execute"],
        &intent,
        &[(
            "ICTUS_DAGSTER_BRIDGE_CMD",
            &format!("sh {}", fixture("fake_bridge_success.sh")),
        )],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let result = stdout_json(&out);
    assert_eq!(result["status"], "succeeded");
    assert_eq!(result["intent_id"], "intent:cli");
}

#[test]
fn execute_reports_backend_crash_as_a_transport_error() {
    let intent = serde_json::json!({
        "schema_version": 1,
        "intent_id": "intent:cli",
        "capability": "demo.verify",
        "target": { "type": "task", "id": "t1" },
        "requested_by": { "provider": "rules.v1", "decision_id": "p1" }
    })
    .to_string();
    let out = run(
        &["execute"],
        &intent,
        &[(
            "ICTUS_DAGSTER_BRIDGE_CMD",
            &format!("sh {}", fixture("fake_bridge_failure.sh")),
        )],
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("backend exited"));
}

// -- flow -------------------------------------------------------------------

#[test]
fn flow_runs_snapshot_to_result() {
    let out = run(
        &["flow", "--approve"],
        &snapshot("WORKER_TIMEOUT", 0, 2, "demo.verify"),
        &[(
            "ICTUS_DAGSTER_BRIDGE_CMD",
            &format!("sh {}", fixture("fake_bridge_success.sh")),
        )],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let trace = stdout_json(&out);
    assert_eq!(trace["proposal"]["decision"], "RETRY");
    assert_eq!(trace["execution_result"]["status"], "succeeded");
}

#[test]
fn flow_stops_when_the_policy_decision_is_not_executable() {
    let out = run(
        &["flow", "--approve"],
        &snapshot("WORKER_TIMEOUT", 2, 2, "demo.verify"),
        &[(
            "ICTUS_DAGSTER_BRIDGE_CMD",
            &format!("sh {}", fixture("fake_bridge_success.sh")),
        )],
    );
    assert_eq!(out.status.code(), Some(3));
    let trace = stdout_json(&out);
    assert!(trace.get("execution_result").is_none());
    assert!(String::from_utf8_lossy(&out.stderr).contains("not executable"));
}

// -- dispatch ---------------------------------------------------------------

#[test]
fn help_and_unknown_subcommand() {
    let help = run(&["--help"], "", &[]);
    assert_eq!(help.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&help.stderr).contains("USAGE"));

    let no_args = run(&[], "", &[]);
    assert_eq!(no_args.status.code(), Some(0));

    let unknown = run(&["frobnicate"], "", &[]);
    assert_eq!(unknown.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("unknown subcommand"));
}

#[test]
fn decide_rejects_invalid_capabilities_json() {
    let path = std::env::temp_dir().join(format!("ictus-bad-caps-{}.json", std::process::id()));
    std::fs::write(&path, "{not json").unwrap();
    let out = run(
        &["decide", "--capabilities", path.to_str().unwrap()],
        &snapshot("WORKER_TIMEOUT", 0, 1, "demo.verify"),
        &[],
    );
    let _ = std::fs::remove_file(&path);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("invalid capabilities JSON"));
}

#[test]
fn execute_rejects_malformed_intent() {
    let out = run(&["execute"], "{not json", &[]);
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("invalid ExecutionIntent JSON"));
}

#[test]
fn flow_rejects_malformed_snapshot() {
    let out = run(&["flow"], "{not json", &[]);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("invalid StateSnapshot JSON"));
}
