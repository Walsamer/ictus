//! CLI integration tests for the `ictus` binary.
//!
//! The binary previously had no tests. These drive it as a real process, with a
//! deterministic fake backend, so subcommand dispatch, flag parsing, stdin
//! handling, exit codes and stdout/stderr separation are all covered.

use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

use ictus_core::{
    ApprovalBinding, DecisionContext, DecisionEnvelope, DecisionKind, EnvelopeOutcome,
};

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
fn decide_reexecute_within_budget_is_executable() {
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
    assert_eq!(trace["proposal"]["decision"], "REEXECUTE");
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
    assert_eq!(trace["proposal"]["decision"], "REEXECUTE");
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
    assert_eq!(trace["proposal"]["decision"], "REEXECUTE");
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
    assert_eq!(trace["proposal"]["decision"], "REEXECUTE");
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

fn write_approvals(name: &str, contents: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("ictus-{name}-{}.json", std::process::id()));
    std::fs::write(&path, contents).unwrap();
    path
}

#[test]
fn decide_approvals_file_with_token_allows_high_risk() {
    let path = write_approvals("approvals-human", r#"["human"]"#);
    let out = run(
        &["decide", "--approvals", path.to_str().unwrap()],
        &snapshot("WORKER_TIMEOUT", 0, 5, "software.promote"),
        &[],
    );
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(stdout_json(&out)["policy_decision"]["decision"], "ALLOW");
}

#[test]
fn decide_approvals_file_without_token_requires_approval() {
    let path = write_approvals("approvals-empty", r#"[]"#);
    let out = run(
        &["decide", "--approvals", path.to_str().unwrap()],
        &snapshot("WORKER_TIMEOUT", 0, 5, "software.promote"),
        &[],
    );
    let _ = std::fs::remove_file(&path);
    assert_eq!(out.status.code(), Some(3));
    assert_eq!(
        stdout_json(&out)["policy_decision"]["decision"],
        "REQUIRE_APPROVAL"
    );
}

#[test]
fn decide_rejects_a_missing_approvals_file() {
    let out = run(
        &["decide", "--approvals", "/nonexistent/ictus/approvals.json"],
        &snapshot("WORKER_TIMEOUT", 0, 5, "software.promote"),
        &[],
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("cannot load approvals file"));
}

// -- decide --context -------------------------------------------------------
//
// The `--context` mode reads a `DecisionContext` and prints the validated
// `DecisionEnvelope`. Both examples are driven verbatim; the variants patch
// one derived field so the non-executable outcomes are reachable too.

const INITIAL_CONTEXT: &str = include_str!("../../../examples/decision-context.initial.json");
const RECOVERY_CONTEXT: &str = include_str!("../../../examples/decision-context.recovery.json");

fn example(name: &str) -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("examples")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

fn envelope(output: &Output) -> DecisionEnvelope {
    serde_json::from_slice(&output.stdout).expect("stdout is a DecisionEnvelope")
}

fn assert_snapshot_digest(envelope: &DecisionEnvelope) {
    let digest = &envelope.snapshot.digest;
    assert_eq!(digest.len(), 64, "digest {digest:?} is not 64 characters");
    assert!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "digest {digest:?} is not lowercase hex"
    );
}

fn patched(context: &str, patch: impl FnOnce(&mut serde_json::Value)) -> String {
    let mut value: serde_json::Value = serde_json::from_str(context).unwrap();
    patch(&mut value);
    value.to_string()
}

fn set_fact(value: &mut serde_json::Value, key: &str, replacement: serde_json::Value) {
    let facts = value["snapshot"]["facts"].as_array_mut().unwrap();
    let fact = facts
        .iter_mut()
        .find(|fact| fact["key"] == key)
        .unwrap_or_else(|| panic!("no {key} fact"));
    fact["value"] = replacement;
}

#[test]
fn decide_context_initial_example_is_executable() {
    let out = run(
        &[
            "decide",
            "--context",
            "--capabilities",
            &example("capabilities.json"),
            "--expiry-minutes",
            "15",
        ],
        INITIAL_CONTEXT,
        &[],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let printed = envelope(&out);
    // The printed object is a complete, self-validating envelope.
    printed.validate().unwrap();
    assert_eq!(printed.outcome, EnvelopeOutcome::Executable);
    assert!(printed.is_executable());
    assert_eq!(printed.proposal.decision, DecisionKind::ExecuteCapability);
    assert_eq!(printed.subject.id, "task-123");
    assert_eq!(printed.snapshot.revision, 1);
    assert_eq!(printed.validation.context_id, "ctx-initial-0001");
    assert_eq!(printed.expiry, "2026-10-01T00:15:00Z");
    // The digest is Ictus's own over the snapshot as received, not an input.
    let context: DecisionContext = serde_json::from_str(INITIAL_CONTEXT).unwrap();
    assert_eq!(printed.snapshot.digest, context.snapshot.content_digest());
    assert_snapshot_digest(&printed);
    let intent = printed
        .intent
        .clone()
        .expect("an executable envelope has an intent");
    assert_eq!(intent.capability, "demo.verify");
    assert_eq!(intent.target.id, "task-123");
    assert_eq!(
        intent.policy_context["snapshot_digest"],
        serde_json::json!(printed.snapshot.digest)
    );

    // A different lifetime moves the expiry with it.
    let later = run(
        &["decide", "--context", "--expiry-minutes", "90"],
        INITIAL_CONTEXT,
        &[],
    );
    assert_eq!(later.status.code(), Some(0));
    assert_eq!(envelope(&later).expiry, "2026-10-01T01:30:00Z");
}

#[test]
fn decide_context_recovery_example_carries_the_recovery_proposal() {
    let out = run(
        &[
            "decide",
            "--context",
            "--capabilities",
            &example("capabilities.json"),
        ],
        RECOVERY_CONTEXT,
        &[],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let envelope = envelope(&out);
    envelope.validate().unwrap();
    assert_eq!(envelope.outcome, EnvelopeOutcome::Executable);
    // The recovery provider's bounded decision, not the generic rules mapping.
    assert_eq!(envelope.proposal.decision, DecisionKind::Reexecute);
    assert_eq!(envelope.snapshot.revision, 2);
    assert_eq!(envelope.expiry, "2026-10-01T00:20:00Z");
    assert_snapshot_digest(&envelope);
    let intent = envelope
        .intent
        .expect("an executable envelope has an intent");
    assert_eq!(
        intent.arguments["recovery_outcome"],
        serde_json::json!("REEXECUTE")
    );
}

#[test]
fn decide_context_missing_grant_pends_without_an_intent() {
    let input = patched(RECOVERY_CONTEXT, |context| {
        set_fact(
            context,
            "capability.id",
            serde_json::json!("software.promote"),
        );
    });
    let out = run(
        &[
            "decide",
            "--context",
            "--capabilities",
            &example("capabilities.json"),
        ],
        &input,
        &[],
    );
    assert_eq!(out.status.code(), Some(3));
    let envelope = envelope(&out);
    envelope.validate().unwrap();
    assert_eq!(envelope.outcome, EnvelopeOutcome::Pending);
    assert!(!envelope.outcome.is_executable());
    assert!(!envelope.is_executable());
    assert!(envelope.intent.is_none());
    assert_eq!(
        envelope.approvals,
        vec![ApprovalBinding::new("human", false)]
    );
}

#[test]
fn decide_context_bound_grant_file_makes_the_envelope_executable() {
    let input = patched(RECOVERY_CONTEXT, |context| {
        set_fact(
            context,
            "capability.id",
            serde_json::json!("software.promote"),
        );
    });
    let out = run(
        &[
            "decide",
            "--context",
            "--capabilities",
            &example("capabilities.json"),
            "--grants",
            &example("grants.example.json"),
        ],
        &input,
        &[],
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let envelope = envelope(&out);
    envelope.validate().unwrap();
    assert_eq!(envelope.outcome, EnvelopeOutcome::Executable);
    assert_eq!(
        envelope.approvals,
        vec![ApprovalBinding::new("human", true)]
    );
    assert_eq!(envelope.grants.len(), 1);
    assert_eq!(envelope.grants[0].grant_id, "grant-0001");
    assert!(envelope.intent.is_some());
}

#[test]
fn decide_context_unregistered_capability_is_denied_without_an_intent() {
    let input = patched(INITIAL_CONTEXT, |context| {
        set_fact(
            context,
            "capability.id",
            serde_json::json!("not.registered"),
        );
    });
    let out = run(
        &[
            "decide",
            "--context",
            "--capabilities",
            &example("capabilities.json"),
        ],
        &input,
        &[],
    );
    assert_eq!(out.status.code(), Some(3));
    let envelope = envelope(&out);
    envelope.validate().unwrap();
    assert_eq!(envelope.outcome, EnvelopeOutcome::Denied);
    assert!(envelope.intent.is_none());
}

#[test]
fn decide_context_invalid_context_is_reported_as_invalid() {
    // An initial context that binds a recovery is contradictory: the envelope
    // still carries the reason instead of an intent.
    let recovery: serde_json::Value = serde_json::from_str(RECOVERY_CONTEXT).unwrap();
    let input = patched(INITIAL_CONTEXT, |context| {
        context["recovery"] = recovery["recovery"].clone();
    });
    let out = run(
        &[
            "decide",
            "--context",
            "--capabilities",
            &example("capabilities.json"),
        ],
        &input,
        &[],
    );
    assert_eq!(out.status.code(), Some(3));
    let envelope = envelope(&out);
    envelope.validate().unwrap();
    assert_eq!(envelope.outcome, EnvelopeOutcome::Invalid);
    assert!(envelope.intent.is_none());
    assert!(envelope
        .capability_validation
        .as_ref()
        .expect("a capability-bearing envelope carries capability_validation")
        .reason
        .contains("invalid context"));
}

#[test]
fn decide_context_rejects_legacy_token_approvals() {
    let approvals = write_approvals("context-tokens", r#"["human"]"#);
    for args in [
        vec!["decide", "--context", "--approve"],
        vec![
            "decide",
            "--context",
            "--approvals",
            approvals.to_str().unwrap(),
        ],
    ] {
        let out = run(&args, INITIAL_CONTEXT, &[]);
        assert_eq!(out.status.code(), Some(2), "args: {args:?}");
        assert!(out.stdout.is_empty());
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("cannot be combined with --context"),
            "stderr: {stderr}"
        );
        assert!(stderr.contains("USAGE"), "stderr: {stderr}");
    }
    let _ = std::fs::remove_file(&approvals);
}

#[test]
fn decide_context_exhausted_budget_escalates_through_the_envelope() {
    // The decision where a human authorizes something crosses the validated
    // envelope as well: exit 3 (non-executable) with a v2 envelope on stdout, not
    // exit 2 with nothing.
    let input = patched(RECOVERY_CONTEXT, |context| {
        set_fact(context, "attempts.semantic", serde_json::json!(2));
        context["attempts"]["semantic_attempts"] = serde_json::json!(2);
    });
    let out = run(
        &[
            "decide",
            "--context",
            "--capabilities",
            &example("capabilities.json"),
        ],
        &input,
        &[],
    );
    assert_eq!(
        out.status.code(),
        Some(3),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stderr.is_empty(),
        "an escalation is not an error: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let printed = envelope(&out);
    printed.validate().unwrap();
    assert_eq!(printed.proposal.decision, DecisionKind::Escalate);
    assert_eq!(printed.outcome, EnvelopeOutcome::NoExecution);
    assert_eq!(printed.schema_version, 2);
    assert!(printed.capability_validation.is_none());
    assert!(printed.intent.is_none());
    assert_snapshot_digest(&printed);
    // The same binding an execution envelope carries: the snapshot digest Ictus
    // computed itself and the digest of the proposal it validated.
    let context: DecisionContext = serde_json::from_str(&input).unwrap();
    assert_eq!(printed.snapshot.digest, context.snapshot.content_digest());
    assert_eq!(printed.snapshot.revision, context.revision);
    assert_eq!(printed.validation.context_id, context.context_id);
    assert_eq!(printed.validation.proposal_digest.len(), 64);
    assert_eq!(
        printed.validation.capability_version,
        ictus_policy::NO_CAPABILITY_VERSION
    );
    let json = serde_json::to_value(&printed).unwrap();
    assert!(json.get("capability_validation").is_none(), "{json}");
    assert!(json.get("intent").is_none(), "{json}");
}

#[test]
fn decide_context_abort_on_exhaustion_crosses_the_envelope() {
    // The second reproduction of the defect: `abort_on_exhaustion` resolves to
    // ABORT, which also carries no capability.
    let input = patched(RECOVERY_CONTEXT, |context| {
        set_fact(context, "attempts.semantic", serde_json::json!(2));
        context["attempts"]["semantic_attempts"] = serde_json::json!(2);
        context["snapshot"]["constraints"] = serde_json::json!(["abort_on_exhaustion"]);
    });
    let out = run(&["decide", "--context"], &input, &[]);
    assert_eq!(
        out.status.code(),
        Some(3),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let printed = envelope(&out);
    printed.validate().unwrap();
    assert_eq!(printed.proposal.decision, DecisionKind::Abort);
    assert_eq!(printed.outcome, EnvelopeOutcome::NoExecution);
    assert_eq!(printed.schema_version, 2);
    assert!(printed.capability_validation.is_none() && printed.intent.is_none());
}

#[test]
fn decide_context_rejects_an_unreadable_grants_file() {
    let out = run(
        &[
            "decide",
            "--context",
            "--grants",
            "/nonexistent/ictus/grants.json",
        ],
        INITIAL_CONTEXT,
        &[],
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("cannot read grants file"));
}

#[test]
fn decide_context_rejects_invalid_expiry_minutes() {
    for value in ["0", "abc", "-5"] {
        let out = run(
            &["decide", "--context", "--expiry-minutes", value],
            INITIAL_CONTEXT,
            &[],
        );
        assert_eq!(out.status.code(), Some(2), "value: {value}");
        assert!(out.stdout.is_empty());
        assert!(String::from_utf8_lossy(&out.stderr).contains("positive integer"));
    }
}

#[test]
fn decide_context_rejects_malformed_context_json() {
    let out = run(&["decide", "--context"], "{not json", &[]);
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("invalid DecisionContext JSON"));
}

#[test]
fn context_flags_are_rejected_outside_decide() {
    let grants = example("grants.example.json");
    let expiry = run(&["decide", "--expiry-minutes", "5"], INITIAL_CONTEXT, &[]);
    let flow = run(&["flow", "--context"], INITIAL_CONTEXT, &[]);
    let missing_context = run(&["decide", "--grants", &grants], "", &[]);
    assert_eq!(missing_context.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&missing_context.stderr).contains("require --context"));
    for out in [expiry, flow] {
        assert_eq!(out.status.code(), Some(2));
        assert!(out.stdout.is_empty());
        assert!(String::from_utf8_lossy(&out.stderr).contains("USAGE"));
    }
}
