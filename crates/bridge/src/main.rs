//! `ictus` — command-line entry point for the generic
//! `StateSnapshot -> proposal -> policy -> ExecutionIntent -> ExecutionResult`
//! flow, and for the Rust side of the JSON-over-stdio Dagster boundary.
//!
//! Subcommands:
//!
//! - `decide`  — read a `StateSnapshot` JSON on stdin, emit
//!   `{proposal, policy_decision, execution_intent}` JSON on stdout. With
//!   `--context` it instead reads a `DecisionContext` JSON and emits the
//!   validated `DecisionEnvelope` that `LocalPolicyComposition` produces.
//! - `execute` — read an `ExecutionIntent` JSON on stdin, invoke the execution
//!   backend, emit an `ExecutionResult` JSON on stdout.
//! - `flow`    — `decide`, and when the policy decision is executable,
//!   `execute`; emit the whole trace.
//!
//! Diagnostics always go to stderr so stdout stays a pure JSON contract.
//!
//! Exit codes: `0` success/executable, `3` policy not executable (no intent was
//! sent to the backend), `4` backend executed but reported a non-success
//! result, `2` usage/parse error.

use std::io::Read;
use std::path::Path;
use std::process::ExitCode;

use ictus_adapters::JsonFileApprovalProvider;
use ictus_bridge::JsonStdioBackend;
use ictus_core::{
    ApprovalGrant, Capability, ContextKind, DecisionContext, DecisionProposal, ExecutionIntent,
    PolicyDecision, RiskClass, SnapshotRef, StateSnapshot,
};
use ictus_policy::{
    AlwaysApproved, DefaultPolicyEvaluator, GenericRecoveryProvider, InMemoryCapabilityRegistry,
    LocalPolicyComposition, NeverApproved, RecoveryRequest, RuleDecisionProvider,
    ValidationRequest,
};
use ictus_ports::{ApprovalProvider, DecisionProvider, ExecutionBackend, PolicyEvaluator};

const USAGE: &str = "\
ictus — typed decision/policy core + Dagster execution bridge

USAGE:
    ictus decide  [--approve | --approvals <file.json>] [--capabilities <file.json>]   # snapshot JSON on stdin
    ictus decide  --context [--grants <file.json>] [--capabilities <file.json>]
                  [--expiry-minutes <N>]                                              # DecisionContext JSON on stdin
    ictus execute                                                                     # intent JSON on stdin
    ictus submit                                                                      # intent JSON on stdin, durable receipt out
    ictus flow    [--approve | --approvals <file.json>] [--capabilities <file.json>]  # snapshot JSON on stdin

NOTES:
    --approve            satisfy every approval requirement (fully-trusted path)
    --approvals FILE     satisfy approvals from a JSON token file (fail closed)
    --context            read a DecisionContext and print the validated DecisionEnvelope
    --grants FILE        domain ApprovalGrant records for --context (legacy tokens are refused)
    --expiry-minutes N   envelope lifetime from the snapshot time, N > 0 (default 15)
";

/// Envelope lifetime `--context` uses when `--expiry-minutes` is absent.
const DEFAULT_EXPIRY_MINUTES: u64 = 15;

/// Built-in generic demo capabilities. A real deployment loads these from a
/// registry file (`--capabilities`) rather than hard-coding them.
fn builtin_registry() -> InMemoryCapabilityRegistry {
    InMemoryCapabilityRegistry::new()
        .with(Capability::new("demo.verify", "1", RiskClass::Low).with_idempotent(true))
        .with(Capability::new("software.verify", "1", RiskClass::Low).with_idempotent(true))
        .with(Capability::new("software.implement", "1", RiskClass::Medium).with_side_effects(true))
        .with(Capability::new("data.quality_check", "1", RiskClass::Low).with_idempotent(true))
        .with(Capability::new("system.diagnose", "1", RiskClass::Low).with_idempotent(true))
        .with(
            Capability::new("software.promote", "1", RiskClass::High)
                .with_side_effects(true)
                .with_required_approval("human"),
        )
}

fn load_registry(path: Option<&str>) -> Result<InMemoryCapabilityRegistry, String> {
    match path {
        None => Ok(builtin_registry()),
        Some(path) => {
            let raw = std::fs::read_to_string(path)
                .map_err(|e| format!("cannot read capabilities file '{path}': {e}"))?;
            let capabilities: Vec<Capability> = serde_json::from_str(&raw)
                .map_err(|e| format!("invalid capabilities JSON: {e}"))?;
            Ok(InMemoryCapabilityRegistry::from_capabilities(capabilities))
        }
    }
}

fn read_stdin() -> Result<String, String> {
    let mut buffer = String::new();
    std::io::stdin()
        .read_to_string(&mut buffer)
        .map_err(|e| format!("cannot read stdin: {e}"))?;
    Ok(buffer)
}

/// Read stdin and refuse a document carrying a number the identity-digest
/// protocol cannot represent exactly.
///
/// The snapshot digest is computed over this document
/// (`docs/IDENTITY_DIGEST.md`), so the number domain has to be enforced on the
/// text: a parser that cannot represent an integer exactly would turn it into
/// the nearest double and the digest would silently disagree with a consumer
/// that parsed the same literal as an exact integer.
fn read_digestable_document() -> Result<String, String> {
    let raw = read_stdin()?;
    ictus_core::digest::check_number_domain(&raw).map_err(|error| error.to_string())?;
    Ok(raw)
}

#[derive(Debug, Default)]
struct Flags {
    approve: bool,
    capabilities: Option<String>,
    approvals: Option<String>,
    context: bool,
    grants: Option<String>,
    expiry_minutes: Option<String>,
}

fn parse_flags(args: &[String]) -> Result<Flags, String> {
    let mut flags = Flags::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--approve" => flags.approve = true,
            "--context" => flags.context = true,
            "--capabilities" => {
                index += 1;
                flags.capabilities = Some(
                    args.get(index)
                        .ok_or("--capabilities requires a path")?
                        .clone(),
                );
            }
            "--approvals" => {
                index += 1;
                flags.approvals = Some(
                    args.get(index)
                        .ok_or("--approvals requires a path")?
                        .clone(),
                );
            }
            "--grants" => {
                index += 1;
                flags.grants = Some(args.get(index).ok_or("--grants requires a path")?.clone());
            }
            "--expiry-minutes" => {
                index += 1;
                flags.expiry_minutes = Some(
                    args.get(index)
                        .ok_or("--expiry-minutes requires a number of minutes")?
                        .clone(),
                );
            }
            other => return Err(format!("unknown argument: {other}")),
        }
        index += 1;
    }
    if flags.context {
        // The trusted composition accepts domain grants only; a legacy token
        // provider would be an approval the envelope cannot bind.
        if flags.approve || flags.approvals.is_some() {
            return Err(format!(
                "--approve/--approvals cannot be combined with --context: \
                 the trusted composition accepts domain grants only (use --grants)\n\n{USAGE}"
            ));
        }
    } else if flags.grants.is_some() || flags.expiry_minutes.is_some() {
        return Err(format!(
            "--grants/--expiry-minutes require --context\n\n{USAGE}"
        ));
    }
    Ok(flags)
}

fn approval_provider(flags: &Flags) -> Result<Box<dyn ApprovalProvider>, String> {
    if let Some(path) = &flags.approvals {
        let provider = JsonFileApprovalProvider::load(Path::new(path))
            .map_err(|error| format!("cannot load approvals file '{path}': {error}"))?;
        Ok(Box::new(provider))
    } else if flags.approve {
        Ok(Box::new(AlwaysApproved))
    } else {
        Ok(Box::new(NeverApproved))
    }
}

fn propose_and_evaluate(flags: &Flags) -> Result<(DecisionProposal, PolicyDecision), String> {
    let raw = read_digestable_document()?;
    let snapshot: StateSnapshot =
        serde_json::from_str(&raw).map_err(|e| format!("invalid StateSnapshot JSON: {e}"))?;
    snapshot.validate().map_err(|e| e.to_string())?;

    let registry = load_registry(flags.capabilities.as_deref())?;
    let proposal = RuleDecisionProvider::new()
        .propose(&snapshot)
        .map_err(|e| e.to_string())?;
    let approvals = approval_provider(flags)?;
    let policy_decision = DefaultPolicyEvaluator::new()
        .evaluate(&snapshot, &proposal, &registry, approvals.as_ref())
        .map_err(|e| e.to_string())?;
    Ok((proposal, policy_decision))
}

fn print_json(value: &serde_json::Value) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(value).map_err(|e| e.to_string())?
    );
    Ok(())
}

/// Domain grants supplied for one `--context` request. Ictus persists none.
fn load_grants(path: Option<&str>) -> Result<Vec<ApprovalGrant>, String> {
    match path {
        None => Ok(Vec::new()),
        Some(path) => {
            let raw = std::fs::read_to_string(path)
                .map_err(|e| format!("cannot read grants file '{path}': {e}"))?;
            serde_json::from_str(&raw).map_err(|e| format!("invalid grants JSON: {e}"))
        }
    }
}

/// `--expiry-minutes` is validated before the composition sees it: a non-numeric,
/// zero or negative lifetime is a usage error, never a silent fallback.
fn parse_expiry_minutes(raw: Option<&str>) -> Result<u64, String> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_EXPIRY_MINUTES);
    };
    let minutes: u64 = raw.parse().map_err(|_| {
        format!("--expiry-minutes must be a positive integer number of minutes, got '{raw}'")
    })?;
    if minutes == 0 {
        return Err(format!(
            "--expiry-minutes must be a positive integer number of minutes, got '{raw}'"
        ));
    }
    Ok(minutes)
}

/// The proposal for a context: rules decide an initial authorization, the
/// bounded recovery provider decides a recovery.
fn proposal_for_context(context: &DecisionContext) -> Result<DecisionProposal, String> {
    match context.kind {
        ContextKind::Initial => RuleDecisionProvider::new()
            .propose(&context.snapshot)
            .map_err(|e| e.to_string()),
        ContextKind::Recovery => {
            let decision = GenericRecoveryProvider::new()
                .decide(RecoveryRequest {
                    context,
                    plan: None,
                    plan_approval: None,
                })
                .map_err(|e| e.to_string())?;
            decision
                .to_decision_proposal()
                .map_err(|e| e.to_string())?
                .ok_or_else(|| {
                    "the recovery provider reports NO_RECOVERY_WORK: a success observation \
                     leaves nothing to decide"
                        .to_string()
                })
        }
    }
}

/// Seconds since the Unix epoch for a canonical `YYYY-MM-DDTHH:MM:SSZ`
/// timestamp, or `None` if it is not that exact shape.
fn utc_seconds(timestamp: &str) -> Option<i64> {
    let bytes = timestamp.as_bytes();
    if bytes.len() != 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
    {
        return None;
    }
    let digits = |range: std::ops::Range<usize>| -> Option<i64> {
        let slice = timestamp.get(range)?;
        if !slice.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        slice.parse().ok()
    };
    let year = digits(0..4)?;
    let month = digits(5..7)?;
    let day = digits(8..10)?;
    let hour = digits(11..13)?;
    let minute = digits(14..16)?;
    let second = digits(17..19)?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second)
}

/// Days since the Unix epoch for a proleptic-Gregorian civil date.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_prime = (month + 9) % 12;
    let day_of_year = (153 * month_prime + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Proleptic-Gregorian civil date for days since the Unix epoch.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// `timestamp` advanced by `minutes`, in the same canonical UTC form.
fn expiry_after(timestamp: &str, minutes: u64) -> Result<String, String> {
    let seconds = utc_seconds(timestamp).ok_or_else(|| {
        format!(
            "cannot derive the envelope expiry: snapshot timestamp '{timestamp}' \
             is not a canonical UTC timestamp (YYYY-MM-DDTHH:MM:SSZ)"
        )
    })?;
    let offset = i64::try_from(minutes)
        .ok()
        .and_then(|minutes| minutes.checked_mul(60))
        .and_then(|offset| seconds.checked_add(offset))
        .ok_or_else(|| "--expiry-minutes is out of range".to_string())?;
    let (year, month, day) = civil_from_days(offset.div_euclid(86_400));
    let second_of_day = offset.rem_euclid(86_400);
    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        second_of_day / 3_600,
        second_of_day % 3_600 / 60,
        second_of_day % 60
    ))
}

/// `ictus decide --context`: read a `DecisionContext`, print the envelope the
/// trusted local composition validated. The digest is computed here over the
/// snapshot exactly as received; no producer-supplied digest is trusted.
fn run_decide_context(flags: &Flags) -> Result<ExitCode, String> {
    let raw = read_digestable_document()?;
    let context: DecisionContext =
        serde_json::from_str(&raw).map_err(|e| format!("invalid DecisionContext JSON: {e}"))?;
    let registry = load_registry(flags.capabilities.as_deref())?;
    let grants = load_grants(flags.grants.as_deref())?;
    let expiry = expiry_after(
        &context.snapshot.timestamp,
        parse_expiry_minutes(flags.expiry_minutes.as_deref())?,
    )?;
    let proposal = proposal_for_context(&context)?;
    let snapshot = SnapshotRef::new(
        &context.snapshot.snapshot_id,
        context.revision,
        context
            .snapshot
            .content_digest()
            .map_err(|e| e.to_string())?,
    );
    let trusted = LocalPolicyComposition::default()
        .validate(ValidationRequest {
            context: &context,
            snapshot,
            proposal: &proposal,
            capabilities: &registry,
            grants: &grants,
            expiry: &expiry,
        })
        .map_err(|e| e.to_string())?;
    let executable = trusted.envelope().is_executable();
    print_json(&serde_json::to_value(trusted.envelope()).map_err(|e| e.to_string())?)?;
    Ok(if executable {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(3)
    })
}

fn run_decide(args: &[String]) -> Result<ExitCode, String> {
    let flags = parse_flags(args)?;
    if flags.context {
        return run_decide_context(&flags);
    }
    let (proposal, policy_decision) = propose_and_evaluate(&flags)?;
    let intent = policy_decision.modified_intent.clone();
    let executable = policy_decision.is_executable();
    print_json(&serde_json::json!({
        "schema_version": ictus_core::SCHEMA_VERSION,
        "proposal": proposal,
        "policy_decision": policy_decision,
        "execution_intent": intent,
    }))?;
    Ok(if executable {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(3)
    })
}

fn run_execute() -> Result<ExitCode, String> {
    let raw = read_stdin()?;
    let intent: ExecutionIntent =
        serde_json::from_str(&raw).map_err(|e| format!("invalid ExecutionIntent JSON: {e}"))?;
    let backend = JsonStdioBackend::from_env();
    let result = backend.execute(&intent).map_err(|e| e.to_string())?;
    print_json(&serde_json::to_value(&result).map_err(|e| e.to_string())?)?;
    Ok(if result.is_success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(4)
    })
}

fn run_submit() -> Result<ExitCode, String> {
    // The backend digests this payload to bind the submission identity
    // (`docs/IDENTITY_DIGEST.md`), so a number it cannot carry exactly has to be
    // refused here: forwarding the rounded literal would bind a different
    // digest to the same intent.
    let raw = read_digestable_document()?;
    let intent: ExecutionIntent =
        serde_json::from_str(&raw).map_err(|e| format!("invalid ExecutionIntent JSON: {e}"))?;
    let receipt = JsonStdioBackend::from_env()
        .submit(&intent)
        .map_err(|e| e.to_string())?;
    print_json(&serde_json::to_value(&receipt).map_err(|e| e.to_string())?)?;
    Ok(ExitCode::SUCCESS)
}

fn run_flow(args: &[String]) -> Result<ExitCode, String> {
    let flags = parse_flags(args)?;
    if flags.context {
        return Err(format!("--context is only supported by decide\n\n{USAGE}"));
    }
    let (proposal, policy_decision) = propose_and_evaluate(&flags)?;

    let mut trace = serde_json::json!({
        "schema_version": ictus_core::SCHEMA_VERSION,
        "proposal": proposal,
        "policy_decision": policy_decision,
    });

    if policy_decision.is_executable() {
        let intent = policy_decision
            .modified_intent
            .clone()
            .ok_or("executable policy decision has no intent")?;
        let backend = JsonStdioBackend::from_env();
        let result = backend.execute(&intent).map_err(|e| e.to_string())?;
        trace["execution_intent"] = serde_json::to_value(&intent).map_err(|e| e.to_string())?;
        trace["execution_result"] = serde_json::to_value(&result).map_err(|e| e.to_string())?;
        print_json(&trace)?;
        return Ok(if result.is_success() {
            ExitCode::SUCCESS
        } else {
            ExitCode::from(4)
        });
    }

    eprintln!("policy decision is not executable; no intent was sent to the backend");
    print_json(&trace)?;
    Ok(ExitCode::from(3))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let outcome = match args.first().map(String::as_str) {
        Some("decide") => run_decide(&args[1..]),
        Some("execute") => run_execute(),
        Some("submit") => run_submit(),
        Some("flow") => run_flow(&args[1..]),
        Some("--help") | Some("-h") | None => {
            eprint!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Some(other) => {
            eprintln!("unknown subcommand: {other}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match outcome {
        Ok(code) => code,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{expiry_after, parse_expiry_minutes};

    #[test]
    fn expiry_advances_by_the_requested_minutes() {
        assert_eq!(
            expiry_after("2026-10-01T00:00:00Z", 15).unwrap(),
            "2026-10-01T00:15:00Z"
        );
        // Hour and second carries.
        assert_eq!(
            expiry_after("2026-10-01T00:00:00Z", 90).unwrap(),
            "2026-10-01T01:30:00Z"
        );
        // Month, year and day-of-month boundaries, including a leap day.
        assert_eq!(
            expiry_after("2026-12-31T23:30:00Z", 45).unwrap(),
            "2027-01-01T00:15:00Z"
        );
        assert_eq!(
            expiry_after("2028-02-28T23:00:00Z", 1_440).unwrap(),
            "2028-02-29T23:00:00Z"
        );
        assert_eq!(
            expiry_after("2026-02-28T23:30:00Z", 45).unwrap(),
            "2026-03-01T00:15:00Z"
        );
    }

    #[test]
    fn expiry_rejects_timestamps_the_binding_would_refuse() {
        for invalid in [
            "",
            "2026-10-01T00:00:00",
            "2026-10-01 00:00:00Z",
            "2026-13-01T00:00:00Z",
            "2026-10-32T00:00:00Z",
            "2026-10-01T24:00:00Z",
            "2026-10-01T00:60:00Z",
            "2026-10-01T00:00:60Z",
            "2026-10-01T00:00:00z",
        ] {
            assert!(expiry_after(invalid, 15).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn expiry_minutes_must_be_a_positive_integer() {
        assert_eq!(parse_expiry_minutes(None).unwrap(), 15);
        assert_eq!(parse_expiry_minutes(Some("1")).unwrap(), 1);
        for invalid in ["0", "-1", "1.5", "abc", ""] {
            assert!(
                parse_expiry_minutes(Some(invalid)).is_err(),
                "accepted {invalid:?}"
            );
        }
    }
}
