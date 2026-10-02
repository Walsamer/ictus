//! `ictus` — command-line entry point for the generic
//! `StateSnapshot -> proposal -> policy -> ExecutionIntent -> ExecutionResult`
//! flow, and for the Rust side of the JSON-over-stdio Dagster boundary.
//!
//! Subcommands:
//!
//! - `decide`  — read a `StateSnapshot` JSON on stdin, emit
//!   `{proposal, policy_decision, execution_intent}` JSON on stdout.
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
    Capability, DecisionProposal, ExecutionIntent, PolicyDecision, RiskClass, StateSnapshot,
};
use ictus_policy::{
    AlwaysApproved, DefaultPolicyEvaluator, InMemoryCapabilityRegistry, NeverApproved,
    RuleDecisionProvider,
};
use ictus_ports::{ApprovalProvider, DecisionProvider, ExecutionBackend, PolicyEvaluator};

const USAGE: &str = "\
ictus — typed decision/policy core + Dagster execution bridge

USAGE:
    ictus decide  [--approve | --approvals <file.json>] [--capabilities <file.json>]   # snapshot JSON on stdin
    ictus execute                                                                       # intent JSON on stdin
    ictus flow    [--approve | --approvals <file.json>] [--capabilities <file.json>]   # snapshot JSON on stdin

NOTES:
    --approve          satisfy every approval requirement (fully-trusted path)
    --approvals FILE   satisfy approvals from a JSON token file (fail closed)
";

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

#[derive(Debug, Default)]
struct Flags {
    approve: bool,
    capabilities: Option<String>,
    approvals: Option<String>,
}

fn parse_flags(args: &[String]) -> Result<Flags, String> {
    let mut flags = Flags::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--approve" => flags.approve = true,
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
            other => return Err(format!("unknown argument: {other}")),
        }
        index += 1;
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
    let raw = read_stdin()?;
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

fn run_decide(args: &[String]) -> Result<ExitCode, String> {
    let flags = parse_flags(args)?;
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

fn run_flow(args: &[String]) -> Result<ExitCode, String> {
    let flags = parse_flags(args)?;
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
