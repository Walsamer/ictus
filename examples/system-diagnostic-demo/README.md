# System-diagnostics domain demo

The third domain workflow: `collect -> inspect -> classify -> report`. It uses
the same versioned contracts, the same Rust policy core and the same bridge as
the software-engineering and data-quality domains.

```text
StateSnapshot(domain=system)
   -> RuleDecisionProvider      -> DecisionProposal(RETRY, system.diagnose)
   -> DefaultPolicyEvaluator    -> PolicyDecision(ALLOW) + ExecutionIntent
   -> ictus_dagster (routing)   -> system_diagnostic_job
   -> ExecutionResult
```

```bash
# Decision only (no Dagster)
./target/debug/ictus decide --approve < examples/state-snapshot.system-diagnostic.json

# Full loop (Dagster): retries the inspect step once, then reports
./target/debug/ictus flow --approve < examples/state-snapshot.system-diagnostic.json
```

Deterministic failure injection via intent `arguments`: `fail_collect`,
`fail_until_attempt`, `fail_classify`, `samples`. A failed `classify_op` maps to
a `VERIFICATION_FAILURE` observation; a failed `inspect_op` to `PROCESS_CRASH`.
