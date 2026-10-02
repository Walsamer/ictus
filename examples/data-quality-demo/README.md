# Data-quality domain demo

The second domain workflow: `ingest -> profile -> check -> publish`. It exists to
show that the generic core, the versioned contracts and the Rust↔Dagster bridge
do not depend on a software-engineering domain.

The same path is used as for software capabilities — only the capability (and
therefore the workflow the backend routes it to) changes:

```text
StateSnapshot(domain=data)
   -> RuleDecisionProvider      -> DecisionProposal(RETRY, data.quality_check)
   -> DefaultPolicyEvaluator    -> PolicyDecision(ALLOW) + ExecutionIntent
   -> ictus_dagster (routing)   -> data_quality_job
   -> ExecutionResult
```

```bash
# Decision only (no Dagster)
./target/debug/ictus decide --approve < examples/state-snapshot.data-quality.json

# Full loop (Dagster): retries the profile step once, then publishes
./target/debug/ictus flow --approve < examples/state-snapshot.data-quality.json
```

Deterministic failure injection is via intent `arguments`:
`fail_ingest`, `fail_until_attempt`, `fail_on_check`, `rows` / `min_rows`.
A failed `check_op` maps to a `VERIFICATION_FAILURE` observation.
