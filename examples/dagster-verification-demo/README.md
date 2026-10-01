# Dagster verification demo

A generic durable-execution demo. The job graph is
`prepare -> execute -> verify -> finalize` and contains no domain semantics.

```bash
./scripts/demo_durable_execution.sh
```

The script proves, with deterministic local runs:

1. **persisted failure** — `fail_hard.yaml` makes every `execute_op` attempt
   fail; the run is recorded as `FAILURE` in the Dagster instance;
2. **retry within one run** — `retry_then_succeed.yaml` fails attempts 1–2 and
   succeeds on attempt 3 through Dagster's `RetryPolicy`;
3. **re-execution** — a second run gets a new durable run id;
4. **inspection** — `dagster run list` shows the persisted history.

Dagster owns step state, retries, re-execution and run persistence. The control
layer owns only the typed decision and the `ExecutionIntent`; it never inspects
or mutates Dagster's step lifecycle.
