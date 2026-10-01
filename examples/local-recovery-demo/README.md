# Local recovery demo

A self-contained demonstration of the generic decision/policy flow **without**
Dagster and **without** the testbed:

```text
StateSnapshot (WORKER_TIMEOUT)
   -> RuleDecisionProvider      -> DecisionProposal(RETRY)
   -> DefaultPolicyEvaluator    -> PolicyDecision(ALLOW) + ExecutionIntent
```

Run it with the CLI, which reads a snapshot on stdin and prints the typed trace:

```bash
cargo build -p agentic-bridge
./target/debug/ac-bridge decide --approve < ../../examples/state-snapshot.worker-timeout.json
```

Each fact the rules provider consults is domain-neutral:

| Fact | Meaning |
| --- | --- |
| `observation.category` | generic outcome token (e.g. `WORKER_TIMEOUT`) |
| `retry.attempt` / `retry.budget` | retry budget, enforced by policy |
| `capability.id` | capability to re-execute |

The same input always produces the same proposal and policy decision; the
provider identity is recorded on the proposal so a human, an LLM or a model can
be substituted later without changing the contracts.
