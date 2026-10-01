#!/bin/sh
# Deterministic fake execution backend: consume the ExecutionIntent on stdin and
# emit a valid ExecutionResult on stdout. Used to prove the Rust boundary
# without requiring Dagster.
cat > /dev/null
printf '%s\n' '{"schema_version":1,"execution_id":"exec-fake-0001","intent_id":"intent:p1","status":"succeeded","observation":{"category":"SUCCESS"},"evidence":[{"kind":"test","uri":"fixture:fake","note":"synthetic"}],"started_at":"2026-10-01T00:00:00Z","finished_at":"2026-10-01T00:00:01Z"}'
