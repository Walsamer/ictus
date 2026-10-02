#!/bin/sh
# Deterministic fake execution backend: consume the ExecutionIntent on stdin and
# emit a valid ExecutionResult that references the real intent_id. Used to prove
# the Rust boundary (and the CLI) without requiring Dagster.
payload=$(cat)
intent_id=$(printf '%s' "$payload" | sed -n 's/.*"intent_id":"\([^"]*\)".*/\1/p')
[ -n "$intent_id" ] || intent_id="intent:unknown"
printf '%s\n' "{\"schema_version\":1,\"execution_id\":\"exec-fake-0001\",\"intent_id\":\"$intent_id\",\"status\":\"succeeded\",\"observation\":{\"category\":\"SUCCESS\"},\"evidence\":[{\"kind\":\"test\",\"uri\":\"fixture:fake\",\"note\":\"synthetic\"}],\"started_at\":\"2026-10-01T00:00:00Z\",\"finished_at\":\"2026-10-01T00:00:01Z\"}"
