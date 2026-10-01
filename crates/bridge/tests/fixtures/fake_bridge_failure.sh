#!/bin/sh
# Fake backend that fails: proves fail-closed behaviour at the Rust boundary.
cat > /dev/null
printf '%s\n' 'simulated backend crash' >&2
exit 7
