#!/bin/sh
# Fake backend that returns non-contract output; the Rust side must reject it.
cat > /dev/null
printf '%s\n' 'not-json'
