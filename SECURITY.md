# Security Policy

## Supported versions

Ictus is early software. Until a `v1.0.0` release exists, security fixes are
made on the `main` branch only.

| Version | Supported |
| --- | --- |
| `main` (pre-release) | ✅ |
| Tagged pre-releases (`0.x`) | ❌ (upgrade to `main`) |

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Preferred: use GitHub's **private vulnerability reporting** for this repository
(the *Security* tab → *Report a vulnerability*). This keeps the report private
until a fix is available.

If private vulnerability reporting is unavailable to you, open a minimal public
issue that asks for a private channel **without including any vulnerable
details, payloads, credentials or reproduction steps**.

There is no dedicated security email address for this project yet. If a private
reporting channel is not available, maintainers should enable GitHub private
vulnerability reporting before announcing a release.

## What to include

- affected version or commit;
- a minimal reproduction;
- impact and any known mitigations;
- whether the issue can be exploited without authentication.

## Scope

Ictus is a library and CLI. Relevant classes of issue include:

- a decision or policy bypass that allows an unauthorized `ExecutionIntent` to
  reach the execution backend;
- secret leakage through logs, results, evidence references or serialized
  contracts;
- unsafe deserialization or schema-validation bypass;
- arbitrary command execution through the execution adapter or bridge.

## Handling secrets

Never commit or paste secrets into issues, pull requests, logs or evidence.
Ictus must never persist credentials in decisions, proposals, intents, results
or generic state snapshots; secrets belong to runtime-specific secret handling.

## Disclosure

We aim to acknowledge a report within a few days and to agree on a coordinated
disclosure timeline with the reporter.
