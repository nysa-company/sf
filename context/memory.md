# Project memory

## Current truth

- `sf` is a new local Go implementation; the legacy factory is not migrated.
- The approved plan and normative state machine live under `docs/plans/`.
- SQLite is the sole application authority; logs and NDJSON are projections.
- Guarded mode is first; autonomy waits for the guarded pilot and native proof.
- Docker and Colima are not prerequisites or silent fallbacks.
- No remote, Nysa mutation, or legacy retirement is currently authorized.

## Log

Record durable decisions, reversals, incidents, and repeated pitfalls. Never store secrets, raw customer or financial data, or raw agent transcripts.

### 2026-08-29 — Local v1 implementation begins

Sofia approved the root-authored replacement plan. The local working repository
uses `github.com/nysa-company/sf` provisionally and the branch
`feat/local-factory-v1`. Homebrew Go 1.27.0 was installed because no Go
toolchain existed. The Nysa repository baseline v3 was applied idempotently.
Remote creation/push, Nysa changes, Nysa autonomy, legacy retirement, and
non-development service installation remain separate approval gates.
