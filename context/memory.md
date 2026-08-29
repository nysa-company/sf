# Project memory

## Current truth

- `sf` is a new local Go implementation; the legacy factory is not migrated.
- The approved plan and normative state machine live under `docs/plans/`.
- SQLite is the sole application authority; logs and NDJSON are projections.
- DBOS failed its bounded SQLite deadline gate; v1 uses one custom Go engine.
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

### 2026-08-29 — DBOS rejected at the declared proof gate

The isolated DBOS Go v1.2.0 spike proved several recovery semantics but a
competing SQLite operation with a 40 ms context deadline waited about 1.03
seconds for the configured busy timeout. This violates the bounded-operation
contract. Per the approved one-day rule, DBOS is rejected for v1 and will not
be a production dependency or receive another proof attempt. The custom Go
engine described in `docs/decisions/0001-workflow-engine.md` is the only runtime
path.
