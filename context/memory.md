# Project memory

## Current truth

- `sf` is a new local Go implementation; the legacy factory is not migrated.
- The approved plan and normative state machine live under `docs/plans/`.
- SQLite is the sole application authority; logs and NDJSON are projections.
- DBOS failed its bounded SQLite deadline gate; v1 uses one custom Go engine.
- Guarded mode is first; autonomy waits for the guarded pilot and native proof.
- Docker and Colima are not prerequisites or silent fallbacks.
- On macOS 26.6.2, the native-profile spike rejects `sandbox-exec` for
  autonomous execution: a `setsid`/double-fork child can retain worktree write
  access after its supervisor returns. Guarded/manual remain the trusted
  provider/repository baseline.
- No remote, Nysa mutation, or legacy retirement is currently authorized.
- The trusted-local v1 does not attempt arbitrary npm/process-tree containment;
  full project verification is authoritative in required GitHub CI. The local
  Reviewer still authors verification before the Builder changes product code.
- Canonical GitHub HTTPS transport uses a packaged credential-protocol bridge
  to `gh auth git-credential`; sf neither requests a displayed token nor stores
  one. The production local runtime now composes planning, test authorship,
  building, review, publication, approval, manual observation, and guarded
  merge/reconciliation through durable Store authorities.
- `sf init` now performs native-only canonical repository registration. Stable
  and dev configuration generations are stored in separate SQLite databases,
  and tickets snapshot exact canonical configuration bytes at start.
- When separately authorized, guarded GitHub merges bind one exact
  reviewed/current local base SHA and GitHub base-ref OID. They may mutate only
  under a freshly observed exact strict-status protected rule with no
  pull-request or force-push bypass allowance; otherwise manual
  merge observation remains the safe path.
- `sf` and `sf-dev` are channel-isolated local binaries backed by separate
  SQLite databases. The foreground daemon, friendly CLI, operator pause/take/
  resume controls, test-before-build workflow, draft PR lifecycle, human
  approval/manual merge observation, and guarded automatic merge path are
  implemented and covered by crash/restart and race tests.

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

### 2026-08-29 — Native-profile capability verdict

The bounded Seatbelt (`sandbox-exec`) probe demonstrates selected credential,
Git-control, network, package, and launchd denials, but fails the detached
writer lifecycle probe. It records `autonomous_eligible=false`; no Docker or
Colima fallback was installed or proposed. See ADR 0002.

### 2026-08-29 — Fixture boundary hardening

The deterministic fake GitHub remote reloads durable state under a bounded
portable lock for every operation, persists with unique atomic temporary files,
and requires factory ownership for PR recovery. Provider fixtures sort writes,
reject lexical and symlink worktree escapes, and keep escaped-child probes
observable with a two-second maximum lifetime.

### 2026-08-29 — Durable local project configuration

Project and machine TOML parsing is strict and bounded. `sf init` validates the
local Git root and base branch, creates only owner-private channel paths, and
registers one immutable configuration generation idempotently. SQLite schema
v8 copies that generation's canonical bytes and digest into a ticket when it
enters planning, preventing later repository configuration changes from
changing active-ticket authority.

SQLite schema v9 is also the only authority for a ticket's unguessable
channel-prefixed Git branch. Concurrent allocation replays return the one
durable value, and allocations are foreign-key bound to real tickets.

### 2026-08-29 — GitHub merge base witness hardened

The GitHub boundary rejects contradictory cleanup proof (`drained` plus
`quarantined`), which leaves the Store mutation gate latched. It now cross-binds
all reviewed/current local and GitHub base identifiers to one exact OID,
persists the original base and strict-protection witness before merge, and
passes the original base witness to protected-branch reconciliation. The
durable fake GitHub direct interface validates exact effect claims and merge
authorization; the command shim remains only the remote protocol exercised by
the real client.

### 2026-08-31 — Local v1 implementation gates complete

The local-only factory now runs the approved verification-before-build workflow
through one Go daemon and one SQLite authority per stable/dev channel. Manual
merge observation and guarded human-approved automatic merge both recover
across lost responses, daemon leadership changes, runner fencing, operator
pause/take/resume, and post-merge reconciliation without replaying mutations.
Generic Store transitions cannot manufacture guarded merge observations or
enter guarded merging outside the dedicated approval/control boundaries.

The final local gate includes the complete normal and race suites, `go vet`,
repository/secret/artifact/docs checks, stable/dev release builds, and compiled
foreground-daemon/client smokes. The project still has no remote and has not
mutated Nysa; autonomous merge remains ineligible under the native-profile
verdict and requires a separate future authorization and containment design.
