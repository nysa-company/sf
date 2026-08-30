# Architecture

The approved normative design is in
[`plans/2026-08-29-software-factory-v1-rewrite.md`](plans/2026-08-29-software-factory-v1-rewrite.md).

## Authority

- One channel-specific local daemon owns mutations.
- SQLite is the only application-state authority.
- The JSON state machine in `docs/plans` is the transition authority.
- NDJSON events and logs are readable projections, never recovery inputs.
- Git and GitHub mutations cross narrow fenced interfaces and reconcile remote
  truth before any retry. Guarded merge binds the reviewed local base SHA and
  GitHub base-ref OID to one exact witness. Because `gh pr merge` exposes only
  an expected-head CAS, sf permits its managed guarded merge only when a
  freshly observed exact protected-branch rule requires strict status checks
  and has no pull-request bypass allowances; GitHub then enforces an
  up-to-date base at the server-side merge. Otherwise sf refuses to mutate and
  leaves manual merge observation available. The durable merge intent records
  the base and protection witnesses; reconciliation proves ancestry from the
  original base rather than expecting the protected branch tip to remain old.

The DBOS proof gate failed its bounded SQLite contention requirement. v1 uses
one custom Go state engine over the application schema; DBOS is retained only
as a reproducible rejected spike. See
[`decisions/0001-workflow-engine.md`](decisions/0001-workflow-engine.md).

## Boundaries

The CLI is a thin client over an owner-only Unix socket, except direct local
setup and diagnostic commands. Each ticket has one channel-prefixed branch and
worktree. Planner, Builder, and Reviewer are logical roles; Reviewer runs once
before build to author verification and again fresh after the candidate and CI
checks exist.

Manual and guarded modes use an explicit trusted-provider/repository baseline.
Autonomous mode additionally requires the exact provider and repository-command
executor to pass an OS-enforced macOS profile. Docker and Colima are deferred,
not hidden fallbacks.

## Change rule

Candidate head, base, operator source, or command-policy changes invalidate
proof result, checks, final review, and approval. Verification-file changes also
invalidate verification intent. No code may add a shortcut around these rules.
