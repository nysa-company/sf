# Architecture

The approved normative design is in
[`plans/2026-08-29-software-factory-v1-rewrite.md`](plans/2026-08-29-software-factory-v1-rewrite.md).

## Authority

- One channel-specific local daemon owns mutations.
- SQLite is the only application-state authority.
- The JSON state machine in `docs/plans` is the transition authority.
- NDJSON events and logs are readable projections, never recovery inputs.
- Git and GitHub mutations cross narrow fenced interfaces and reconcile remote
  truth before any retry.
- Project configuration is parsed strictly, resolved beneath machine policy,
  and stored as immutable canonical bytes plus a digest. A queued ticket copies
  the exact current generation when it first enters planning, so a later file
  edit cannot change that ticket's command or provider authority.
- Each ticket's unguessable channel-prefixed branch is allocated once through
  SQLite and protected by ticket and channel uniqueness. Git does not own a
  second branch-name ledger.

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
