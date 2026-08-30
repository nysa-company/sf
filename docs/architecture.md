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
  and has no pull-request or force-push bypass allowances; GitHub then enforces
  an up-to-date base at the server-side merge. Otherwise sf refuses to mutate
  and leaves manual merge observation available. The durable merge intent
  records the base and protection witnesses; reconciliation proves ancestry
  from the original base rather than expecting the protected branch tip to
  remain old.
- Project configuration is parsed strictly, resolved beneath machine policy,
  and stored as immutable canonical bytes plus a digest. A queued ticket copies
  the exact current generation when it first enters planning, so a later file
  edit cannot change that ticket's command or provider authority.
- Each ticket's unguessable channel-prefixed branch is allocated once through
  SQLite and protected by ticket and channel uniqueness. Git does not own a
  second branch-name ledger.
- GitHub publication preserves the exact local candidate SHA through an
  ordinary fast-forward Git push. Canonical
  `https://github.com/<owner>/<repository>.git` remotes use the packaged
  `sf-git-credential` bridge, which delegates only Git credential `get` to
  `gh auth git-credential`; it never displays or stores a token. The optional
  port-443 SSH path uses the packaged `sf-ssh` helper, a pinned GitHub host-key
  asset, and an explicitly supplied SSH agent socket. `make build` packages
  these helpers with `bin/github_known_hosts` and `bin/sf-git-exec`.
  The transport boundary is implemented and hermetically tested; the real
  workflow worker must still supply the absolute helper and gh configuration
  paths before HTTPS publication becomes reachable in production.
  `sf-git-exec` performs an fd-pinned `fchdir` and confirms the
  live worktree `.git` pointer, linked-worktree gitdir, and common directory
  against inherited descriptors immediately before Git exec. Every Git
  mutation additionally requires a caller-supplied, fenced durable mutation
  claim and non-self-attested supervisor lease which holds the repository's
  no-live-writer exclusion from that final authentication through the effect.
  macOS does not accept `/dev/fd/N` as
  `GIT_DIR` (and this host also rejects it as an executable path), so this is a
  trusted-repository boundary rather than a claim to isolate a malicious
  concurrent same-UID process. Production rejects local origins; they exist
  only under the explicit hermetic test transport. The SQLite supervisor is
  the pending production implementation of this claim/lease contract; until
  it is wired, Git mutations fail closed.
  An ordinary candidate-ref push has no server-side CAS for a separate base
  ref: sf observes BaseRef before and after that bounded publication effect
  and treats any movement as stale rather than final. The build performs no
  service installation, channel-state mutation, or network operation.

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

The local guarded repository-command executor is intentionally narrow: its only
Go recipe is `go test ./...`, with no caller flags, CGO, downloads, module
updates, overlays, compiler/linker selection, or output paths. It also records
the exact Nysa-local intents `npm test` and `npm run build`, never arbitrary npm
arguments or CI/install/download commands. The current macOS primitive cannot
prove inherited containment for npm/Node's shell process tree, so those npm
recipes fail before a lease or child process starts and require operator
takeover. Test binaries run under a default-deny profile and cannot fork or
exec; tests that require subprocesses require an explicit operator takeover.
This is a local guarded baseline, not evidence that ADR 0002's autonomous
capability has changed.

## Change rule

Candidate head, base, operator source, or command-policy changes invalidate
proof result, checks, final review, and approval. Verification-file changes also
invalidate verification intent. No code may add a shortcut around these rules.
