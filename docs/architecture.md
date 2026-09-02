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
  The transport boundary and local production composition are implemented and
  hermetically tested. The local runtime supplies the absolute helper and gh
  configuration paths only after the explicit authenticated publication
  preflight; otherwise publication remains unavailable.
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
  only under the explicit hermetic test transport. Store-backed mutation claims
  and repository-command leases are the wired production authority for this
  contract. They are issued and checked by the local runtime before Git
  mutation; missing or stale authority still fails closed.
  An ordinary candidate-ref push has no server-side CAS for a separate base
  ref: sf observes BaseRef before and after that bounded publication effect
  and treats any movement as stale rather than final. The build performs no
  service installation, channel-state mutation, or network operation.
  Draft PR creation and correction edits likewise have no GitHub
  expected-base/head OID CAS. Their fenced adapter makes final exact
  observations immediately before dispatch, but those observations are not a
  remote lock: refs or PR output may change before GitHub processes the
  command. A non-exact or unavailable post-dispatch result remains a durable
  uncertain effect and is never blindly retried; explicit exact reconciliation
  is required before another mutation.

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
Autonomous selection and merge are unavailable in v1 pending the guarded pilot
and a stronger native execution-profile proof. Docker and Colima are not
required and are never hidden fallbacks or silently installed.

The local guarded repository-command executor is intentionally narrow. Its Go
recipe is `go test ./...`, with no caller flags, CGO, downloads, module updates,
overlays, compiler/linker selection, or output paths. It accepts only a
dependency-free module or a checked-in compatible vendor closure. A separate
dependency-free Node 22 recipe supports only source `node --test`: a bounded
regular `package.json`, no dependency/workspace declarations, `node_modules`,
native addons, or symlinks, and at least one official JavaScript/CJS/MJS test
discovery file. sf resolves code-owned Node paths, stages an authenticated
private Mach-O dylib closure, and applies a Node-specific Seatbelt plus Node
permissions before executing that staged copy. npm, package scripts,
dependency-bearing Node projects, and TypeScript remain credential-free CI or
operator takeover, except the configured `nysa_api_pure_v1` recipe. That
recipe admits one ticket-bound canonical `.test.ts` entrypoint, a minimal
allowlist of `node:test` and `node:assert` builtins, and confined relative
`.js`-to-`.ts` resolution through a
factory-owned loader whose digest is part of the command identity. It uses no
npm, package imports, `node_modules`, network, subprocesses, or writes and is
bounded to 60 seconds; a missing confined implementation is permitted only for
the expected prebuild-red proof. No Docker or Colima prerequisite is introduced.
Autonomous execution remains unavailable.
This is a local guarded baseline, not evidence that ADR 0002's autonomous
capability has changed.

## Change rule

Candidate head, base, operator source, or command-policy changes invalidate
proof result, checks, final review, and approval. Verification-file changes also
invalidate verification intent. No code may add a shortcut around these rules.
