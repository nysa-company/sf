# CLI onboarding acceptance checkpoint

Status: partial acceptance, not a completed beta or delivered-ticket verdict.

## Executed path

The isolated macOS acceptance used a fresh HOME, a separate dev database,
and a new private dependency-free Go project. It did not change the existing
factory's database, runtime, or worktrees. Authentication referenced existing
operator-owned GitHub and Codex configuration; credentials were not copied.

The installed full helper bundle was built from SF commit
`70c507281c7493d01528a3eeb945417feb7e7db9`. Both independent Codex model
families qualified, and doctor passed its required host/recipe checks.
`run ticket.md --project onboarding-counter --json` submitted and started
ticket `SF-543bc4cd3b9a9a6291c2bbc7ca20b3b1` at
2026-09-05T21:48:45Z. Its deadline is 23:48:45Z and cost ceiling is $20.

The factory produced an implementation and independent tests, published a
draft PR, passed required CI, and reached `waiting_approval`. The observed
PR head is `ee35025e60092cfd25480121537acec4e4f33a1d`; only
`count_nonempty.go` and `count_nonempty_test.go` changed. CI run 33994270868
passed. Human approval remains pending, so merge/reconciliation/done are not
proven by this checkpoint.

Short-ID status lookup (`SF-543bc4`) succeeded. A live status watcher was
interrupted with Ctrl-C and exited successfully; a separate status call
confirmed the ticket remained waiting for approval rather than cancelled.
Numbered picker behavior is separately covered by real-PTY compiled tests,
including duplicate titles, exact dispatch and cancellation without mutation.

## Failures retained in the result

The initial isolated setup required author intervention and code repairs:

- GitHub authentication selection and daemon publication selection disagreed
  about explicit configuration directories. Commit `5d36c8d` aligned them.
- Codex authentication status ignored `CODEX_HOME` while runtime composition
  honored it. Commit `70c5072` aligned the paths without copying credentials.
- The installer accepted a private leaf beneath a shared writable ancestor,
  but runtime activation correctly refused that location. Commit `67f718f`
  makes installation enforce the runtime's existing ancestry requirements
  before creating the destination and authenticate installed helpers afterward.
  The unchanged acceptance binary activated successfully from trusted ancestry.

The installer regression failed before the fix and passed afterward. The
host race tests for bundle/runtime assets passed. The integrated normal suite,
vet, repository, secret, docs and artifact checks passed. A sandbox-only run
could not preserve special-mode fixture bits; the exact host rerun passed.

This is not an unassisted clean onboarding success, and no <=10-minute setup
claim is made. A repeat trial on the corrected installer remains necessary.

## Remaining acceptance

- Exact-head human approval, merge and terminal reconciliation of this ticket.
- Repeat clean installation/onboarding without the repairs above.
- Supported-stack expansion and execution fixtures for dependency-bearing
  Node/TypeScript, Python and Rails; actual Claude execution composition.
- Three unfamiliar external users and the ten-ticket reliability target.
- Public signing/distribution and upgrade experience, where separately
  authorized; no public release has been published by this checkpoint.

Passing the current Go path must not be presented as evidence that the other
stacks/providers or the complete self-serve beta are ready.
