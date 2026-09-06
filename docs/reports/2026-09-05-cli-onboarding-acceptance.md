# CLI onboarding acceptance checkpoint

Status: partial acceptance. The real change is merged on GitHub, but SF's
terminal `done` reconciliation remains unproven; this is not a completed beta.
The executed-path notes below retain historical observations. The September 6
checkpoint supersedes their earlier approval-pending and open-PR status.

## Post-reboot result: acceptance environment lost

After the operator reboot, `/private/tmp/sf-onboarding-acceptance.R2QvPm`
no longer existed. The installed onboarding5 bundle and private SQLite backup
survived in the SF checkout; `PRAGMA quick_check` on that backup returned `ok`.
The OS boot differs from the recorded checkpoint. The backup still records
one quarantine, one checkpoint, zero recoveries, and ticket `merging` v15/r6.
No backup was installed as a new runtime and no recovery or second merge ran.

This is a failed persistence setup for the real acceptance trial, not a passing
post-reboot recovery. Only the database was preserved before reboot; the
registered repository and linked worktree were not. The merge-proof coordinator
requires a snapshot of that worktree even before reusing a confirmed proof;
ordinary worktree authentication also validates its persisted identity.
Recreating the path or cloning the remote is therefore not a supported way to
complete this ticket. The delivered GitHub change remains recorded separately
from the unproved terminal outcome.

Future live and reboot acceptance must start in a durable owner-only directory
and retain the registered directories in place. Temporary fixtures remain
appropriate for single-process automated tests, not cross-reboot acceptance.
Keep this trial in the reliability denominator; do not reset or relabel it as
successful. A fresh trial must be separately identified and cannot repair this
trial's evidence. Supported lost-checkout recovery is not established.

## Requirement evidence map

Test names identify the scope to preserve. The fixture extension's integrated
verdict is below; fixture success does not prove live delivery.

| Gate | Evidence and limit |
| --- | --- |
| Local install bundle | `internal/bundle` inventory/install tests and the clean installed-binary repeat below. Public publisher authentication/signing is not proved by checksums. |
| Clean local onboarding | `TestCompiledDevOnboardingUsesPrivateHomeAndLocalCommands` uses the full helper bundle, isolated HOME, real registration/replay and no stable-channel writes. It deliberately does not launch a provider. |
| Explicit stack readiness | `TestInitCheckExplainsUnsupportedStacksWithoutRunningThem` refuses unprepared Python, Rails and dependency-bearing Node before writes/execution. Prepared Python has separate compiled acceptance; refusal is not language support. |
| Ticket selection and run/watch | CLI selection tests cover exact resolved identity, ambiguity, stale state and terminal-control sanitization; run tests cover submit/start/watch, replay and uncertain mutation refusal. Real-PTY tests exercise the interactive path separately. |
| Human decision | Decision picker tests bind confirmation to the displayed full head. The user approved the real ticket's exact head, and SF recorded the approval; see September 6 below. |
| Python workflow and faults | Real interpreter execution, Store/executor cancellation/recovery and compiled workflow fixtures pass as described below and in the beta plan. Controlled model/GitHub fixtures are not live-model delivery. |
| Isolation and capacity | Existing channel-coexistence and capacity suites remain required. The Python extension does not change their production settings or replace those tests with its single-ticket fixture. |
| Fresh delivered ticket | GitHub delivery proved by the September 6 merge. Terminal CLI acceptance is incomplete: cleanup quarantine prevents local reconciliation to `done`. |
| External beta | Three unfamiliar users and the ten-ticket reliability target remain unobserved; no success rate or onboarding-time claim. |

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

A later live CLI read after 23:48:45Z reported `waiting_approval` with
`deadline_elapsed=true` and `remaining=0s`. No approval was supplied and no
budget extension or merge was performed. The status clock does not itself
transition the ticket; this is an elapsed-budget approval wait, not evidence
of completed delivery or a terminal cancellation. Do not count this trial as
delivered, and do not reset it to manufacture a passing acceptance result.

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
claim is made.

A subsequent clean build at `8fed9d407fd6e62c677ac95d4d4e0d9ead678382`
produced onboarding3. Installation beneath trusted ancestry passed, including
the new runtime helper checks, installed version identity and ticket template.
The same binary refused a private leaf below `/private/tmp` before creating
the destination. Installed `init --check` passed against the Go fixture with
a fresh HOME and reported providers/runtime/publication as not checked.
This repeat proves local installation and preview, not an independent user's
complete setup or a second provider delivery. The active acceptance daemon
was not replaced during its approval wait.

A further clean build at `87c61724a9b1a3a561dfdd8af4c7e44f1fccb377`
produced onboarding4 after the Python workflow checkpoint. Full helper-bundle
manifest verification and exclusive installation beneath trusted ancestry
passed. The installed binary reported that exact commit and dev channel;
`start --help` documented title selection and short IDs. With a fresh, empty
HOME, installed `init --check` accepted a disposable Go repository while
labeling runtime/provider/publication checks as not checked. Ticket validation
accepted the sample and reported omitted budget fields; Python preparation
preview reported the pinned runtime and explicit download next action.
HOME remained empty after these commands. No provider, daemon, download, PR,
or existing project was touched. This is installation/local-readiness evidence,
not a replacement for the real delivered-ticket gate.

## Remaining acceptance

- Terminal reconciliation of this ticket. Exact-head human approval and the
  GitHub merge are now verified; see the September 6 checkpoint below.
- Complete clean onboarding without author intervention beyond the passing
  repeat installation/local-preview checks above.
- Dependency-bearing Node/TypeScript and Rails support; actual Claude execution
  composition. Python now has a narrow pinned runtime and passing compiled
  setup/workflow fixtures, separately described below; live-model Python
  delivery remains unproven.
- Three unfamiliar external users and the ten-ticket reliability target.
- Public signing/distribution and upgrade experience, where separately
  authorized; no public release has been published by this checkpoint.

Passing the current Go path must not be presented as evidence that the other
stacks/providers or the complete self-serve beta are ready.

## Subsequent isolated Python acceptance

The Python preparation/setup implementation is committed at `86bf334` with
full normal Go, vet, repository/secret/docs/artifact and release-build checks
passing. A subsequent test-only workflow extension has passed its first run,
two repetitions, and the explicit `make test-python-e2e` target. The target
requires macOS ARM64 and `SF_TEST_PYTHON_CLI_DOWNLOAD=1`; it refuses missing
consent or an unsupported host instead of counting skipped tests as coverage.

This acceptance uses the compiled CLI and daemon, pinned public Python/pytest
artifacts, real repository commands, Store evidence and ordinary transitions.
It observes a nonzero pre-build test, a passing post-build command, publication
of the actual Python implementation, exact guarded fixture approval and merge,
and terminal reconciliation. Provider and GitHub processes are controlled
fixtures, with a disposable local bare remote. No real PR was approved or
merged by this test. Existing Go guarded acceptance also passed after the
shared fixture extension; fixture race tests passed. The extension's full run
failed two Go workflow fixtures with explicit disk-exhaustion diagnostics;
all other packages passed, but chained static checks did not run. After
clearing verified obsolete Go build caches (not project or runtime data),
the exact regressions passed, followed by a fresh full Go suite, vet,
repo-check, secret-scan, docs-smoke and artifact-check (terminal exit 0).
These results do not complete the real
Go acceptance ticket above or establish unattended live-model Python delivery.

## September 6 real merge checkpoint

The user explicitly approved PR #1 at
`ee35025e60092cfd25480121537acec4e4f33a1d`. The installed onboarding4 CLI
recorded that exact approval through SF. After normal provider qualification
activated the restarted runtime, SF marked the PR ready and merged it.
GitHub reports `MERGED`, at `2026-09-06T05:03:08Z`, with merge commit
`00860455167278a63b17ad40d5599b74aae5f636`. No direct `gh pr merge` or database
mutation was used to substitute for SF's workflow.

This is a delivered GitHub change, but not yet a passing terminal acceptance.
SF remains in `merging`; its merge and child protected-ref-fetch effects are
uncertain. Restart recovery advanced the ticket to version 13 / runner 4.
The restarted daemon required fresh leader-bound provider qualification.
Ownerless sockets were preserved under inode-specific names after process
and listener checks, rather than deleted. These are author interventions,
not evidence of unattended self-service.

A read-only diagnostic using the production Git runner, authenticated helper
and an authority stub that always refuses acquisition successfully reached
the mutation boundary. Checkout authentication and protected remote lookup
therefore passed in that probe; no fetch was permitted by the stub. The
subsequent local proof failure remains under investigation. Do not count the
ticket as `done`, or this run as intervention-free, until Store confirms it.

### Diagnosed launch and shutdown failures

The protected-ref proof succeeded in a disposable checkout with both a test
lease and the real launch recorder backed by a copied Store. The same binary
failed under the daemon's manually scrubbed environment without `TMPDIR`,
then passed when the trusted macOS per-user temp directory was supplied.
The GitHub runner's snapshot can be placed beneath sticky `/private/tmp`,
but Git's executable-parent validation rejects that shared writable ancestor.
This is a composition/readiness mismatch, not a reason to relax validation.

Stopping the retrying daemon during a GitHub check then recorded persistent
`cleanup_uncertain` quarantine. A regression reproduced that a canceled
request context was passed into cleanup, preventing a drain proof even when
the runner could supply one. A bounded independent cleanup-context fix passes
focused GitHub and real-process runner suites. Full serialized Go tests, vet,
repository checks, secret scan, docs smoke and diff checks passed (session
43135, terminal exit 0).
The original command is still canceled, and uncertain cleanup still refuses.

The existing quarantine is preserved. The installed acceptance binary has no
supported command to clear this latch. New source adds explicit host-checkpoint
and post-reboot recovery commands through Store. Focused CLI/daemon/Store tests,
targeted race tests, full Go tests, vet, repository/secret/docs checks and diff
checks pass on the final source (session 50924, exit 0). Reboot and host-inspection
requirements return operator-action exit 3; rejected recovery evidence returns
policy exit 5, with regression coverage. Real recovery is still pending.
The generic ticket recovery command is not a substitute for that host proof.
The ticket's nonterminal state remains an acceptance blocker; passing
prevention or simulated-reboot tests does not resolve the live evidence.

### Validated recovery bundle and live checkpoint

Recovery source is committed at `3f36eb799cf6a4f3a40941ab26ddac5b3cfa82e8`.
The private onboarding5 macOS ARM64 bundle passed manifest creation,
verification, exclusive installation, installed identity and cleanup-help
checks. No release, PATH change, or background-service installation occurred.

The old isolated acceptance daemon was identified by its exact executable and
open database/socket descriptors, then stopped gracefully (exit 0). The new
installed dev daemon started normally against the same acceptance HOME,
including ordinary schema migration and runner recovery. Stable and existing
project runtimes were not changed.

Installed `daemon cleanup prepare --json` saved one checkpoint. Repeating it
returned `observed=true`, `attempted=false`. Installed `daemon cleanup recover`
on the same boot returned `host_reboot_required`, exit 3, with no mutation.
A read-only database check confirmed one quarantine, one checkpoint, zero
recovery audits, and the ticket still `merging` at version 15 / runner 6.
No effect was confirmed by these commands. This proves the real checkpoint
and refusal paths, not post-reboot recovery or terminal delivery.

The remaining real prerequisite is a manual reboot of the original host after
saving other work. After restarting the same isolated channel/HOME, explicit
cleanup recovery may inspect the new boot; normal exact-merge reconciliation
must still finish independently. A daemon restart alone is insufficient.
