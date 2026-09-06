# Self-serve CLI beta

Status: active implementation goal. Audience approved by the operator:
open-source developers already using Claude/Codex, working in Go,
Node/TypeScript, Python, and Ruby on Rails.

## Outcome

A developer unfamiliar with SF can install a macOS beta, add a supported
trusted project, submit and run a ticket, understand waits, and approve the
exact reviewed head without help from the author or database/worktree surgery.
Support must be explicit for each provider and stack; authentication support
alone is not a qualified execution adapter. Python is limited to the explicit
experimental pinned profile described below. Additional Python dependencies,
Rails, and general dependency-bearing Node/TypeScript remain expansion work,
not current capabilities.

## Sequence

1. CLI foundations: every command explains its purpose; errors link to the
   relevant command; linked quickstart with a complete ticket example.
2. Guided setup: safe cwd/project defaults, read-only compatibility preview,
   authentication/provider/runtime/recipe/publication readiness shown separately.
   Missing capabilities must be reported before provider work starts.
3. Ticket facade: local template/validation and safe submit/start/watch
   composition with replay tests; retain low-level commands and merge approval.
   Add a ticket list and interactive selection by title/state/short ID, so
   operators do not copy long IDs. Missing ticket arguments may open a picker
   only in an interactive terminal; noninteractive and JSON calls stay
   deterministic. Resolve short IDs only when unique within the selected
   channel/project; disambiguate before any mutation and pass the full resolved
   identity to existing authority. Guided ticket creation previews the complete
   spec before submission; Markdown remains supported. Explicitly requested by
   the operator during the first checkpoint.
4. Operational clarity: expose sanitized readiness errors, last progress,
   queue/runtime duration, remaining deadline, and exact corrective actions.
5. Distribution: reproducible checksummed helper bundle, clean-install test,
   explicit upgrades with backup/compatibility checks and channel isolation.
   Public publication and service installation are not implicit in this plan.
6. Stack/provider expansion: evaluate dependency provisioning and isolated
   execution for Node/TS, Python, and Rails, plus actual Claude runtime
   composition. Define narrow supported recipes with integration fixtures;
   never enable arbitrary shell commands as a substitute for this work.
7. Acceptance: fresh CLI-led end-to-end delivery, clean environment tests,
   restart/outage/dirty-worktree/duplicate-request tests, then external beta.

## Gates

- Each checkpoint has tests and truthful docs; broad verification at integrated
  checkpoints before declaring implementation complete.
- Supported combinations have executable integration coverage. Unsupported
  combinations are rejected with an explanation and safe next step.
- Preserve SQLite lifecycle authority, independent verification, exact-head
  human approval, stable/dev isolation, and production capacity two.
- No manual live DB writes or factory worktree repair to make acceptance pass.
- Selection tests cover duplicate titles, ambiguous prefixes, empty lists,
  cancelled selection, changed state between list/action, channel/project
  separation, and piped stdin/JSON. Never choose a mutating target implicitly.
- Target: <=10 minutes from authenticated supported prerequisites to a first
  started ticket, measured separately from clean-machine installation time.
- External beta target: three unfamiliar users without author assistance;
  report this as pending unless actually observed.
- Reliability target: ten preselected small tickets, >=9 delivered without
  code/DB/worktree repair and with intended approvals only; zero leaked writers,
  duplicate mutations, or safety violations. Do not hide failed trials.

### External-cleanup recovery checkpoint

The real acceptance run exposed a persistent GitHub cleanup quarantine with
no recorded process/boot identity. Prevention and diagnostic fixes cannot
retroactively prove its process drained. Recovery must not delete that row
because a PR merged, a process is absent, a timeout elapsed, or tests passed.

The recovery implementation binds an immutable checkpoint to the
exact existing quarantine, a hashed OS machine identity, and the current OS
boot-session UUID while leaving the gate sealed. A later recovery may retire
that exact quarantine only after observing a different boot on the same
machine, under current daemon/Store authority and mutation serialization.
Missing/malformed checkpoint, same boot, different machine, changed quarantine,
stale authority, and ambiguous evidence must refuse. The audit history remains;
uncertain effects still require normal remote reconciliation. Never reboot
the operator's host automatically or claim that a daemon restart proves drain.

The bounded macOS identity reader has isolated native tests. Store checkpoint
and retirement APIs and append-only v58 audit tables are implemented; focused
tests cover replay, same-boot/different-host/stale-authority refusal, mutation
serialization, changed quarantine, tamper, and atomic rollback. Production
Store APIs obtain OS identity themselves, never from caller-supplied evidence.
CLI/daemon composition is implemented as `daemon cleanup prepare` and
`daemon cleanup recover`. Focused owner/channel/caller-evidence, checkpoint
replay, and same-boot daemon-restart tests pass. Targeted race tests, the full
Go suite, vet, repository/secret/docs checks and diff checks pass on the final
source (session 50924, exit 0). The installed onboarding5 bundle now saved the
real acceptance checkpoint through the CLI; exact replay was observed and
same-boot recovery refused with exit 3. The quarantine remains present and
no recovery audit exists yet. Real post-reboot acceptance remains incomplete.

## Estimate and constraints

Planning estimate: 2–3 working days for onboarding on today's supported runtime;
1–2 weeks for the broader four-stack beta, contingent on dependency/runtime
constraints. Re-estimate after compatibility and provider feasibility checks.
This is not an unattended multi-platform stable-v1 delivery promise.

## Current checkpoint

Python preparation and explicit profile setup are committed at `86bf334`;
compiled clean-HOME acceptance and that checkpoint's full integrated validation
pass. The subsequent workflow fixture passes explicit compiled acceptance and
repeat runs. Its fresh full Go, vet, repository/secret/docs/artifact rerun also
passes after clearing verified obsolete build caches. The earlier run failed
two Go workflow fixtures with disk-exhaustion diagnostics and remains recorded.
This is not a released beta or live-provider Python delivery proof.
The prepared-content manifest binds regular-file paths, modes, sizes and
digests. The environment binding adds the selected executable, dependency
snapshot, lock digest and factory-bootstrap digest, with strict canonical
decoding and retained-directory verification. These are integrity checks, not
publisher authentication or permission to execute. Focused race tests pass.
A disposable Python 3.13/pytest sandbox probe passes seven tests, including
project-write, network, subprocess and scratch-symlink escape refusal, with
environment verification before and after execution. This probe does not
exercise Store claims or production cancellation/restart; those separate
internal acceptance results are recorded below. Preparation UX and production
composition are now connected and being validated with the prepared fixture.

The shared materializer/supervisor identity resolver now supports an explicitly
composed private prepared-Python directory, without ambient PATH/HOME fallback.
The typed pytest entrypoint disables plugin auto-discovery and project pytest
configuration; it is a deliberately limited recipe, not arbitrary pytest argv.
The production profile generator passes the disposable OS-backed fixture.
Child-only hard limits cover per-file size, open descriptors and core dumps;
they do not provide an aggregate scratch quota. The internal exact Python recipe
now requires an explicitly composed prepared root; production composition
supplies the explicit channel cache, with a read-only start check of frozen
runtime identity. The internal compiled gate passes an OS-backed pytest fixture and
refuses EOF before release. A retained-descriptor scratch monitor enforces
bounded observations (16 MiB per file, 128 MiB logical/allocated file bytes,
4096 entries); it is not an atomic filesystem quota. Store-backed compiled-gate
acceptance now passes with an explicitly supplied Python 3.13/pytest fixture:
passing test, red assertion, timeout, post-launch cancellation, aggregate
scratch abort, and per-file kernel limit. All six also pass under the race
detector. The factory bootstrap restores SIGXFSZ's default action so CPython
does not turn that limit into an ordinary EFBIG test failure. Abort cases
retire without reusable test results; ordinary outcomes persist exact results.
Separate executor-crash/reopened-Store cases pass three repetitions, including
ambiguous-drain quarantine, competing-writer refusal, and exact recovery replay.
Transient process-group ambiguity stays quarantined until disappearance is
independently observed; the fixture explicitly retries recovery at that point.
These fixtures do not prove provisioning, whole-daemon restart, CLI readiness,
or Python workflow delivery. The new preparation command now builds the pinned
environment without executing project code, and explicit profile registration
checks that environment before committing Store configuration.
Fresh full normal Go tests (with the explicit prepared fixture), vet,
repo-check, secret-scan, docs-smoke and artifact-check passed for the earlier
execution checkpoint. The subsequent no-overwrite publisher also passed its
integrated checks. The current preparation/profile composition is a distinct
checkpoint whose full Go, vet, repository/secret/docs/artifact and release-build
validation now passes. The first broad run exposed disk exhaustion in Go
workflow fixtures; clearing only verified obsolete build caches restored the
expected red-to-green sequence without a production change. Ordinary init remains
network-free; preparation must not run project code, mutate a live ticket, or
replace an executing snapshot.
The internal prepared-cache publisher is now implemented with independently
verified identities and Darwin exclusive rename. Focused repeated race tests
cover competing preparations, exact replay, corrupt/partial destinations,
symlinks, cancellation, private-directory checks and retained handles. Full
normal Go, vet and repository/secret/docs/artifact checks pass for that helper;
the current working checkpoint adds the separate download/setup command.

### Remaining acceptance evidence

Do not substitute one layer's green result for another layer's contract:

| Requirement | Current evidence | Next proof |
| --- | --- | --- |
| Python clean setup | Compiled private-HOME pinned preparation, init, replay and readiness plus full integrated validation pass | Preserve these checks when extending workflow coverage |
| Python bounded execution and recovery | Real prepared runtime through Store/executor, including cancellation and reopened-Store recovery, passes | Preserve fault/recovery checks as workflow coverage expands |
| Python workflow | Compiled CLI/daemon/Factory workflow, repeat runs and integrated validation pass with controlled provider/GitHub processes, real Python red-to-green commands, publication and terminal reconciliation | Live-provider delivery remains separate |
| Fresh CLI delivery | Go acceptance PR 1 was human-approved at `ee35025e60092cfd25480121537acec4e4f33a1d` and merged by SF; merge commit `00860455167278a63b17ad40d5599b74aae5f636` | Terminal reconciliation is blocked by persistent cleanup quarantine; no database reset or expired-budget extension is permitted |
| External adoption | No unfamiliar-user observations | Report the three-user and ten-ticket targets as pending until observed |

The Python composition fixture must use a disposable repository and Store, the
channel's explicit prepared root, production runtime composition, and the
ordinary phase transitions. It must not fabricate evidence rows or replace
the repository executor with a passing stub. Controlled provider/GitHub
adapters are acceptable for this automated composition test but must be
reported as fixtures, not live provider or remote delivery. Assert persisted
verification/build evidence and no residual active command lease. Keep a
separate real-provider delivery acceptance gate.

Run the explicit Python acceptance on macOS ARM64 with
`SF_TEST_PYTHON_CLI_DOWNLOAD=1 make test-python-e2e`. This downloads pinned
public artifacts into disposable private homes and runs both cold setup and
the compiled workflow. The named target refuses a missing opt-in or unsupported
host instead of silently reporting skipped coverage. It uses no live provider
credentials or GitHub mutation; the approval is exclusively a fixture action.

The candidate-bound decision picker now covers approval and rejection too.
Omitted IDs in a terminal (or `--select`) show title/project/state, then fetch
and display the complete reviewed commit. The operator must type the decision;
the daemon refuses a changed head before its existing Store authority checks.
Explicit `--head` supports noninteractive binding, while omitted legacy fields
remain compatible. Older daemons refuse the new field instead of ignoring it.
Full normal Go, vet, repository/secret/docs/artifact checks, full CLI race,
focused daemon race, and compiled real-PTY confirmation/cancellation tests
passed. The PTY test uses a private fake authority socket, not a live approval.
The isolated Go acceptance ticket passed CI and was subsequently approved and
merged. Terminal reconciliation remains pending. See the
[acceptance report](../reports/2026-09-05-cli-onboarding-acceptance.md).

Earlier checkpoint history follows; pending items below describe those earlier
checkpoints, not replacements for the current evidence above.

Started CLI descriptions/examples, contextual help on input errors, and a
discoverable first-ticket guide. Added read-only project-scoped ticket listing,
titles in daemon status projection, interactive numbered selection, and unique
6–31 character hex prefixes for most lifecycle commands. Incomplete or
ambiguous inventories refuse noninteractive dispatch. Approval/rejection retain
explicit full IDs pending candidate-bound confirmation. No schema, provider,
or execution-policy changes in this checkpoint. Full normal Go suite, CLI race,
vet/repository/secret/artifact/docs/release checks passed on 2026-09-05. Compiled
interactive acceptance and the overall beta gates remain pending. The second
checkpoint adds current-directory/project defaults and `init --check`, a
non-mutating existing-configuration/recipe preview. Unsupported Python/Rails
and dependency-bearing Node diagnostics are explicit. CLI/config focused tests
pass; full normal and static/release checks passed for the setup checkpoint. Runtime executable,
provider and publication readiness are deliberately not inferred from the
preview. Ticket template/validation and interactive creation are implemented:
the full draft is previewed, saving requires explicit confirmation, existing
files are never overwritten, and no submission occurs. Focused normal and
full CLI race tests pass. Compiled interactive acceptance is still pending.
Run/watch composition is implemented through existing daemon requests, with
scope/identity validation and no automatic mutation retries or pause recovery.
Real socket/Store tests cover replay, start refusal, and lost committed
submit/start responses; focused daemon race and full CLI race pass. Compiled
interactive onboarding and fresh full delivery still remain to be proven.
Compiled local onboarding passes three repetitions using a full dev helper
bundle and controlled private HOME: init preview/defaults/replay, channel
isolation, local template/validation, and real PTY creation/cancellation.
This does not prove provider delivery, installation or compiled ticket picking.
Compiled selection also passes three repetitions against a private socket
fixture and real PTY: duplicate titles, exact ID dispatch and cancel-without-
start. Next: explicit readiness and distribution work.

## Distribution checkpoint contract

Build a local macOS bundle containing the channel executable, all three
matching Git/SSH helpers, and pinned GitHub known-hosts data. Include version,
commit, architecture, file modes and SHA-256 hashes in a manifest. Verification
must reject missing, extra, modified or symlinked payloads before installation.
Checksums establish bundle integrity, not publisher authenticity; public release
signing/notarization and distribution are separate pending gates.

Installation is explicitly invoked against a user-selected private directory;
it must not replace existing files, modify shell startup files, register a
service, migrate a database, or restart a daemon. Keep each channel's bundle
separate. Test installation under a fresh temporary home, verify helper layout,
run version/help/local onboarding there, and test corrupt payload/refused
overwrite. No public release or installation into the operator's active PATH
is required to exercise this checkpoint.

Implemented and verified at `894165a`: clean-source `bundle-dev`, manifest
verification, exclusive CLI installation, and installed version identity in
a private temporary directory. Full normal regression and static/release
checks passed. Public signing, downloads and in-place upgrades remain pending.

## Operational visibility checkpoint

Status now projects the stored submission deadline and remaining time. Queue
and pause time count; submission age is not presented as execution duration.
Terminal tickets omit the active countdown, and missing budgets are not
guessed. This display does not authorize execution or change lifecycle state.

The readiness checkpoint now protects actual start, not just `init --check`:
production supplies a stored command-recipe/macOS preflight. Store admission
compares the exact checked configuration, refusing a concurrent configuration
advance without reserving capacity or leaving queued state. This is necessary
preflight only, not a complete readiness verdict.
Provider qualification, repository command support, and publication capability
must remain separate checks, not one inferred green flag.
