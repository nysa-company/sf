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
- Sofia separately authorized the SF remote and private `sf-v1-relay-pilot`
  repository. Nysa mutation, stable-channel changes, and legacy retirement
  remain out of scope.
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

### 2026-09-05 — Ten-ticket campaign terminal, stricter confirmation pending

Original campaign ended4done/6cancelled,37 provider attempts/33 immutable
completed results. Exact delivered PRs7/8/9/11 each contain only the declared
tool/test pair and one confirmed create/merge. Main3eb86bb9 passed hosted CI.
No active/quarantined writer, Git/command/admission lease or executing/uncertain
effect remained. Cancelled PR10 stays draft; no source/DB repair or silent
budget extension. The audit found real capacity-two provider overlap only in
done/cancelled pairs, although successful ticket lifetimes overlapped. Do not
call that two successful simultaneous provider workflows. A fresh confirmation
pair is required, reported separately from the four-of-ten result.

PR10 exposed a real GitHub old-base snapshot: its BaseOID stayed at original
61f9b6f while main advanced3eb86bb9. Published refresh now accepts only that
authenticated original base or the independently observed exact new base,
never a third value. No merge/ancestry/CAS authority is relaxed. Plain FakeGH
regression retains the old PR base instead of overwriting it, including lost
Apply recovery; negative third-base case refuses before reservation. Focused
race36372 passed publication45.998s. Final full normal1802 passed; compiled
acceptance234.557s and all static/release gates passed. Final rollout and
fresh live confirmation pair still pending. Details are in
docs/reports/2026-09-05-relay-concurrency-campaign.md.

### 2026-09-05 — First campaign delivery and native contention

Relay28/cf5fbf1 demonstrated two simultaneously released planners and exactly
two machine/project/provider leases. Third admission refused without mutation.
PR8 merged0fe4514343fe4afde2a60e2d3176d382630bf01a, but its protected-ref
proof collided with a sibling provider and became uncertain; the existing
merge recovery intentionally requires a restart. Extend only the same typed
pre-insert bounded wait to protected-ref-fetch, not uncertain merge replay.
Race45003 passed Git2.366s for proof wait and ambiguous response refusal.
The full suite preceded this two-line extension and must be rerun before the
campaign is declared complete. Supported idle dev restart is the recovery path;
no manual live rows or worktree changes. The sibling automatically refreshed
its base, reran Builder, produced generation2/PR9, passed CI and final review.

Relay27/031ba66 passed full normal tests, focused race and static verification.
Its native absence recovery unstranded the second ticket's worktree creation
without DB or checkout edits. The third ticket delivered PR7 through native
proof, CI, independent review and the user-authorized exact-head SF approval;
merge54d3c9514185a9434555ba0b51bdfdac8727d785, durable donev11. This is one
campaign delivery, not two concurrently executing providers or stable-v1 proof.

Two scheduler workers and two ticket slots did not produce model overlap:
the composed shared provider/auth route defaulted to one. The opt-in
SF_CODEX_PROVIDER_CAPACITY=2 retains default1 and rejects all other values;
Store still owns ticket and provider lease admission. A restart also requires
current-leader qualification before runtime activation, even when doctor's
host-capability checks pass. Report this intervention rather than treating an
idle daemon as executing work.

The second Builder completed, but ordinary sibling-writer contention during
repository-command acquisition left an unleased executing intent. Its cancel
therefore entered cancelling and refused drain. Existing startup recovery
retires that exact closed-gate claim before reconciliation/fencing; a new
Store regression proves cancellation can then finish without fabricated command
evidence. No live rows or files are changed manually. Prevention keeps the
same Worker invocation alive for only native Store-proven active contention:
repository commands share one caller/spec deadline; Git commit acquisition has
a two-minute upper bound. Same-claim leases, quarantine and uncertain responses
are never blindly retried. The adjacent commit wait matters because retiring
an unprepared Git intent does not clean the completed Builder's source files;
ordinary pristine scheduler admission cannot re-enter that dirty checkout.
Full serialized Go suite46348 passed, including Git171.973s, Store117.874s,
workflowruntime108.149s and worktreecoord125.525s. Targeted contention/capacity
race91807 passed Git2.307s, Store37.893s, repositoryexec1.585s and
Codex4.601s. The complete static gate (vet, format, repository/secret/artifact/
docs checks and release-build smoke) passed. The next frozen dev rollout and
actual two-provider overlap are still pending; these checks are not delivery
evidence.

### 2026-09-05 — Capacity-two live campaign exposed recovery gaps

The authorized concurrency campaign merged pilot setup-only PR6, applied a
distinct immutable capacity-two/90-minute/$20 dev configuration generation,
and started two tickets on relay26. A third start correctly refused capacity.
This is admission evidence, not two concurrent deliveries. Preserve all prior
pilot records and the registered primary checkout; stable and Nysa remain out
of scope. Campaign IDs and command evidence are in ignored
`.context/concurrency-campaign/PLAN.md`.

The second ticket became uncertain before worktree creation when repository
lease acquisition contended. The new runner distinguishes a current invocation
that never crossed mutation handoff. Historical uncertainty requires a separate
exclusive native absence proof for directory, branch, private base ref, and Git
registration. Additive schema57 marks its lease observation-only: it cannot
launch a child, and restart abandons only the observation while retaining the
uncertain effect. Never infer absence from a missing directory or lease alone.

The first ticket's completed Builder failed a contradictory Reviewer test.
No live test was edited. A durable nonzero postbuild result now produces
`postbuild_command_failed`, preserving evidence without repeating Builder or
publishing a candidate. The supported disposition is cancel and fresh submit,
not an invented verification amendment. The current source passed the complete
serialized suite; final race/static verification and dev rollout follow. This
does not close the concurrency goal or establish stable v1.

### 2026-09-05 — Real delivery and bounded repeatability

The factory delivered Relay PR #3 through test-first verification, independent
review, exact-head human approval, guarded squash merge, and `done`; hosted
main advanced to `add08cff903e8c9bbe46b1a4a97d5c6eabcea53b`. This is one real
delivery, not a general stable-v1 or load-test verdict.

The next ticket exposed stale local-main selection: new worktrees omitted the
hosted merge, and publication correctly refused the mismatch. Preserve that
Inspect Job trial as failed and paused, with its worktree/evidence intact. Do
not rewrite its candidate, restore the database, or count it as a clean trial.
The repair observes the hosted base for new tickets, fetches it under the
existing repository lease into a per-ticket cache ref, and authenticates the
pinned base against SQLite without rewriting the operator's primary branch.

An isolated blocked-provider restart regression also exposed use of the new
daemon leader against old phase evidence. Its repair records both authenticated
leader endpoints atomically with operator recovery and preserves the attempt
window. Freeze only after tests, then run two serial nonoverlapping Relay
tickets without SF code changes between deliveries. No concurrency or live
fault injection; recovery probes use isolated fixtures.

The fresh-base repair passed live registration, but a later Planner asked a
whitespace-ID question and entered `paused`. There is no supported answer
command yet; preserve this trial instead of modifying its immutable result.
An explicit pause of an already semantically paused ticket previously returned
early while its capacity leases remained held. The control path now joins the
runtime, obtains sealed Store drain proof, and releases only the exact paused
version/leader/runner's capacity. It does not infer an answer, change the
lifecycle, or weaken outstanding-effect protection. The missing answer loop
remains a product limitation, not a passing unattended-delivery claim.

Final frozen relay25 (`adb3ef5`) delivered linked-event PR #4 and status-count
PR #5 serially to `done`, with no code/runtime/worktree changes between their
actual starts and completion. All eight provider phases succeeded on first
attempts; required and post-merge CI passed. Actual start-to-done times were
6m33s and 5m33s; the first also had a separately recorded 22-minute pre-start
queue/repair wait. Guarded approvals were applied under Sofia's standing
delegated authorization. The bounded isolated recovery matrix passed afterward.
See `docs/reports/2026-09-05-relay-repeatability.md` for exact OIDs, CI, retained
failed trials, unavailable billing detail, and scope limitations. This closes
the bounded repeatability goal, not a general stable-v1 or load-test gate.

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

### 2026-09-05 — Concurrency campaign, not yet a delivery verdict

The user approved a capacity-two Relay stress campaign, two concurrent real
deliveries, and ten tickets, with fixes for reproduced blockers. Stable/Nysa
remain excluded. Seeded Store/runtime tests (200 cases) passed under race;
full normal suite and repository/secret checks passed at stress baseline
`029424b`. The report is `docs/reports/2026-09-05-concurrency-stress-baseline.md`.

Real private-bare reproduction shows protected-base drift safely refuses every
publication retry but leaves the sibling in publishing. That reproduced the
concurrency-delivery blocker addressed by the unshipped refresh source below,
not a reason to relax base equality. `eb70756` adds exact typed drift
diagnostics and a pure two-parent merge-tree parser with private-bare tests;
it does not enable production refresh execution.

Draft unshipped v56 work in progress adds immutable refresh intent/preparation/
completion records and strict canonical payloads. Do not run a dirty v56 binary
against live databases before the complete authority/recovery flow is tested.
Current source preserves old registration/history while Store completion
CAS-updates the current worktree projection. Refresh reruns
proof/CI/review/approval and retain the exact existing PR when one exists.

No new live tickets, capacity changes, or runtime restarts have occurred in
this campaign. A separate two-line setup PR #6 updates the pilot's capacity
documentation; it is not a factory-delivered ticket. It passed33 Node tests and
required GitHub CI. Auto-review blocked merging exact head27cd365; explicit
approval is pending, no merge occurred. The frozen dev runtime remains relay25-adb3ef5;
pilot hosted main was observed at `92b048aaa4d1c16eba2e7079d0eb078373876326`.
Ten reviewed ticket drafts and the execution checkpoint are ignored local
files under `.context/concurrency-campaign/`. The goal remains active.

### 2026-09-05 — Protected-base refresh source checkpoint

Store/Git refresh source now includes authenticated completion/recovery, fresh
Builder/candidate anchoring, and same-PR Apply integration. Root session 6827
passed in 17.157s (real Store/proof/prepare plus typed Apply). Fresh Builder
old-result rejection was reproduced and repaired; session 34459 passed in
0.967s for no-publication and with-publication cases. Full affected session
55594 passed all six packages. Session59005 passed lost proof/Apply responses,
same-PR fresh generation and actual Worker-to-fresh-Builder admission.
Race session46798 passed Git/runtime/publication but caught a test-only
off-by-one in the eight-proof-reclaim assertion. Corrected bounded-reclaim
and seeded Store/runtime rerun84505 passed (34.039s/1.610s); production was
unchanged. Full serialized suite69767 passed all packages, including Store
114.522s, workflowruntime106.984s and worktreecoord112.143s. Final test-only
extension55469 passed lost-Apply recovery before and after first publication
(15.149s); the latter preserves one existing PR, one push, no edit. Go vet
passed. Compiled isolated guarded/manual delivery, takeover and stable/dev
coexistence acceptance13337 passed225.635s. Final schema-registry-only checks
43631 passed1.081s. Format, repo/secret/artifact/docs checks and isolated
release-build smoke passed. The full suite preceded only the final test
extension and four redundant FK-target registry entries, validated separately.
V56 is ready for a local frozen candidate commit on parent `eb70756`.
Live campaign remains gated on exact setup PR6 approval; no live deployment,
database migration or capacity change has occurred. Isolated passing tests do
not close the live delivery goal.
