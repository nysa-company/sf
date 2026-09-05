# Project memory

## Current truth

- Interactive decision picker now implemented atop optional --head:
  omitted ID / --select / prefix+project interactive path selects exact ID,
  reads ticket.status, requires waiting_approval plus valid full head, displays
  full head/title/channel/project, requires typed approve/reject (not yes),
  dispatches that head through existing decision path. JSON/noninteractive
  must supply full ID, should --head; no silent head replacement. Existing
  explicit full-ID path retained. readSelectionAnswer reads one bounded line
  without discarding subsequent confirmation input (old Scanner read-ahead).
  Normal picker fixture initially failed only because fake stale-head response
  used invalid NextAction(nil); fixed fixture argv. Full CLI race81136 PASS
  46.665s; compiled real-PTY picker48230 PASS4.088s (confirm + cancel; private
  fake socket, no real approval). Focused prior daemon head-binding race passed.
  Full normal/static gate27735 TERMINAL exit0: all packages (Store133.926s,
  workflowruntime119.657s, worktreecoord136.404s), vet, repo/secret/docs/artifact
  checks PASS. Source review found no additional blocking issue. Ready to commit.
  Broad goal still includes stack/provider expansion; do not claim complete.

- Approval-head checkpoint uncommitted on0c23998. CLI approve/reject now
  accept optional --head and transmit reviewed_head only when explicitly set;
  api.ValidReviewedHead requires full lowercase40/64hex. Daemon RawMessage
  distinguishes omitted legacy field from null/empty/invalid; mismatched
  candidate refuses before ApplyOperatorDecision, whose version/fence CAS
  remains unchanged. Existing no-head commands retain compatibility. New
  real Store fixture can stop at waiting_approval. Normal44856 PASS CLI0.549s,
  daemon0.917s, both matching and mismatched/malformed/null heads. Race73574
  TERMINAL exit0 (CLI1.708s,daemon9.846s), docs-smoke PASS; no full suite yet. Docs describe
  explicit binding and old-daemon refusal. Interactive approval picker is now
  implemented and validated in the checkpoint above, keeping JSON/noninteractive
  deterministic and never substituting a changed head.
  No live binary restart or PR approval. Untracked API/test files intentional.

- Mixed-stack fix verification complete:69395 TERMINAL exit0, full normal
  suite (Store137.631s, workflowruntime122.455s, worktreecoord137.215s), vet,
  repo/secret/docs/artifact PASS. Focused config/CLI race73529 also passed.
  All previous disk-full failures passed after cache-only cleanup; no runtime
  safety changes made to obtain green. Investigation DONE: early default
  selection hid Python/Ruby markers behind Go/Node; scan known markers before
  selecting, refuse ambiguity, retain explicit supported recipes. Durable
  lesson: mixed Rails roots often contain package.json; dependency-free Node
  fixture must include a discovered test to reproduce actual acceptance.
  Next CLI checkpoint: add caller-specified reviewed-head binding before
  considering interactive approval selection. daemon.operatorDecision currently
  loads its current candidate, but approve sends only operator, no expected
  head. Store CAS must remain authoritative. Live acceptance PR1 still needs
  human approval; no response has been received and no merge attempted.

- Disk recovery:54904 TERMINAL exit1 (worktreecoord passed132.788s).
  Cache-only cleanup48453 TERMINAL exit0 via explicit GOCACHE go clean -cache;
  freed5.7GiB, df now6.2GiB available. No DB/worktree/runtime files deleted.
  Exact failed Store/runtime cases plus new detection regressions10584 TERMINAL
  exit0: config0.397s, Store2.225s, workflowruntime109.509s. Same source now
  passes after cache cleanup, supporting disk exhaustion as the staging cause.
  Full normal/static gate69395 restarted after this result with6.0GiB free;
  must pass before commit.54904 is terminal failed and must not be polled.

- Mixed-stack detection repair in progress on1904dc2 (not committed).
  Root cause: detectRepositoryCommands returned for go.mod/package.json
  before checking Gemfile/Python markers. Eight mixed-root regressions
  reproduced nil error before fix (Node fixture needs discovered .test.js
  to reach actual acceptance). Fix inspects all known markers before default
  selection and refuses mixed stacks pending explicit supported commands;
  runtime policy unchanged. Narrow red/green verified, explicit-config and
  standalone-Go tests pass. Added symlink-marker rejection. Investigate
  skill root-cause/regression workflow used; no global hooks/analytics altered.
  Focused race73529 TERMINAL exit0, config PASS1.467s, CLI PASS50.328s. Full frozen
  gate54904 still live but has failed: Store two tests report database/disk
  full(13), workflowruntime two gate-staging failures. df confirms444MiB free;
  /private/tmp/sf-gocache3 is5.7GiB disposable build cache, canonical Go env
  verified. Await terminal handle, then clear only that cache with go clean
  before rerunning failed packages/full suite. No live state cleanup.
  normal -p2 was followed conditionally by vet/repo/secret/docs/artifact,
  so those trailing gates have NOT run after this failure. Poll exact
  handles before new tests; no commit until complete. Scope: config/load.go,
  config/load_test.go, first-ticket docs, this memory only. Skill completion
  remains pending broad verification; no claim of Python/Rails execution.

- Installer fix committed67f718f; acceptance/picker docs8fed9d4. Clean
  bundle build42834 PASS (onboarding3, exact8fed9d407fd6e62c677ac95d4d4e0d9ead678382).
  Installed at /Users/sofiagonzalez-2/Projects/sf-clean-install.bTBsFX/installed;
  runtime core/publication validation, version, template PASS. Fresh HOME
  beneath same root init --check against acceptance project PASS, no registration.
  Unsafe /private/tmp acceptance/rejected-install3 refused before creation,
  absence verified. Active acceptance daemon19839 unchanged, no new ticket
  or approval. Full unassisted onboarding/four-stack beta remain unproven.

- Fresh onboarding acceptance ticket SF-543bc4cd3b9a9a6291c2bbc7ca20b3b1
  is waiting_approval v9/r1 (live CLI rechecked), not delivered yet. Created
  2026-09-05T21:48:45Z with2h/$20 ceiling; deadline23:48:45Z. Private repo
  nysa-company/sf-cli-beta-acceptance-20260905 PR1 is draft, exact head
  ee35025e60092cfd25480121537acec4e4f33a1d, required test SUCCESS
  (run33994270868). Human exact-head approval requested; none recorded.
  New run command submitted/started it; short SF-543bc4 lookup worked.
  Watcher63612 stopped via Ctrl-C exit0; ticket remained waiting_approval.
  Acceptance daemon19839 is the current handle (50093 stopped), leader2,
  running unchanged70c5072 onboarding2 from trusted Projects ancestry:
  /Users/sofiagonzalez-2/Projects/sf-onboarding-runtime.2AOeAW/installed.
  HOME remains /private/tmp/sf-onboarding-acceptance.R2QvPm/home; project
  onboarding-counter. Qualification1343 passed both independent roles;
  doctor passed required checks. Existing live dev factory was not changed.
  Installer ancestry repair is uncommitted: regression30656 RED before fix,
  host race8932 PASS bundle/runtimeassets; full frozen gate46189 TERMINAL
  exit0: all normal packages, vet, repo/secret/docs/artifact checks PASS.
  Do not restart active acceptance work to deploy an install-only fix.
  Earlier chronological notes below describe superseded checkpoints, not
  current process state. Four-stack/provider expansion and external beta
  acceptance remain incomplete; this Go ticket is not the whole goal.

- Acceptance activation investigation: qualification43344 TERMINAL exit3;
  both roles qualified_guarded/independent, zero failed probes/model calls,
  but runtime_activation_failed. Confirmed root cause via read-only
  .context/onboarding-runtime-probe: runtimeassets.ResolveCore(installed2/sf-dev)
  returns unsafe executable metadata. secureParents rejects /private/tmp1777;
  installer only checked immediate canonical parent and therefore accepted
  a location runtime cannot use. Do NOT weaken runtime secureParents.
  Next fix: installer prechecks the same parent-chain authority before mkdir,
  regression for private leaf beneath writable ancestor + trusted success;
  docs clarify trusted ancestry. Then install under a fresh private directory
  beneath /Users/sofiagonzalez-2/Projects (not /private/tmp). Existing bundles
  retained as evidence, no delete/overwrite. Investigation skill fully read
  (initial cat truncated middle recovered by lines300-700), root-cause-first
  workflow used; no global setup/hooks/telemetry changes. New acceptance
  daemon50093 stopped cleanly (Ctrl-C, exit0, daemon stopped). No ticket
  submitted, all original live factory state untouched.

- Codex auth follow-up committed70c507281c7493d01528a3eeb945417feb7e7db9.
  Clean bundle/install80258 PASS at acceptance/{bundle2,installed2}, version
  0.1.0-dev.onboarding2. Installed isolated auth18614 confirms both GitHub and
  Codex authenticated; no login/model/credential copy. Project onboarding-counter
  registered only in acceptance HOME (digest10de8b55333abfd7fb2267e9515773ed80b3c51162721c8f73f108bea8211d52).
  Acceptance foreground daemon session50093 LIVE, leader1/socket ready. HOME
  /private/tmp/sf-onboarding-acceptance.R2QvPm/home; GH_CONFIG_DIR references
  /Users/sofiagonzalez-2/.config/gh; CODEX_HOME references existing ~/.codex;
  SF_CODEX_PROVIDER_CAPACITY=2. Qualification command session43344 RUNNING;
  poll before retrying. No ticket submitted. Existing dev DB still shows
  16cancelled/11done/2paused and zero admission leases; untouched.

- GitHub checkpoint committed5d36c8d7f4853e09a994e45232e0f58538d1881d;
  artifact/release94235 PASS. Clean build+verified install45405 PASS at
  /private/tmp/sf-onboarding-acceptance.R2QvPm/{bundle,installed}, version
  0.1.0-dev.onboarding1. Isolated installed auth status proves GitHub active,
  but Codex unauthenticated: Manager ignored CODEX_HOME while composer honors
  it. Follow-up now implements Codex-only validated directory forwarding,
  tests missing/unsafe paths and no cross-provider leakage; not committed.
  Official docs verified via OpenAI Docs skill:
  https://learn.chatgpt.com/docs/auth (file credentials under CODEX_HOME),
  https://learn.chatgpt.com/docs/config-file/config-advanced (default ~/.codex).
  Auth race13234 PASS1.569s. Fresh affected-package + vet/repo/secret/docs/
  artifact gate49361 PASS (auth0.506s,CLI4.123s,codexprovider1.472s,cmd88.469s).
  Committed build and isolated auth
  recheck still required. No model invoked or login performed.

- GitHub configuration checkpoint is uncommitted on4fb00b4. Shared auth
  selector serves both daemon and auth status/login: GH_CONFIG_DIR, then
  XDG_CONFIG_HOME/gh, then HOME/.config/gh. Existing directories require safe
  ownership/mode/canonical identity; missing selection never falls back to a
  different account. Only GitHub receives this path. No tokens forwarded or
  credentials copied. Focused48417 PASS; compiled38155 predates shared helper.
  Baseline75623 PASS all normal packages + vet/repo/secret/docs. Follow-up
  changed auth/CLI/codexprovider/cmd/sf (confirmed reverse imports); final
  uncached four-package + auth race + static gate73877 PASS (auth0.463s,
  CLI4.147s,codexprovider1.488s,cmd88.434s,auth race1.459s; vet/repo/secret/docs).
  Report baseline plus final affected-package evidence, not a frozen full run.
  Clean4fb bundle/install exists at /private/tmp/sf-cli-beta.q2uEO9; new
  committed build/install is still required before the fresh daemon trial.

- Isolated acceptance: /private/tmp/sf-onboarding-acceptance.R2QvPm/project,
  HEAD22e26b50d480839dd4273b2584d9f8b607a0591f, dependency-free Go counter.
  Private remote https://github.com/nysa-company/sf-cli-beta-acceptance-20260905
  created/verified private. Initial CI33993171561 completed SUCCESS at that
  exact SHA. New main protection verified strict required test/admin-enforced,
  force pushes/deletions false. No existing project/protection changed.
  GraphQL recheck confirms main exact strict/admin-enforced, required context
  test, both bypass counts0; parent-inclusive ruleset inventory empty.
  HOME sibling /home mode0700; ticket sibling /ticket.md (CountNonEmpty, six
  criteria, guarded,2h/$20), digest
  ac3974b9d9ada11daa9acd965a5f689917a337dfc8d6871b7823a996620d04e9.
  Installed4fb ticket validate/init --check and seed go test pass. Requested
  implementation is absent. Preserve seed/ticket for trial; use explicit
  project name. No registration/submission/provider work or delivery yet.

- Combined readiness/deadline/Doctor/scheduler-diagnostics checkpoint on parent
  894165a passed full normal suite and vet/repo/secret/artifact/docs/release gate
  43551. Source stayed frozen during that gate (only documentation/audit updates).
  Focused race evidence: readiness52521/33524, Doctor96616, diagnostics80605;
  full CLI/daemon timing race66139. Ready for checkpoint commit.
  Read-only acceptance preflight: live dev PID37274 remains relay32/c6913ab,
  leader38;16 cancelled/11 done/2 paused, no admission/Git/repository-command
  leases, provider rows only completed/failed, effects only confirmed/failed.
  Existing paused tickets were retained untouched. GitHub auth exit-status
  probe passed with all output discarded. No live writes or bundle changes.

- Runtime support source audit is recorded in ignored
  .context/2026-09-05-runtime-support-audit.md. Confirmed Go/narrow Node-only
  execution and Codex-only composition; corrected stale Go-only sentence in
  docs/configuration.md. Primary Python/Bundler references inform next design:
  discovery imports code, interpreter flags/lockfile settings are not OS
  containment. No recipe or provider support widened during frozen gate43551.

- Scheduler diagnostics follow-up: Runtime retains at most64
  latest completed meaningful tick observations (no Err/output/full Ticket),
  skips idle/pool contention, exposes owned snapshots through managedRuntime.
  Daemon status/single-ticket status project channel/ref-scoped diagnostics;
  TryLock prevents waiting on runtime reconfiguration. Historical version/time
  labels make no lifecycle/replay claim. Focused runtime/daemon/CLI race80605
  passed (1.740s/4.839s/1.627s), including retention, copies, concurrent readers,
  live loop failure, scoped socket status and no state mutation. Full45818
  passed normal/static/release but predates these diagnostic changes; no claim
  it verifies the complete final tree. Final frozen normal/static/release
  gate43551 subsequently passed.

- Doctor readiness follow-up: selected --repo (including .)
  runs read-only init recipe preview, separate from host/provider qualification.
  Report scope explicitly disclaims execution/merge approval; failed preview
  prevents guarded eligibility. No registration, code execution or config write.
  New injected and production-preview tests added. Initial45384 failed only
  existing Unix socket fixtures denied by sandbox; host CLI race96616 passed
  (55.600s), including production recipe preview without config/channel writes.
  Full gate45818 predates this Doctor follow-up (do not claim it covers it).

- Status timing: daemon projects immutable submission budget
  into age/deadline/remaining (queue+pause included), without changing lifecycle.
  Terminal countdown omitted; missing budget unavailable; clock skew clamped.
  Pure and real Store/socket timing tests pass (21300). Full CLI/daemon race
  regression66139 passed (45.823s/300.032s). Broad distribution regression32148
  passed all packages plus vet/repo/secret/artifact/docs/release checks; its
  normal suite predates timing edits.
- Readiness safeguard: StartWithCheckedProjectOwnership
  compares the exact checked generation/digest/bytes/path/base inside admission
  before capacity/state writes. Doctor-backed starts use it; Store race test
  proves changed config stays queued without ownership/phase rows, fresh config
  and replay work (52521). Production now injects localruntime.CheckProjectStart:
  static stored verify/review recipe allowlist and macOS check, no repository
  code/TOML execution. Safe typed refusal messages preserve queued tickets.
  Local recipe matrix passes (54763); that run's compiled regex matched no tests.
  Corrected exact compiled onboarding plus new refusal/CLI-run race tests
  passed33524 (localruntime1.741s/daemon20.102s/compiled4.594s).
  This is necessary preflight only; dependency/executable,
  provider and publication readiness are not yet unified onboarding checks.
  Full normal/static/release gate45818 passed; it preceded the scheduler
  diagnostic follow-up. Final combined gate43551 passed.
- Clean-source bundle-dev -> verify -> CLI install -> installed version passed
  at commit894165a (65452), isolated at /private/tmp/sf-bundle-check.L9aqVr.
  No PATH, active bundle, database or service modification.
- Distribution checkpoint in internal/bundle: exact inventory,
  bounded regular files, canonical manifest, permissions/SHA-256, build path
  and Mach-O linked version/commit/channel strings. Real Makefile bundle and
  tamper tests pass (70116). Reproducible trimpath builds omit linker flags;
  helpers now retain version fields through exact read-only --sf-build-info.
  Normal helper invocations unchanged; version/helper package compile tests
  pass. CLI bundle manifest/verify/install is wired; installer verifies before
  and after copying, refuses existing paths, keeps partial failure explicit,
  and does not execute payloads. Real install/local intake tests pass (54566).
  Make bundle/bundle-dev require clean source; tutorial local-bundle.md added.
  Compiled CLI install/overwrite tests pass (19762); failed copies retain
  non-executable partial files. Focused bundle/version race passes (27647)
  after permission and directory-sync tightening. Full normal/static
  regression32148 passed. Clean-source bundle-dev/verify/install/version also
  passed at894165a (65452).
  Linked .str symbol representation is a fail-closed toolchain dependency;
  checksums are not publisher signatures. No live bundles replaced.
- Compiled local onboarding acceptance now passes three repeats (92809): full
  dev helper bundle, controlled private HOME/PATH, cwd-derived registration
  and replay, preview with no writes, stable isolation, template/validation
  without submitted rows, JSON no-prompt refusal, real macOS PTY draft save
  and cancellation. This is local intake evidence, not provider delivery or
  installer acceptance. Compiled ticket selection now also passes three
  repeats (29883): private owner-only socket fixture plus real PTY proves
  duplicate-title disambiguation, exact full-ID dispatch, and cancellation
  without a start request. No real provider/publication claim from this test.
- Run composition checkpoint: `run <file> --project <name> [--watch]`
  reuses daemon submit/start/status, validates returned scope/identity, starts
  queued only, never retries mutation or implicitly resumes blocked/paused
  tickets. Narrow tests pass (51289); full CLI race passes (53118). Real
  socket/Store tests prove same-source replay, start refusal retaining queued
  submission, and lost committed submit/start responses without duplicate
  tickets/start events; focused daemon race passes (46124). Initial
  fake status used the wrong envelope and hung; corrected to the actual nested
  daemon shape and bounded the fixture context. No live runtime changes.
- 2026-09-05 second onboarding checkpoint is committed as 7e1f67e: cwd/project defaults
  and read-only `init --check`, with explicit unsupported-stack explanations.
  Focused CLI/config tests pass (session51388); full normal and static/release
  verification passed (53101).
  Preview never claims provider/publication/executable readiness. Explicit
  profile creation is not previewed yet. No live channel/project changes.
- Ticket facade checkpoint: `ticket template` prints editable
  Markdown; `ticket validate` uses the shared parser locally, reports syntax
  only, bounds files, refuses special/symlink files and omits invalid values
  from diagnostics. `ticket new` collects title/problem/acceptance, previews
  the entire draft and requires yes before an exclusive private-file write.
  No daemon calls. Narrow tests pass (57456/68516), full CLI race passes
  (13034); the later preview-file-race regression passes separately (95937).
  Run/watch composition, compiled interactive acceptance, runtime expansion
  and packaging remain incomplete. Broad setup suite predates these facade
  edits; CLI race covers the new facade production code.
- 2026-09-05 self-serve CLI beta goal is active, separate from the completed
  concurrency campaign. Audience: Claude/Codex users on Go, Node/TS, Python,
  and Rails. Scope/acceptance: `docs/plans/2026-09-05-self-serve-cli-beta.md`.
  First checkpoint adds help, quickstart, titled ticket inventory, project
  filtering, unique hex prefixes and terminal selection. Approval/rejection
  retain explicit IDs pending candidate-bound UI; incomplete inventories refuse
  selection. Full `go test -p 2 -count=1 -timeout=30m ./...`, CLI race, and
  `make verify-static` passed (session6232). No live runtime changes. Setup,
  stack/provider expansion, packaging, and final acceptance remain incomplete.
- 2026-09-05 scoped capacity-two stress/delivery campaign completed: PR15 and
  PR16 both merged through SF and are done, with177.439s successful provider
  overlap at peak2. Final pair10/10 provider calls succeeded, no retries or
  repair/restart intervention; two guarded exact-head approvals. PR16 refreshed
  in place and ran a fresh Builder/CI/final Reviewer. Final live authority
  inventory is zero. See `docs/reports/2026-09-05-relay-concurrency-campaign.md`.
- Current dev runtime is Relay32/c6913abbc34e2b3f26cec608f8f8889e840604ff,
  leader38, qualified pair63/64, capacity2, idle after delivery. The final
  review-cutover regression fails before/passes after; full normal/race/static
  session34163 passed. No stable/Nysa change, no live DB/worktree repair.
- Preserve cohort counts: original10=4done/6cancelled; follow-ups3=1done/2cancel,
  then2=1done/1cancel, final2=2done. Total8/17 is not a reliability benchmark.
  Draft PR1/2/10/14 and failed worktrees remain evidence. PR14 expired and was
  cancelledv14/R2; never approve it. Next priorities: generated-test quality,
  intake/budget visibility, and explicit refresh-plus-further-repair coverage.
  Do not raise live concurrency above2 or call this unattended stable-v1.
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

- 2026-09-05: Relay30 confirmation delivered PR12 (attempts-since-retry),
  merge37bc539209819ed7b7075611c8dd79a8c8d4fc94, localdonev11. Its prepared
  checkpoint safely reconciled at the same claim after sibling provider drain.
  Sibling d6c217 failed two Builder artifacts after editing Reviewer-owned proof;
  supported retry refused dirty state, then cancel preserved evidence. Fresh
  eb3bcc planner returned indeterminate and was cancelled without manual repair.
  No successful-provider-overlap pair yet; original batch remains4done/6cancel.
  Live Relay30/b0fac2f leader36 PTY33181 is idle, zero leases/unresolved effects.
  Diagnostic repair under validation: closed reasons, atomic same-state event,
  no output text/schema change/retry permission. Focused66582+75391 PASS;
  full normal/targetedrace/static session16271 PASS. Race contracts1.467s,
  adapter1.573s, coordinator7.837s, Store10.927s; all static gates PASS.
  Empty-output classification corrected to protocol_invalid and covered by the
  final race pass. Ready for dev-only rollout and fresh draft08/09 pair.

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
