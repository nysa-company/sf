# Project memory

## Current truth

- User authorized consolidating the validated multiCLI tree and Codex beta
  through sf PR #2 into main. Preparing one commit and updating the PR;
  main's ruleset requires one approving review and squash merge. Do not
  bypass protection or replace a live daemon. Earlier local-only delivery
  notes below are historical; consult GitHub for the current merge state.

- MultiCLI implementation/validation goal completed with declared capability
  limits; receipt docs/plans/2026-09-07-multi-cli-completion-audit.md.
  Full87903 TERMINAL PASS exit0 (workflowruntime113.036s final), live28572
  PASS752.36s both Done/restart/separate approvals/base refresh, vet and
  repo/docs/diff/secret gates PASS. All test handles terminal, no paid calls
  active. Candidate10576 ready locally, not installed. MultiCLI source remains
  uncommitted and NOT in Codex beta PR #2. Cursor API-error automatic retry
  lacks authoritative terminal protocol; Grok Low unqualified. These remain
  explicit unavailable capabilities; no fallback/containment/cost claims added.

- Local development candidate10576 built successfully at
  .context/multicli-candidate.9H4KK6/bin (complete four binaries/known hosts/LICENSE).
  Version0.1.0-dev.multicli-local, base6f12229-dirty, channeldev. Version JSON
  and provider/config help pass. Not a release bundle or daemon replacement.
  Baseline87903 remains active; supervisor74.006s and Store cached PASS,
  workflow packages pending. Original default-cache build was sandbox-refused;
  private GOCACHE rerun succeeded with no source change.

- Completion audit drafted at docs/plans/2026-09-07-multi-cli-completion-audit.md.
  Requirement/source/test mapping inspected; Cursor automatic API retry is
  explicitly unavailable because pinned CLI prints unstructured error/exit
  (docs/cli.md), not signed terminal evidence. Grok Low unqualified; neither
  bypassed. Current full vet PASS; repo-check/docs-smoke/diff-check PASS after
  audit document. Baseline87903 still running (cmd/sf106.491s,
  daemon22.745s, GitHub64.534s passed; supervisor onward pending).
  Poll87903, not beta91548 or live28572, which are terminal PASS.

- Full multiCLI baseline87903 NOW RUNNING after passing live28572:
  GOCACHE=/private/tmp/sf-multi-cli-go-cache go test -p 1 ./...
  native host, all paid/live opt-ins off. Poll87903 exact handle; no paid
  runs remain active. Next after terminal: completion audit against full plan,
  current source/coverage and declared unavailable Grok/usage capabilities.

- Live Cursor concurrency28572 TERMINAL PASS752.36s/package752.998s,
  exit0: real Cursor Luna Low Builder/Planner + Claude Sonnet5 Reviewer,
  two tickets, restart/requalification, separate approvals, fresh sibling
  Builder/review after exact protected-base refresh, both Done. Disposable
  native daemon/Store/local Git/FakeGH only, not hosted Relay. Initial roles
  and fresh Builder all completed without failed attempts in observed DB.
  Test cleanup removed disposable DB; do not query it as live. No duplicate
  trial needed. Full current multiCLI baseline refresh is the next gate;
  goal remains active pending requirement-by-requirement completion audit.

- Isolated exact beta6f12229 full suite91548 TERMINAL PASS exit0:
  cmd/sf128.018s, Git258.021s, GitHub87.700s, supervisor81.991s,
  publication136.905s, Store182.126s, workflowruntime163.628s,
  worktreecoord171.418s. PR #2 remains exact beta, dirty multiCLI excluded.
  Serialized orchestrator3327 completed and launched live multiCLI28572:
  GOCACHE=/private/tmp/sf-multi-cli-go-cache SF_TEST_LIVE_CURSOR_CONCURRENT=1
  go test -tags sf_e2e -p 1 -count=1 -timeout 25m ./cmd/sf
  -run '^TestCompiledLiveCursorConcurrentTicketsRestartBeforeSeparateApprovals$' -v
  Native host, authorized paid models, disposable Git/GitHub only. Poll28572;
  no duplicate launch on silence. No passing live result yet. Current-tree
  repo/secret checks83523 PASS after assertion correction.

- MultiCLI assertion fix validated: regression65374 PASS0.865s, race count3
  87126 PASS2.159s, exact Store/Coordinator no-process authority57908 PASS
  (0.910s/0.750s). Production unchanged. Beta baseline91548 remains live,
  latest ghrunner12.875s; poll exact handle to completion before launching
  paid concurrency to avoid host contention. Native expiry-only Claude check
  reports462 minutes, sufficient. Next: original bounded live Cursor concurrent
  test command below, then full current-tree validation; goal not complete.

- Published exact committed Codex beta6f12229 as SF PR #2:
  https://github.com/nysa-company/sf/pull/2, branch feat/codex-beta-testing.
  User requested immediate publication without review; dirty multiCLI excluded.
  Isolated beta baseline91548 is still running; no fresh green claim in PR.
  MultiCLI goal remains active. Live55528 TERMINAL FAIL476.08s:
  attempt-history assertion rejected failed/invocation_failed. Store's sole
  writer proves no process launched, so this is a test-only false rejection,
  not an uncertain execution. Regression80021 RED,65374 GREEN0.865s after
  permitting that exact failed outcome; indeterminate/cancelled/unknown and
  quarantined outcomes still reject. No production retry-policy change.
  Original live scenario rerun remains pending; do not poll55528 as active.

- Baseline86750 TERMINAL PASS, workflowruntime171.412s completed last;
  paid opt-ins off, some unchanged packages cached. Current full vet/static/
  secret checks passed. Live Cursor concurrency55528 NOW RUNNING:
  GOCACHE=/private/tmp/sf-multi-cli-go-cache SF_TEST_LIVE_CURSOR_CONCURRENT=1
  go test -tags sf_e2e -p 1 -count=1 -timeout 25m ./cmd/sf
  -run '^TestCompiledLiveCursorConcurrentTicketsRestartBeforeSeparateApprovals$' -v
  Native host, authorized paid models, disposable local Git/GitHub, no hosted
  Relay/live channel mutation. Poll55528; do not duplicate on silent output.

- Claude lifetime blocker CLEARED without user action: one authorized native
  Sonnet5 tool-disabled/safe-mode/no-session request in disposable temp dir
  57959 PASS, response discarded. Native CLI refreshed its own browser login;
  expiry-only Keychain check now479 minutes (>=46). No SF credential policy
  bypass, token printing or Cursor charge. Wait for baseline86750 terminal,
  then run bounded live Cursor concurrent restart acceptance. Do not ask user
  to renew based on the superseded four-minute observation below.

- Baseline86750 still RUNNING, now Store159.858s and supervisor79.492s
  PASS; final workflow/worktree packages pending. Keep polling same handle.
  User asked which stable branch to test: clarified local tested beta6f12229
  on feat/local-factory-v1, not dirty multi-provider source. Fresh GitHub read:
  public sf main remains ff2f9e2 (Sept2), releases empty. Local tracking branch
  origin/feat/local-factory-v1 is8dab8f6; localHEAD ahead71. Do not imply the
  local beta commit has been published or can be fetched from a fresh clone.

- Current baseline86750 remains RUNNING (last output codexprovider/config/
  contracts/cursorprovider passed; cmd/sf91.465s passed). Poll this exact
  handle, do not duplicate. Full vet independent exit0; repo-check and
  secret-scan84871 PASS (644 commits +90.55MB, no leaks); diff-check PASS.
  Native Claude auth status is logged in, but expiry-only metadata check found
  only four minutes remaining, insufficient for 46-minute launch window. The
  sandbox status falsely showed logged out; native host result is authoritative.
  No paid run launched. Live concurrent restart remains pending renewed lifetime.

- Joined synthetic process/Store retry regression now PASS: real supervised
  rejection receipt -> Store finish -> close/reopen -> durable backoff -> same
  binding attempt 2 -> real Git checkpoint inspection -> second process and
  persisted rejection. All five receipt scenarios race count3 PASS (63066,
  33.254s). Initial joined fixture admission failure was missing exact input
  fence/provider/auth fields, fixed in test only. Native supervisor full race
  29469 also PASS79.264s before joined fixture addition. No paid calls. This
  still uses synthetic CLI/credentials/qualification/test gate, not a hosted
  vendor rejection or full Coordinator acceptance. Claude renewed-auth/live
  concurrency restart acceptance remains unverified.

- New nonpaid native-process receipt coverage: private runWithCLISecrets seam
  (public Run still fixed lookupCLISecret; no caller/config/env override).
  Synthetic CLI + credential + existing test gate/recorder; actual supervisor
  Run capture, completed process/drain, signed receipt exact/single-use.
  Positive and partial/false-success/stderr-only cases race41349 PASS5.960s
  count3. Early fixture failures corrected missing recorder and noncanonical
  digest/identity fields; no production authority relaxed. This is NOT installed
  Claude nor production gate nor Store retry integration. Full supervisor race
  active29469.67096 baseline below predates this small refactor/new tests.
  No paid calls; Claude renewal/live restart and combined Store retry still open.

- Full current-tree67096 PASS exit0: cmd/sf91.778s,supervisor65.283s,
  providercoord37.695s,publication91.274s,Store127.843s,
  workflowruntime109.769s,workflowworker1.453s,worktreecoord132.015s.
  Some unchanged packages cached; paid/live opt-ins off. Current full vet,
  repo-check/secret-scan59473 and diff-check PASS. All test handles terminal.
  No paid calls active. Live Cursor restart confirmation awaits Claude auth
  renewal; combined native rejection Run→receipt→retry remains a coverage gap.
  Goal NOT complete; no commits or remote/live Relay mutations made.

- Reverse live80038 PASS308.16s (package308.684s): Claude Builder/Cursor
  Reviewer disposable compiled ticket. Both Cursor/Claude directions now pass.
  Concurrent2248 FAIL336.65s at test-only hardcoded Claude/Codex identity
  assertion, observed correct Cursor Luna. No production defect established.
  Root: concurrent helper did not receive selected builder/reviewer; final
  review also hardcoded Codex. Pass selected pair and share exact identity
  matcher with single-ticket assertion. Nonpaid regression15497 red Cursor;
  26550 green0.606s. Compiled nonpaid concurrency82480 PASS160.88s
  (package161.415s), including restart/separate approvals/base refresh.
  Corrected69217 FAIL365.56s (package366.037s): restart daemon before socket,
  recover stranded Git mutations -> SQLite write deadline/context deadline.
  Prior closed diagnostics repeatedly CHECKPOINT=commit. No raw model output.
  No active paid run; investigate recovery locally before spending again.
  Goal remains active, final baseline pending.
  Source hypothesis: daemon.Start starts default5s startupCtx before calling
  ProviderCoordinatorFactory; cmd/sf factory calls multiprovider.Compose with
  Background. Compose observes Cursor/Claude runtimes before ComposeQualified
  checks QualificationCurrent. On restart all qualifications are old-leader;
  expensive unnecessary observation can exhaust startupCtx before first Git
  recovery query. Need deterministic pre-observation stale-qualification test
  and early same-authority refusal, not blanket timeout increase. Unproven
  live root until reproduction. Local recovery race count5 active40953.
  Update:40953 PASS188.017s. Deterministic composeLocal injected candidate
  factory62917 red proved stale pair still observes runtimes. Early exact
  QualificationCurrent/profile/auth/probe/signature gate before all runtime
  observation returns idle through existing ComposeQualified; current authority
  rechecked after inspection as before. Race93500 PASS4.775s. Added positive
  current-pair inspection case;38839 racePASS4.684s. Two production/test files only.
  Focused81138 PASS daemon22.412s/providercoord37.974s/multiprovider0.674s.
  58623 FAIL67.99s at initial qualification: Cursor qualified, Claude auth
  cannot cover launch window. No restart exercised, startup live confirmation
  still pending. No active paid run. Do not repeat until Claude auth renewed.
  Active67096 full current-tree nonpaid baseline, native host Go -p1 ./....
  Repo-check/secret-scan59473 PASS644commits/tree no leaks; diff-check clean.
  Current full go vet PASS (terminal, no session). Acceptance requirement table
  refreshed: both Cursor directions passed; Cursor restart/live auth and combined
  native supervised retry remain incomplete.67096 confirmed still live; through
  Github/localruntime/multiprovider passed, later packages pending.
  Latest67096: supervisor65.283s/providercoord37.695s/publication91.274s PASS;
  Store and later packages pending. Native combined retry remains engineering
  coverage gap, not solely auth: existing local Claude probe uses bare synthetic
  API auth (not Supervisor.Run); Run uses fixed Keychain lookup and qualified
  subscription environment. Do not relabel layered proof as combined or expose
  an arbitrary credential/endpoint override to production to satisfy a test.
  Latest67096 Store PASS127.843s/testkit3.120s; workflowruntime/worktreecoord
  remain pending. No duplicate baseline launched. Original plan retry assertions
  map to Store/providercoord real-Git tests, while combined native Run evidence
  is still not proven; do not describe the layered test as native end-to-end.
  No paid run. User informed renewal via claude auth login needed for remaining
  live acceptance; no API key/additional spending request. Continue nonpaid work.
  Live causal confirmation still pending; do not infer SQLite locking fix.
  Post-repair35189
  PASS: Store129.582s/workflowruntime113.814s/workflowworker1.527s.
  35781 repo-check and secret-scan PASS. No real Relay/remote mutation.

- Repair root now reproduced/fixed: deterministic compiled63389 failed outcome
  because fixture claimed missing but ran green; corrected fixture to genuine
  failing replacement. Compiled77973 then failed at CHECKPOINT=commit, proving
  ordinary base-parent path cannot commit on existing reviewed candidate.
  Added Store.ReviewRepairVerificationCheckpoint read-transaction proof:
  current fence, existing authenticated final-review repair/budget/ledger,
  exact historical candidate/head and immutable Builder artifact. Materializer
  uses that parent and protects inherited Builder files unchanged. No HEAD
  inference or generic authority bypass. New Store53392 PASS; existing repair
  restart68209 PASS; compiled1793 PASS75.89s to fresh verification/building,
  ready0/merge0. This is not full repair delivery or paid reverse acceptance.
  Full98977 PASS (predates this repair change). All paid handles terminal.
  Broader Store/workflowruntime/workflowworker started after fix; capture handle
  from current tool result. No commits/remotes/live Relay state touched.

- Current goal turn: native Sonnet metadata probes52675/65781 PASS (~20s each),
  exact closed metadata `Claude Sonnet 5 300K Low No Thinking`; pinned CLI
  format-param-summary appends No Thinking when thinking=false. Added exact
  catalog1M Low->measured300K No Thinking SessionDisplay mapping, no inference
  during parsing. Red test failed then cursorprovider race89083 PASS1.326s.
  Native signed Sonnet qualification63082 PASS61.67s. Five model-capable Cursor
  launches this turn (2 probes +3 qualification incl cancel startup), dollars
  unknown. Luna full-ticket pass remains valid; reverse/full concurrency pending.
  Local invalid-syntax reproduction24237 proved it can reach recorded proof
  (then fixture's intentional response-loss signal); syntax alone is NOT the
  checkpoint-stall cause. Temporary fixture edits removed completely. Added
  deterministic compiled final-review repair marker in fake-provider/test;
  active69013 `go test -tags sf_e2e -p1 ./cmd/sf -run ^TestCompiledDevFinalReviewVerificationRepair$ -count=1 -v -timeout5m` (nonpaid).
  Full98977 still observed live; most packages passed, final worktreecoord pending.

- Active non-paid full post-fix baseline98977: `GOCACHE=/private/tmp/sf-multi-cli-go-cache go test -p 1 ./...` host escalated. Poll exact handle; no paid run active.

- Latest terminals: reverse Claude Builder/Cursor Reviewer66737 FAILED756.13s
  at verifying v8 after completed planning/verification/build/final-review and
  second verification. Events include checks_green,budget_correction,review_repair.
  Cursor verification repair produced duplicate package declaration in add_test.go;
  local draft created1, ready0, merge0. Do NOT assert this alone proves the
  materialization rejection cause. Added closed sf_e2e checkpoint stage hook
  (command/outcome/parent/policy/evidence/commit), production noop; test83445 PASS.
  No paid retry of this failure yet. Cursor concurrency remains held, unrun.
  Backoff race32893 PASS57.856s. Grok low native74121 FAILED33.86s terminal_artifact;
  Sonnet low native27813 FAILED35.36s init model mismatch (catalog 1m low,
  observed shape other low other thinking). No guessed alias, both unqualified.
  All paid handles terminal. Need diagnose verification repair checkpoint and
  missing final-review findings in fresh verification prompt (source concern,
  not yet proven cause); no production repair-path edits. Goal active/incomplete.

- Latest regression60864: workflowruntime PASS109.323s; providercoord FAILED
  second-server-error (one launch, needs_operator). Root timing boundary:
  authenticated not-before may elapse between Store check and clock read;
  helper rejected negative delay. Regression50198 fails red at -1ns; helper
  now returns to Store admission when elapsed (no launch authorization), still
  rejects >3s delay and cancellation, preserves bounded retry loop. Narrow
  backoff+budget/checkout tests40638 PASS count5,42.909s. Full package pending.
  Reverse paid66737 still active as of19:12UTC; concurrency14562 compile/skip
  only (not acceptance). No other paid run active.

- New terminal result: compiled Cursor Builder/Claude Reviewer77965 PASS328.87s
  (package329.480s), first complete Cursor ticket through disposable local
  publication/merge, not hosted Relay. Planner OUTPUT_BINDING fix is validated
  live. Prompt/artifact race13336 PASS; vet/diff clean; repo-check/secret-scan
  37181 PASS644 commits. Reverse Claude Builder/Cursor Reviewer66737 active.
  Added opt-in TestCompiledLiveCursorConcurrentTicketsRestartBeforeSeparateApprovals
  using existing strict two-ticket restart/independent approvals fixture;
  not run yet. No claim that intermittent prior model failures are eliminated.

- Sept7 latest: baseline16725 `go test -p 1 ./...` PASS (before Planner fix).
  Compiled95843 FAILED223.35s: two Cursor planning invalid_artifact attempts,
  both closed schema diagnostic proof_kind; qualification passed, no PR.
  Root: generic Planner schema offered six proof kinds but prompt omitted
  ticket-type mapping. Added canonical RequiredProofKind + controller-derived
  OUTPUT_BINDING to Planner; all-six-types regression failed red then prompt/
  phaseartifact packages49482 PASS1.339s/0.331s. No validator relaxation.
  Active bounded compiled Cursor/Claude77965 uses corrected prompt, same opt-in
  and disposable fixture. Earlier paid handles all terminal; known Cursor CLI
  model-capable launches54 plus up to5 unverified before77965, not API calls or
  known dollars. Old persisted prompt bytes remain immutable; no live upgrade.

- Sept7 current investigation: prior turn was progress, not blocked. Added
  counts-only cursorFixtureWriteShape diagnostic (no raw messages/paths/content;
  never admission evidence). Unit98353 PASS0.471s, race18228 PASS1.412s.
  Standalone native qualification57426 PASS57.65s unchanged. Compiled98403
  FAILED241.34s after Cursor planning completed and Claude verification wrote
  add_test.go but returned result_indeterminate. Ticket safely blocked, no PR
  mutation. Qualification passed; missing-write root remains intermittent.
  Added sf_e2e-only closed command/Claude parser-stage diagnostic; tests63615
  PASS0.564s. Active compiled95843 repeats exact acceptance with diagnostics.
  Active nonpaid full baseline16725. Repo-check/secret-scan51462 PASS644 commits.
  Source pinned Cursor shouldBlockWrite resolves
  paths before explicit permission checks; relative/absolute mismatch is not
  established. Do not widen sandbox or accept missing result.txt. Added7 Cursor
  model-capable launches this turn (3 native +3 qualification +1 planner),
  excluding active95843; charges unknown. Pinned CLI4347.index.js print-error
  handler emits String(error) and exits1 without terminal rejection envelope.
  Untyped503/no-final adapter regressions PASS0.438s; docs explain why Cursor
  automatic API retry remains unavailable, rather than guessing safe replay.

- Sept7 newer Cursor checkpoint supersedes the diagnostic history below:
  signed native Luna Low qualification73465 PASS59.82s, covering role writes,
  outside-read denial, readonly Reviewer, owned cleanup and launch cancellation.
  Root fix: pinned ReadToolResult.error.errorMessage, not ReadResult.error.error.
  Paired strict parser regression failed red then passed; no raw diagnostics.
  Full host non-paid `go test -p 1 ./...`37400 PASS, including Store127.619s
  and workflowruntime110.554s; repo-check/secret-scan29345 PASS644 commits.
  Production multiprovider/daemon now route Cursor via signed Store qualification
  and exact binding; no fallback. CLI/config Cursor presets and independent
  model picker added after baseline; focused73241 PASS cli/config.
  Native after-write cancellation58825 PASS28.95s (signed drain + preserved
  marker). Earlier77132/44063 failed before tools; canonical macOS worktree
  fixture path resolved startup, no sandbox widening. CLI/config62567 PASS.
  Compiled Cursor/Claude and reverse ticket opt-in cases added; compilation-only
  skip87726 PASS (not acceptance). Terminal paid handle90145 ran only Cursor
  Builder/Claude Reviewer with SF_TEST_LIVE_CURSOR_TICKET, sf_e2e, -timeout15m;
  disposable compiled daemon/Git/FakeGH, no hosted/live channel. Race75944 PASS
  cli/config/cursorprovider. Latest CLI32895 PASS5.524s after two old
  unsupported-Cursor expectations were updated; package45075 otherwise passed
  config/cursorprovider/multiprovider/processsupervisor66.707s. Focused vet and
  diff-check PASS; repo-check/secret-scan11578 PASS644 commits.
  90145 qualified both providers but FAILED620.76s planning with zero attempts.
  Read-only test DB/show confirms worktree registered and no provider attempt.
  Root: workflowruntime.configuredProvider still allowed only codex/claude.
  Regression39103 failed red; explicit cursor selector fix75231 PASS0.449s.
  Store/coordinator/supervisor qualification remains mandatory. Nonpaid
  host workflowruntime full64018 PASS114.924s. Native90145 cleaned up.
  Corrected compiled16903 FAILED40.34s at qualification: file_inventory_builder
  result_present=false forbidden_unchanged=true (strict stream had passed).
  No admission/pair selection; do not weaken required writes. One unchanged
  bounded repeat10478 FAILED37.09s with the same missing result.txt inventory.
  Native inventory variability is a reliability concern, not repaired by the
  selector. Nonpaid race89553 selector PASS1.659s + repo-check/diff PASS.
  Latest workflowruntime vet PASS. No full Cursor ticket success yet.
  ALL known handles terminal; no paid/background tests active. Do not blindly
  repeat missing-write qualification again. Next diagnose paired write-tool
  presence/outcome using code-owned booleans only (no raw stream), and pinned
  CLI permission matching for relative/absolute tool paths. The failures passed
  strict session/denied-read evidence but never created result.txt; omitted
  write versus refused file tool is not yet distinguished. Keep guard intact.
  This is still NOT full Cursor ticket/concurrency acceptance or automatic API
  retry support. Ambient hooks are user-approved trusted dependencies, not
  contained/disabled. Cursor actual spend remains unknown; current continuation
  adds at most thirteen Cursor model-capable launches to prior29(+up to5
  unverified): eight probes plus three in90145 qualification and one each in
  16903/10478. No compiled ticket provider attempts occurred. API counts and
  actual dollar charges remain unknown.
  Broad shared dirty tree preserved, no commit/live daemon/remote/DB mutation.

- Sept7 Cursor continuation supersedes older "invocation unchanged" notes:
  adapter/catalog/metadata observer and Supervisor trusted-hooks execution are
  implemented, but CLI/composition and signed qualification remain unfinished.
  Native metadata observer43450 PASS9.617s (status/catalog only). SF-owned
  lifecycle wrapper fixed Cursor's lingering local worker: native9192 and all
  subsequent completed qualification probes passed drain, then failed Builder
  result validation. Fresh tests2495/3415/98801/52076/3047 isolated a catalog
  versus parameterized-session model display mismatch, not a hook failure.
  Installed7932.index.js constructs session display separately from3279's
  catalog rendering. Native76119 identified exact context mismatch:
  observed GPT-5.6 Luna 272K Low vs catalog GPT-5.6 Luna 1M Low. Removed the
  attempted generic formatting normalization (it did not help); stream labels
  remain exact. SessionDisplay now requires the exact Luna-low catalog entry
  and binds its measured 272K session label. No 1M support claim. Native38873
  passed model/terminal validation then failed tool_shape. Installed index.js
  ToolCall schema includes toolCallId/startedAtMs/completedAtMs and hook contexts
  alongside one tool; parser now validates/removes only those known metadata
  fields, then demands one tool and paired calls. Native51361 advanced to
  read_args: completed calls omit args. Start/completion pairing now carries
  the exact read target and rejects retargeting. Native83289 then reached
  missing denial; native82901 (latest, FAIL43.801s) narrows it to
  outside_denial_unrecognized, not missing call/completion. No passing signed
  verdict yet. Generic ReadError now checks its schema's error field for fixed
  EACCES/EPERM/OS denial strings; if its optional redundant path is absent it
  uses the paired start, but contradictory paths refuse. Relative paths are
  resolved against the known worktree. These latest source changes have unit
  coverage88378 PASS0.449s but have NOT had a native rerun. Next diagnosis
  should inspect fixed outcome/field presence categories in one probe, not
  repeatedly run full fixtures for single diagnostic flags; never print raw
  output or accept generic errors as permission proof. Native paid runs are
  all terminal. Non-paid host focused80796 PASS (cursorprovider0.378s,
  processsupervisor1.141s), including real physical sandbox writes. Current
  focused vet and diff-check PASS. Cursor race54301 PASS1.338s. No active
  tool/test handles remain. Full baseline/scripts still need final-source run.
  Do not enable Cursor until signed
  qualification and composition tests pass. No arbitrary provider text logged.
  Earlier full baseline70933 completed PASS, but predates this integration;
  previous focused51431/final native handle from compaction are unavailable,
  so their outcomes are unverified. Process inventory confirmed no old go/
  Cursor probe remained before these runs. Sandbox-only82118 physical test
  failed host sandbox_apply EPERM; rerun that fixture on authorized host.
  This continuation added eleven completed Builder CLI launches; earlier
  known18 plus up tofive unverified launches remain
  separately unconfirmed. Actual billing unknown. No live SF/Relay/DB/remote
  changes, no commit; preserve broad shared dirty/untracked work.

- Sept7 latest Cursor checkpoint: Reviewer-only native fixture49409 PASS19.326s.
  The prior Reviewer protocol error was pinned CLI `thinking` delta/completed
  events, verified in4347.index.js and shape-only session diagnostics. Stream
  parser now permits only those subtypes with same-session checks; never logs
  their contents or treats them as result. Unit/race33876 PASS1.400s. Builder
  native file+artifact passed repeatedly in43893,48563,69363. Reviewer physical
  file immutability and terminal artifact now both pass49409. Runtime still
  NOT qualified/enabled: production invocation not switched, outer profile
  is experimental, cancellation/drain/observer/composition gates remain.
  Known incremental-window CLI launches total16 (includes startup failures),
  plus up to2 for unverified54711; actual billing unknown. Do not launch more
  paid compatibility probes without a distinct new evidence need. Full baseline
 70933 remains running (last package phaseartifact); started before parser fix,
  so rerun affected packages/full baseline on final source. Repo-check and
  secret-scan82921 PASS644commits+workingtree; focused vet passed before parser
  fix. No currently running paid probe. No live project/DB/remote changes.

- Sept7 Cursor trusted-hooks continuation: native allowlist-only build fixtures
  55727 and 64269 changed forbidden.txt, both with and without --force. This
  is NOT a hook blocker; it proves Cursor allow entries are not a physical
  default-deny write boundary. Added experimental cursorRoleSandboxProfile
  (not production wired) and credential-free physical write test PASS0.536s,
  repeated PASS0.363s. macOS dyld needed literal / read (kernel deny evidence);
  Cursor launcher needed exact basename/dirname/realpath helpers. Short private
  /private/tmp/sf-cursor-* homes avoid pinned CLI's >84-character fallback to
  /tmp/.cursor. Native nested Cursor sandbox refused startup; fixture now
  disables INNER sandbox only while retaining SF OUTER file restriction.
  Production Invocation remains unchanged until qualification is measured.
  Native runs 43893/48563 passed Builder file+artifact invariants and Reviewer
  physical unchanged-file checks, but Reviewer StreamArtifact rejected.
  Extra classifier run 69363 pending; full non-opt-in baseline 70933 pending.
  Earlier handle54711 is no longer retrievable: result unverified, do not mark
  pass. Known new-window CLI launches before69363 total12, plus up to2 from
  unverified54711;69363 adds at most2. Some failed before model dispatch, but
  actual billing remains unknown, not zero. No more vendor/hook approval is
  required. Cursor is still unqualified; observer/drain/Store composition and
  native delivery are unfinished. Focused cursorprovider race PASS1.386s;
  focused vet and diff-check pass. No live channel/DB/remote mutations.

- Sept7 USER SUPERSEDES HOOK BLOCKER: explicitly assume Cursor hooks do not
  interfere and continue. Treat ambient hooks as trusted dependencies in a
  separately named policy, not disabled or proven contained. Do not keep asking
  for vendor support on that waived requirement. Role permissions, exact model/
  family, Store authority, process drain, and native delivery remain mandatory.
  New native TestCursorNativeTrustedHooksStdinProbe PASS19.223s (98677): one
  staged browser-auth CLI launch, gpt-5.6-luna-low, stdin prompt and bounded
  stream artifact, disposable home, no requested tools. Dollar charge unknown;
  this is first launch against the additional $100, not qualification or a
  role-permission proof. Added exec-free cursorprovider Invocation/Permissions/
  MatchesInvocation with exact version/model, role modes, stdin-only schema/
  prompt and tamper validation; unit package PASS0.554s (78154). Proposals are
  NOT yet wired to Supervisor/Store admission and must not be exposed as ready.
  Next: native write/read-only policy fixtures, runtime registration/observer,
  adapter/composition, then qualified disposable delivery. Existing hard Cursor
  refusal remains until passing policy exists. Old blocked notes below are
  historical and superseded by this explicit user scope decision.

- Sept7 resumed blocked audit reached three consecutive turns with the same
  Cursor qualification gap. Final read-only check: installed CLI unchanged at
  2026.09.02-c22c1a3; Supervisor.Run still deliberately refuses Cursor and
  ProviderPolicyDigest supplies no Cursor policy. Local-loader and ACP checks
  found separate ambient/team/prompt-hook paths; no verified supported control
  covering them was established. Full goal remains incomplete and is marked
  BLOCKED again, not complete. Additional $100 remains unspent. No active
  process handle, new paid call, credential change, or live runtime mutation.
  Reopen on concrete supported isolation evidence or an explicitly revised
  provider requirement; do not silently downgrade authority to enable Cursor.

- Sept7 follow-up checked the supported ACP alternative: installed 5421.index.js
  retains promptHookClient plus asynchronous team-hook merges and hook-wrapped
  session resources. ACP permissions and documented sandbox filesystem/network
  policy do not establish fixed hook/model/cost isolation. No model call, ACP
  session, credential lookup, production edit, or live-state change. New Cursor
  allowance remains wholly unspent. Last turn made evidence progress by ruling
  out this alternate path; Cursor qualification remains unresolved. No live
  test handle is waiting.

- Sept7 user explicitly authorized an additional $100 Cursor test budget and
  continuation. Treat this as a new incremental ceiling, not evidence that
  earlier spending was zero; new-window spend is $0 so far. No paid model call
  was launched in this resumption. Claude's combined native forced-rejection
  test is an engineering coverage gap, not an operator login prerequisite.
  Passing Claude/Codex delivery remains usable independently of Cursor.
  Offline installed Cursor loader probe passed: `loadProjectHooks=false`
  still checks enterprise, team, user, and Claude-user configuration sources.
  Probe uses the extracted unmodified loader and a fake filesystem reporting
  no files; it does not execute hooks, validate hook payloads, read actual
  config, or qualify native role execution. Scratch reproduction:
  `.context/cursor-hook-path-probe.cjs`; loader SHA256
  0b7e60bf9d8642df918dd8a789dc16b6a953975ffc944d120f3e8f8adbff87c5.
  Additional installed source in 4347.index.js updates hook configuration
  directly from teamHooksResultPromise, so denying local hook-file reads alone
  does not prove isolation from managed in-memory hooks. No managed policy
  was removed, CLI modified, credentials inspected, or live SF state changed.
  Metadata-only CLI config check found authInfo.teamId present (no value or
  identity printed); do not assume a team-free account or remove this binding.
  This does not prove installed team hooks or enterprise management.

- Goal marked BLOCKED after three consecutive external-gate audits. Installed
  Cursor remains 2026.09.02-c22c1a3 without a verified role isolation policy;
  combined native Claude subscription rejection/capture/receipt/retry acceptance
  remains unproven without a supported fault-injection mechanism. Prior turn
  was no progress, not a running-test wait. All test handles are terminal;
  baseline23540 and recorded native Claude/Codex delivery evidence are preserved.
  No additional paid probes, CLI changes, credentials, or live state mutations.
  Resume when capability evidence changes; full objective is not complete.

- Post-test baseline23540 PASS, exit0: native host `go test -p 1 ./...`, all
  paid/live opt-ins disabled (some unchanged packages cached). cmd/sf90.214s,
  Git183.626s, supervisor65.448s, providercoord37.529s, Store127.310s,
  workflowruntime111.178s, worktreecoord131.928s. repo-check/secret-scan40522
  PASS (644 commits + working tree, no leaks), diff-check PASS. Sept7 source/
  official-doc gate recheck: agent/cursor-agent symlinks still point to
  2026.09.02-c22c1a3; current parameters/config docs provide no demonstrated
  all-ambient-hook isolation, and prompt-hook model evaluation remains a
  distinct path. Claude CLI reference provides no demonstrated way to force
  a subscription server rejection through the current authenticated SF Run.
  This is missing supported evidence, not a proof that no solution can exist.
  Do not send OAuth to a mock endpoint, change billing, or manufacture a
  qualification. No paid calls/install/config changes in this gate check.
  Original plan explicitly time-boxes incompatible provider spikes and says not
  to block the passing Claude slice on Cursor. Full goal is not complete;
  safe external-gate alternatives remain exhausted after this baseline.

- Retry acceptance advanced with test-only changes: real registered Git
  worktree now joins Store/coordinator signed-rejection retry flow. New
  TestServerRejectionRetryReauthenticatesRealGitWorktree covers clean success/
  idempotent replay and tracked, untracked, ignored writes after receipt:
  dirty cases preserve bytes, terminate both durable attempts, and perform only
  one provider Run. Targeted race94831 PASS58.638s including prior synthetic
  budget/recheck suite; initial real-Git normal73509 PASS12.786s. Full
  providercoord/vet/repo-check/secret-scan12026 PASS (providercoord40.761s;
  scanner644 commits + working tree, no leaks); diff-check PASS. No live handle.
  Only estimated_retry_test.go/provider_rejection_test.go changed; actual
  provider process/qualification/signing remains an explicit fixture. No
  native Claude rejection claim or credential/endpoint bypass. Native Run→
  capture→receipt→retry acceptance and Cursor gates remain open. No paid calls.
  Prior full baseline49283 predates these test-only fixture additions.

- User renewed Claude login; host boolean lifetime gate PASS. Native fresh
  qualification29331 PASS22.639s after investigating real-stream compatibility.
  Earlier native trials failed closed, first on thinking_tokens, then native
  permission_denied/rate_limit_event metadata, then Reviewer Edit/Write attempts
  that the CLI refused as disabled. Installed 2.1.263 source corroborates all
  shapes, including the exact NO_SUCH_TOOL disabled-session suffix. Parser now
  validates bounded thinking counters (not usage), same-session allowed quota
  metadata (not billing), exact pending denial/tool-result pairing, and only
  exact unavailable Write/Edit results for read-only roles. Ordinary write
  failures, successful writes, shell, foreign identities and missing results
  still refuse. Actual CLI tool restrictions and physical role checks unchanged.
  Policy v4 now binds complete-stream-json-v2, requiring fresh qualification.
  Fixed code-owned diagnostic stage/index/role errors retained; temporary
  fingerprint/lookahead diagnostics removed. New regressions each reproduced
  before fix; full Claude adapter tests PASS. Focused full race16087 PASS:
  claudeprovider/providerjson/processsupervisor, native localhost mock probes on,
  paid/live opt-ins off (supervisor79.418s, process exit0). Disposable real-model
  Claude/Codex delivery59836 PASS213.094s (local publication fixtures, not hosted).
  Reverse Codex/Claude45777 PASS231.612s. Full vet73699 PASS. Focused vet/repo-check/
  secret-scan89527 PASS (644 commits + working tree, no leaks).
  current-policy concurrency/restart15849 FAIL86.871s before publication:
  Planner exhausted two schema-invalid artifacts, clean checkout, zero PRs.
  Root cause not yet known. Added sf_e2e-only
  closed Planner validation categories (never raw artifacts); diagnostic tests
  61509 PASS0.560s. Instrumented concurrency92204 PASS391.931s (two tickets,
  restart/requalification, separate approvals, fresh sibling build/review).
  Prior Planner failure did not reproduce; keep reliability caveat, no claimed
  root-cause fix. Full serialized baseline49283 PASS, exit0, paid/live opt-ins
  off: cmd/sf102.714s, Git275.181s, GitHub86.995s, supervisor82.616s,
  providercoord14.927s, publication96.612s, Store130.428s,
  workflowruntime113.608s, worktreecoord136.795s; all other packages passed.
  Refreshed full vet79999/repo-check/diff-check PASS; secret-scan58422 PASS
  (644 commits + working tree, no leaks). All handles terminal. No Cursor paid
  calls, live SF/Relay changes, commits or remotes. Full goal still incomplete:
  combined native supervised server-rejection→signed receipt→physical check→
  durable retry acceptance missing; Cursor isolation/spend gates unresolved;
  Planner schema-exhaustion reliability observation retained. No more blind
  paid retries; do not bypass Keychain/auth/endpoint policy for a green fixture.

- Post-baseline acceptance audit: actual Supervisor.Run uses fixed host
  Keychain lookup and exact registered Claude policy before launch; the native
  local API fixture cannot stand in for that production credential/qualification
  path. Do not add a credential or endpoint bypass merely to obtain a green
  combined retry test. Renewed-login policy-v4 acceptance remains necessary.
  Corrected docs/cli.md's stale blanket "automatic retries disabled" statement:
  documents the narrow signed Claude server-rejection path, shared attempt
  budget, persisted backoff, prelaunch physical reinspection, no fallback,
  and ordinary ambiguous-error refusal. Documentation-only since baseline36092;
  repo-check and diff-check PASS. No processes, paid calls or live state changed.

- Claude streaming policy implemented: invocation now stream-json+verbose,
  success parser authenticates bounded full event sequence/session/model,
  root-only messages, role-allowed tool use/result pairing, internal retry
  notices and one final success. Only final envelope enters existing artifact/
  cost parsing; malformed/missing/truncated stream remains indeterminate.
  Policy v4 / fixture v3 require fresh qualification; Codex policy unchanged.
  Qualification checks whole-stream canary disclosure before extraction.
  Native synthetic localhost success+503-then-success PASS68052 2.088s;
  full Claude/providerjson race with native local success/rejection probes15972
  PASS6.012/1.293s (external endpoint refusal asserted, no real credentials or
  paid requests). Targeted parser/qualification race39582 PASS. Full vet70077,
  repo-check and secret-scan6322 PASS644commits+workingtree, no leaks.
  Full native go test -p1 -count=1 ./... handle36092 PASS, exit0: paid/live
  opt-ins explicitly zero; cmd/sf111.427s, Git244.012s, supervisor66.747s,
  Store130.050s, workflowruntime112.092s, worktreecoord135.098s. All handles
  terminal; no active test run remains. Status-only host
  Keychain check confirms OAuth present but expired; requested login renewal
  asynchronously again. No live SF DB/daemon/Relay/remote touched. Remaining:
  renewed-login v4 qualification and live delivery/retry
  acceptance; Cursor still lacks qualified hook isolation and spend balance.

- Coordinator signed-rejection orchestration is now wired: optional combined
  drain/receipt, unknown-cost recording, atomic finish, bounded persisted
  backoff, same-binding repeat. Pending Store rejection pins binding before
  probes/restart; new active retry has Store checkpoint -> trusted physical
  inspector -> Store recheck before Run. Refusal finishes unlaunched and stops,
  never fallback; ordinary uncertainty still cannot retry. Store exposes typed
  authenticated backoff deadline and active-retry checkpoint metadata.
  Full providercoord/Store20360 PASS6.094/130.765s; combined Store+Coordinator
  synthetic process/filesystem test28027 PASS9.164s (success, second-server
  exhaustion, changed checkout=no second Run, replay=no extra launch).
  Focused race45165 PASS20.163/16.439s; Store/providercoord vet PASS. Final
  cancellation/missing-route hardening: full providercoord race47126
  PASS152.953s. All test handles terminal; repo-check/diff-check PASS.
  Streaming policy/parser/new
  qualification and native combined retry remain pending. No paid/live changes.

- Signed rejection finish now supports exact same-current-fence receipt replay
  without deleting another lease or changing the deadline. Altered receipt
  replay refuses. New close/Open/AcquireLeader/Fence/requalification regression
  proves persisted backoff and two-attempt budget survive restart. Fixture
  initially reused a stored qualification ID; normalizeQualification correctly
  refused it. Resetting only the new fixture qualification ID fixed setup, with
  no production qualification relaxation. TestServerRejection normal49910
  PASS1.162s and race22680 PASS16.120s; both terminal. Coordinator orchestration,
  physical retry reinspection and qualified streaming remain unfinished.

- Atomic signed server rejection finish and receipt-aware Begin admission now
  implemented (provider_rejection.go/provider.go). New server_rejected outcome
  is allowed by v60's generated exact provider/phase triggers; generic Finish
  cannot mint it. Signed checkpoint/current exact active claim, receipt insert,
  failed attempt+phase and exact lease delete share existing write transaction.
  Admission authenticates all entry rejection rows (missing receipt refuses),
  same full binding/role, deterministic persisted deadline, and immediate retry
  logical input with only exact endpoint/attempt/shortened timeout normalization.
  Existing total entry count2/4 remains authority; exhaustion authenticates
  receipts and handles invalid-artifact -> server-rejected repair as same budget.
  Schema/basic8544 PASS; atomic97737 PASS0.766s; rejection/schema/backup race26922
  PASS15.744s incl late phase-write rollback/missing receipt; full Store55745
  PASS130.572s incl new reverse mixed-order pause regression. Store vet,
  repo-check/diff-check PASS. All handles terminal. No paid/live/remote changes.
  V60 is still uncommitted/unshipped; changed its own trigger list in same repair,
  no historical migration altered. Test fixture now accepts optional planner
  preference to mint synthetic attested Claude via real Store APIs.
  Remaining: exact finish replay/restart+new qualification recovery tests,
  coordinator combined drain receipt/backoff orchestration and physical retry
  reinspection, qualified streaming policy/parser+live auth acceptance. Current
  production terminal-JSON/Coordinator never invokes new writer yet. No full
  go./... result after new changes; goal incomplete and Cursor/login gates persist.

- Receipt canonicalization prerequisites implemented before atomic finish:
  Store canonicalServerRejection verifies exact immutable claim/supervisor
  signature and derives 2.0-2.9s deterministic backoff solely from signed
  observed time (not load/commit time or CLI internal Retry-After). Decoder
  verifies exact bytes, digest, duplicate observed time/deadline and signature;
  malformed/noncanonical/foreign/modified deadline refuse. Checkpoint digest
  encoding moved unchanged to Store.ProviderAttemptCheckpointDigest and physical
  inspector now shares it, preventing independent wire-format drift.
  Focused encoding/checkpoint/schema race80796 PASS34.693s; Store/worktree vet
  14748 +diff-check PASS. All handles terminal. NO writer/admission change yet;
  first inspected admission/exhaustion paths: Begin counts all entry attempts
  (limit2/4 with operator epoch), repair helper ignores non-invalid-artifact;
  new server_rejected outcome must be checked with receipt/backoff before
  acquisition and authenticated by exhaustion-pair logic. Preserve exact input
  except legitimate attempt/fence/shortened deadline fields; missing receipt
  must not fall through as generic failed. Atomic finish has 4 internal callers,
  should use a wrapper/optional receipt to avoid altering generic failure APIs.
  Still no paid/live/remote changes. Goal incomplete, broad baseline pending.

- Append-only v60 schema now reserves provider_server_rejections with exact
  seven-column attempt FK, bounded canonical receipt+hex digest, positive
  observed timestamp and immutable bounded not-before. One row per attempt;
  UPDATE/DELETE refuse. No legacy backfill or public writer/admission change.
  store.go version/checksum/migration dispatch, backup fixture and required
  columns/composite FK/triggers wired. Schema9496 PASS1.054s; focused schema+
  stable/dev backup+history refusal+legacy v1/v10 upgrades race55993 PASS26.054s;
  Store vet/diff-check PASS. All handles terminal. Migration only applied to
  disposable test databases, not live SF. Shared dirty worktree uncommitted.
  Next must implement atomic receipt/finish with admission backoff together;
  don't expose a generic failed-receipt writer while BeginProviderAttempt could
  ignore its deadline. Existing finish helper has four callers plus definition;
  safeOutcome/state triggers currently lack server_rejected. New outcome needs
  explicit schema and shared budget integration, not invalid_artifact disguise.
  Full49030 predates these changes; goal incomplete; login/Cursor gates unchanged.

- Trusted inspector wired through localruntime.Factory -> providercoord optional
  ConfigureRejectionCheckpoint -> supervisor private, lock-protected inspector.
  Configuration refuses active runs/shutdown; no adapter-supplied evidence.
  worktreecoord now implements contracts.RejectionCheckpointInspector, using
  Store.ProviderAttemptCheckpointForRequest (full persisted claim + exact full
  DrainRequest equality in one snapshot) before/after physical inspection.
  Store race37805 PASS30.944s incl request auth/model/policy/digest/lease/fence
  tamper. Initial54825 FactoryPASS; providercoord no selected tests; broad New
  regex selected existing Git setup which sandbox-refused. Correct native
  race8889 PASS supervisor1.524/localruntime23.065/worktreecoord27.150s. Focused
  six-package vet2261 +diff-check PASS. All handles terminal.
  Automatic retry still unwired: coordinator must choose combined receipt,
  Store atomic signed receipt/backoff/shared-window admission migration after59,
  qualified streaming policy + live acceptance. Inspector is now configured,
  not merely a standalone helper. No paid/live/remote changes. Uncommitted.

- Supervisor rejection receipt lifecycle implemented: private Run capture uses
  only own complete output/normal exit1/uncancelled all-server classification;
  public Run still returns conservative ExitCode=-1+error. New optional
  DrainServerRejection waits finished/process/I/O, invokes trusted inspector,
  rechecks ownership/control/closing under lock, signs own exact drain+receipt,
  removes run once. Ordinary Drain irrevocably clears receipt eligibility at
  entry; control during inspection suppresses signing. Missing/dirty/cancelled
  inspection returns no receipt; ordinary Drain remains conservative fallback.
  New tests seed private completed metadata (not live streaming evidence):
  exact/single-use/wrong-request/control-race/unclear refusal race32927 PASS1.536s.
  Full native supervisor suite8088 PASS68.467s; focused vet/diff-check PASS.
  All handles terminal. Current Claude terminal-JSON policy cannot supply an
  eligible stream; no automatic retry/paid call enabled. Trusted inspector
  interface still needs request-to-Store/worktree implementation+composition,
  then Store receipt migration/backoff/shared budget, new streaming policy and
  qualification. No paid/live/remote changes, broad worktree uncommitted.

- Physical checkpoint composition added in new worktreecoord/provider_checkpoint.go
  and sequencing tests. It loads exact active Store authority, calls existing
  strict registered Git identity/clean-HEAD inspection (including ignored files),
  then reloads identical Store authority; revocation/change/cancellation yields
  no result. Returns domain-separated metadata digest, not signed retry admission.
  Race37694 failed in existing real-Git setup at durable child identity gate
  before inspection; identical native-host race51037 PASS26.666s (sequencing plus
  real-Git dirtiness/ignored/foreign-head/replacement/cancel tests). No policy
  weakened. Focused worktreecoord vet10719 and diff-check PASS. All handles
  terminal. Tests separate sequencing from Store and real-Git proofs; a single
  active-attempt/real-Git/supervisor end-to-end fixture is still required.
  No paid call, live runtime/DB, repository remote or login change. Retry
  production wiring/backoff/migration/qualification remain incomplete; full49030
  predates new checkpoint/signature code. Changes uncommitted.

- Active-attempt checkpoint authority now exists in new
  `internal/store/provider_attempt_checkpoint.go` (plus tests). One read
  transaction rehydrates the full immutable claim, checks live ticket/leader,
  active attempt/phase and exact provider lease, phase-entry binding, immutable
  worktree creation and semantic checkpoint/commit lineage. It accepts no
  caller HEAD, performs no filesystem inspection and grants no retry itself.
  Internal connection-scoped form is for the eventual atomic terminal receipt.
  Focused17879 initially failed only a malformed test tamper (phase outcome);
  fixed fixture to use valid failed/invalid_artifact tuple, preserving schema.
  Race27011 PASS17.406s; expanded four-role checkpoint race32214 PASS27.925s;
  focused Store/contracts vet and diff-check PASS. No process active.
  Supervisor combined drain/capture, physical inspector, signed receipt Store
  migration/backoff/shared budget and streaming qualification remain unwired.
  Broad full49030 still predates both new checkpoint and signature files.
  No live login/paid call/runtime/remote mutation; goal remains incomplete.

- Signed rejection primitive added in `internal/contracts/provider_rejection.go`
  with exhaustive field-binding, wrong-key, exact-drain, signature-domain and
  malformed-evidence regressions. Focused test92885 PASS0.356s; full contracts
  race3874 PASS1.422s; diff-check PASS. The prior unknown focused process was
  confirmed absent before rerunning. No test remains active.
  This primitive signs bounded metadata only, requires its own exact drain
  proof, and grants no retry admission. Supervisor capture/physical checkpoint,
  Store atomic persistence/backoff/shared attempt budget and qualified streaming
  invocation are still unwired. No new broad baseline run for these files;
  full49030 predates them. Claude renewal and Cursor isolation gates unchanged.
  Changes remain uncommitted; no paid call/live runtime/remote mutation.

- Full49030 TERMINAL PASS after rejectionobserver: cmd/sf98.885s,
  GitHub64.453s, supervisor67.789s, workflowruntime113.920s; otherpackagesPASS
  includingcachedStore/publication/worktree. Freshvet98594/diffcheck PASS;
  no process/model remainsactive. Observerunit/native/race63167 and
  repo-check/secret-scan90312 previouslyPASS. Goal remainsincomplete. No live
  credentials renewed, Cursor unsupported, signedrejection/Storebackoff notwired.
  Integration source reading is in approved plan; begin with domain-separated
  exactclaim attestation requiring own signeddrain and separate checkpoint
  inspection, then production streamingpolicy+coordinator/Store integration.
  Preserve publicRun error semantics and oldindeterminate recovery. No source
  edits thisturn beyondplan/memory; broadworktree remainsuncommitted.

- Full49030 still active; cmd/sf98.885s/GitHub64.453s and packages through
  phaseartifact PASS (manycached), supervisor rebuilding with expected Darwin
  warnings. Samehandle; source unchanged this turn. New integration design in
  approved multi-cli plan records concrete gaps: Run collapses nonzero exits
  to-1/commandErr so adapter parsing is skipped; Drain removesrun, so optional
  combined drain+signed-rejection must retain supervisor-only realexit/metadata.
  Existing ProviderRetryWorktreeProof requires exhausted operator epoch and
  cannot authorize firstautomatic retry. Separate exactattempt checkpoint +
  trustedphysicalinspection +atomic signedreceipt/not-before/finish/lease release
  afterv59 needed; sharetwo-attemptwindow withartifactrepair. Nothing wired yet,
  no production policy change; keep unknown/429 quota/mixedhistory ineligible.

- ACTIVE49030 full go test -p1 ./... after new pure rejection observer; poll
  samehandle, no duplicate. secret-scan90312 PASS644commits+workingtree;
  repo-check/diffcheck PASS. Latest49030 output only expected Darwin sandbox
  deprecation warnings during rebuild; no overall result yet.
  internal/claudeprovider/rejection.go +rejection_test.go new; native_retry_test
  expanded to known503 +unknown400. Unit30856/race6081 passed. Nativefailures
  32534/96731/26944 proved400 category=unknown; retained strict refusal, fixed
  testexpectation, removeddiagnostics. Native46650 PASS3.231s; final native+unit
  race63167 PASS (Claude4.407s/providerjson1.313s). Fuzz32519 smokePASS6.559s but
  only3mutations, not substantialcoverage. Observer validates completebounded
  session/model/UUID/order/type/HTTP retry consistency, text-only APIerror,
  terminalerror, retains all-server-errors vs mixedauth/quota history, returns
  digest/counters/category only; NO authority/policy/production invocationchange.
  Goalstillincomplete: qualifiedstreaming +signedretry/worktreeproof +Store
  durablebackoff/shared2attemptbudget next; Claude loginrenewal/Cursor gate
  unchanged. No paid call/liveSF/Relay/remote mutation. Changesuncommitted.

- Full43603 TERMINAL PASS: go test -p1 ./... including Git187.759s,
  GitHub64.027s, supervisor66.379s, publication95.643s, Store126.727s,
  workflowruntime113.214s, worktreecoord135.839s. Fresh go vet PASS after it.
  Native local retry race15519 PASS3runs, repo-check/secret-scan63423 PASS.
  No paid call; no live/remote mutation. Local synthetic all429 probe40639
  PASSframing: CLAUDE_CODE_MAX_RETRIES=1 yields2message requests,1retry event
  (attempt1/max1/delay1000),1terminal error,typed assistant.error=rate_limit,
  no toolmessages/model/session mismatch/truncation/timeout. This is API mock,
  notsubscription qualification or safe retry proof.503 probe98482 TERMINAL:
  same2requests/1retry(max1)/1terminalerror,typed assistant.error=server_error,
  0toolmessages,exactmodel/session,bounded/no timeout. No process now active.
  Next implementation candidate: strict complete-stream rejection validator;
  keep observations separate from supervisor-signed admission and physical
  worktree proof; do not authorize another launch from last error alone or
  conflate429 with transient capacity vs exhausted quota. No policy change.
  Cursor/login gates
  remain, goal incomplete. Older ACTIVE records below are historical.

- Full43603 remains active, waitcell2044 completed with no new output; retain
  samehandle. Read-only official SDK reference confirms assistant.error closed
  categories (not isApiErrorMessage); official Claude changelog confirms
  CLAUDE_CODE_MAX_RETRIES knob/watchdog distinction. Next NON-PAID probe after
  fullsuite: loopback-only all429 with explicit small maxretry, no watchdog,
  inspect typed category and fullstream ordering; installed semantics unverified.
  Sources linked acceptanceledger. No production policychange; do not infer
  preexecution from last retry/status or zero usage. This research changes the
  next safe protocol experiment; loginrenewal stillneeded forpaidlivecheck.

- Verified wait checkpoint: full43603 is still active; wait cell2042 completed
  (its nested write_stdin returned active43603). cmd/sf96.291s, Claude0.406s,
  CLI5.598s, daemon22.526s/runtimecontrol7.520s and packages through ghrunner
  PASS. No overall verdict yet. Poll43603 only. No source changed this turn.
  Review confirms DrainProof/FinishIndeterminate cannot be reused as API
  rejection authority; unknown failure stays indeterminate across recovery.
  Credential renewal/Cursor isolation still unresolved; no paid call launched.

- Local retry race15519 PASS5.542s (-count3). Full non-paid Go baseline is now
  ACTIVE43603 (go test -p1 ./..., no native opt-in). Poll43603, do not duplicate.
  git diff --check and repo-check PASS; secret-scan63423 PASS644commits plus
  working tree. Latest poll43603 still running (cmd/fake-provider PASS).
  No paid/model work active; login renewal still needed for live restart.

- ACTIVE15519: opt-in local native retry regression, race -count3; no paid
  calls. New internal/claudeprovider/native_retry_test.go (Darwin, explicit
  SF_TEST_CLAUDE_LOCAL_RETRY=1) uses pinnedCLI/privateHOME/syntheticAPIkey,
  Seatbelt mock-only network (independent forbidden endpoint assertion), no
  tools. Initial55941/95692 failed retry cardinality: firstHTTP request wasn't
  necessarily /v1/messages. Endpoint-specific429 injection fixedfixture;
  55285 PASS1.996s (two message requests, one retry, terminalerror,indeterminate).
  Production invocation/policy/Store retry unchanged. No claim that a stream
  event alone authenticates safe relaunch. Baseline rerun pending after race.
  Current blocker login renewal/Cursor isolation unchanged; no live SF/Relay,
  remote or paid model mutation. Earlier handles below are terminal/historical.

- Latest checkpoint: no test/model process active. Full58282/vet8778/repo-check/
  secret-scan3179 PASS; recovered focused credential race29262 PASS1.861s.
  Previous race output was unavailable; ps confirmed no process before rerun.
  Native non-paid protocol probe completed: pinnedClaude2.1.263, synthetic API
  key/private HOME/bare/restricted/safe/no tools, Seatbelt loopback-only. Mock
  429->401 made6requests/retryevents, max_retries10, killed at35s. Mock429->400
  made2requests,1retryevent,1terminalresult,0toolmessages,exit1 withouttimeout.
  Scratch .context/claude-retry-probe.py; no real account/API/model usage,
  rawoutput discarded. Details in multi-cli acceptance ledger. This is NOT
  subscription qualification or safe SF relaunch proof. No retry policy changed.
  Live restart remains blocked on Claude login renewal (user asked via async
  tool; no ready reply). Cursor hook-isolation gate remains. Goal active and
  incomplete; no Cursor spend, live SF/Relay/DB or remote mutations. Work remains
  uncommitted. Earlier ACTIVE records below are historical.

- Full58282 PASS after renewal-diagnostic change: Store126.526s,
  workflowruntime112.631s, worktreecoord135.176s. Vet8778 and repo-check PASS;
  secret-scan3179 PASS644commits + working tree. Focused credential/observer
  race now running (current handle); no paid/native job in parallel.
  New official-doc evidence to investigate next: headless system/api_retry
  emits status/category/delay/attempt bounds, but these are CLI-internal retries,
  not SF relaunch authority. A pinned local mock-transport test with synthetic
  credentials/private HOME is the next non-paid evidence step, not a claim
  that safe automatic retries already work. URL code.claude.com/docs/en/headless.

- ACTIVE58282 full non-paid go test -p1 -count=1 ./... after a small
  Claude credential-renewal diagnostic repair; poll58282, source frozen.
  Live36505 TERMINAL FAIL6.286s at initial qualification, before ticket submit.
  InstalledClaude version still2.1.263. Status-only28850 FAIL0.946s; fixed-service
  Keychain boolean-only read proves OAuth present but expired and below46m
  requiredwindow. No tokens/accounts persisted. No blind paid retry.
  Added opt-in live restart entrypoint (skip62329 PASS); this is NOT live success.
  Regression9215 FAIL0.483s (lost renewal guidance); repair preserves fixed
  expiry category through prepareCredentials -> observer -> qualification,
  without changing credential validity or policy. Focused35271 PASS0.951s.
  Native status-only55826 still refuses expired auth, now with exact safe
  'run claude auth login' guidance. Three production files and one new test
  cli_auth_renewal_test.go. User login renewal required before further live calls.
  Cursor qualification and safe API retry remain open; no Cursor spend.

- ACTIVE36505: one authorized real Claude Sonnet5/Codex Luna concurrent
  restart + separate two-delivery trial. Exact command SF_TEST_LIVE_MIXED_CONCURRENT=1
  GOCACHE=/private/tmp/sf-multi-cli-go-cache go test -timeout 20m -tags sf_e2e
  -p1 -count=1 ./cmd/sf
  -run '^TestCompiledLiveMixedConcurrentTicketsRestartBeforeSeparateApprovals$' -v.
  Poll36505 only; no parallel paid/native jobs and no retry on silence.
  Added opt-in-only entrypoint reusing passing fixture80111 helper, no production
  changes since full10444/race36110. Unset opt-in skip62329 PASS0.615s.
  Local FakeGH/bare/private SF state only, no Cursor call or live Relay changes.
  Both tickets retain20m/$10 estimated budgets and existing request bounds.
  Cursor isolation and authenticated pre-execution API failure remain open.

- No test/model job active. Recovery repair validation COMPLETE locally:
  compiled80111 PASS162.672s, full10444 PASS, focused Store race36110
  PASS123.115s, vet76172 PASS, repo-check and secret-scan61161 PASS.
  No paid calls, live daemon/DB, Relay or remote changes this turn. All code
  remains uncommitted. Goal remains active/incomplete: Cursor hook isolation
  and safe automatic transient API retry are not delivered. Paid mixed-model
  restart is not claimed; prior Claude v3/Codex live two-delivery remains valid
  evidence for its tested pre-repair source. Next work must preserve these
  boundaries; no blind Cursor probe or unsafe retry to force completion.
  Earlier ACTIVE records below are historical, not active handles.

- Full baseline10444 PASS: go test -p1 -count=1 ./..., including Store127.161s,
  workflowruntime113.632s, worktreecoord134.856s. Vet76172 PASS; repo-check PASS;
  secret-scan61161 PASS644commits + working tree. Current source includes the
  complete pending-before-restart recovery repair, compiled80111 PASS.
  Focused Store protected-base-refresh race is now running as36110; poll36110.
  No paid calls this turn; goal remains incomplete for Cursor and safe API retry.

- Compiled80111 PASS162.672s: two fixture-backed concurrent tickets survive
  restart/requalification; first delivered, sibling refreshed onto merged base,
  fresh Builder result/review accepted and separately approved/delivered.
  Exact history/PR/head checks pass, no duplicate mutations. FakeCodex/FakeGH
  and local bare only, not a paid model or hosted GitHub delivery.
  Full go test -p1 -count=1 ./... is now running as10444; poll10444 only.
  Source frozen during baseline. Cursor/API retry remain unavailable.

- ACTIVE80111: fixture-only compiled restart/concurrent separate-approval test
  on the expanded Store repair. Poll80111; no paid/native test in parallel.
  31263 FAIL248.905s: current result/reuse/fence false, refresh context and
  verification true. Diagnostic91667 FAIL130.579s showed reviewed prefix
  itself PASSES. Root difference: native CI pending can precede first restart;
  initial recovery anchor rejected those same-state poll versions. Added that
  ordering to Store fixture34254, reproducing stale result in0.855s.
  Repair authenticates original publication lifecycle plus exact recovery row
  inside historical CI/review chain. 36061 PASS2.907s: all three orderings and
  six tamper cases, including pending observation, plus wrong source runner.
  Temporary diagnostic type/tagged file removed. Full baseline predates repair.
  Earlier ACTIVE entries below are historical and must not be polled.

- ACTIVE31263: compiled restart regression with read-only boolean authority
  probes in tagged test (current result, result fence, refresh context,
  verification, reusable). Same non-paid command as20385; poll31263 only.
  20385 FAIL250.216s: sibling building v11/r2 after an extra checks_pending,
  Builder2 complete. Store positive now includes pending->green variant and
  passes21826 (1.096s), so remaining compiled failure needs probe evidence.
  Do not claim original scenario fixed yet. No new production changes since
  the bounded prefix repair; source frozen during31263. No paid calls.

- ACTIVE20385: original fixture-only compiled restart reproduction on repaired
  Store source. GOCACHE=/private/tmp/sf-multi-cli-go-cache go test -timeout 10m
  -tags sf_e2e -p 1 -count=1 ./cmd/sf
  -run '^TestCompiledConcurrentTicketsRestartBeforeSeparateApprovals$' -v.
  Poll20385 only; no paid model/native job in parallel. Source frozen while
  running. Direct positive+tamper regression44808 PASS; no temporary diagnostics.

- Recovery repair checkpoint: direct Store test84834 FAILED0.828s with fresh
  Builder current-loader stale fence; diagnostic93831 proved live AssertFence
  passes, recovery authority/refresh pre-prefix fail, refresh suffix passes.
  Added bounded historical CI/review prefix in protected_base_refresh_recovery.go
  and historical cutoff wrapper in evidence_read.go. Generic ledger rules are
  unchanged. Direct positive + five tamper/source-fence negatives44808 PASS1.965s.
  Test64755 failed only fixture corruption values/column (corrected, not a
  production failure). Original compiled restart regression now rerunning;
  inspect the current tool handle before any other native run. No paid call.
  Four Store files changed (two production/two tests), tagged compiled test
  from prior turn retained. Needs original scenario plus fresh broad validation.

- No native/model test active. Restart diagnostic83551 terminal FAIL249.829s,
  reproducing74089 exactly: first Done; sibling fresh Builder2 completed,
  building v10/r2, clean/testPASS, no duplicate external mutations. Closed
  diagnostics after daemon stop/restart: other + worker_stale_evidence.
  Temporary workflowworker instrumentation removed immediately (call + both
  files); no production repair made. The tagged regression/helper remains.
  Next investigate fresh Builder LoadCurrentProviderAttemptResult ->
  validateRunnerRecoveryAuthority and protectedBaseRefreshRecoveryGap. Source
  hypothesis: latest recovery at waitingCI v7 cannot traverse checks_green v8,
  review_pass v9 before refresh v10 because gap's first prefix validator lacks
  that exact authenticated CI/review bridge. Need a direct failing Store proof
  before changing safety semantics; this hypothesis is not yet confirmed.
  New regression TestCompiledConcurrentTicketsRestartBeforeSeparateApprovals
  reproduces with fixture providers, no paid calls. Baseline69479/vet/scripts
  passed before tagged-test-only additions. Goal incomplete; source uncommitted.

- ACTIVE83551: same fixture-only restart regression rerun with temporary
  workflowworker acceptance diagnostics (closed categories only, once/process).
  Poll83551 only. 74089 terminal FAIL248.757s: restart/requalification,
  unchanged history/exact fencing, first approval/Done all passed; sibling
  refreshed Builder2 completed but remained building v10/r2, clean checkout,
  tests pass, only 2PR/1ready/1merge. Root boundary not yet identified.
  Temporary files internal/workflowworker/acceptance_diagnostic{,_e2e}.go and
  one worker.go call MUST be removed after diagnosis; no production fix yet.
  Do not claim this new matrix passed. Baseline69479 earlier PASS remains valid
  for pre-instrumentation source; full goal incomplete. No paid run active.

- ACTIVE74089: fixture-only compiled two-ticket graceful restart at waiting-CI,
  then separate approvals and both deliveries. Exact command: GOCACHE=/private/tmp/sf-multi-cli-go-cache
  go test -timeout 10m -tags sf_e2e -p 1 -count=1 ./cmd/sf
  -run '^TestCompiledConcurrentTicketsRestartBeforeSeparateApprovals$' -v.
  No paid flags, live channel, hosted GitHub or Cursor requests. Poll74089 only.
  Full baseline69479 is terminal PASS (Store124.847s, workflowruntime112.096s,
  worktreecoord134.908s); vet25154, repo-check, secret-scan60039 also PASS.
  Only subsequent Go change is the tagged compiled restart regression/helper.
  Goal remains incomplete; source uncommitted. Earlier ACTIVE entries below
  are chronological history, not currently running jobs.

- ACTIVE69479: final full baseline on frozen Go source after exact-v3 livePASS.
  GOCACHE=/private/tmp/sf-multi-cli-go-cache go test -p 1 -count=1 ./...
  No paid opt-in flags. Poll69479 only. Vet25154 PASS; repo-check PASS;
  secret-scan60039 followup. All earliernative/livehandles areterminal. Do not
  edit Go source duringthisbaseline orstartanothernativejob. Final code still
  uncommitted; goalnotcomplete (Cursor/APIretry plus remainingmatrix evidence).

- Final-policy LIVE70537 PASS368.284s: Claude Sonnet5/Codex Luna two-ticket
  separate approvals→bothDone, same2PR/2ready/2merge, exactprotectedhead,
  preservedhistory andsamebindingfreshBuilder/review. Initialattemptscompleted
  withoutrepair; freshBuilder2completed. This is exact Claudev3 provider-only
  guidance with sharedCodexprompt restored, notthe superseded globalplacement.
  No modelrunactive. Nativecancel81911 andscopedrace37943alsoPASS. Starting
  finalnonpaidvet/baseline onthissource; Cursor/APIretryremainunavailable.

- ACTIVE70537: final Claude-only policy v3 live two-delivery confirmation.
  SF_TEST_LIVE_MIXED_CONCURRENT=1 GOCACHE=/private/tmp/sf-multi-cli-go-cache
  go test -timeout 30m -tags sf_e2e -p 1 -count=1 ./cmd/sf
  -run '^TestCompiledLiveMixedConcurrentTicketsDeliverWithSeparateApprovals$' -v.
  Poll70537 only. Baseline70197, race37943, nativecancel81911 allterminalPASS.
  No other native run. No installed/liveSF/Relay/remote changes. 98997PASS is
  prior placement; exact final v3 confirmation remains pending this handle.

- Native Claude after-write cancellation 81911 PASS9.292s (fixture8.84s).
  Actual Sonnet5 Write observed independently, then cancel→bounded Runerror→
  authenticated Drain proof→exact retained marker bytes. Recorder isfixture,
  notStore/daemonrestartproof. Race37943 PASS (Claude1.472s,supervisor3.284s),
  including priorClaudev2 refusal and retained private homes/credentialfixture
  until escapedpipe Wait finishes. Original Codex sharedprompt/policyunchanged.

- Baseline 70197 finished PASS (last worktreecoord 135.131s). Compatibility
  review then found shared Builder prompt changes would reject exact old Codex
  input reuse. Restored all workflowprompt files to HEAD (no diff); original
  ordinary-prompt golden reproduced failure before restoration. Moved the same
  inventory guidance to Claude Invocation only for PhaseBuild + sf.builder/v1
  schema ID. Canonical PhaseInput is unchanged; qualification fixtures unaffected.
  Bumped Claude policy v2→v3; Codex policy unchanged. New regression 96085 failed
  before fix; scoped 25393 PASS (Claude .419s, prompt .300s, supervisor 2.186s).
  Retained-pipe environment assertions passed there. ACTIVE 37943 is scoped
  native race test (Claude/policy/retained pipes), no paid flags. Must revalidate
  live two-delivery under v3; 98997 proves prior global-placement variant only.
  New native after-write cancellation probe still NOT RUN. No live model active.

- Baseline70197 verifiedACTIVE throughStore127.303s/workflowprompt1.240sPASS.
  Added opt-in TestInstalledClaudeCancellationAfterWritePreservesWorktree in
  claude_cancel_live_test.go, refactoring existing launchcancel helper. Waits
  for independentlyreadmarkerfromrealWrite, cancelsbeforefinal, requiresRunerror
  plus signedDrain and exactretainedmarkerbytes. One60sSonnet5fixture invocation,
  notStoredaemonrestartproof; NOT RUN yet. No morepaidcalls since98997PASS.
  Afterbaseline: exact retainedpipe regression/race, then native newClaudeprobe
  (SF_TEST_CLAUDE_CANCEL=1) underexistingauthorization. Noothernativejobactive.

- Cursor boundedalternative sourcecheck: native no-fork alone NOT sufficient.
  Officialhooks docs describe prompt hooks+model override; pinned190.index.js
  uses promptHookClient.evaluatePromptHook andindex.js forwards RPC. Read-only,
  no modelcalls/nativeprototype/authchanges. Updatedcompatibilitygate to require
  in-process hooks/MCP proof aswellascommandchildren. This is new narrowing
  evidence, NOT Cursorqualification. Baseline70197throughsupervisor67.192s,
  providercoord5.924s/providerjsonPASS, stillactive lastpoll.

- Baseline70197 verified ACTIVE this turn, through CLI/daemon/runtimecontrol/
  ghrunner PASS. Repo-check PASS; secret-scan79604 PASS644commits+noleaks.
  Added source-only assertions to existing native retained-pipe supervisor
  regression: capture fixture HOME/CODEX_HOME paths; verify both/private copied
  credential file survive ErrUnclear Run+Close until Wait completion; afterward
  both disappear and source fixture remains. Cleanup now releases escapee even
  on setup failure. No production behavior change, no models called. Gofmt/diff
  clean. Must run exact retained-pipe test/race after baseline slot clears;
  baseline started before this test edit, so do not infer coverage from timing.

- ACTIVE70197: full nonpaid native baseline after live98997PASS.
  GOCACHE=/private/tmp/sf-multi-cli-go-cache go test -p 1 -count=1 ./...
  Live opt-in flags unset. Poll70197; do not startanothernative run. Latest
  production change onlyBuilderinventory prompt; diagnostic hook isnoopin
  ordinarybuilds. Goalnotcomplete. No livepaid modelprocess remains.

- PASS98997 exit0,401.319s: original compiled LIVE Claude Sonnet5/Codex Luna
  concurrent two-ticket separate-approval delivery after Builder prompt fix.
  BothDone; firstmerge→siblingbase refresh→freshBuilder2completed(no repair)
  →freshreview→separatesecondapproval→secondmerge. Initial Planners eachused
  permittedrepairthencompleted. Same2PR/2ready/2merge/exactprotectedhead and
  immutablehistory/runtimebinding assertionspassed. FakeGH/localbare only,
  nothosteddelivery. No paidrunactive. Fullgoalstillincomplete(Cursor/APIretry/
  remainingnativelifecyclegates). Nextfullnonpaidbaseline requiredpostprompt.

- ACTIVE98997: original live two-delivery regression after Builder inventory
  prompt clarification. Exact same authorized SF_TEST_LIVE_MIXED_CONCURRENT=1
  private cache / timeout30m / sf_e2e / p1 command as24517. Poll98997 only.
  24517 and48636 are terminal failures, not active. Prompt2571 PASS1.420s and
  phaseartifact PASS0.274s. Initial2571 predecessor34631 failed only the old
  expected prompt fixture; updated golden intentionally, preserving nil-repair
  exclusion. No schema/Store validation changes, no historical payload rewrite.

- Diagnostic24517 FINISHED FAIL335.896s: firstDone; sibling fresh Builder2/3
  both closed category protected_verification_changed. Worktree clean and go
  test passed. This proves artifact declares protected paths without amendment,
  NOT an actual file mutation. Prompt ambiguity is the repair hypothesis:
  changed_files must inventory implementation contribution, not entire branch
  diff containing preserved Reviewer files. Added explicit base-refresh/no-op
  guidance and exclusion of preserved verification. Prompt regression65937
  FAIL before fix; full workflowprompt+phaseartifact34631 running afterward.
  Artifact/Store validation unchanged. No paid calls currently active.

- ACTIVE24517: one diagnostic live two-delivery rerun with closed sf_e2e Builder
  validation categories and immediate paused-ticket diagnostics. Exact command
  SF_TEST_LIVE_MIXED_CONCURRENT=1 GOCACHE=/private/tmp/sf-multi-cli-go-cache
  go test -timeout 30m -tags sf_e2e -p 1 -count=1 ./cmd/sf
  -run '^TestCompiledLiveMixedConcurrentTicketsDeliverWithSeparateApprovals$' -v.
  Poll24517 only;48636 terminal FAIL. Closed diagnostic/tagged history tests28927
  PASS (providercoord0.557s,cmd/sf0.522s); ordinary providercoord22421 PASS6.004s.
  No production artifact-policy weakening, Cursor call, live DB/remote changes.

- Live48636 FINISHED FAIL exit1 (770.933s). Read-only exact disposable Store
  showed first ticket Done v12; sibling Paused v10 after fresh Builder attempts
  2/3 failed schema_validation (attempt IDs9/10). No second delivery. Harness
  waited for push until its bound instead of detecting pause; tagged loop now
  stops on Paused/Blocked/Cancelled and emits existing closed diagnostics.
  Root cause of Builder validation remains UNKNOWN; do not infer empty files.
  Added sf_e2e-only closed Builder error-category reporting (no raw errors,
  artifact, paths, or credentials; ordinary build noop). Targeted compile/test
  handle28927 active. No further paid run launched, no Cursor calls.

- ACTIVE48636: authorized live Sonnet5/Luna two-delivery trial, exact command
  SF_TEST_LIVE_MIXED_CONCURRENT=1 GOCACHE=/private/tmp/sf-multi-cli-go-cache
  go test -timeout 30m -tags sf_e2e -p 1 -count=1 ./cmd/sf
  -run '^TestCompiledLiveMixedConcurrentTicketsDeliverWithSeparateApprovals$' -v.
  Poll48636 only. Baseline76915 and fullCLI11119 (5.653s) terminal PASS. Paid
  Claude/Codex only; no Cursor, no live Relay/channel/remote mutation. Disposable
  models real, GitHub/approval/local bare fixtures. Includes new exact runtime/
  base/history assertions after fresh sibling build/review. No restart on silence.

- BASELINE76915 FINISHED PASS exit0; final worktreecoord135.826s. Includes
  test-only fake PR ref synchronization fix. Predates latest run consent flag
  (CLI race34824 PASS47.202, vet PASS, full CLI follow-up launched). Secret
  scan92897 PASS, repo-check PASS. New tagged history assertions unit/race pass;
  live two-delivery trial ready, not yet launched as of this checkpoint.

- CLI onboarding gap reproduced10293: run --accept-cost-estimates was unknown.
  Fixed internal/cli/run.go to forward explicit consent only to exact queued
  ticket.start, never submission or active replay. No default opt-in. New
  run_test regression validates queued/active and flag omitted/present. Race
  TestRun suite34824 launched. Updated CLI and first-ticket docs to remove
  stale "editor/model picker under development" claims and clarify estimates
  vs hard dollar cap, safe artifact repair vs unavailable API retry. Baseline
  76915 predates this tiny CLI change; scoped full CLI rerun required afterward.

- Full baseline76915 still ACTIVE, through daemon/runtimecontrol/ghrunner PASS.
  Tagged test-only additions since launch do not change default baseline source.
  Prepared live opt-in TestCompiledLiveMixedConcurrentTicketsDeliverWithSeparateApprovals
  (SF_TEST_LIVE_MIXED_CONCURRENT=1), NOT LAUNCHED. Strengthened both-delivery
  assertions validate historical rows unchanged, only fresh Builder/review,
  exact original binding/new base, durable attempt increments and bounded repair.
  Pure regression41503 PASS0.464; added same-entry repair/runner drift case and
  race16461 PASS1.640. Wait baseline before another native run to
  avoid shared-host process/SQLite contention. All provider calls remain idle.

- ACTIVE76915 full native baseline: GOCACHE=/private/tmp/sf-multi-cli-go-cache
  go test -p 1 -count=1 ./... . Poll same handle; no duplicate run. Live paid
  gates unset. Required post-fix broad verification, do not mark passed yet.

- PASS42045 (168.030s): compiled two-ticket separate-approval full delivery.
  Root cause verified: fake GH persisted original PR head/base after real Git
  push; explicit private-bare ref sync fixed fixture, production worker diff
  empty. Two tickets Done, same two PRs, two ready/two merge exactly, second
  protected tip exact. Unit18000 PASS0.463; focused race38099 PASS1.404;
  repo-check and secret-scan76291 PASS. All tests simulated models/GitHub;
  live two-delivery proof still pending. No diagnostic instrumentation remains.

- ACTIVE42045 original separate-approval compiled reproduction after TEST-ONLY
  fix: FakeGH.SyncPullRequestRefsFromBareForTest explicitly reads actual private
  bare head/base and updates existing PR snapshot, preserving metadata and
  mutations. Unit18000 PASS0.463. Diagnostic84541 failed255.981, repeatedly stage
  draft; source confirmed bare bridge only serves pre-PR refs, existing PR/head
  snapshot stays stale. Production worker temporary diagnostics fully removed
  (git diff empty). Poll42045; no paid calls. Root cause verification pending.

- ACTIVE84541: one diagnostic reproduction of separate-approval two-ticket
  compiled fixture. Temporary internal/publication/worker.go instrumentation
  prints only SF_TEST_PUBLICATION_STAGE + fixed stage on error (no raw error,
  provider/credential content). MUST remove after evidence captured. No policy
  fix yet; hypothesis is pre-push/history refusal vs fake PR stale head/base.
  Poll84541 only, no paid calls. Investigate skill active; exact fixture86446
  already reproduced failure. No live DB or remote involved.

- FAILED86446 (255.814s): TestCompiledConcurrentTicketsDeliverWithSeparateApprovals,
  native compiled SF, fake providers/GitHub, no paid calls. Extends first merge
  through sibling fresh publication on same PR, CI, review and separate
  approval. Outer timeout10m, bounded stage waits. Initial compile placement
  error fixed before this run. Handle terminal, no active test. Actual result:
  sibling publishing v10/r1, fresh Builder attempt2 completed, candidate recorded,
  clean checkout, go test passed, no artifact failures, two creates/one ready/
  one merge. Thus identical-file speculation did not prevent fresh build.
  Next diagnose refreshed publication vs fake GitHub PR/base response; do not
  infer a production defect before tracing exact publication refusal. Ephemeral
  Store cleaned by test; next diagnostic must expose bounded effect/error codes
  rather than raw provider claims. No production changes this turn.

- PASS1170 (127.312s): compiled fake-provider sibling base-refresh regression (no paid
  calls), TestCompiledConcurrentSiblingRefreshesAfterApprovedMerge. Uses real
  daemon/Store and disposable Git/GitHub; after first approval/merge waits for
  sibling effective worktree base to equal merged head, with advanced authority
  and no second ready/merge. Handle terminal; no active test. Test-only
  change, no production behavior changes. Full second delivery still pending.

- ACTIVE TEST4730: live two-ticket approval isolation after verified fixture
  count fix. Command includes -timeout30m so outer harness allows bounded
  phase waits and graceful cleanup (prior default10m could preempt cleanup).
  Same paid scope: Sonnet5/Luna, private Store, fake GitHub/bare only. No
  Cursor calls. Regression98685 PASS0.613; tagged race86902 PASS1.776.
  UPDATE:4730 finished PASS277.460 (test276.93). Both tickets reached approval;
  first approval delivered only first, sibling not authorized. Real Sonnet5/
  Luna, simulated GitHub; no hosted delivery or second-ticket merge claim.
  No active test4730 remains. Do not call initial Planner
  invalid-artifact exhaustion fixed. Skill investigate still verifying test
  correction, no production changes. All work uncommitted, goal active.

- DIAGNOSTIC99335 TERMINAL FAIL236.891: reached waiting_ci, then fixture's
  len(attempts)!=3 assertion failed with err=nil and huge claim dump. This is
  a verified test contract bug: allowed bounded repair adds an attempt. It is
  NOT evidence that initial74998 two-invalid Planner exhaustion is fixed.
  Removed claim dump and added validateCompiledAttemptHistory: exactly one
  successful terminal per expected phase, at most one prior invalid_artifact,
  attempts1/2, same binding/fence, no indeterminate retry. Final review same
  rule; overlap uses actual last attempt, not fixed index2. New unit regression
  rejects model/fence drift, third attempt, uncertain retry, missing review.
  Investigation scope limited cmd/sf test harness. No workflow/schema/runtime
  fix made. Schema proof-kind mismatch hypothesis remains unconfirmed.
  Native trial now stopped/cleaned; no live paid process active.

- ACTIVE ONLY TEST99335: same live concurrent approval fixture, one bounded
  diagnostic reproduction. Poll this exact handle; no other test remains.
  No fixes without artifact framing/schema evidence. Prior failed run74998
  ephemeral DB gone; safe reason reporting compiled1582 PASS0.627.

- BASELINE12177 FINISHED PASS exit0, final worktreecoord135.221. Predates latest
  picker/preflight; latest full CLI6916 PASS covers them. Native74998 remains
  failure, NOT overwritten by baseline success. Investigation skill read fully
  (initial cat truncated, missing ranges365-735 and330-365 read separately).
  Relevant prior learnings queried: no matching artifact root cause, only
  retry rearm stop tuple and terminal capacity history. Freeze script absent;
  no global tool/config/telemetry changes made. Investigation confined to
  test diagnostics, no root cause established, no production fix.
  Diagnostic reproduction of same paid two-ticket fixture launched next;
  records closed ProviderArtifactFailures reasons, never raw failed output.

- LIVE CONCURRENT74998 FAILED86.366: one Planner invalid_artifact twice,
  paused atv3/r1 after bounded repair; no PR/ready/merge, clean worktree.
  This is NOT a pass and no blind retry launched. Old fixture diagnostics
  omitted Store ProviderArtifactFailures closed reason codes; added safe
  artifact_reasons query to walkingSkeletonWaitStateBounded (no raw output).
  Fixture's ephemeral DB was cleaned on failure, so exact prior reason cannot
  be reconstructed. Need bounded diagnostic reproduction before attributing
  cause; may be provider artifact/schema rather than concurrency authority.
  Full CLI6916 PASS6.313, vet/repo/secret PASS. Baseline12177 still running;
  no paid process remains. Previous successful live single58549 remains valid.

- COMPILED CONCURRENT APPROVAL7858 PASS136.496 (test135.93): fake provider
  processes, native SF/commands, two PRs and fresh reviews, exactly first
  approved ticket Done/main matches, sibling not merging/reconciling/Done,
  exactly one ready/merge. Real-model counterpart74998 NOW RUNNING with
  SF_TEST_LIVE_MIXED_CONCURRENT=1, exact Sonnet5/Luna/local fixture state only.
  No Cursor calls. Poll same74998, do not duplicate. Model validation now also
  rejects aliases/provider mismatch before daemon with executable picker
  guidance; focused69321 PASS0.639. Full CLI+vet+repo/secret6916 running.
  Baseline12177 still running (predates latest picker/preflight; separate full
  CLI rerun covers those). Goal still incomplete: hosted both-ticket delivery,
  safe API retries, Cursor and remaining acceptance. All changes uncommitted.

- LIVE EXACT-MODEL58549 PASS214.508 (test214.11): compiled real ClaudeSonnet5
  Planner/Builder + CodexLuna verifier/final review using new explicit flags
  delivered guarded Done with existing exact identity/mutation/lease assertions.
  Local fake GitHub/bare/approval only, no hosted or live channel mutation.
  Added compiled concurrent approval isolation variant: both CI/final reviews,
  approve only first, first Done and exact main, sibling not merging/reconciling/
  Done, exactly two PRs/one ready/one merge. Fake-provider7858 RUNNING; live
  variant exists but NOT launched. Baseline12177 running through statemachine,
  processsupervisor68.123/providercoord5.881/publication95.304 PASS.
  No test process restarted. New edits this turn are tagged fixture/docs only.

- NEW NEGATIVE RETRY CONTRACT: providerjson TestProviderRetryHintsDoNotProvePreExecution
  proves zero turns/tokens/cost, retryable/Retry-After and claimed pre_execution
  metadata never convert an ambiguous terminal error into success/artifact
  repair. Race65794 PASS1.300. This does NOT implement safe transient retry;
  missing authenticated no-execution receipt remains open. No production edit.
  Live exact-model58549 remains running (no terminal output); baseline12177
  running through phaseartifact (Git198.382/GitHub63.408/multiprovider0.497 PASS).
  Poll both existing handles, do not duplicate or infer terminal from silence.

- INTERACTIVE MODEL PICKER IMPLEMENTED: providers qualify --preset select
  --models select selects both exact models before request; q/EOF/invalid
  either answer makes no request, JSON/nonterminal/conflicting flags refuse.
  Reviewer menu excludes Builder family; catalog explicitly not account or
  qualification proof. SupportedModels fresh slices on adapters; Store and
  daemon qualification remain authority. Normal36779 PASS0.484; race66562
  PASS1.654; sf_e2e compile24880 PASS0.570; vet/repo/secret82232 PASS.
  Compiled live fixture now passes explicit model flags. Authorized native
  Claude Sonnet5/Codex Luna exact-flag acceptance58549 RUNNING (real models,
  local fake GitHub/bare/approval, no live SF/Relay state). Poll same handle.
  Full baseline12177 still RUNNING through ghrunner PASS, cmd/sf91.904,
  CLI5.458/daemon22.452 passed; predates latest picker. No Cursor paid calls.
  All edits uncommitted, overall goal active; safe API retry/Cursor and full
  acceptance gaps remain. No production edits during the live trial.

- EXACT MODEL FLAGS IMPLEMENTED (uncommitted): providers qualify accepts
  --builder-model/--reviewer-model, including with presets. Daemon optional
  ProviderModelQualifier preserves legacy callback behavior but explicitly
  refuses model requests through legacy-only callback; error guidance keeps
  exact IDs. cmd/sf wires multi-provider model callback. Preflight resolves
  defaults, rejects unknown/alias/same-family before paid IO. Codex selected
  candidates reconstruct from Store qualification models on restart rather
  than only environment defaults; existing routes retained where matching.
  No schema change, env mutation, live runtime change or paid call.
  Test82671 initially failed selected-model fixture (missing sibling bundle),
  fixed to use adapterFixture;79936 PASS. Full affected48251 had only that
  stale fixture failure (CLI5.460/daemon22.548 PASS). New focused race98510
  PASS all four codexprovider/multiprovider/cli/daemon; focused vet PASS;
  repo/secret28324 PASS. Full UPDATED baseline12177 is RUNNING with native
  GOCACHE=/private/tmp/sf-multi-cli-go-cache go test -p1 -count=1 ./...;
  poll same handle, do not duplicate. Earlier56339 predates these edits.
  Interactive exact-model picker still incomplete; safe API retry and Cursor
  qualification remain open. Acceptance ledger/doc CLI updated, goal active.

- BASELINE56339 FINISHED PASS, exit0; final worktreecoord138.114. No test
  process remains. This full run predates provider editor; newer production
  editor independently covered by full config/CLI70559 PASS, config race24373
  PASS1.575 and latest picker-cancellation47701 PASS1.014. Focused vet,
  repo-check and secret-scan84090 PASS. No paid calls/live state/remote changes.
  Acceptance ledger updated; overall goal remains active/incomplete for exact
  model editor, safe API retries, Cursor qualification and remaining acceptance.
  All implementation changes remain uncommitted. Earlier running-status entries
  below are historical and superseded by this terminal result.

- EXISTING CONFIG EDIT DELIVERED LOCALLY (uncommitted): `config providers
  --project p --preset select|claude-codex|codex-claude|codex-codex` edits only
  provider preferences under canonical descriptor lock; original-byte backup,
  atomic rename, source/directory recheck, idempotence, cancelled-context refusal.
  Does not write Store generations or qualify/call providers; separate config
  apply required. Existing config required. Full config/CLI70559 PASS0.484/5.998
  after incomplete-stage identity-safe cleanup. Focused68196 PASS. Docs updated.
  Exact-model interactive editor remains open. Static checks84090 running;
  full baseline56339 still running (through workflowruntime115.397 PASS) and
  predates this editor; full changed packages independently covered by70559.

- EXISTING CONFIG EDIT FOUNDATION: new config/provider_edit.go implements
  bounded pure RewriteProviderPreset with pinned TOML AST ranges. Replaces
  only provider values, handles table/dotted/inline/quoted/partial forms,
  preserves unrelated bytes/comments, strictly re-parses and checks unrelated
  typed settings unchanged; no filesystem/Store actions yet. Tests initially
  found table Raw range unset in parser70054; fixed using actual key/header
  line span. Focused61303 PASS0.386, diff-check clean. Must still wire locked
  file install/recovery + future generation apply + CLI; do not call existing
  project editing delivered. New tests used private provider-edit cache only.
- BASELINE56339 still confirmed live, packages through ghrunner PASS (cmd/sf
  92.031,CLI5.202,daemon22.768). Poll same handle. It predates new provider_edit
  files, so those need independent validation/final baseline accounting.

- ACTIVE CURRENT-TREE BASELINE56339: native GOCACHE=/private/tmp/sf-multi-cli-go-cache
  go test -p 1 -count=1 ./... . Last output fake-provider PASS0.392;
  handle confirmed live. Poll same handle; don't duplicate or treat silence as
  terminal. No paid model flags enabled. Previous baseline70857 passed.
- REQUIREMENT AUDIT added docs/plans/2026-09-06-multi-cli-acceptance.md:
  full goal explicitly incomplete; distinguishes local fake-GitHub delivery,
  artifact repair vs missing safe API retry, initial picker vs existing-project
  editing, Store profile capacity vs live profile replacement, Cursor unavailable.
  Follow remaining implementation/acceptance priorities without shrinking goal.
  No production changes or paid calls this turn; latest edits documentation only.

- SHARED ACCOUNT CAPACITY: new providercoord/estimated_capacity_test.go proves
  two signed Claude-profile claims occupy capacity2, a separately qualified
  second model on same auth identity cannot admit third, no attempt consumed
  by refusal, DB reopen retains exclusion, cancellation releases only exact
  claim and third then admits on new model; remaining claim untouched and all
  terminal claims drained. Direct Store boundary, not live runtime rotation.
  Fixture first failed missing bound input fields50976; corrected to match
  production claimInput. Normal72563 PASS0.651; expanded cross-profile plus
  estimated retry tests race83033 PASS18.242. Focused vet/diff-check PASS.
  No paid models/runtime/remote mutations. No test running; goal still active.

- CURSOR GATE RECHECK: installed agent/cursor-agent both2026.09.02-c22c1a3;
  help + installed JS + current official hooks/permissions/sandbox docs do not
  establish all-hook isolation. Added docs/plans/2026-09-06-cursor-compatibility-gate.md
  with exact reopening requirements (no bypass). Added direct supervisor test
  known Sonnet/Luna/Grok Cursor bindings cannot reuse Claude/Codex policies;
  focused25281 PASS0.455. No model calls/auth changes/installed-file edits.
  Do not repeat this same spike absent new capability evidence; Cursor slice
  remains open/unavailable, not completed. Work can continue on remaining
  retry/setup/acceptance requirements. Diff-check clean; no test running.

- QUALIFICATION PRESET UX: `providers qualify --preset select` reuses numbered
  terminal picker; explicit codex-codex/claude-codex/codex-claude works in JSON.
  Rejects mixed preset+explicit flags, empty/unknown preset and cancelled/input
  failure before daemon request. Existing --builder/--reviewer remains valid.
  Picker discloses paid qualification; no hidden model selection/config writes.
  New focused74788 PASS0.643. FullCLI91427 caught stderr JSON regression;
  fixed by preserving Cobra input-error path, fullCLI32335 PASS5.274.
  Taggedcompile22649 PASS0.521 before final error-only change. Diff-check clean.
  No paid calls/live-state changes. Goal remains active, all edits uncommitted.

- LATEST STATIC GATES: full go vet82298 PASS (only known Darwin Seatbelt
  deprecation warnings); repo-check+secret-scan91797 PASS,644 commits and
  working files scanned/no leaks. No test process remains active. Latest
  new retry test covered by full provider race32388; earlier full baseline
  passed before this test-only addition/comment update. Goal not complete.

- ESTIMATED RETRY COVERAGE: new providercoord/estimated_retry_test.go uses
  signed credential-free Claude/Codex qualifications, real Store/coordinator,
  frozen config and estimate consent. Validates same-binding repair once,
  exhaustion at two, partial-write command uncertainty no relaunch, and exact
  outcomes/counts/unknown estimates after DB reopen + fresh coordinator.
  Normal44978 PASS1.006; full provider race32388 PASS coordinator127.665,
  claudeprovider1.493,providerjson1.384,multiprovider4.713. No model calls.
  General safe-transient API retry remains unimplemented: current CLI terminal
  errors do not prove pre-execution rejection. Do not label artifact repair as
  general API retry. All changes uncommitted; goal remains active.

- CONCURRENCY6701 TERMINAL PASS219.412s (test218.90): two real Claude Sonnet5
  planner/builder + Codex Luna verifier tickets reached separate draft PRs in
  fakeGitHub, exact model/role assertions, overlapping pipelines, separate
  worktrees, successful command proofs, no active provider/command leases.
  Uses real init --providers preset. No hostedPR/merge or Cursor calls.
  All paid handles terminal; no test currently running at this checkpoint.
  Full baseline70857 and latest focused46214 also PASS. Full goal remains open;
  remaining release/retry/config completeness audit and Cursor capability gate.

- SUPERSEDING CHECKPOINT: full baseline70857 exited0, all packages PASS
  (Store125.203,workflowruntime113.052,worktreecoord135.215). Latest setup/
  qualification-guidance focused46214 PASS config0.463,CLI1.222,daemon0.593.
  Failed qualification guidance now preserves selected mixed provider pair.
- ACTIVE PAID CONCURRENCY6701: SF_TEST_LIVE_MIXED_CONCURRENT=1 GOCACHE=
  /private/tmp/sf-multi-cli-go-cache go test -tags sf_e2e -p 1 -count=1
  -timeout 30m ./cmd/sf -run '^TestCompiledLiveMixedConcurrentTicketsReachPRs$' -v.
  Poll6701, do not duplicate. Two real Claude Sonnet5/Codex Luna tickets;
  isolated fixture DB/HOME/worktrees/fakeGitHub; no live Relay or Cursor calls.
  Not yet a pass; goal still active and changes uncommitted.

- BASELINE70857 STILL RUNNING: packages through phaseartifact passed, including
  cmd/sf95.117, daemon23.044, Git190.614, GitHub65.267. Poll same handle.
  repo-check+secret-scan82141 PASS (644 commits + working files, no leaks).
  Later config/CLI edits need focused rerun/final baseline audit; do not call
  the still-running pre-edit baseline proof of every latest source line.
- CONCURRENCY HARNESS prepared (NOT RUN): opt-in
  SF_TEST_LIVE_MIXED_CONCURRENT=1 TestCompiledLiveMixedConcurrentTicketsReachPRs,
  same compiled daemon,2 real Claude/Codex pipelines, separateworktrees/PRs,
  overlappingpipeline timestamps, exactrole/model assertions,20m/$10estimate
  per-ticket and16launch guard. Stops at waiting_ci; no approval/merge.
  Tagged compile71745/61870 PASS before latest picker edits. Live harness now
  uses actual init --providers instead of manualconfig, so future trial testsUX.
- PRESET EDGE FIX: missingconfig+providers must preserve detectedcommands,
  not suppressstackdetection and inheritGo. PrepareInitialConfig now detects
  and encodescommands beforeaddingproviders; unsupportedstack rejects.
  Targeted65130 PASS config0.416/CLI1.258. Invalidpreset rejectedbeforechannel
  setup. New `init --providers select` terminalonly numberedpicker, noautomatic
  action/qualification/billing, q/EOF/invalidinputnochanges;JSON/pipedrefuse.
  Picker54910 PASS1.399. Existingconfig/modelediting stillpending. Alluncommitted.

- ACTIVE BASELINE70857: native `GOCACHE=/private/tmp/sf-multi-cli-go-cache
  go test -p 1 -count=1 ./...` launched after full config+CLI94365 PASS
  config0.492/CLI5.102. Paid live environment flags NOT enabled. Poll70857
  until terminal; do not duplicate. Both paid delivery handles terminal PASS.

- REVERSE LIVE DELIVERY PASS:98292 PASS231.306s (test230.90). Real Codex Luna
  planner/builder + Claude Sonnet5 verifier/final reviewer, explicit persisted
  identity assertions, exactly4 completed phases, guarded Done/one local fake
  PR+ready+merge/no lease residue. Includes current fixturev2 schema probe.
  Both live directions now proven locally, NOT hosted GitHub/Relay acceptance.
  No model process/test remains running. Race20308 PASS Claude1.402,
  providerjson1.396,multiprovider4.731.
- INITIAL CONFIG UX implemented: `init --providers claude-codex` (also
  codex-codex/codex-claude), Planner follows Builder. Uses existing descriptor
  lock/no-overwrite Install/rollback and immutable RegisterProject, combines
  recipe flags; existing config must match, never overwritten. No qualification
  or billing minted; result exposes preferences+qualification_required.
  Generated bytes are prevalidated by LoadLockedProject before install.
  --check refuses mutating options. New targeted tests63254/82281 PASS
  (freeze/replay/no-qualification/no-overwrite/rollback/read-only). Full config
  39295 PASS0.606; native full config+CLI94365 launched, check terminal result.
  Interactive editing/model selection still not complete. Diff-check clean.

- FIRST REAL MIXED DELIVERY PASS: integrated50489 passed216.444s (test215.95),
  actual Claude Sonnet5 planning/build and Codex Luna verification/final review,
  compiled daemon/Store/gates/repository commands through guarded Done, local
  fake GitHub/bare only; exactly4 completed phases, one PR/ready/merge, no lease
  residue. Not hosted Relay delivery. This used policyv2 schema projection but
  pre-v2 fixture declaration. Qualification now exercises draft2020 schema and
  FixtureDigestv2, preventing recurrence; focused15690 PASS0.975.
  Reverse-role test added plus explicit persisted provider/model/family asserts
  for each live role. Tagged compile71785 passed (check tool terminal);
  reverse live launched next, see handle. No Cursor calls/live channel writes.

- TERMINAL CHECKPOINT: integrated62466 FAIL525.689s, clean blocked/planning
  command_error, zero GitHub mutations, cleanup completed. Schema fix native
  72283 PASS9.849, all four workflow schemas projection PASS; suites26799 PASS
  claude0.376/multiprovider0.584/providercoord5.249. Tagged compile11376 PASS
  0.596 (no tests), targeted vet completed exit0; diff-check clean. A new
  integrated run50489 launched after terminal cleanup and remains running;
  poll that exact handle, do not duplicate. No Cursor/live hosted mutations.

- CURRENT LIVE62466: compiled mixed qualification/runtime activation/start
  succeeded after timeout repair; private ticket reached blocked v3 with one
  Claude planning attempt failed/result_indeterminate, signed drained,
  diagnostic command_error. No fallback/retry/external mutations. Test still
  waiting original8m bound as of last poll; MUST poll62466 until terminal before
  another integrated run. Test daemon18812 private DB observed read-only at
  /private/tmp/sfh-3094186123/Library/Application Support/sf/dev/sf.sqlite.
  Do not touch live onboarding daemon84951. New future wait loop fails fast on
  paused/blocked/cancelled instead of wasting8m.
  ROOT CAUSE proven: Claude CLI rejects SF draft2020-12 declaration with
  missing_schema_ref (private native probe34536 FAIL3.845, no raw output).
  Added claudeprovider/schema.go common-subset translation to draft07, retains
  every constraint and original authenticated input; refuses newer/unknown
  keywords. Four workflow schemas tested. Native same probe72283 PASS9.849
  after fix. Supervisor Claude policy bumpedv2 with projection marker.
  Full Claude tests pass; provider suites26799 launched, needs terminal poll.
  Cursor now reports concrete unverified hook isolation before qualifier;
  daemon negative5724 PASS0.595, docs updated. No Cursor calls.

- LATEST LIVE/TRANSPORT: provider suites98682 PASS codexprovider1.556,
  multiprovider0.581, providercoord5.361. Live77347 terminal FAIL43.366:
  qualification returned daemon_unavailable. Source shows server transport
  retained 30s connection/handler deadline despite CLI4m; earlier successes
  just fit bound. serveConnection now extends only decoded/authenticated
  provider.qualify to4m, retaining30s decode/ordinary requests. Added socket
  handler-deadline regression (checks4m qualification and30s status without
  sleeping). Native full transport6567 launched; inspect terminal result.
  No live test remains running; no Cursor calls. Next rerun bounded mixed
  ticket after transport test passes. Full goal still active/incomplete.

- LATEST: live56747 FAIL34.502s, BOTH Claude Sonnet5 and Codex Luna qualified
  independent=true; runtime activation failed. ComposeQualified rejects duplicate
  exact identities; configuredProfiles yielded Luna for both Codex roles.
  LocalRuntimeCandidates now deduplicates configured model before New (same
  executable/auth source), preserving ComposeQualified ambiguity refusal.
  Provider suites98682 running/need terminal poll. No ticket was started;
  no Cursor calls. Previous live handle56747 is terminal, not running.

- MIXED ROLE CHECKPOINT: integrated38294 completed FAIL29.579s before ticket:
  Claude qualified successfully, Codex unavailable because both configured
  Codex defaults became Luna when reviewer override selected Luna. Separated
  configuredProfiles (individual candidates) from defaultProfiles (independent
  Codex-only pair); actual selected mixed pair still independently validated.
  New profile regression plus full codexprovider/multiprovider2190 PASS
  1.663s/0.537s; diff-check clean. Bounded live retry56747 RUNNING; poll that
  exact handle, do not duplicate. No Cursor calls or live channel mutations.

- LIVE MIXED HARNESS (2026-09-06): opt-in sf_e2e TestCompiledLiveClaudeCodexTicket
  uses actual installed Claude/Codex via temporary symlinks, private daemon
  HOME/Store/worktrees, fake GH/local bare Git, explicit Claude planner/builder
  and Codex Luna reviewer plus estimate opt-in. 20m ticket/8m state waits,
  30m test bound; no hosted Relay/live channel changes. Initial76042 failed
  qualification before ticket creation. Status-only isolated-HOME22384 failed;
  direct lookup diagnostic52795 confirmed Keychain lost credentials because
  lookupCLISecret forwarded private daemon HOME. Fix resolves OS user.Current
  home with effective UID/absolute-path check for security lookup only; provider
  environment remains private. Status-only44707 PASS3.789s. Integrated38294
  RUNNING after fix; no delivery verdict yet. Default hermetic harness path
  unchanged apart from bounded wait wrapper; no production runtime bypass.
  No Cursor calls; exact model charges unknown. All other handles terminal.

- ALL PHASE CONFIG ROUTING (2026-09-06): PhaseRunner admits exact configured
  Claude/Codex reviewer/builder, sends ExpectedProvider, and matches current
  and historical provider results to the immutable ticket role snapshot.
  Recovered amendment-review shortcut also rejects wrong provider. Shared
  configuredProvider rejects unknown/auto/Cursor/fallback arrays. Focused
  phase tests5529 PASS0.631; full48507 PASS providercoord5.676/workflowruntime
  172.974s (before subsequent Store guard compiled). New Store admission guard
  validates config_snapshot_bytes digest and exact role for every estimated-
  policy ticket, including its Codex roles; legacy non-opted-in Codex untouched.
  Focused97035 PASS2.849 after correcting initial column-name typo (33911
  failed before fix). Accounting fixture now explicitly configures cursor
  planner. Full Store66094 PASS126.193s. Vet49509 PASS for workflowruntime,
  providercoord, Store. Diff-check clean; all handles terminal. Docs now explain
  selected-pair versus immutable project config and exact no-fallback arrays,
  with manual config apply instructions (interactive selection still pending).
  No paid calls/live mutations; goal remains incomplete (Cursor policy,
  setup/model UX, retry completeness, and actual mixed ticket delivery).

- CONFIGURED PLANNER ROUTE (2026-09-06): discovered PlannerRunner, PhaseRunner
  admission and result readers still hard-coded codex despite mixed qualified
  composition. PlannerRunner now accepts exactly one configured codex/claude,
  sends ExpectedProvider to coordinator, and requires returned provider match.
  Coordinator rejects explicit mismatch before Begin/launch/fallback, and
  historical reusedInputMatches enforces the same expectation. New focused
  9087 PASS providercoord0.686/workflowruntime0.462: configured Claude positive,
  Codex compatibility, wrong returned provider rejection, and no-attempt/no-call
  mismatch. Full82313 providercoord PASS5.334s; workflowruntime failed only
  expired-budget integration because provider mismatch preceded deadline
  exhaustion. Moved mismatch check after existing deadline check (still before
  claim/launch); targeted10948 PASS0.650/0.602s for original expired-budget,
  mismatched route, configured Claude, legacy reuse. Legacy exact configured
  provider assertion46643 separately PASS0.427s. All handles terminal. Full
  workflow package not yet rerun after ordering fix; no full-green claim.
  Remaining PhaseRunner verification/build/final-review and Store-side exact
  configuration admission still need systematic wiring; do not call mixed
  ticket delivery complete. No paid calls or live mutations this turn.

- CURRENT REGRESSION / BILLING DISPLAY (2026-09-06): full Store21263
  PASS124.221s after signed selection tightening. New JSON/human status exposes
  durable estimated policy, actual_total_known=false, hard_dollar_cap=false,
  16 SF launches/45m per invocation (not API request count). Native targeted
  CLI/daemon52055 PASS0.622/0.772s. Full affected36238 passed multiprovider,
  providercoord, CLI; daemon failed six lifecycle fixtures at admission because
  credential-free mocks were named cursor/claude and lacked explicit consent.
  Investigation traced exact BeginProviderAttempt policy requirement; changed
  only shared mock names to fixture-cursor/fixture-claude. Full daemon56202
  PASS22.331s. Production consent/auth was NOT weakened. This recurring fixture
  pitfall also affected earlier Store/engine/publication fixtures: mocks must
  not impersonate production providers when testing unrelated lifecycle paths.
  New opt-in compiled-Claude qualification test uses real production gate,
  disposable Store signature/replay/stale-leader checks. Native24795 PASS
  27.211s (test26.60s); two model-bearing Claude CLI launches plus cancelled
  startup, actual charge unobserved. No Cursor call/live project changes.
  All test handles above terminal. This is qualification, not ticket delivery.
  Cursor installed190.index.js hook loader defaults loadProjectHooks=true and
  independently reads enterprise/team/user/project plus Claude settings hooks;
  disable-project-configs alone is not proof. Cursor remains unavailable.
  Goal active; no commit, remote, live daemon or channel DB mutation.

- MIXED SELECTION VALIDATION (2026-09-06): native daemon qualification and
  compiled Codex qualification regression38088 PASS1.528/5.358s; no model calls
  (fake fixtures). New real Store mixed selection test12093 found stale signed
  pair replay accepted after leader takeover. SelectProviderSet now verifies
  credential-bearing signatures/current leader inside same write transaction,
  including exact replay. Credential-free legacy fixtures unchanged. New
  multiprovider race16454 PASS4.796s: valid independent pair selects; missing/
  same-family preserves old selection; stale leader refuses. All handles
  terminal; diff-check clean. Broad Store regression remains needed after this
  selection tightening. Cursor role policy still unavailable.

- PRODUCTION MIXED COMPOSITION CHECKPOINT (2026-09-06): new multiprovider
  local package qualifies one Codex/one Claude role then atomically selects
  pair only after both pass independence. Claude uses fresh Supervisor qualifier
  and Store attestation validation. Codex exports single-role qualification and
  default runtime candidate discovery, preserving codex/codex path. Composition
  adds selected Claude identity via status observer + Adapter; Cursor refused.
  cmd/sf now uses multiprovider qualification/composition, daemon accepts Claude
  and no longer falsely says no model call on failures. CLI default qualify
  timeout4m; explicit custom timeouts preserved. Docs disclose model calls.
  multiprovider/Codex full normal39394 PASS0.550/1.515s. Compile-only99782
  cmd/sf/daemon/CLI PASS (no tests selected, compilation only). All handles
  terminal. No new live model/Store run.
  Need real Store mixed-pair tests, compiled qualification/gate acceptance,
  Cursor strategy, and broad regression before claiming feature complete.

- CALLABLE CLAUDE QUALIFIER (2026-09-06): new Supervisor.QualifyClaude runs
  fresh write/outside-denial, readonly, and cancel-at-recorder fixtures through
  a private nested supervisor, then reobserves exact runtime/auth and signs
  passing attestation. No Store selection wiring yet. Private qualification
  recorder grants no ticket authority. Cleanup keeps files if Close unclear.
  Invalid authority/model unit91576 PASS0.477s. Native qualification test
  session66800 EXIT0 PASS20.944s, two Sonnet fixture calls + cancelled
  startup (actual charges unobserved). Gate is test harness, not compiled
  Store E2E. No production qualification or live DB record issued yet.

- CLAUDE SUPERVISOR CANCEL CHECKPOINT (2026-09-06): added opt-in
  TestInstalledClaudeSupervisorCancellationDrains. Resolves canonical native
  CLI, status-only observes binding, registers staged runtime, constructs exact
  Claude invocation, launches through real Supervisor.Run credential branch,
  cancels after recorder/startup, requires <=10s join and verified Drain proof.
  Native24026 PASS4.141s. Non-opt-in87671 compiled/skipped (not live evidence).
  Recorder and gate are fixtures, not Store/full production-gate integration.
  Possible brief Claude API startup; actual usage unobserved, no Cursor call.
  Still need production qualification runner/signing/composition, not merely
  manual test results. All sessions terminal; diff-check clean.

- CLAUDE READONLY RECHECK (2026-09-06): native role fixture now shares setup
  for builder and final-review invocations. Review challenges Write/Edit and
  independently checks sentinel unchanged plus exactly-one-file inventory.
  Shared validator requires successful done=true artifact and exact bytes;
  model assertion cannot override a changed file. Race74402 PASS1.522s;
  authorized live Sonnet readonly23482 PASS10.590s (one additional Claude call,
  actual charge unobserved; no Cursor calls). Test remains opt-in, 60s bound.
  No signed qualification issued: still need production qualification runner
  and native cancellation/Store→Supervisor composition evidence. All sessions
  terminal; diff-check clean. No live SF DB/worktree changes.

- CLAUDE NATIVE ROLE RECHECK (2026-09-06): extracted strict reusable
  validateClaudeRoleEvidence into production supervisor package; live fixture
  uses it. Requires successful structured done=true, exact outside Read denial,
  independently read SF_WRITE_OK file, no canary in stdout/stderr. Rejects
  duplicate nested keys/paths and malformed evidence. Race17591 PASS1.530s.
  One authorized native Sonnet live probe61205 PASS11.271s (60s bound,
  disposable files, no raw output logged). This was one additional Claude
  model invocation; exact charge not observed. No Cursor calls. Still NOT
  signed production qualification: readonly-role/cancellation fixture execution
  and production qualification composition remain to implement. All handles
  terminal; diff-check clean.

- CLI ACCOUNTING OPT-IN (2026-09-06): `start --accept-cost-estimates` sends
  explicit boolean; ordinary start payload unchanged. Daemon strictly decodes
  start parameters separately from general ticket references and records
  policy BEFORE StartWithProjectOwnership makes queued ticket planning.
  Store opt-in now accepts queued or planning with zero prior attempts; exact
  policy replay stays allowed. CLI forwarding test PASS; native daemon99772
  PASS0.591s proves queued opt-in/start/replay. Sandboxed27893/3401 failed only
  socket bind permission; rerun used native approved test permissions. No
  model calls/live state. `run` convenience flag not added; use submit+start.
  Production Claude qualification/Cursor policy and config discovery remain.

- ESTIMATED COMPLETION CHECKPOINT (2026-09-06): Store
  ProviderResultAccountingAccepted now accepts verified charges OR exact
  opted-in durable estimated/unknown observation with UsageUnits==0. Complete
  uses it without setting UsageTrusted; coordinator uses it for success and
  clean invalid-artifact repair. Command ambiguity still refuses repair.
  Receipts explicitly label verified_charge/reported_estimate_v1/unknown.
  Real Store test now completes/replays/loads historical result with opted-in
  unknown estimate; missing/mismatched observation and untrusted currency
  reject. Normal53498 PASS1.191s; coordinator full55111 PASS6.602s; focused
  Store race5899 PASS19.437s including legacy completion test. All handles
  terminal, diff-check clean. No new model/live DB calls.
  Next: CLI opt-in/config wiring BEFORE any attempt (start scheduling must not
  race approval), production native qualification and Cursor role strategy.
  Earlier statements that trusted-only completion is unchanged are superseded.

- ESTIMATE OBSERVATION CHECKPOINT (2026-09-06): full Store89486 EXIT0
  PASS175.004s after fixture provider renaming (before newest estimate table).
  Extended unshipped v59 with immutable provider_cost_estimates: optional
  microUSD, nil=unknown, FK to attempt and opt-in. Record requires exact loaded
  claim/current fence/signed drain; exact replay only; no charge mutation.
  Coordinator records after drain; stale-fence error continues existing
  retirement path. Current trusted-usage success gate remains unchanged.
  Focused54641 PASS1.604s; accounting race23038 PASS14.549s; coordinator full
  normal15860 PASS10.194s. Added separate estimated-ceiling stop query in Begin
  afterward; latest focused54597 checks next admission stops at estimate cap.
  Production estimated success + CLI policy wiring remain pending, no paid
  calls/live data/commits. Do not call missing estimates actual zero spend.

- ACCOUNTING POLICY CHECKPOINT (2026-09-06): append-only v59 adds immutable
  provider_accounting_policies per-ticket opt-in, request/time limits, estimate
  ceiling copied from ticket, and approving fence. No legacy backfill. New
  Store ApproveProviderEstimatedAccounting requires current planning fence,
  zero prior attempts; exact replay allowed; mutation/delete rejected. Begin
  Claude/Cursor now requires this policy. Focused10199 PASS after correcting
  test qualification→runtime conversion. Full Store88064 EXIT1: shared unsigned
  setupProviderPair fixtures named cursor/claude hit new opt-in guard. Renamed
  those mocks fixture-cursor/fixture-claude, no production exception. Targeted
  93809 PASS1.621s (policy + V28 migration + candidate repair). Full rerun RUNNING
  session89486; poll that handle, no duplicate. No paid calls/live DB changes.
  CLI opt-in wiring, terminal
  estimate persistence and estimated success accounting are STILL pending;
  do not mark UsageTrusted on estimates or claim production qualification.

- REQUEST LIMIT CHECKPOINT (2026-09-06): BeginProviderAttempt now checks a
  code-owned Claude/Cursor limit inside its existing write transaction: all
  prior ticket provider attempts count against 16 SF launches, regardless of
  phase/provider/outcome; timeout <=45m. Existing attempt rows are durable
  reservations, with no new ledger/migration. Claude signed policy digest now
  includes request-policy identity and Invocation shares the timeout constant.
  Codex-only admission unchanged. These are SF launches, NOT internal API calls
  or a dollar guarantee. Accounting policy/production qualification remain
  incomplete; no trusted-charge bypass. Isolated count-query reopen/boundary/
  namespace/Claude-to-Cursor test PASS normal8026/race52691; registration tests
  race52691 PASS. Full Claude package race93894 PASS1.279s (initial regex did
  not select its invocation tests). All handles terminal. No paid calls or live
  DB changes. Next: persist explicit estimated accounting policy/results and
  teach coordinator/Store to distinguish estimates from charges without
  changing historical canonical PhaseInput encoding or trusted usage meaning.

- BILLING APPROVED (2026-09-06): user approved labeled estimates plus time/request
  limits, explicitly not a guaranteed dollar cap. $100 total Cursor testing
  ceiling remains unchanged; no additional live calls made. Goal resumed active.
  Adding separate optional provider-reported micro-USD estimate metadata; it
  does not set UsageTrusted and is not yet durable budget/admission policy.
  Missing estimate is unknown; decimal parsing rounds upward and rejects
  malformed/overflow values. Production qualification remains gated pending
  persisted accounting/limits and native qualification. Prior unanswered note
  below is superseded by this approval.
  Focused normal + race providerjson/claudeprovider/cursorprovider PASS after
  estimate handling, including unknown/null/duplicate/overflow/rounding and
  estimate-not-charge regressions; diff-check clean. No paid calls. Next:
  durable explicit estimated-budget policy and prelaunch request reservations;
  do not bypass current coordinator/Store trusted-charge gate to enable it.

- BASELINE FINISHED (2026-09-06): native full session74976 EXIT1 only for
  engine/publication legacy unsigned fixtures using AuthMode="test" and
  "subscription". Corrected fixture identities/modes in engine_test.go and
  publication/worker_integration_test.go; full affected-package rerun57907 EXIT0
  engine1.671s/publication95.118s. All other baseline packages passed, including
  cmd/sf100.354s, Git237.646s, GitHub82.633s, supervisor86.838s, Store197.739s,
  workflowruntime138.159s/worktreecoord135.102s. This is baseline + corrected
  package reruns, not a single green whole-suite invocation. Vet16836 EXIT0;
  repo-check/diff-check pass. All sessions terminal. No model/live SF calls.
  Billing decision still unanswered (also asked directly): may browser-login
  providers use clearly labeled cost estimates + time/request limits, or must
  they enforce verified dollar charges? Do not make UsageTrusted=true or enable
  production qualification/role execution by assuming estimates or zero cost.

- SHARED ROLE COMPOSITION (2026-09-06): providercoord.ComposeQualified now
  resolves exactly Store's signed Planner/Builder/Reviewer set, without
  fallback, and checks current signature/auth/runtime digests before supervisor
  registration. Codex ComposeProfiles uses it (default candidates still Codex;
  Claude/Cursor qualification/default discovery not yet enabled). Missing,
  duplicate, drifted or stale selection yields an unavailable coordinator.
  Real Store three-provider signed-fixture race79383 PASS4.779s; Codex full
  normal72123 PASS1.560s. Added explicit route.Fallback-empty assertion afterward.
  Baseline native `GOCACHE=/private/tmp/sf-multi-cli-go-cache go test -p 1
  -count=1 -timeout 10m ./...` is RUNNING session74976; poll that handle,
  do not launch a duplicate. Model opt-ins disabled, no live SF data mutation.

- CLAUDE RUNTIME OBSERVER (2026-09-06): supervisor ObserveClaudeRuntime now
  measures staged CLI version/capability flags and private OAuth status using
  only status/help commands, fixed Keychain handoff, private cwd/home, bounded
  outputs/timeouts and source/stage digest recheck. Returns binding metadata,
  not a qualification signature or billing proof. Required fixture digest names
  role/drain tests that still must be run by qualification. Shared providerjson
  Object rejects duplicate auth keys. Native installed observer76082 PASS3.523s;
  fixture72208 race PASS2.023s; focused15423 all PASS (supervisor1.864s).
  Full protocol race90795 PASS json1.306s/Claude1.310s/Cursor1.314s. No model calls; raw account
  status/credentials never logged. Full production qualification still pending.

- CLAUDE SUPERVISOR REGISTRATION (2026-09-06): RegisterRuntime now accepts
  Darwin Claude 2.1.263 with exact family/auth/policy and staged bundle digest;
  preserves Codex policy/digest behavior. Run selects the private Keychain OAuth
  environment for Claude instead of Codex's auth-home copier. Cursor remains
  explicitly unqualified. ProviderPolicyDigest exposes separate Claude policy;
  registration is NOT a passing Store qualification or billing authority.
  Focused56336 race PASS1.583s (initial26210 compile typo in test Close call
  fixed). Full native supervisor race74562 EXIT0 PASS77.243s. repo-check PASS.
  Coordinator now rejects Claude/Cursor missing/cross-provider auth modes.
  Initial full race1033 failed legacy credential-free fixtures named as real
  providers; renamed those route/identity fixtures fixture-cursor/fixture-claude
  in coordinator and adjacent daemon/workflow budget tests (no production
  exception). Full normal66773 PASS5.142s; full race77459 PASS107.777s.
  Targeted native daemon/workflow86867 PASS0.879s/0.756s for take/drain/resume
  and budget/restart regressions. Secret-scan28540 PASS644 commits + worktree.
  All test sessions terminal; no model calls or live runtime changes.
  Still required: trusted observer + signed qualification production route,
  billing decision, role configuration, retry integration and broad baseline.

- FRIENDLY AUTH READINESS (2026-09-06): auth status/login JSON now includes
  additive scope explaining login is not qualification/model independence/
  billing/readiness; authenticated entries point to channel-correct doctor.
  Human output has a dedicated concise auth renderer instead of raw JSON.
  CLI docs and stable/dev regression coverage updated. Focused auth CLI race
  session15522 EXIT0 1.771s; diff-check clean. No login, model, daemon, or
  remote mutation. Full CLI/baseline suite still pending.

- CLAUDE ADAPTER / CURSOR CONFIG REVIEW (2026-09-06): added exec-free
  contracts.Provider Claude Adapter with injected runtime observation, fixed
  role binding and re-observation rejecting model/family/version/binary/policy/
  fixture/auth drift. Parse rejects foreign identities and preserves unknown
  billing (UsageTrusted=false); observer errors are sanitized. This is not
  production qualification/composition. Focused race87980 EXIT0 Claude1.473s,
  providerjson1.304s, Cursor1.382s; diff-check clean. No paid calls.
  Cursor installed package exposes hidden --disable-project-configs for
  .cursor/cli.json only; separate 190.index.js hook loader reads project
  .cursor/hooks.json, .claude/settings.json, user/team/enterprise hooks.
  Therefore do not equate that flag or permissions config with hook isolation.
  Production Cursor role gate still needs measured startup/permission isolation.

- CURSOR STREAM BOUNDARY (2026-09-06): added bounded StreamArtifact parser
  requiring one browser-login init, exact qualified display model/worktree,
  consistent session IDs, and one terminal artifact at EOF. Rejects duplicate
  keys, model/auth/session drift, missing/truncated/repeated terminal output;
  never uses tool output as the artifact. Display labels are not catalog IDs
  and do not replace pinned invocation/runtime authority. Cursor official
  output-format docs establish this framing; production wiring and native
  compatibility remain pending. Focused race session97307 EXIT0: cursorprovider
  1.459s/providerjson1.333s. Initial default-cache attempt was sandbox-denied;
  rerun used /private/tmp/sf-multi-cli-go-cache. No model calls this checkpoint.
  Billing-policy async question remains unanswered: permit clearly estimated
  costs with time/request bounds, or require verifiable hard monetary cap?
  Do not infer browser login is free or weaken UsageTrusted meanwhile.

- CLAUDE ROLE POLICY / LIVE DENIAL (2026-09-06): canonical MatchesInvocation
  now shared by Claude proposal and supervisor check; tests reject broader
  tools/bypass/session/positional prompt/stdin/auth/output/executable changes.
  Run explicitly rejects unqualified Cursor instead of fixture fallback. New
  providers still cannot RegisterRuntime in production; no enabling bypass.
  Focused race53158 EXIT0 Claude1.437s/supervisor1.440s. Live native staged
  Sonnet5 role probe20714 passed9.843s; strengthened mandatory exact Read denial
  then70334 EXIT0 PASS9.409s. Both used private auth HOME/staged binary, disposable
  worktree, fake outside canary, allowed Write and structured output. Second
  pass requires CLI permission_denials Read/file_path match outside fixture;
  no canary leak. Tests cleaned all temporary copies. Two small Claude model
  calls; no Cursor spend/new project changes. This proves a native built-in
  permission probe, NOT Store qualification/billing/process-tree containment.
  Full registration/adapter/retry/configuration integration remains unfinished.

- ENVIRONMENT LIFETIME / FULL SUPERVISOR RACE (2026-09-06): private env cleanup
  now waits for BOTH Run return and cmd.Wait completion: early cancel preserves
  live credentials; early Wait preserves final artifact until Run reads it.
  New two-owner once-only cleanup tests include concurrent completion. Added
  vettedCLIEnvironment wrapping existing fresh-home policy + fixed credential
  handoff + exact qualified auth-digest check, with API/NODE_OPTIONS inheritance
  and credential replacement negatives. Initial environment test caught macOS
  /var→/private/var alias; canonicalized private HOME before strict validation.
  Focused race96781 PASS1.434s. COMPLETE supervisor race86608 native-host EXIT0
  PASS91.677s (-race -p1 -count1 -timeout5m ./internal/processsupervisor).
  All handles terminal. Plan checkpoint updated with actual installed/auth
  evidence. Production CLI registration/role policy/billing/retry/UI remain
  incomplete. No new model calls/spend; edits uncommitted.

- PRIVATE-HOME AUTHENTICATION PASS (2026-09-06): added supervisor-owned
  cli_credentials.go. Fixed Keychain-service allowlist reads only Claude Code
  credentials or Cursor access/refresh token items, bounded5s/16KiB, no secret
  argv/errors/logging. Claude access token passed via documented OAuth env with
  >46min expiry required; Cursor gets only access+refresh in private0600
  .cursor/auth.json with file credential store. Fresh0700 HOME required; no
  settings/history/hooks/MCP/cloud/API keys copied. Digest binds credentials,
  NOT billing entitlement. Fixture race38527 PASS1.484s. Explicit host opt-in
  test68037 EXIT0 PASS1.887s: BOTH installed CLIs report authenticated using
  isolated private HOME, no model/network-inference call. Test cleans temporary
  credential copies; no raw tokens/account details emitted. User browser logins
  were not altered. Production Run still not enabled for new providers.
  Next: couple environment lifetime to completeWait (existing cleanupEnvironment
  defer runs at early Run return), qualified registration/role invocation,
  billing policy and real tool/denial/cancellation tests. Full goal incomplete.

- SUPERVISOR CLI SNAPSHOT WIRING (2026-09-06): trustedExecutable can now carry
  a cliruntime bundle; stage uses its verified private copy and cached snapshot
  validation hashes the full closure. Existing refcount/retirement machinery
  owns it. RegisterRuntime admission and Run/env remain Codex-only: no unsafe
  enabling. CLI snapshot mutation/source-change tests88234 PASS race1.329s.
  Initial focused74181 failed existing blocked-launch test under sandbox;
  native-host rerun39555 PASS2.705s; complete native focused33489 PASS3.727s
  (race regex CLI|Replacement|Close|Register). Diff-check clean, all terminal.
  Auth isolation spike: env-i with private HOME loses BOTH browser logins;
  private HOME plus real CLAUDE_CONFIG_DIR also fails (looks for nested
  .claude.json). Normal Claude login rechecked intact (claude.ai/team). No
  credential contents read, no model calls. Need narrowly scoped credential
  handoff, not real unrestricted HOME or user-settings inheritance. Cursor
  package confirms CURSOR_CONFIG_DIR/CURSOR_DATA_DIR and credential-store
  selector exist, but semantics not qualified. Full goal remains incomplete.

- CLI RUNTIME SNAPSHOT (2026-09-06): new internal/cliruntime implements
  Claude single-file and Cursor full-version-directory authentication/staging.
  Private bundle fields, canonical digest includes kind/entry/member names,
  executable bits/sizes/byte hashes; trusted ownership/parents; symlinks and
  writable members refused. Explicit 4096 entries/256MiB file/1GiB bundle/64
  levels/context30s limits; directory enumeration itself bounded. Stage verifies
  source snapshot before copying and rehashes copied bytes; partial private
  stage cleaned, successful lifetime reserved for supervisor process completion.
  No supervisor admission changed yet. Fixture race42832 PASS2.091s before
  bounded-walk change. Installed opt-in5778 EXIT0 all tests5.207s: actual Claude
  1 member digest73c4baef...087c25; Cursor570 file+directory members
  digest41d786dd...05f70fa. Both copied/authenticated successfully, no process,
  credential or model calls. Race69737 after bounded-walk change EXIT0 PASS1.802s;
  all handles terminal. Full goal not complete, edits uncommitted.

- STRUCTURED RESULT / RUNTIME SPIKE (2026-09-06): Claude live schema smoke
  session10859 EXIT0 produced structured_output={ok:true} using Sonnet5,
  restricted/safe/no-tools/no-MCP. CLI list-price estimate $0.022203 (not actual
  subscription charge); auxiliary Haiku4.5 appeared again in modelUsage. Added
  providerjson.Command to classify supervisor exit/truncation/terminal/artifact
  failures without trusting reported cost/tokens or retaining raw transcripts.
  UsageTrusted remains false until authenticated billing policy is supplied;
  coordinator therefore refuses unaccounted completion/automatic repair. This
  decoder is not yet wired into a production adapter. Race48056 EXIT0:
  providerjson1.441s/claudeprovider1.305s/cursorprovider1.272s. Diff-check clean.
  Installed runtime discovery: Claude is one native Mach-O at version2.1.263;
  Cursor launcher is a shell script executing bundled node+index.js, with JS
  chunks/native modules/helpers in version directory. Cannot stage only launcher
  or hash only node; need bounded whole runtime closure. Supervisor currently
  RegisterRuntime/Run/vettedEnvironment still Codex-specific, no bypass added.
  All handles terminal; no new Cursor model calls. Full goal incomplete.

- BROWSER AUTH / MODEL IDENTITY (2026-09-06): new cursorprovider identity
  boundary consumes bounded status --format json, rejects duplicate/missing/
  malformed auth fields and trailing output. Observed status flags are
  authenticated/isAuthenticated/hasAccessToken/hasRefreshToken; no raw secrets
  inspected. Exact selected Cursor Sonnet5/Luna/Grok model IDs map to underlying
  families (anthropic-claude/openai-gpt-5.6/xai-grok), preventing cross-CLI false
  independence. Store now admits signed cursor_browser separately from cursor_api;
  tests refuse switching between them without matching qualification. No runtime
  enabled. Normal focused76550 PASS cursor .390s/Store .965s; after aligning Luna
  family with existing Codex family, race60123 EXIT0 cursor1.432s/Store14.430s.
  Full baseline not yet run; all edits uncommitted. Next: runtime strategy and
  credential isolation, structured artifact live probe, spend-policy authority.

- LIVE MULTI-CLI SMOKE (2026-09-06): user authorized installation and up to
  $100 TOTAL Cursor credits, then selected normal Cursor browser login instead
  of API key. User completed both logins. Official native installs verified:
  ~/.local/bin/claude 2.1.263; ~/.local/bin/agent 2026.09.02-c22c1a3 (legacy
  cursor-agent alias also installed). Claude auth status confirms claude.ai
  Team with normal Keychain access; sandboxed status falsely reports logged out.
  Four minimal model requests in empty /private/tmp/sf-provider-smoke.sECHj6
  passed with SF_SMOKE_OK and exit0: Claude claude-sonnet-5; Cursor
  claude-sonnet-5-low, gpt-5.6-luna-low, cursor-grok-4.6-low. Cursor used ask
  mode, sandbox enabled, trust limited to new empty directory; no project data.
  Claude used restricted/safe mode, no tools/MCP/session persistence. Its
  variadic --mcp-config requires -- before positional prompt (production uses
  stdin). Claude reports $0.0074268 list-price estimate, NOT subscription charge;
  modelUsage includes Sonnet5 plus auxiliary Haiku4.5. Cursor JSON has token
  usage but no dollar cost: exact credits spent not verified; do not claim zero.
  All process handles terminal. Browser-auth binding, spend accounting,
  supervisor qualification, tool/role integration and recovery remain unbuilt;
  smoke success is NOT production-provider GO. Prior install-blocker notes below
  are historical and superseded. Requested model preferences: Sonnet5 Claude,
  Luna/Sonnet5/Grok Cursor, no silent fallback.

- MULTI-CLI COMPATIBILITY GATE (2026-09-06): all test handles terminal.
  Race67313 EXIT0 Store22.704s/Claude1.323s/providerjson1.356s. Scripts17790
  EXIT0 repo-check/docs-smoke/secret-scan PASS. Full focused50526 also PASS
  (see checkpoint below). Full ./... and live gates NOT run for this patch.
  Rechecked PATH, Homebrew, /usr/local, npm-global and nvm executable paths:
  no Claude or Cursor CLI found. Installation/live-credit approval requested
  earlier remains unanswered across multiple goal continuations. Source
  groundwork is saved uncommitted; goal paused at the actual compatibility
  gate, not complete. Need user to authorize official CLI installation and a
  bounded Cursor live-credit budget (suggested $10), or supply installed paths.
  Do not invent runtime packaging/auth evidence or enable adapters from fixture
  tests. No active sessions, models, installs, charges or live state mutations.

- MULTI-CLI CONTINUATION (2026-09-06): session50526 EXIT0 full focused after
  signed-mode work: Store196.491s/Codexprovider2.273s/providercoord7.962s.
  New internal/claudeprovider invocation proposal keeps prompts on stdin,
  explicit model family, role-specific file tools, restricted/safe mode,
  no shell/MCP/permission bypass/session reuse. Not registered in production;
  qualification/runtime isolation and billing are prerequisites, not assumed.
  Tests19364 EXIT0 Claude .399s/providerjson .294s. Official current Claude
  CLI docs describe --safe-mode retaining auth (unlike --bare); live capability
  remains unverified without CLI. Race67313 RUNNING: GOCACHE=/private/tmp/
  sf-multi-cli-go-cache go test -race -p1 -count1 -timeout5m Store/claudeprovider/
  providerjson with regex ProviderSet|CredentialBearingProviderQualification|
  AttestedProviderAuthModes|ClaudeInvocation|TerminalArtifact. Poll exact handle.
  All edits uncommitted; no models/installs/live state. Full goal incomplete.

- MULTI-CLI SOURCE CHECKPOINT (2026-09-06): session31230 EXIT0 full focused
  Store161.628s/Codexprovider1.873s/providercoord6.540s for atomic role selection.
  Added credential-bearing qualification support for explicit auth classes
  claude_subscription/cursor_api; signed rows/runtime admission bind exact
  auth evidence and current supervisor. These classes do not imply zero cost.
  Credential-free legacy fixture path remains; production runtime registration
  is still Codex-only, and new adapters must never use that fixture path.
  New signed-mode/tamper/takeover/runtime SQL tests91505 EXIT0 Store1.442s.
  Current full focused rerun session50526 still live, exact command uses -p1
  -count1 -timeout5m Store/Codexprovider/providercoord; do not duplicate.
  Added providerjson bounded terminal envelope decoder for future Claude/Cursor
  adapters: strict consumed fields, duplicate top-level keys/multiple results
  refused, artifact vs terminal failure separated, no billing/launch authority.
  Its tests PASS .385s. It is not yet wired to an adapter. All source remains
  uncommitted; runtime adapters, generalized supervisor, live qualification and
  retry policy still incomplete. Install/$10 live-credit question unanswered.

- MULTI-CLI GOAL ACTIVE (2026-09-06), baseline 6f12229. Approved plan:
  docs/plans/2026-09-06-multi-cli-providers.md. Add Claude Code and Cursor
  alongside Codex, explicit roles/models, durable same-role retry, no fallback.
  Accounts: Claude/Codex paid; Cursor credits, actual CLI billing mode unproved.
  Host PATH/usual install checks found Codex only. Installation and $10 live
  Cursor test ceiling were asked but not yet authorized; no installs/models.
  Source progress: SelectProviderSet now authenticates/writes all three roles
  in one transaction; failed Planner cannot partially replace Builder/Reviewer;
  ProviderPair loads Planner too. New provider_set_test covers failed-set
  rollback, planner-only change, replay and explicit pair reset. Focused Store
  selection/qualification tests PASS (session68801 EXIT0,1.293s). Full focused
  Store/Codexprovider/providercoord command still running session31230 with
  GOCACHE=/private/tmp/sf-multi-cli-go-cache; poll that exact handle, do not
  duplicate. Code uncommitted; only qualification.go, new provider_set_test.go,
  plan and this memory modified. New adapters/attestation generalization/retry
  policy/runtime isolation/live matrix remain incomplete. No full-go/race or
  repository baseline claimed. Goal is not complete.

- CONCURRENCY ACCEPTANCE PASSED (2026-09-06): all test handles terminal.
  Compiled46457 EXIT0 twice90.34/90.54s: production CLI/daemon/native test
  executor + controlled provider/GH, two same-repo tickets with overlapping
  pipelines per trial, distinct worktrees/PRs, passing proofs, active provider
  and command leases zero, ready/merge zero. Four fixture PRs, NOT live delivery.
  Race12438 daemon55.459/runtime1.680 count3PASS; race56714 worktree57.034,
  Store113.276, publication99.443PASS; final slot-transfer race73293 11.975PASS.
  73293 fullGo/vet/repo/secret/docs/artifact/diffPASS. New real socket CLI test
  covers three concurrent runs, two admitted/third queued, exact replay,
  cancel retains capacity until drain, sibling unchanged and third then starts.
  Two-ticket/two-restart fixture preserves exact occupied slots and fences.
  Only fix was testIDs mutex; no production behavior changes. Initial harness
  race and malformed restart requests retained in scorecard, not hidden.
  Reproduce with make test-concurrency; exact target constituents ran, combined
  target dry-run checked. Plan/report dated2026-09-06 saved. Existing live
  runtimes/projects/DBs/remotes untouched; no merge/push/release. Next live-model
  concurrency and ten-ticket reliability remain separate, unproved gates.

- CONCURRENCY GOAL ACTIVE (2026-09-06), baseline bfae1fd. Approved bounded
  isolated capacity-two campaign; plan docs/plans/2026-09-06-concurrent-ticket-acceptance.md.
  New daemon concurrent_cli_run_test covers 3 simultaneous real CLI/socket run
  requests, exact replay, retained third queued, cancel/drain gate, slot reuse
  and sibling unchanged; also two occupied tickets through two daemon restarts.
  Normal initial10 repetitions passed; race found fixture testIDs unsynchronized
  counter (not production crypto IDs), fixed with mutex. Restart fixture initial
  requests incorrectly used test-only operator alias then omitted parameters;
  corrected to authenticated peer plus exact channel/project. Race12438 EXIT0:
  daemon55.459s/workflowruntime1.680s, count3 across new tests and existing
  two-worker control/lost-response/50-seed scheduler cases. Extended runtime test
  now adds next ticket after cancel, proving active sibling+new slot; NOT YET
  rerun after this addition. Extended real-Git worktree command-holder test
  proves release unblocks sibling and replay preserves exactly one worktree.
  RUNNING56714: race count3 worktreecoord/store/publication targeted contention,
  seeded capacity, stale terminal, lost create/push, two publishing restarts.
  Poll exact handle; do not start competing tests. No production behavior edits,
  live DB/project/provider/remote mutations, commits or push. Goal remains active;
  next finish targeted validation, baseline suite/static checks and scorecard.
  Boundary tests are not live-model ten-ticket reliability evidence.

- LOCAL CLI BETA ACCEPTANCE COMPLETE (2026-09-06): final licensed six-payload
  bundle67867 EXIT0, sourceb79166a4ea965b70914b0c460d191aaefa53d769,
  version0.1.0-dev.beta-check.2, `.context/beta-licensed.fC5WjT/bundle` and
  `/installed/sf-dev`; manifest/verify/exclusive install/identity/LICENSE cmp
  and release-build-smoke PASS. 15839 fullGo/bundle race/staticPASS;4537 explicit
  Go/Node/TS + compiled guarded/manual/takeover/coexistence + Python cold/setup
  workflowPASS;44741 eight explicit Python execution/crash casesPASS. All
  test/build handles terminal. Fresh ticket remainsDONE18/r5 approvals1,
  mergeintents1,leases0; no duplicate mutation or live DB/worktree surgery.
  Complete requirement map and limitations are at top of acceptance report.
  Source is now MIT licensed under authorized best judgment, notice included
  in bundles. No public release/push, PATH/service install or live daemon
  replacement. External3-user/10-ticket/10-minute targets remain unobserved;
  no unassisted reliability claim; Rails/general NodeTS/Claude execution remain
  unsupported, Python experimental/live-model unproved; third-party public
  distribution review/signing still pending. Goal completion is local beta
  implementation + requested acceptance/explicit limits, NOT stable/public v1.

- LICENSE PACKAGING15839 EXIT0: bundle race6.948s, fullGo (compiledCLI90.115s,
  bundle4.781s, CLI4.478s, daemon21.717s, GitHub64.232s, supervisor65.058s,
  workflowruntime111.349s), vet/repo/secret/docs/artifact/diff PASS. No active
  test handles. Commit all intended MIT/license-preserving package changes,
  then build/verify/install new six-payload bundle. Final local audit and
  explicit unsupported/external limits are recorded; no live rollout planned.

- FINAL LICENSE GAP REPAIRED, VALIDATING15839: user open-source direction +
  best-judgment authority used to add standard OSI MIT LICENSE, Copyright2026
  SF contributors; no public push/release. Makefile copies notice; bundle
  inventory requires6 payloads (including LICENSE); missing/tampered notice
  and installed-copy tests PASS11479 (5.542s). README/local-bundle docs updated.
  15839 LIVE: bundle race then fullGo/vet/repo/secret/docs/artifact/diff.
  Re-poll exact session. Earlier build47718 at3f6179a was five-payload internal
  artifact; MUST rebuild clean licensed successor after15839 before closing
  goal. No further runtime feature change needed. Final report explicitly
  leaves external adoption/reliability/third-party notices/signing/public
  distribution/unsupported combinations pending, not claimed passed.
  44741 Python fault acceptance and4537 tags both terminalPASS;86129 staticPASS.

- FINAL LOCAL BETA AUDIT: 47718 EXIT0 clean bundle at3f6179a verified/installed
  `.context/beta-final.rYUXD6/{bundle,installed}`, identity0.1.0-dev.beta-check.
  71948 EXIT0 installed CLI prepared pinned Python under new fault-home (no
  DB/project/daemon). 44741 EXIT0 all8 explicit Store Python execution/crash
  cases28.578s; ambiguity retained quarantine until independently gone, no
  reusable abort results, no surviving group/lease residue. 4537 and41791 are
  terminal PASS. Fresh live read-only acceptance DB: PR2 ticket DONE18/r5,
  approvals1/intents1/leases0. No runtime rollout, real model/PR action or
  release in final validation. Requirement-by-requirement audit is at top of
  docs/reports/2026-09-05-cli-onboarding-acceptance.md. External3 users/10-ticket
  reliability/10-minute setup remain unobserved; unsupported stacks/providers
  remain explicit; no LICENSE exists (choose before public distribution), no
  publisher authenticity, universal rollback or public release is claimed.
  Final documentation/static check pending before saving this receipt.

- ACCEPTANCE4537 EXIT0: native Go18.87s / Node13.45s / NysaPure13.17s all
  PASS (no skips); make test-compiled-e2e PASS241.088s (guarded/manual,
  takeover, stable/dev); explicit Python cold setup/workflow PASS68.240s.
  No active test suite remains. Docs now explain deliberate bundle replacement,
  stable-only automatic migration backups, no universal rollback/dev backup.
  Next clean-source final bundle, use installed CLI in a new private HOME to
  prepare pinned Python, then explicit Store fault/crash fixture with returned
  code-owned digests. No live project/daemon changes or public release.

- DIAGNOSTICS COMMITTED899db19 (clean immediately after commit). Explicit
  acceptance4537 LIVE: native verbose Go/Node/Nysa-pure gate tests, then
  make test-compiled-e2e, then SF_TEST_PYTHON_CLI_DOWNLOAD=1 make test-python-e2e.
  Go PASS18.87s, dependency-free Node PASS13.45s (not skipped); Nysa pure
  started. Poll4537; no competing suite. Full41791 is terminal EXIT0.
  Next after tags: explicit prepared-Python Store execution/crash fault fixture
  requires SF_TEST_PYTHON_SNAPSHOTS/DIGEST/LOCK (ordinary Go skips it), then
  fresh clean-source bundle/verify/exclusive install and final requirement map.
  No live runtime/project edits, provider call, PR action or release. Existing
  installed onboarding11 and delivered ticket stay unchanged.

- VALIDATION41791 EXIT0: review diagnostic targeted race PASS; full Go PASS
  (cmd/sf91.242s, daemon21.628s, processsupervisor66.210s, publication94.826s,
  Store122.632s, workflowruntime111.856s, worktreecoord131.571s), vet,
  repo-check/secret-scan/docs-smoke/diff PASS. Commit diagnostics checkpoint
  then run explicit tagged compiled/isolation + Python acceptance next. The
  diagnostic gap is implemented and validated, not rolled into the live daemon.

- REVIEW DIAGNOSTICS IMPLEMENTED, VALIDATING: eight intended source/test/doc
  files add a snapshot-scoped LatestReviewDiagnostic (newest attempt only,
  immutable historical result authentication), bounded/redacted daemon
  evidence.review_diagnostic and historical CLI rendering. Five findings max,
  JSON512 runes/human160 with truncation; no raw artifact/transcript, no
  transition/recovery authority changes. Store test covers missing, foreign
  channel, leader change, failed latest and tampered result. Daemon/CLI tests
  cover redaction, controls, bounds, historical labels and unavailable verdict.
  Focused48885 EXIT0; targeted race in41791 PASS. Full validation41791 is LIVE
  (go test -p1 ./... then vet/repo-check/secret-scan/docs-smoke/diff); cmd/sf
  PASS91.242s, CLI PASS4.359s so far. Re-poll this exact handle, do not start a
  competing suite. No commit yet and no installed/live daemon change. Earlier
  focused test failed only fixture column attempt_id, fixed to provider_attempt_id.
  Full objective audit still needs explicit skipped/tagged gate accounting
  (Python test-python-e2e and compiled coexistence), not ordinary Go PASS alone.

- BETA COMPLETION AUDIT 2026-09-06: fresh GitHub read confirms PR2 MERGED
  at f6e2afc; no repeated approval/mutation. Corrected stale terminal-pending
  summaries in the beta plan and acceptance report, preserving failed-trial
  history. docs-smoke and diff-check PASS. Full goal remains active: next
  concrete implementation gate is sanitized final-review findings in status.
  Source inspection of daemon/view.go confirms it exposes phase outcome and
  artifact counts, not the typed review finding. Use authenticated immutable
  result readers, bounded/redacted text and historical labels; never raw model
  output or a second lifecycle authority. Then finish current evidence audit
  for installed onboarding, supported profiles, fault tests and explicit gaps.
  No new runtime/model action or live project change in this audit.

- REAL TERMINAL DELIVERY 2026-09-06T20:49:36Z: PR2 ticket
  SF-bf816eaad3a060153d28c99b3a3be7ef is durable DONE v18/r5. Installed
  onboarding11 `.context/onboarding11.1XBwtL/installed/sf-dev` at5216d13;
  build/verify/install90250 EXIT0. Daemon30089 LIVE isolated HOME
  `/Users/sofiagonzalez-2/Projects/.sf-beta.24jdFP`, leader8. Qualification86834
  EXIT0 independent Luna5.6/GPT5.5, model_call_made=false. Normal CLI
  `resume SF-bf816e --json` EXIT0 observed=true/attempted=false, then scheduler
  reconciled old confirmed effects to current fence, merge_observed v17 and
  reconcile_pass v18. SQLite: approvals1, merge_intents1, capacity leases0;
  attempts planning1/verification1/build1/review2 (no new model attempt).
  GitHub fresh confirms PR2 MERGED f6e2afc9117bf5b101742314ff9736335f417b0b,
  exact source e9d40f0922130e72edc6d439f7ce6b78d71862bc. No DB/worktree edit,
  budget reset, duplicate approval or merge. This is repaired delivery, NOT
  unassisted reliability success. Full beta goal requires remaining requirement
  audit; external unfamiliar users/general unsupported stacks are not passed.

- VALIDATION38718 EXIT0: targeted race35.539s; fullGo PASS (Store222.815s,
  compiledCLI212.577s, publication200.971s), repo-check/secret-scan/diff PASS.
  Commit review-control merge recovery and build onboarding11 next. Isolated
  daemon is still stopped; existing merged PR2 needs ordinary startup/rearm.

- MERGE CONTROL REPAIR VALIDATING38718: source-only helper
  recoveredReviewMergeControlFrom recognizes retained typed review-block stop
  only for merging; authenticates blocked recovery, review ledger prefix,
  immutable review completion, exact approval and authority/current endpoints.
  Startup still checks normal merge intent/effect predecessor; PostPublication
  rearm repeats proof under sealed/drained authority and merge evidence.
  Completion reader now authenticates historical prefix (later merge recovery
  is checked by callers), instead of rejecting legitimate later ledger rows.
  Extended real Store fixture includes approval, terminal merge effect/proof,
  close/reopen/Fence and ActivateRearm/openRuntimeAdmission. RED6183 startup,
  RED26968 historical reader after merge fence, GREEN81477 all4 scenarios.
  Added tampered stop leader and missing review recovery negatives; diagnostics
  removed. 38718 LIVE: targeted race then fullGo/repo/secret/diff. No rollout,
  daemon remains stopped after onboarding10 startup refusal. No live SQL edits.

- ONBOARDING10 ROLLOUT: committed d191b75ae9e6377fac49ff6ae0fb10d6c103db8d.
  Build/manifest/verify/install/version20250 EXIT0, installed at
  `.context/onboarding10.vdzluo/installed/sf-dev`. Old daemon4460 Ctrl-C clean
  EXIT0. New daemon76305 EXIT7 before socket: invalidate recovered runners:
  ErrPublicationEvidence. Do NOT repeat startup blindly. Isolated daemon is
  currently STOPPED, PR2 remains merged. No direct DB edits.
  Read-only diagnostic28406 proves the repaired approval and confirmed-merge
  endpoints now both authenticate v15/r4/L6; normalPostPublicationRecoveryPredecessor
  returns L6,true,nil. But postPublicationRecoveryBaseline returns error first,
  so lease.go:737-741 never reaches the valid normal predecessor. Old review
  control row remains after review_pass/approval; investigate exact control
  shape authentication, not a blanket swallow/reorder of malformed evidence.
  Temporary diagnostic removed. Next add regression for full recovered-review
  pass -> approval -> confirmed merge -> reopen/Fence, then scoped repair.

- VALIDATION57535 EXIT0: recovered-review regression race28.589s, full Go
  suite PASS (Store264.445s, Git359.465s, compiled CLI228.460s), repo-check,
  secret-scan and diff-check PASS. Commit recovered-review completion repair
  and build fresh onboarding10 bundle next; ticket PR2 already merged, no
  second approval or merge needed. Only normal restart/reconciliation pending.

- RECOVERED REVIEW FIX VALIDATING: runtime_control.go completion reader now
  selects the latest completed review source at/before the pass endpoint,
  requires one result there, authenticates its canonical pass/head/proof, and
  when older requires the exact signed recovery row plus full historical
  provider-result chain. Existing CI/control endpoint authentication remains.
  Extended review_blocked_rearm_test reproduces real close/reopen after a fresh
  passing review, normal rearm, reuse to review_pass, and completion read.
  RED43178 failed completion reader; GREEN10391 passed (2.867s). Added missing
  recovery-row negative. Session57535 LIVE runs regression race then full Go,
  repo-check, secret-scan, diff-check. Await that exact handle before commit or
  isolated daemon rollout. No production DB/worktree edits or new merge call.

- PR2 APPROVED AND MERGED 2026-09-06T20:18Z: user explicitly said
  "Approve PR #2". Fresh GitHub check confirmed head
  e9d40f0922130e72edc6d439f7ce6b78d71862bc and test SUCCESS. Installed onboarding9
  approve with full ticket ID SF-bf816eaad3a060153d28c99b3a3be7ef and --head
  succeeded, state merging v15/r4. Short ID in nonterminal approve was refused
  without mutation; scripts must use full ID. SF itself merged PR2 at
  f6e2afc9117bf5b101742314ff9736335f417b0b, mergedAt20:18:35Z. No direct gh merge.
  Deadline had elapsed but Store accepted approval; no budget change.
  GitHub PR is MERGED/non-draft. Ticket is NOT done: scheduler worker_failed,
  state merging v15. Merge, pr_ready and git/protected-ref-fetch all confirmed.
  Daemon4460 was polled live (no output, handle alive), not restarted.

- POST-MERGE DIAGNOSIS 2026-09-06T20:24Z: temporary read-only Store diagnostic
  TestLiveMergeDiagnosticReadOnly PASS46469 proved singleRecoveryMergeIntent
  and guarded observation match succeed; approvalRecoveryEndpoint fails, hence
  confirmedMergeRecoveryEndpoint fails. Temporary test removed immediately.
  runtime_control.go reviewCompletionRecoveryEndpoint requires result at
  waitingVersion-1 (13). Actual passing review attempt2 is v12/L5/R3; signed
  recovery ledger bridges v12/L5/R3 to v13/L6/R4, and review_pass event is v14.
  Thus exact-version result query rejects legitimate recovered verdict reused
  by prior fix746216a. Next: regression for recovered review -> approval ->
  guarded merge observed; authenticate immutable pass via signed recovery to
  review endpoint, preserving candidate/proof/cardinality. No production edits
  yet. Full beta goal remains incomplete. Earlier "approval missing" notes below
  are historical, superseded by this explicit approval and remote merge.

- DISPLAY VALIDATION35083 EXIT0 2026-09-06T16:47Z: full CLI race50.856s,
  full Go suite, repo-check/secret-scan/docs-smoke/diff PASS. Display-only fix
  ready/saved; no installed daemon replacement required. Live read-only DB
  recheck: waiting_approvalv14/r4, approvals0, merge_intents0. Explicit PR2 human
  approval remains missing after repeated goal continuations; do not treat those
  messages as consent. Pause goal as blocked on that required decision after
  saving this checkpoint. Reply approval can resume normal installed onboarding9
  `approve SF-bf816e --head e9d40f0922130e72edc6d439f7ce6b78d71862bc --json`, then
  observe merge/reconciliation (never direct gh merge). Recheck head/CI/deadline
  first. Daemon4460 LIVE, isolated HOME `.sf-beta.24jdFP`; deadline18:32:16Z.
  No DB/worktree surgery, budget reset, approval or merge has been performed.

- DISPLAY DIAGNOSTIC PATCH UNCOMMITTED: while PR2 approval is outstanding,
  investigated retained blocked_code shown as current "Blocker" even after
  authenticated recovery. CLI human renderer now labels it "Recorded blocker"
  whenever state is not blocked; JSON/Store/lifecycle/approval unchanged.
  Six-line source/test diff only internal/cli/{human.go,cli_test.go}. Regression
  85333 RED; full CLI race35083 PASS50.856s. Same35083 LIVE runs full Go suite,
  then repo-check/secret-scan/docs-smoke/diff; compiled CLI89.386 and daemon21.537
  already PASS, remaining packages still running. Wait exact35083, no duplicate
  suite/source edits. Commit after exit0; no daemon rollout required for this
  cosmetic change. Installed onboarding9 daemon4460 remains live. User has NOT
  answered explicit PR2 approval request; automatic goal continuation is not
  approval. Preserve waiting_approval and original deadline until user responds.

- REAL WAITING_APPROVAL 2026-09-06T16:37Z: repair committed746216a, complete
  onboarding9 bundle/verify/exclusive install/version PASS56958 at
  `.context/onboarding9.g17C0B/{bundle,installed}`. Old daemon21758 Ctrl-C EXIT0;
  new isolated daemon4460 LIVE, same durable HOME/env, leader6. Qualification
  91310 PASS independent pair, no model call. Normal `recover SF-bf816e` EXIT0
  observed prior handoff, then scheduler reused passing attempt2 and committed
  review_pass reviewing->waiting_approval v14/r4. SQLite confirms review
  attempts2, approvals0. Fresh gh read PR2 OPEN/draft, exact head
  e9d40f0922130e72edc6d439f7ce6b78d71862bc, required test SUCCESS. Next ask user
  simple approval for this PR2, then normal installed CLI approve and observe
  guarded merge/reconciliation to done. Old PR1 approval is not reusable.
  Deadline18:32:16Z unchanged. No DB/worktree edits or new model attempt.
  Goal remains active, terminal delivery still unproven. Current blocked_code
  projection retains historical review_needs_operator even though state is
  waiting_approval; fix/report diagnostic separately, never treat it as state.

- VALIDATION98415 TERMINAL EXIT0 2026-09-06T16:35Z: targeted Store race47.246s,
  full Go suite (Store122.522, workflowruntime108.553, worktreecoord130.381s),
  vet/repo-check/secret-scan/docs-smoke/diff all PASS. Current reader repair
  ready to commit and bundle as onboarding9; no source changes during run.
  Important learning: current final-review readers must use the same strict
  CI/publication authority as launch and transition, including pending CI
  self-transitions before first recovery. Generic lifecycle audit differs.

- VALIDATION98415 LIVE 2026-09-06T16:22Z: full Store37767 EXIT0 (132.807s).
  Focused race Store PASS47.246s for recovered review, pending-CI tamper and
  connection-scoped transition. Same98415 now runs full `go test -p 1 ./...`,
  then vet/repo-check/secret-scan/docs-smoke/diff. Wait this exact handle; do not
  start another suite or edit Go files. Source remains two-file reader repair;
  docs memory/report also intentional. Live daemon21758 still onboarding8,
  reviewingv12/r3; read-only count review attempts2, approvals0. No source commit
  or bundle yet. After exit0 commit exact four files, build verified onboarding9,
  stop only21758 via Ctrl-C, restart same durable HOME/env, qualify if required,
  and observe existing passing review reused (no manual Signal/DB mutation).
  New startup will advance fence; verify runtime admission normally before any
  recover request. Ask approval for PR2 only once normal waiting_approval.

- REVIEW PASS READER FIX 2026-09-06T16:18Z UNCOMMITTED: live reviewer attempt2
  completed with pass/no findings at v12/L5/R3, but worker reuse returned stale.
  Read-only diagnostic proved candidate/FinalReviewAuthority/finalReviewerResult
  all authenticate while LatestReusableProviderAttempt fails. Root: current
  result readers still use generic initial-lifecycle recovery, which rejects a
  pending CI self-transition before review followed by startup recovery.
  Regression with pending CI + block/recover + reopen failed63061 in both reopen
  cases. provider.go now authenticates current final-review results through the
  existing strict FinalReviewAuthority and exact reviewed-head/proof validation.
  Regression20674 PASS; read-only actual DB probe47495 now reuse PASS (no writes
  or model call). Temporary opt-in diagnostic removed. Two source/test files
  changed; full Store37767 is running. Next full/race validation, commit, verified
  onboarding9 bundle, normal isolated replacement/reuse. Daemon21758 remains
  onboarding8; no approval/merge or budget change. Deadline18:32:16Z unchanged.

- ONBOARDING8 RECOVERY SUCCEEDED 2026-09-06T16:08Z: source1423edb,
  verified bundle/install `.context/onboarding8.UEs4Ss/{bundle,installed}`
  PASS8553. Old94247 Ctrl-C exit0; replacement21758 LIVE same durable HOME/env.
  Qualification25641 PASS independent pair leader5. Normal recover exit0,
  observed=true/attempted=false, reviewingv12/r3. Normal status confirms fresh
  GPT5.5 review attempt2 active since16:08:19.076326Z. Original candidate/PR2
  retained; no budget reset/worktree repair/DB edit/approval. Read-only status
  watch7748 LIVE (large JSON stream; compact latest object when polling).
  Next observe reviewer verdict. Only fresh passing review may reach approval;
  user approval for old PR1 does not cover PR2. Deadline remains18:32:16Z/$20.

- REPLACEMENT RUNTIME REPAIR UNCOMMITTED: regressions67604 RED proved both
  missing volatile stopped entry in fresh Controller and compensated authority
  rejection. Controller.Rearm now reconstructs runtime.Drain after loading the
  durable stop, before proof/installation (does not overwrite Store stop).
  reviewBlockedRearmFrom accepts only compensated authority on exact signed
  recovery chain, with prefix validation for historical segment and full
  validation to current. Merge-retry counter-shape dispatch now restricted to
  merging/reconciling, including startup helper; review has overlapping counters
  but different authority. Regression48996 PASS Store1.359/controller.701;
  includes reopen after compensated install, new attempt2, fresh-result replay.
  Real daemon/controller/scheduler regression16920 PASS .886s: recovery installs
  token, Store stays sealed before Begin, exact Tick opens and enters worker.
  Added authority-runner tamper refusal. Final race+full validation38621 EXIT0;
  race Store22.159/daemon6.057/controller7.867s; full Go/vet/repo/secret/docs/diff
  all PASS. Six intended source/test files changed. Live94247
  still onboarding7, reviewingv11/r2 sealed; no repeated recovery, DB edits or
  review/approval/merge. Next wait38621, commit/bundle onboarding8, normal restart
  and recover through the compensated path. Maintain original ticket budget.

- ONBOARDING7 LIVE ROLLOUT 2026-09-06T15:43Z: validated source d848697,
  bundle/install `.context/onboarding7.xzLCPG/{bundle,installed}` all verify
  PASS60550. Old41299 Ctrl-C exit0; new isolated daemon94247 LIVE same durable
  HOME/env. Qualification16027 exit0 independent pair leader4. Normal recover
  returned runtime_rearm_failed, observed=true/attempted=false (CLI replay
  discrimination fixed), no fresh review. DB reviewingv11/r2; sealed stop
  remains9/L3/R1 but authority is now11/L4/R2. This proves ActivateRearm advanced
  authority and its install callback failed/compensated by sealing.
  Source: runtime.ControlBundle.ApplyRearm -> admission.Rearm requires an
  in-memory stopped entry. Controller.Rearm after restart loads durable stop
  but does not call runtime.Drain/Stop, so fresh scheduler can lack that entry.
  Needs real controller/runtime restart composition regression (existing Store
  and daemon tests use callback/fake runtime). Also reviewBlockedRearmFrom
  requires authority==stop, so compensated failed installation now blocks its
  own retry; support only authenticated current authority plus same original
  stop/ledger, never arbitrary drift. Do not blindly recover again or edit DB.
  Next reproduce these two composition conditions before further source fix.

- REVIEW REARM FOLLOW-UP: affected full Store/daemon/runtimecontrol suite83756
  completed PASS (121.334s/21.476s/7.467s). Regression76952 then proved old
  needs_operator review was reused after recovery in all three cases: same
  leader, replacement leader, reopen after committed recovery. Provider result
  selector now excludes that consumed verdict only after authenticating the
  result/current CI/recovery chain and locating the typed block/recover pair.
  Regression12293 PASS (1.117s) also opens runtime admission, completes a fresh
  review as attempt2 without budget reset, and proves new result replay works.
  Six source/test files are intentional and uncommitted (including untracked
  review_blocked_rearm_test.go); no runtime rollout/DB edits/approval performed.
  Full integrated validation10632 TERMINAL exit0: GOCACHE=/private/tmp/sf-gocache3
  go test -p 1 ./... then vet, repo-check, secret-scan, docs-smoke, diff-check
  all PASS. Targeted recovery race6852 exit0: Store20.101s, daemon6.029s,
  including ordinary completed-review recovery preservation. Ready to commit
  and build onboarding7; old isolated daemon41299 remains live/sealed.
  Goal remains active. Next validate, checkpoint, bundle and recover through
  the new CLI replay path; do not claim the real ticket delivered yet.

- LIVE REARM BLOCK after validated prompt rollout: commit91a3632, bundle
  `.context/onboarding6.qzmmk5/{bundle,installed}` manifest/verify/install/version
  all pass (61286 exit0). Old isolated daemon35724 Ctrl-C exit0. New daemon
  41299 LIVE, same durable HOME `.sf-beta.24jdFP`, same env as onboarding5;
  normal qualification18606 exit0 independent Luna5.6/GPT5.5, leader3.
  Normal `recover SF-bf816e --json` returned runtime_rearm_failed exit4.
  IMPORTANT committed transition despite response attempted=false: ticket now
  reviewingv10/r1, blocked_code retained review_needs_operator; runtime sealed
  generation2 stop=authority=(v9,L3,R1). No fresh review/approval/merge.
  Do not repeat recover blindly (only accepts blocked). Existing candidate
  and PR unchanged; status runtime observations empty.
  Read-only diagnosis: eventv9 typed_blocker reviewing->blocked; eventv10
  operator_recover carries canonical sf.provider-blocked-recovery/v1 bridge
  priorL2->L3, sameR1. Controller.Rearm dispatches Reviewing to
  PostPublicationRearmProof, whose ordinary branch requires stop.runner>1
  and pause/take->drained->resume triplet. This exact provider-blocked recovery
  has neither; it is an authenticated distinct shape, not malformed data.
  Existing validProviderBlockedRecoveryGap/validateProviderBlockedRecoveryAdvance
  authenticate the shape, but rearm lacks composition. Next add a narrow real
  Store/controller regression before repair; preserve sealed/drained counts,
  exact event/phase/candidate/CI authority, and safe lost-response replay.
  No production fix yet. CLI reports attempted=false and suggests recover
  after committing reviewing: separate diagnostic/replay defect to cover.

- PROMPT VALIDATION81372 TERMINAL exit0: full serialized Go suite (including
  workflowprompt regression), vet, repo-check, secret-scan, docs-smoke and
  diff-check all pass. Source unchanged during run; acceptance report updated
  to retain both trials and diagnostic limitations. Next clean commit/bundle,
  isolated daemon replacement, qualify and ordinary recover of SF-bf816e.
  Live status rechecked blockedv9/r1 review_needs_operator; no approval or
  Store review substitution. Original reboot trial remains failed acceptance.

- PROMPT VALIDATION ACTIVE81372: full `go test -p 1 ./...`, vet, repo-check,
  secret-scan, docs-smoke, diff-check. Go source frozen. Guidance-only probe24494
  returned needs_operator claiming JSON schema prevents tool calls. Added explicit
  final-response-only schema clarification. Probe27063 completed actual source
  reads and ended pass (43.024s), though intermediate JSON chatter still occurred.
  No deterministic reliability claim. Temporary live probe moved to
  `.context/diagnostics/codex_read_probe_test.go.txt`; not in normal test tree.
  Production diff only workflowprompt.go + its test. Read minimal PATH/final-only
  schema regression, preserve no-escalation/read-only rules. Wait81372 terminal
  before commit/bundle/rollout; live daemon35724 remains old onboarding5 and ticket
  remains blocked. Probe verdict is never substituted for Store review.

- EXACT-PROMPT DIAGNOSTIC21787 exit0 (23.510s): original recorded review
  prompt/schema under staged runtime/vetted env emitted early needs_operator,
  then `rg` command-not-found, then successful source/test reads and final pass.
  Parse uses OutputLastMessage, not first agent_message, so no selection defect
  found. This is model/tool behavior evidence, not replacement Store review.
  Narrow final-review prompt clarification now UNCOMMITTED in workflowprompt.go:
  minimal PATH, optional rg absence != sandbox denial, cat/sed fallback, no
  escalation, exact denied-read evidence. Regression test RED before/GREEN after
  (workflowprompt .335s). Needs live amended-prompt probe and full validation;
  no live daemon rollout/state recovery/approval yet. Temporary diagnostic test
  remains uncommitted; retain or remove deliberately after investigation.

- STAGED MODEL PROBE42058 exit0 (22.364s): temporary opt-in diagnostic
  `internal/processsupervisor/codex_read_probe_test.go` copied authenticated
  Codex bundle, used production vettedEnvironment and exact read-only flags,
  GPT5.5, and a narrow read-two-files prompt. Both command_execution items
  completed and read expected source/tests. Original blanket sandbox claim
  still not reproduced. Probe lacks original review prompt/schema, so do not
  infer original root cause or pass final review. Test file is UNCOMMITTED
  diagnostic, not full validated production change; next compare exact original
  prompt/schema with bounded retained tool evidence. No permission relaxations,
  ticket mutations, second review or approval performed. Live ticket stays blocked.

- REVIEW INVESTIGATION: direct native `codex sandbox --permission-profile
  sf-guarded` with exact SF read-only filesystem/network config successfully
  ran `/usr/bin/wc -c` on both candidate files (304/1566 bytes, exit0). No model
  call or file mutation. Therefore blanket filesystem denial is NOT reproduced.
  Supervisor uses staged codex + code-mode-host, vetted HOME/TMPDIR, fixed argv;
  no outer Seatbelt wrapper in Run. Remaining hypothesis is model tool dispatch
  or staged-runtime/environment behavior. Need bounded same-invocation inspection
  probe with retained sanitized tool outcome before any permission/prompt fix.
  Investigate skill read fully; no fixes applied without confirmed root cause.

- DURABLE TRIAL REVIEW BLOCK 2026-09-06T14:37:06Z: final reviewer completed
  attempt1 with needs_operator/operator. Ticket now blockedv9/r1,
  resume=reviewing, code review_needs_operator. Typed review finding says its
  shell tool was rejected by read-only sandbox/current approval policy, so it
  could not inspect candidate files. This is provider-reported evidence, not
  yet a confirmed root cause. CLI show/status omit the finding (only blocker
  and completed phase), so a read-only typed_artifact query was needed.
  Next investigate actual read-only Codex tool composition without weakening
  sandbox or overriding review. No review pass/approval/merge should be claimed.
  Candidate remains e9d40f0..., draft PR2/CI green; no model retry or state repair.

- DURABLE TRIAL PROGRESS 2026-09-06T14:37Z: ticket SF-bf816e... reached
  reviewingv8/r1; planning, verification, build all completed attempt1 with
  Luna/GPT5.5/Luna. Candidate e9d40f0922130e72edc6d439f7ce6b78d71862bc.
  Factory created draft PR2 in sf-cli-beta-acceptance-20260905; fresh gh read
  confirms exact head, OPEN/draft, CI test SUCCESS (run34039674698). Independent
  GPT5.5 final review active attempt1. No code/worktree/DB repair or approval.
  Daemon35724 live; status watcher67907 also live/read-only. Do not confuse
  publication/CI with approval or terminal done. Existing trial failure retained.

- DURABLE TRIAL STARTED 2026-09-06T14:32:16Z: normal `run` submitted/started
  `SF-bf816eaad3a060153d28c99b3a3be7ef`, onboarding-durable, planningv2/r1.
  Ticket source `/Users/sofiagonzalez-2/Projects/.sf-beta.24jdFP/ticket.md`:
  Count distinct nonempty values, explicit exact string semantics/no mutation,
  4h/$20, guarded. Doctor initially failed only missing provider pair; ordinary
  qualification71823 exit0 selected Luna5.6 Builder/GPT5.5 Reviewer independent,
  no model call during qualification. Repeated doctor exit0 guarded_eligible.
  Daemon35724 remains the exact installed onboarding5 process in durable HOME.
  No approval exists for this new ticket/head. Old PR1 approval is not reusable.
  Poll normal status/daemon, do not start duplicate ticket or reset budgets.

- DURABLE ACCEPTANCE SETUP 2026-09-06: new owner-only HOME
  `/Users/sofiagonzalez-2/Projects/.sf-beta.24jdFP`; project at `project/` beneath
  it is a new clean clone of private `nysa-company/sf-cli-beta-acceptance-20260905`
  main at 0086045. Remote PR1 freshly confirmed merged; no remote writes.
  onboarding5 bundle reverified, installed version/source exact. Normal init
  registered `onboarding-durable`. Daemon session43061 status leader1/tickets0,
  Ctrl-C exit0; restarted session35724 now active, status leader2/tickets0,
  projects1/quarantine0. This is clean daemon restart evidence, NOT an OS-reboot
  test or delivery. Same env as saved handoff except HOME above (no /private/tmp).
  Next: normal doctor/qualification, then separately tracked bounded acceptance
  ticket through CLI. Old backup/trial remain untouched and are not repaired by
  this new registration. Do not reuse old trial's approval for a new head.

- POST-REBOOT 2026-09-06: original `/private/tmp/sf-onboarding-acceptance.R2QvPm`
  is gone. OS boot differs from checkpoint. Private DB backup quick_check=ok;
  quarantine1/checkpoint1/recoveries0 and mergingv15/r6 preserved. Installed
  onboarding5 exists and version/source match. No daemon started, no backup
  restored, no authority changed. Mergeproof requires original worktree snapshot;
  a replacement clone/path is not authenticated recovery. This acceptance trial
  delivered its GitHub PR but has failed local persistence/terminal acceptance.
  Do not erase that result or claim done. Next: durable-location acceptance setup
  and explicit restart/reboot retention checks; assess lost-checkout recovery as
  separate scope without DB surgery. First-ticket/reboot docs now warn against
  temporary HOME/project roots and database-only preservation.

- REBOOT HANDOFF SAVED 2026-09-06T14:15Z: user will restart the Mac.
  Authoritative next-session instructions: `context/checkpoints/20260906-141545-acceptance-reboot-recovery.md`.
  Native process/socket inspection found no running sf daemon and no owner of
  the isolated acceptance socket; session75245 is no longer available. No signal
  or reboot was issued in this save turn. Read-only DB still reports quarantine1,
  checkpoints1, recoveries0, ticket mergingv15/r6. Installed onboarding5 exists.
  Private SQLite backup `.context/reboot-backup.Lt2dWD/sf.sqlite` passes quick_check;
  it is preservation only, not a replacement runtime or recovery proof.
  No code changes/tests/remote writes. Goal remains blocked pending same-host
  reboot and verified normal reconciliation to done. Older bullets below are history.

- RECOVERY CHECKPOINT LIVE (original isolated acceptance host): source commit
  3f36eb799cf6a4f3a40941ab26ddac5b3cfa82e8; verified full dev bundle + exclusive
  install at `.context/onboarding5.Ga8mYI/{bundle,installed}`. Installed version
  0.1.0-dev.onboarding5, exact source commit, darwin/arm64. Build session88508
  exit0. Old daemon PID91647/session36311 was exact-path/socket/DB verified,
  gracefully SIGINT-stopped, exit0. New daemon SESSION75245 is active using
  the same private acceptance HOME and trusted per-user TMPDIR; no stable or
  other project runtime touched. Normal startup/migration succeeded.
  CLI prepare saved checkpoint; second prepare observed=true/attempted=false;
  same-boot recover refused host_reboot_required with exit3/no mutation.
  Read-only DB: quarantine1/checkpoints1/recoveries0, ticket mergingv15/r6.
  No host reboot, quarantine retirement, effect confirmation or new approval.
  NEXT EXTERNAL PREREQUISITE: user saves work and manually reboots original
  Mac. Never reboot automatically, forge boot facts, delete the row, or replace
  DB/worktree. After reboot start exact installed onboarding5 binary in SAME
  HOME `/private/tmp/sf-onboarding-acceptance.R2QvPm/home`, with TMPDIR
  `/private/var/folders/01/fnjrykjs5k721nqj3wf04t3r0000gn/T`, actual GH_CONFIG_DIR
  `/Users/sofiagonzalez-2/.config/gh`, CODEX_HOME `/Users/sofiagonzalez-2/.codex`,
  capacity2, PATH including .local/bin,/opt/homebrew/bin,/usr/bin,/bin,/usr/sbin,
  /sbin. Run ordinary `daemon cleanup recover --json`, then status and normal
  qualification/reconciliation if requested; no model rerun or merge approval
  is implicitly needed. Ticket SF-543bc4cd3b9a9a6291c2bbc7ca20b3b1 is NOT done.
  First external-reboot prerequisite encounter, not three-turn blocked yet.
  Remaining external adoption/unsupported combinations are explicitly reported.

- FINAL50924 TERMINAL exit0: targeted recovery/CLI/hostidentity race tests,
  full `go test -p 1 ./...`, vet, repo-check, secret-scan (617 commits/no leaks),
  docs-smoke and diff-check all PASS. No Go source changed during this run.
  Recovery+diagnostics source is ready to commit and package as a new private
  dev bundle. No live checkpoint/migration/quarantine retirement or host reboot
  performed. Acceptance PR is merged but ticket is not `done`; next real steps
  are validated isolated bundle, ordinary daemon startup, checkpoint prepare,
  MANUAL host reboot, then ordinary cleanup recover + exact reconciliation.

- FINAL-TREE validation SESSION50924 is running: targeted -race for hostidentity,
  Store/daemon cleanup and CLI exit classification, then `go test -p 1 ./...`,
  vet/repo-check/secret-scan/docs-smoke/diff-check. Hostidentity race passed
  1.397s, Store22.909s, daemon16.642s and CLI1.629s all PASS. The chained full
  normal suite is now running. Source is frozen. Poll50924; do not start
  another suite. No terminal final-tree verdict yet.

- Frozen recovery validation94280 TERMINAL exit0: all Go packages, vet,
  repo-check, secret-scan (617 commits/no leaks), docs-smoke, diff-check PASS.
  Follow-up CLI-only exit classification regression60834 RED: new recovery
  errors incorrectly defaulted to7. Explicit reboot/host-inspection action3
  and evidence-refusal policy5 mappings now implemented; native CLI/daemon
  regression30742 PASS (0.488s/1.318s), including exact same-boot exit3.
  No recovery authority rules changed in this follow-up. Final-tree focused
  race and integrated validation are next; do not claim those already passed.
  No live daemon/DB/quarantine changes, no host reboot, no remote writes.

- Read-only review during frozen94280 found a remaining CLI classification
  defect: response.go has no cases for host_reboot_required,
  host_identity_unavailable, external_cleanup_recovery_refused, so all default
  to ExitInternal7. After94280 is terminal, add table regressions + explicit
  action3/action3/policy5 mappings, assert exact same-boot CLI exit3, and run
  focused then final-tree validation. Do not edit Go while94280 is active.

- Fresh frozen-tree integrated validation SESSION94280 recovered from this
  thread's own tool transcript; write_stdin successfully resumes it. CLI,
  daemon/runtimecontrol, bundle, ghrunner and Git (214.659s) have passed so far;
  full command is still running, not a terminal pass. Continue polling94280.
  No new suite launched and no Go source changed during this run.

- Fresh frozen-tree integrated validation is CONFIRMED LIVE: shell PID12241,
  Go PID12248; last native process inspection shows git.test PID38727 running
  under that Go parent. Command is full serialized Go ./... then vet,
  repo-check, secret-scan, docs-smoke and diff-check. Tool session ID was lost
  when previous output was truncated; no terminal result is available yet.
  Do not duplicate the suite or infer success from elapsed time. No Go source
  changed after launch. Acceptance report's top evidence map now correctly
  distinguishes verified GitHub merge from still-unproven local `done`;
  earlier approval-pending narrative is explicitly historical.

- CLI/daemon cleanup composition IMPLEMENTED, still uncommitted. New commands
  daemon cleanup prepare/recover via owner-authenticated socket, exact channel,
  no caller host fields; runtime close serialization + Store current leader.
  Prepare replay reports observed; same-boot recover requires host reboot.
  Native real daemon/CLI tests60564 PASS (daemon1.239s/CLI0.650s/Store1.351s),
  lost response + restart53939 PASS1.376s. Held-gate same-boot regression44558
  RED (timeout hid reboot requirement); inspection-before-gate plus full
  revalidation-under-gate55678 PASS Store1.425s/daemon1.325s. Missing channel
  leader now maps ErrStaleFence rather than no-quarantine; final focused69177
  PASS1.444s. Held-gate test deadlines raised to1s for host-load tolerance,
  no timing-success assertion changed. Docs include manual reboot sequence; no host reboot,
  live checkpoint, live schema migration or quarantine retirement performed.
  Broad78125 TERMINAL exit1 (Store build file-list mismatch after in-flight
  additions), not valid final coverage. Source must now freeze for fresh full
  validation. No other test remains after69177. No remote updates authorized.

- Full78125 INVALIDATED by concurrent source additions: old Go file enumeration
  omitted new external_cleanup_recovery.go while seeing schema dispatch58,
  producing undefined migrationV58 in Store. This is NOT a full pass. Focused
  fresh invocation30530/56244 compiles/passes new Store code. Remaining old
  workflow fixtures are draining naturally (go PID66142/shell66139 observed);
  do not restart another broad suite or claim old run validates V58. Finish
  composition, then freeze source for a fresh full run. Prior full43135 and
  committed cancellation0598b6f remain valid. Lesson: new files + schema wire
  cannot be edited during go test package discovery/build; earlier packages
  may compile from different source lists. No daemon was signalled/stopped.

- V58 recovery Store foundation UNCOMMITTED: new external_cleanup_recovery.go
  and tests; schemaVersion/checksum/dispatch/testMigration wired to58; required
  columns and immutable checkpoint/recovery triggers. Prepare obtains OS
  identity and records exact quarantine without taking/unlocking latched gate.
  Recover obtains OS identity, acquires mutation gate, validates current daemon
  leader plus canonical checkpoint, requires same machine/different boot,
  atomically appends audit + CAS deletes exact quarantine. Effects untouched.
  Focused new tests30530 PASS1.667s + hostidentity0.419s; expanded changed-row
  and rollback tests56244 PASS1.855s. No CLI/daemon wiring or live migration yet.
  Worktree-creation schema test now compares schemaVersion instead of hardcoded57.
  Full78125 predates V58 source: it is NOT final-tree coverage for this foundation;
  let it terminate, then run new full validation after composition. No public
  API accepts caller-supplied host identity; only private test helpers do so.
  Remaining: daemon/CLI checkpoint + recover surface, reopen/replay integration,
  integrated tests, new isolated dev bundle, then real checkpoint/host reboot
  prerequisite (never reboot automatically) and exact merge reconciliation.

- Recovery foundation added (UNTRACKED internal/hostidentity, not yet wired):
  fixed /usr/sbin/ioreg + kern.bootsessionuuid; hashed platform UUID, 64KiB
  output cap, 5s context plus bounded pipe wait, no inherited environment/raw
  identifier output. Isolated native test52691 PASS0.653s; duplicate malformed-key
  hardening rerun1800 PASS0.881s. No Store/CLI recovery capability yet.
  Plan records exact-quarantine checkpoint now -> different boot on SAME host
  before retirement. This handles legacy missing identity without guessing:
  checkpoint never clears, unknown old writers cannot survive later reboot.
  No automatic reboot authorized/performed. Need immutable Store checkpoint,
  same-host/changed-boot/current-authority/CAS/gate tests and CLI composition.
  Full78125 still tests diagnostics; it started before new package existed, so
  do not claim its package enumeration covers hostidentity. Native reader has
  separate focused evidence only. No live state changed.

- Cancellation prevention committed as 0598b6f after full43135 exit0.
  Follow-up diagnostics are implemented but UNCOMMITTED: ObserveMergeIntent
  preserves viewNumber errors (red48867, green6780); doctor adds read-only
  external_mutation_recovery as a mandatory guarded-readiness check. New
  tests cover clear/quarantined/unavailable/unconfigured inspection and a real
  Store latch remaining unchanged. Initial doctor fixture omissions corrected;
  sandbox-only Unix bind failures40922 reran on host2056 PASS (CLI1.953s,
  GitHub0.607s). Broad final-tree validation78125 is RUNNING; poll exact session,
  do not start a second suite. Include untracked merge_observation_error_test.go
  when committing. No live daemon, DB, quarantine, worktree or remote changed.
  Remaining acceptance blocker is safe recovery of existing external quarantine,
  not another merge approval. Goal stays active, ticket not yet done.

- Follow-up source audit: ObserveMergeIntent combines viewNumber error with
  nonmatching merged identity and maps both to ErrExternalMerged. This hides
  ErrProcessCleanup before any GitHub call when the durable quarantine is set.
  Next isolated regression should require preserving the original read error,
  while retaining ErrExternalMerged for an actually observed identity mismatch.
  Full validation43135 exited 0: all Go packages, vet, repo-check,
  secret-scan, docs-smoke and diff-check PASS. External quarantine schema has only reason/timestamp, no
  process or boot identity; prevention is not authority to clear legacy rows.

- Cancellation cleanup VERIFIED:
  internal/github/cleanup_cancel_test.go reproduced ErrProcessCleanup when
  Run cancels the request before Cleanup; red2828. github.go now invokes
  Cleanup with context.WithTimeout(context.WithoutCancel(ctx),5s), leaving
  Run and subsequent commands canceled. Focused new/contradictory/quarantine
  tests28312 PASS; full github+ghrunner48002 PASS63.144s/12.556s. New test is
  included with this checkpoint. Broad validation43135 exited 0 (Go ./...,
  vet,repo-check,secret-scan,docs-smoke,diff-check). Existing live quarantine remains
  untouched and unresolved; this prevention fix cannot retroactively prove
  the old child drained. Daemon36311 last live, leader6, already quarantined;
  do not requalify/restart repeatedly. Goal not complete.

- Root cause CONFIRMED with env differential: same disposable proof binary
  passes normal environment, fails env-i without TMPDIR with "runner execution
  helper is unsafe", passes env-i + trusted per-user macOS TMPDIR. ghrunner
  snapshotExecutable uses os.MkdirTemp and accepts sticky ancestors; Git
  secureExecutableParents rejects all writable ancestors. Without TMPDIR,
  gh snapshot under /private/tmp fails Git validation. Original daemon launch
  env was author-provided scrubbed env; do not weaken Git check. Daemon36311
  restarted with TMPDIR=/private/var/folders/01/fnjrykjs5k721nqj3wf04t3r0000gn/T,
  requalified normal CLI; ticket now merging v14/r5 leader6.
  SECOND BLOCKER: external_mutation_quarantine singleton cleanup_uncertain at
  2026-09-06T07:03:36.433731Z was recorded during prior Ctrl-C shutdown. It
  blocks Client.runWithHandoff before any gh call, so ObserveMergeIntent masks
  it as external_merged. Read-only production-client diagnostic on copied
  Store confirmed no runner invocation. There is no quarantine-clear API.
  Do NOT delete the live row or bypass it. Source likely cancellation bug:
  github.runWithHandoff calls runner.Cleanup(ctx) with already-canceled request
  ctx; ghrunner boundedCleanupContext inherits cancellation, fails proof,
  quarantines and Close returns runner already in use. Needs regression and
  bounded independent cleanup context, plus safe recovery for existing latch.
  .context/merge-observe-diagnostic.go and proof-fetch-diagnostic.go contain
  repro harnesses, disposable copy only. Goal remains incomplete.

- Reconciliation diagnosis checkpoint: daemon60628 deliberately stopped with
  Ctrl-C to stop repeated child-proof retries; terminal exit7 included context
  canceled / gh runner already in use. Do not treat it as still live. Live
  ticket remains merging; PR already merged. No live DB modifications by
  diagnostics. Fresh SQLite backups at /private/tmp/sf-proof-diagnostic.4L86Wt
  prove current ReclaimProtectedRefFetch + Acquire + Check + Release all pass.
  Stopped WAL DB requires immutable=1 for read-only backup when sidecars absent;
  use only after proving daemon stopped (live reads must not use immutable).
  .context/proof-preflight.go proves persisted CleanWorktreeHead passes and
  remote preflight reaches rejecting authority. .context/proof-launch-diagnostic.go
  uses COPIED Store and gates off fetch: all read-only commands and durable
  launch records pass through the fetch gate. .context/proof-fetch-diagnostic.go
  uses disposable cloned repository/worktree and real HTTPS packaged helpers:
  entire protected-ref proof PASS first with fixture lease, then with COPIED
  Store real launch recording. No remote writes. Thus standalone Git/Store
  paths work; actual daemon composition/context remains unisolated. Do not
  claim root cause fixed. Scope next diagnostic to actual boundary error,
  not more broad source guessing. No production code edited this checkpoint.

- September 6 exact user approval is no longer a blocker. Acceptance PR1 in
  nysa-company/sf-cli-beta-acceptance-20260905 is MERGED at approved source
  ee35025e60092cfd25480121537acec4e4f33a1d; merge commit
  00860455167278a63b17ad40d5599b74aae5f636, 05:03:08Z. SF performed the merge.
  Ticket SF-543bc4cd3b9a9a6291c2bbc7ca20b3b1 still merging v13/r4, leader5;
  merge and protected-ref-fetch uncertain. No direct DB repair or gh merge.
  Acceptance HOME /private/tmp/sf-onboarding-acceptance.R2QvPm/home;
  installed onboarding4 binary .context/onboarding4.ChUnCq/installed/sf-dev.
  Daemon PTY session60628 last confirmed live; each new leader needs normal
  provider qualification (QualificationCurrent requires attested epoch).
  Ownerless socket inodes62558095 and66628685 preserved, not deleted.
  Read-only .context/proof-preflight.go uses production Git runner and refusing
  authority; reached acquisition refusal, proving pre-acquire snapshot/remote
  lookup pass. No proof fetch launched by diagnostic. Local proof failure
  remains to isolate; full goal NOT complete. Earlier approval-pending notes
  below are historical and superseded.

- Python compiled workflow checkpoint committed87c6172, full74056 exit0; clean
  tree at commit. Fresh clean bundle onboarding4 built from exact
  87c61724a9b1a3a561dfdd8af4c7e44f1fccb377 into ignored
  .context/onboarding4.ChUnCq/bundle and installed exclusively to sibling
  installed/. Version/manifest/core/publication helper validation PASS.
  Installed start help has title/prefix selection. Fresh empty home/ Go init
  --check, sample ticket validate and Python preparation preview all PASS,
  mutation=false, HOME still empty. No download/provider/live mutation.
  Earlier 'ready to commit' below is superseded. No active test/build process;
  real acceptance exact-head approval remains unanswered.

- Python onboarding checkpoint committed86bf334; full21715 passed. New uncommitted
  fixture integration adds cmd/fake-provider/python.go and extends codex.go plus
  compiled_walking_skeleton_test.go. Existing Go wrappers unchanged; Python
  opt-in test uses explicit preparation/init and real compiled Factory/executor,
  exact controller recipe, actual .py test/implementation and fixture-only PR.
  Compile/fake-provider77951 PASS. First Python E2E60263 failed invalid fixture
  Markdown (marker after acceptance list); moved marker into problem paragraph,
  no parser change. E2E31128 PASS48.591s: real prepared Python prebuild nonzero,
  postbuild0, candidate .py published, guarded fixture approval/merge/done and
  terminal cleanup. Controlled provider/GitHub only, NOT live-model delivery.
  57865 PASS: fake-provider race1.473s with exact-binding/write-scope tests;
  two repeated Python E2Es95.522s. Added explicit make test-python-e2e, requiring
  Darwin/arm64 and SF_TEST_PYTHON_CLI_DOWNLOAD=1 (no silent skip-as-pass).
  Missing-download-opt-in target check refuses with exit2 before Go/network.
  Named target84087 PASS66.432s (cold preparation + Python workflow), then
  existing Go guarded workflow PASS65.931s. Current docs distinguish automated
  Python workflow from pending live-model delivery. New full normal Go/vet/
  repo/secret/docs/artifact verification11376 TERMINAL FAIL: the two materializer
  workflow tests again reported explicit 'no space left on device FAIL'. All
  other package results passed; chained static checks did not run. Disk491MiB.
  Verified six obsolete Go cache directories (README, owner, no active builds),
  then used Go clean -cache only on sf-integrated-review-gocache,
  sf-provider-v51-all-cache, sf-provider-v51-all2-cache,
  sf-provider-v51-final-all-cache, sf-v52-host-full-cache and
  sf-v52-postfix-full-cache in /private/tmp. Free space now2.7GiB. No live state,
  source or runtime snapshot removed. Session74056 TERMINAL PASS: exact two
  regressions111.563s, then full serialized Go/vet/repo/secret/docs/artifact.
  cmd/sf88.291s, CLI4.179s, Git182.169s, GitHub61.084s, supervisor64.996s,
  publication92.124s, Store120.960s, workflowruntime112.283s,
  worktreecoord130.293s; chain exit0, repository/secret/docs/artifact all PASS.
  Disk2.4GiB before last workflow packages. No Python
  opt-in env; explicit Python tests already ran above, do not count normal skips.
  No live project/provider/PR mutation. New fixture checkpoint ready to commit.
  Source-build guide corrected for Python profile creation and isolated Go-cache
  disk requirements; acceptance report has a requirement evidence map. Real
  approval explicitly requested for ee35025e60092cfd25480121537acec4e4f33a1d;
  no answer received. Automated goal continuations are not that approval.

- Integrated validation21715 TERMINAL PASS: serialized full Go + vet/repo/secret/
  docs/artifact/release chain and Python fixture env as96360 below. Started with
  1.2GiB free after verified obsolete-cache cleanup. Go source frozen until
  terminal result now received: exit0. Store154.487s, workflowruntime118.903s,
  worktreecoord129.206s; repo/secret/docs/artifact/release smoke PASS; vet PASS.
  No new failure, only expected deprecated Seatbelt API build warnings.
  Bounded observer cell2396 completed; underlying21715 remains active.
  Earlier observer notes are superseded by the terminal result above.
  Ignored .context/python-codex-fixture.go is a draft for the next compiled
  Python workflow acceptance, NOT integrated or tested. It uses exact pinned
  recipe plus OUTPUT_BINDING and actual .py source; preserve normal Go fixture.

- Validation96360 TERMINAL FAIL: serialized `go test -p 1 -count=1 ./...` with
  SF_TEST_PYTHON_PREPARED/SNAPSHOTS=.context/python-onboarding-cache,
  DIGEST=e65fcb...f8b1a, LOCK=a9b45f...f428 and CLI_DOWNLOAD=1, followed by
  vet/repo/secret/docs/artifact/release smoke (these chained checks did NOT run).
  Two workflowruntime failures: TestRepositoryMaterializerRealStoreGitReplay
  expected candidate evidence response loss but got nil; source-resume prepared
  observation-loss case also got nil. All other package results passed, including
  Store159.787s and worktreecoord129.553s. Investigate skill: no root cause yet.
  Added state/version/transitioned assertion diagnostics only in those tests.
  Narrow reproduction96052 PASS both cases without production edits:58.10s
  and78.90s, package137.575s. Root cause remains unproven; do not call it fixed.
  Whole workflowruntime67553 FAIL129.357s with original Python fixture env:
  source-resume case returns nil but state=blocked v9 transitioned=true.
  Added test-only embedded-Store CompleteRepositoryCommand diagnostics for exit,
  duration and fixed error categories only (no raw output); actual recording
  still delegated unchanged. Reproduction4398 FAIL70.973s: all three command
  results exit1 with fixed category 'no space left on device FAIL'; blockedv9.
  Root cause for reproduced failure is disk exhaustion, not proof replay.
  Verified old SF Go-cache README/ownership and no consumers, then Go clean
  -cache ONLY /private/tmp/sf-qualification-go-cache, sf-config-v51-go-cache,
  sf-publication-store-go-cache. Regenerable artifacts only, no source/runtime/
  live state removed; free space343MiB ->1.2GiB. Exact two-test rerun48660
  PASS111.311s (44.29s/66.57s): expected red/missing exit1 then postbuild exit0,
  no disk-exhaustion category. No production change required. Retain safe
  diagnostics; fresh full integrated validation still required before commit.
  Narrow run used exact two-test regex and host permission,
  GOCACHE=/private/tmp/sf-gocache3, no Python fixture env needed for these Go tests.
  Do not commit or claim integrated green. Archive extractor
  pinned-local-source acceptance was separately52908/33140; this full run
  does not set SF_TEST_PYTHON_ARCHIVES. Do not claim skipped tests passed.
  Processsupervisor passed75.969s. Disk last observed534MiB free before broad
  process cleanup; no cleanup or parallel build.
  Read-only acceptance PR check still OPEN/draft, unmerged, exact head
  ee35025e60092cfd25480121537acec4e4f33a1d. No approval was supplied or sent.
  Next Python composition can reuse compiledDevWalkingSkeleton, but its
  cmd/fake-provider/codex.go artifacts/file writers currently hard-code Go.
  Adapt the fixture only after validation finishes; keep real Factory and
  repository executor, use exact configured Python argv and actual .py files.

- Python profile composition now implemented, UNCOMMITTED: config
  python-pytest-v1 generates both exact pinned argv under existing config lock;
  selected .py/tests path verified without symlinks or executing code. CLI init
  refuses missing runtime before creating config/state; explicit existing config
  also rechecked before registration. Init-check/doctor recipe preview verifies
  prepared runtime; main passes channel cache to Factory and ProjectStartChecker,
  which checks BOTH frozen Python recipes before budget. Tests93168 PASS all
  config/CLI/localruntime/pythonclosure; only initial58111 failure was stale
  Python unsupported-message assertion, updated to new explicit profile guidance.
  CLI/current-start race72414 PASS11.973s/3.062s using real prepared cache.
  Actual public Prepare74456 PASS produced e65...f8b1a in ignored private
  .context/python-onboarding-cache, no live project/install/daemon change.
  Store native 8-case acceptance81367 PASS29.399s on that factory-produced
  environment: pass/red/timeout/cancel/quota/filelimit/restart/unclear restart.
  Compiled clean-HOME cold-download/profile-init/replay/readiness/isolation
  acceptance78599 PASS18.663s; explicit SF_TEST_PYTHON_CLI_DOWNLOAD=1,
  no credentials, provider or daemon. Not a workflow/PR delivery proof.
  Corrupt existing cache now has ErrCacheInvalid/runtime_cache_unverified,
  retained-evidence inspection guidance rather than blind download retry;
  narrow48106 PASS CLI0.624s/prepare0.764s. Docs updated. Broad integrated
  validation running as96360; serialize packages to limit fixture overlap.

- Python preparation integrated source is UNCOMMITTED on26bda9c: catalog,
  bounded downloads, retained-archive hash reauthentication, descriptor-relative
  TAR/ZIP extraction, fixed alias omission (targets required), pure-wheel shape
  admission, independently pinned deterministic environment, exclusive cache
  publication and staging cleanup. Actual pinned archive extraction33140 PASS
  11.289s: env e65fcb836e0f19815114cf5a06349ef7260e03f60d4a62cce63b7406f4cf8b1a,
  1800 runtime/510 dependency entries. Full pythonprepare race52908 PASS37.905s
  with SF_TEST_PYTHON_ARCHIVES=.context/python-modern.OC2EHL explicitly enabled:
  offline transport copies pinned bytes, cache replay performs no second fetch,
  corruption remains untouched, temporary staging gone, retained handles valid.
  Adversarial archives and failure cleanup included. No actual network download
  or live installation performed. Early focused runs20297/23099/28176 all PASS.
  CLI now exposes `runtimes prepare python` preview (read-only), --download
  explicit preparation; channel-only directories (no DB/config/socket), human
  output hides hashes, JSON includes exact digests. CLI/config race96971 PASS
  46.158s/1.568s incl preview/no mutation, both channels, unsupported/failure/
  returned identity mismatch and rendering. Broad integration remains pending.
  Next: compose prepared root into Factory, add doctor/runtime readiness and
  explicit Python init profile through config lock+Store generation, then
  compiled clean-HOME preparation/init acceptance and real Python workflow.
  Do not advertise registration yet; CLI/docs explicitly say not enabled.
  No active tool process. Keep broad test until this composition checkpoint.

- Cache publication committed26bda9c after full32843 PASS. New uncommitted
  internal/pythonprepare catalog/download files: code-owned Darwin ARM64
  Python3.13.15 and five pytest wheels, exact public sizes/URLs/SHA256 plus
  lockdigest a9b45f...f428. HTTP client has no ambient proxy, two-minute bound,
  HTTPS host-limited redirects, exclusive FD-relative private staging writes,
  length/hash checks before0400+sync retention, own partial cleanup only,
  sanitized failures (no raw redirect/server error). Offline race70913 PASS1.568s
  covers success/hash/short/long/status/encoding/cancel/transport error,
  overwrite/symlink/private-parent/traversal/credentials/foreign URL/redirect
  bounds/catalog-copy isolation. No actual download invoked, no CLI use yet.
  Next implement bounded archive extraction and deterministic environment
  capture, then CLI preparation + channel-root/doctor/init composition. Keep
  focused tests while developing that integrated checkpoint; broad suite at
  its end, not after each helper. No current test handle remains active.

- Validation32843 TERMINAL exit0: full normal Go (explicit Python fixture),
  vet, repo/secret/docs/artifact PASS. Store178.408s, workflowruntime135.629s,
  worktreecoord135.672s. Prepared-cache publisher checkpoint ready to commit.
  Prior active-handle notes below are historical; no broad test is running.

- Full publication-helper validation32843 remains ACTIVE; latest poll includes
  Git196.955s, GitHub59.545s, supervisor66.277s PASS. Keep Go files frozen until
  this handle is terminal; do not restart on silent polls. Fresh public release
  metadata agrees with the tested runtime artifact (25,147,663 bytes, d3904b...)
  and all five pinned pytest wheels. New preparation-provenance report records
  exact filenames/sizes/hashes, platform boundary and extraction constraints;
  docs-smoke and diff-check pass. Wheel metadata inspected: all Wheel-Version1.0,
  Root-Is-Purelib true, py3-none-any; no .data/.pth/native-library entries found.
  Runtime archive has eight relative symlink aliases: materialize internally or
  explicitly omit unused aliases, never let them pass the regular-file verifier.
  No new archive download or install. Implement catalog/downloader/extractor
  together with focused tests after32843, then one broad integrated checkpoint.

- Prepared Python execution committed as1bf36f4. New uncommitted publication
  helper authenticates staged content under retained private parent FDs, uses
  Darwin RENAME_EXCL, verifies an existing exact snapshot for idempotence, and
  never removes/replaces corrupt destination evidence. Parent synchronization
  errors report whether publication occurred; no downloader/CLI integration yet.
  Focused whole-pythonclosure race98979 PASS2.142s; expanded retained-handle,
  corrupt-cache and concurrent tests race45610 count3 PASS2.980s. Unsupported
  hosts refuse publication. Broad validation for these new files pending.

- Python execution checkpoint: full validation27393 TERMINAL exit0. Fresh
  `go test -p 2 -count=1 ./...`, `go vet ./...`, repo-check, secret-scan,
  docs-smoke and working-tree artifact-check PASS. Explicit prepared fixture
  variables enabled all eight Store Python cases, including restart and
  ambiguous recovery (Store174.412s, supervisor66.227s). Six non-crash cases
  separately passed race70276; restart pair passed count3 in68225.
  Production Factory still does not compose PythonSnapshots: setup remains
  unsupported, and these are not full Python workflow/delivery acceptance.
  Next: bounded no-overwrite prepared-cache publication, verified provisioning,
  then channel-root/doctor/init composition and compiled onboarding acceptance.
  Preserve the pending exact-head human approval for external Go delivery;
  do not treat the expired waiting-approval ticket as delivered. No live project,
  daemon, installed runtime or remote changed. Disk last1.7GiB free.

### Historical checkpoint notes

The entries below describe earlier checkpoints, not current process status.
The terminal validation and remaining work above supersede their pending notes.

- Real Python executor-crash/reopen recovery now tested: executor is killed
  after released launch; surviving child is drained by production Store/native
  drainer; failed effect/no result/zero lease and recovery replay checked.
  Ambiguous-drainer case retains quarantined lease, refuses a newly issued
  current-leader competing writer, then exact native recovery clears it.
  Native Darwin first drain can return ErrUnclear with group EPERM during exit;
  tests now preserve quarantine and wait for independent ESRCH before explicit
  retry (no production weakening). Repeated restart pair68225 PASS27.557s,
  three repetitions. Earlier86654 PASS6.952s;44598/87795 exposed that transient.
  Cleanup registered immediately after authenticated child capture.
  Full74042 TERMINAL FAIL: Store compile saw new call before new helper file
  entered its startup file inventory. Not a green run. Freeze Go source for
  fresh full rerun27393 now active; do not add test files during an already-
  started full build. Poll27393 before claiming completion or restarting it.

- Six-case real Python Store race70276 TERMINAL PASS44.045s: pass/red/timeout/
  post-launch cancel/aggregate quota/per-file SIGXFSZ. Corrected environment
  9471ec...da78 with lock a9b45f...f428 now required for the fixture. Regression
  fails before bootstrap fix and passes after; no pytest exception parsing is
  used as authority. Fresh full normal/vet/repo/secret/docs/artifact74042 launched
  with SF_TEST_PYTHON_* explicitly set, so its Python fixture will not skip.
  Disk2.4GiB free; no cleanup/new installs performed. Goal remains active;
  restart/ambiguity/provisioning/CLI Python workflow remain incomplete.

- SIGXFSZ investigation confirmed/fixed: pre-fix real Store regression9870
  failed as observed ExitCode1/EFBIG (ordinary red). Factory bootstrap now
  restores signal.SIGXFSZ to SIG_DFL before pytest. New snapshot generated
  without changing prior cache by .context/python-rebind-bootstrap.go;
  authenticate-old -> copy -> new canonical bootstrap binding -> verify-new.
  New env9471ec153b03a83367aadaeb830815a8d32f096509a7e33347c3b2c48802da78,
  bootstrap aa3a660e211092f1cf3001d3b1c25c2b6d5fc24e3d4031bf783865cf6e9a0654,
  same root/lock. Post-fix16545 PASS6.302s: filelimit is observed resource abort,
  no reusable result and no lease residue. Whole six-case race newly launched;
  no code checkpoint complete until broad normal/static checks. This is
  trusted-repository behavior, not hostile Python signal-handler containment.

- Prepared Python real Store/Executor/compiled-gate acceptance44150 PASS16.366s:
  pass3.22s, red2.11s, timeout5.68s, aggregate quota2.07s. Abort rows have no
  result and zero lease residue. Post-launch cancel4944 PASS6.595s (case3.68s).
  Explicit test root .context/python-launch-snapshots, env digest
  a9721c1ecd236cf5d323a496d9e459dd08ee0e88fcbf2a5fb0cb51d2d12fec13,
  lock a9b45fd1379d4d31cd7765c0a8b53b547b42762ba359d6aaaf0af949e726f428.
  Runtime copied from previously authenticated disposable Python3.13 snapshot;
  no download/install/live project changes. Full test skips unless independently
  supplied SF_TEST_PYTHON_SNAPSHOTS/DIGEST/LOCK; skip is not acceptance.
  Typed exact policy and Supervisor dispatch now wired ONLY with explicit root;
  production Factory leaves PythonSnapshots empty, setup remains unsupported.
  Focused race43604 PASS policy1.398s/executor1.363s/supervisor2.788s. All new
  launch/policy/Store acceptance changes uncommitted pending remaining faults.
  Next important case VERIFIED: exact prepared Python -I/-S/-B reports
  signal.getsignal(SIGXFSZ)==SIG_IGN. RLIMIT_FSIZE can become caught EFBIG/ordinary
  red. Restore default SIGXFSZ in factory bootstrap before pytest, regenerate
  expected bootstrap-bound environment (do not mutate an old prepared cache),
  and add exact per-file-limit abort regression before resource acceptance.
  Restart/ambiguous launch and provisioning remain unproven.

- Validation97852 TERMINAL exit0: full normal Go, vet, repo/secret/docs/artifact
  checks PASS (Store139.706s, workflowruntime124.183s, worktreecoord137.618s).
  Resource retirement and bounded wait checkpoint can be committed; internal
  runPython and its retained-path test additions remain unfinished/uncommitted.
  Later wait/root tests passed race68906. Full suite is not evidence of a real
  Python launch: dispatch/policy are still disabled and no positive Run test yet.

- Internal runPython composition now written but NOT dispatched by Run/Preflight
  and policy still denies Python. Binds recipe/argv/spec/policy/prepared identity,
  authenticates worktree and retained-root path identity, creates per-launch
  scratch/bootstrap/staged gate, records launch before gate release, monitors
  scratch and bounded wait, durably finishes before Observed, retains ambiguity.
  Final monitor channel + scratch scan catch completion races; SIGXFSZ is a
  resource abort. Cleanup runs after monitor stop and proven drain. OS-child/
  retained-root race68906 PASS1.654s (plus compilation), NOT a successful
  Store-backed Python launch. Next must exercise real prepared Python and Store
  lifecycle before wiring dispatch/setup. Full97852 still live at latest poll.

- Python wait helper added but not launch-wired: separate exit/abort/reaped
  values; authenticates boot/start/PGID before TERM/KILL, soft/hard bounded
  waits, uncertain when Wait notification absent, rejects settings over30s.
  Resource inspection failures are ErrUnclear, only ErrLimit maps to factory
  resource sentinel; normal owner must inspect scratch again and prove drain
  before Finish/Observed. Focused OS-child race99425 TERMINAL PASS1.684s:
  quota/inspection/closed monitor, cancellation, wrong identity no signal,
  overlong limits no signal, withheld Wait returns bounded uncertainty.
  Full validation97852 still running, latest processsupervisor65.813s PASS;
  independent focused run covers latest wait-helper edits. No policy admission.

- Resource-abort authority uncommitted atop329344b: distinct contracts sentinel
  and optional resource-retirer; Executor handles only Observed resource aborts
  before ordinary result recording. Store shares exact current/drained atomic
  retirement with cancellation but uses resource-limit:repository-command-observed.
  No result row, no new schema, no Python admission. Store normal75590 PASS1.480s;
  focused race5593 TERMINAL exit0 (executor1.459s, Store26.055s), covering
  dedicated dispatch/bounded persistence, missing authority/failure quarantine,
  unrecorded/active/quarantined/wrong claim refusal, rollback and fresh retry.
  Full normal/static validation97852 newly launched; poll same handle before commit.

- Python gate/scratch checkpoint validation99018 TERMINAL exit0: full normal
  Go, vet, repo-check, secret-scan, docs-smoke and working-tree artifact-check
  PASS. Store138.123s, workflowruntime127.572s, worktreecoord138.587s. Afterwards
  added test-only preexisting scratch-limit refusal and concurrent Stop checks;
  focused monitor race45589 PASS1.584s. These do not admit Python execution.
  Next integration must treat factory resource-limit abort as non-evidence,
  with exact durable retirement or retained uncertainty, not ordinary red test
  evidence or a falsely labeled operator cancellation. No live runtime changed.

- Scratch monitor uncommitted: owns F_DUPFD_CLOEXEC descriptor, initial bounded
  inspection then100ms polls, single error channel, silent cancellation, Stop
  waits for own FD close and never deletes scratch. Constructor refuses bad
  root/context; lifecycle owner must terminate/drain on error. Race49055 PASS
  1.637s: caller-FD closure survives, later oversize reported once, cancellation
  and repeat Stop clean. No Store-backed Python Run yet. Gate race46758 PASS
  2.866s (malformed input + EOF child). Combined normal/vet/repo/secret/docs/
  artifact validation99018 passed (see latest checkpoint above).

- Python internal launch gate now uncommitted: strict canonical path envelope
  plus typed test path, waits FD3 byte1, fchdir FD4 and exact cwd, verifies
  factory bootstrap/profile, applies hard rlimits+Seatbelt, closes inherited
  gate/worktree descriptors on exec, executes fixed -I/-S/-B bootstrap argv
  with minimal environment. Main dispatches internal gate only; Python remains
  denied by repository policy/Preflight pending Store-backed Run integration.
  Malformed gate normal67084 PASS0.635s. Disposable compiled SF build40074
  exit0 (Go module stat-cache permission warning, build succeeded). Actual
  compiled-gate probe72071 TERMINAL exit0: before-release EOF refuses125,
  released gate passes7 pytest tests0.01s under production limits/profile;
  pre/post environment verification passes. Scratch script
  .context/python-profile-probe.go gate, binary .context/python-gate-sf.
  No live runtime/ticket/install changed. Added permanent EOF child regression;
  full combined validation passed; Store-backed launch admission remains pending.

- Scratch accounting uncommitted atop81c5aea: InspectScratchDirectoryFD uses
  private retained root, fresh directory cursors, nofollow stat/open,250ms
  observation deadline,16MiB individual file/128MiB logical+allocated file
  bytes/4096 entries/depth32 limits; refuses specials/errors, tolerates removed
  temp entries, accounts symlink itself without traversal. Not atomic quota;
  caller must monitor, stop/drain on errors and recheck completion. Limits now
  share file-size constant with gate rlimit. Tests: root replacement, symlink,
  repeatedFD, sparse oversized file/total, entry/depth/FIFO/private/cancel.
  Race90367 pythonclosure PASS2.035s, supervisor compile PASS0.371s. No monitor
  launch wiring or admission yet; broad combined validation passed.

- Combined gate96357 TERMINAL exit0: full normal Go, vet, repo/secret/docs/
  artifact checks PASS; Store136.140s, workflowruntime122.393s,
  worktreecoord137.199s. Python child-only resource limits added: hard file
  size16MiB/open descriptors128/no core, never raises stricter inherited limit.
  OS-backed focused race8732 PASS2.504s: EFBIG and file bounded by kernel;
  parent/daemon limits untouched. This is NOT aggregate scratch quota. Latest
  complete pythonclosure/processsupervisor race85180 TERMINAL exit0:
  pythonclosure1.473s, processsupervisor75.923s. Diff-check clean. Ready for
  checkpoint commit; no process/admission/setup support claim.

- Acceptance fresh read after deadline: waiting_approval, deadline_elapsed=true,
  remaining0s. No approval, budget extension or merge performed. Updated
  acceptance report to retain this outcome, not claim delivery or cancellation.

- Production Python sandbox profile generator now uncommitted: validates
  canonical roots, private scratch/runtime/dependencies, exact factory
  bootstrap bytes, executable inside runtime, and no scratch/read-root overlap.
  Deny default/network/fork; only selected executable; project/runtime/deps
  read-only, private scratch and /dev/null writable. Separate launch resource
  limits still required; profile alone does not claim aggregate disk bounds.
  Focused profile race90513 PASS1.629s. OS-backed probe94327 TERMINAL exit0:
  production profile + existing prepared runtime/bootstrap passed7 pytest
  tests0.01s, pre/post environment identity and no-project-write checks.
  Retained .context/python-profile-3585785728.sb, reproducible through
  .context/python-profile-probe.go (no extra runtime copy). No gate/allowlist
  admission yet. Full96357 still running; Store136.140s already PASS.

- Shared Python openPythonRuntime now returns the retained prepared handle to
  launch owners; identity-only wrapper closes its own handle. Resolver test
  also injects the existing staged-artifact settlement helper: ambiguous drain
  quarantines without closing verified FDs, proven disappearance finishes and
  closes once. Race64025 PASS1.362s. This is helper composition coverage, not
  an actual Python process/Store launch test. Full96357 remains running.
  Fresh acceptance status read at age1h58m29s: waiting_approval,90s budget left;
  no approval/budget/runtime mutation made. Re-read before claiming outcome.

- Python prepared recipe/bootstrap and shared supervisor identity resolver are
  now uncommitted with the prepared loader. Materializer calls supervisor-owned
  CommandExecutableIdentity(ctx,argv); existing Go/Node behavior retained,
  python3 typed recipe requires explicitly composed private channel snapshot
  root and frozen environment/lock plus factory bootstrap digests. No production
  root composition or Python executionpolicy/Preflight admission yet. Recipe
  excludes custom pytest config/plugin autodiscovery; no flags/shell/PATH input.
  Factory bootstrap real sandbox probe24701 PASS7 tests0.01s; pre/post expected
  environment verification passed, stage .context/python-environment-1209705896
  digest sha256:a9721c1ecd236cf5d323a496d9e459dd08ee0e88fcbf2a5fb0cb51d2d12fec13.
  Focused recipe/loader race41222 PASS1.538s; compile26006 PASS. Resolver tamper
  fixture initially tried writing0500 file and got permission denied; changed
  fixture to temporarily0700 then restore0500 so byte change is tested without
  weakening production seals. Rerun25172 PASS: supervisor race1.416s and
  pythonclosure race1.408s. Full combined normal/vet/repo/secret/docs/artifact
  gate96357 running; do not restart or claim terminal until polled.

- Prepared Python loader now uncommitted atop f921ac7: opens digest-addressed
  environment.json/runtime/dependencies relative to a caller-authenticated
  channel FD, no symlinks/ambient HOME/fallback; requires private same-owner
  roots/manifest and independent expected identities. Retains root descriptors;
  Close never deletes cache evidence. Tests cover wrong channel, link/FIFO,
  permissions, digest/lock/bootstrap/content tamper, cancellation, replacement
  root and idempotent Close. No execution admission yet. First positive test
  failed because Go testing.TempDir children are0755 (testing.go Mkdir0777),
  confirmed by diagnostic stat755 for all three roots. Fixed fixture to0700;
  production check unchanged, diagnostics removed. Investigation scope only
  internal/pythonclosure; global skill setup/telemetry not part of task.
  Broad validation for this loader checkpoint remains pending.

- Python environment binding checkpoint atop bd3d065: combines runtime and
  dependency manifests, executable interpreter relative path, lock digest and
  bootstrap digest. Exact canonical decoding rejects alternate/duplicate/
  unknown wire shapes; bounded input and pre-marshal allocation; both retained
  directory FDs verified under one overall deadline. Content identity only,
  not provenance or execution permission. Focused race88237 PASS1.469s.
  Added AuthenticateEnvironmentFD requiring independently supplied environment,
  lock and factory-bootstrap digests before retained-root verification; stale
  expected identities refuse. Added oversized pre-allocation bound regression.
  Latest focused race70463 PASS1.465s; prior helper race48723 PASS1.459s.
  Real disposable modern runtime probe15535 TERMINAL exit0:2142 selected
  regular files/70,823,634 bytes,7 pytest tests PASS0.01s; pre/post environment
  verification passed. Stage .context/python-environment-642701669 retained,
  digest sha256:5a07ad5adcb04188ad3e4a883c222fa4103e7a20f559c9e30c9d627223cc8489.
  Probe code .context/python-environment-probe.go; no production admission or
  live change. Full normal/vet/repo/secret/docs/artifact handle80093 TERMINAL
  exit0: Store134.287s, workflowruntime121.707s, worktreecoord138.234s; all
  static gates PASS, no leaks. Latest tests also covered by race70463.
  Acceptance ticket freshly
  read through installed CLI remains waiting_approval; no approval performed.

- Committed bd3d065 internal/pythonclosure manifest layer (not execution
  admission): descriptor-relative bounded capture of prepared regular files
  and directories; hashes bytes/path/kind/mode/size; canonical shape/parent/
  ordering checks; no ambient root reopen or shared directory cursor.
  Rejects symlinks/special files, changed bytes/mode/inventory, malformed
  evidence; caller must authenticate root and exclude writers separately.
  Focused race57458 PASS1.457s; depth tightening race+real stage11958 PASS
  1.453s plus pre/post verification of .context/python-stage-3937252858
  manifest sha256:f56fd5e38e1ce35b8468c53a49df88d1d601946654540d4891ecbe45a7eedb03.
  No Store/schema/command allowlist change. Full normal/vet/repo/secret/docs/
  artifact gate10486 TERMINAL exit0: Store133.305s, workflowruntime123.965s,
  worktreecoord135.747s, all static checks PASS. Ten-repeat race86365 PASS
  1.531s. Runtime preparation must materialize aliases as regular layout.
  Next integration anchors recorded in .context/python-integration-next.md:
  shared resolver/supervisor identity, explicit channel root, combined runtime/
  dependency/bootstrap binding, and scratch ownership through proven drain.

- Modern Python feasibility now PASS in disposable scratch, not SF support:
  .context/python-modern.OC2EHL holds public standalone3.13.15 ARM64 release
  20260901, archive SHA256 verified against GitHub API. Private pip wheelhouse
  and requirements.lock pin pytest8.4.2+4deps; installed offline with hashes.
  Fixed isolated bootstrap + deny-default profile + private TMPDIR passes7
  tests (count/read-only input, project write/network/subprocess refusal,
  private tmp_path roundtrip, symlink escape refusal), normal FD capture.
  Needs /dev/null device read/write, fixture cwd; no SF root read added.
  No global install, production allowlist, live daemon, or ticket changed.
  Next convert feasibility into authenticated runtime/dependency/scratch
  composition with Store binding and cancel/recovery tests, not unsupported
  execution claims. Evidence in .context/python-runtime-feasibility.md.

- Python relocation spike passed (not production support): ignored Go tool
  .context/python-stage-probe.go creates bounded private hashed snapshot,
  preserving interpreter/framework/stdlib layout, no site-packages/dev cache.
  21060 PASS first;11491 PASS strengthened with original-runtime read denial.
  Latest .context/python-stage-256035698 contains778 files/36,742,075 bytes;
  sys.executable/prefix/json module use stage. File write/network/subprocess
  and original json module read all EPERM. No production policy or live state
  changed. Still needs modern runtime/native/dependency closure, Store identity
  binding and cache/lease/cancel/recovery tests before Python can be admitted.

- b23a042 is the committed ticket/decision picker checkpoint; worktree was
  clean after commit. Python feasibility is now recorded in ignored
  .context/python-runtime-feasibility.md: host CLT Python3.9.6 -I/-S/-B flags
  work, but deny-default Seatbelt probe fails before code (symlink EPERM then
  canonical path exit134; cause unproven). No passing sandbox claim, no runtime
  allowlist/qualification/dependency install/live change. Next expansion needs
  explicit authenticated CPython+stdlib/native/dependency snapshot and its
  real execution/fault tests, not a pytest allowlist exception.

- Python probe follow-up DONE_WITH_CONCERNS: dyld crash was missing literal
  root read (sandbox log proof); allowing /System alone did not fix it. Then
  canonical CLT bin/python3.9 revealed posix_spawn of framework Python.app.
  Direct framework interpreter under deny-default + exact exec passed three
  times, EPERM for file write/network/subprocess, forbidden file absent.
  .context/python-runtime-profile.sb and python-runtime-probe.py reproduce.
  This is not production qualification: runtime tree is unstaged, Apple3.9.6
  only, dependencies/lease/recovery untested. No production code changed.

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
