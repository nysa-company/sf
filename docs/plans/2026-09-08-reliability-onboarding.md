# Reliable ticket recovery and first use

Status: approved outcomes; implementation design and validation in progress.
Baseline: main b79fd7a37a23eb38cd0aa40d08adb9ae091ba35e.

## Goal

An operator can start a bounded ticket, understand a failure, and use a supported
recovery path without database edits, lost work, or help interpreting internal
state. Deliver all four approved recommendations: generated-test recovery,
actionable status, first-use validation, and hosted regression/concurrency tests.

## Acceptance

1. A contradictory independent test has a bounded diagnosis and independent
   amendment path. Original acceptance, commands, protected files and old proof
   remain authoritative until Store accepts the exact reviewed amendment.
   Rejection preserves the old proof. Exhaustion stops with one actionable next
   step. Never assume a failing test is wrong merely because Builder says so.
2. Human and JSON status explain the cause, retained-work disposition, whether a
   writer has been proven drained, and the exact supported next action. Unknown
   evidence says unknown; retained does not mean safe to edit. No shell injection,
   secrets, implicit retry, or guessed approval authority.
3. The first-ticket guide and hosted clean-environment checks cover Go, Node/TS,
   Python and Rails compatibility decisions. Supported paths reach a visible
   ticket; unsupported recipes fail before a paid launch and explain why.
   Target less than ten minutes of operator setup, excluding tool installation,
   login, model runtime and CI waits. Record these clocks separately. A human
   unfamiliar with SF is the preferred usability check; agent-only evidence is
   explicitly labeled and is not a claim of external-user success.
4. All changed behavior has hosted positive, negative and crash/replay tests.
   Then run four bounded Relay tickets at capacity two on one exact final
   candidate: prove overlap, refusal/queue behavior, safe cancellation/retry,
   restart, same-PR correction, protected-base movement, separate exact-head
   approvals and all intended deliveries Done. Report every retry/intervention.
   No duplicate mutations, lost work, manual database repair or guard bypass.

## What already exists

- Store verification amendments authenticate a completed Builder request, prior
  revision, frozen command, correction budget and new independent Reviewer.
- Worker supports accepted/rejected amendments and recovered immutable results.
- Post-build command failure currently becomes `postbuild_command_failed`; a
  completed result cannot simply be replayed as a new diagnosis attempt.
- CLI already renders next-action argv, ticket selectors, setup preview, doctor,
  short IDs and composed run. Reuse these instead of introducing a new wizard.
- Hosted baseline has 15 lanes and a fail-closed required aggregate. Existing
  compiled concurrency fixtures test capacity two and a third refused start.
- Existing local recipes are deliberately restricted. Rails is not locally
  supported; this sprint does not silently expand execution authority.

## Implementation order

1. Trace the exact completed-Builder/postbuild failure boundary. Add a failing
   hosted composition regression for contradictory tests and stale/replayed
   callbacks. Design the smallest Store-owned retry context that can reuse the
   amendment flow; do not relabel a completed provider result or mutate it.
2. Add bounded proof-consistency instructions to verification, Builder and
   amendment Reviewer prompts. Include eval cases for true contradiction versus
   legitimate implementation failure; prompt text alone is not repair authority.
3. Add diagnosis/recovery projections to status from authenticated Store facts,
   then human/JSON golden tests and executable-next-action tests.
4. Audit the clean first-use commands for all four stack categories; improve
   documentation and refusal guidance. Run hosted compiled clean-HOME fixtures.
5. Run hosted focused and full/race/upgrade/security/compiled checks. Independently
   review the exact candidate before its native campaign and protected merge.
6. Execute and record the four-ticket campaign with the hosted artifact. Preserve
   other runtimes and all evidence. No broad local tests/builds.

## Recovery test map

```text
postbuild failure (authenticated command result)
  -> bounded diagnosis, no test-writing authority
     -> implementation defect -> bounded Builder correction -> fresh proof
     -> alleged contradiction -> existing amendment request
        -> fresh independent Reviewer rejects -> original proof remains
        -> fresh independent Reviewer accepts exact change -> new checkpoint
           -> fresh Builder/proof/CI/review/approval
     -> ambiguous / budget exhausted / unsafe writer -> stop + exact next action

Every arrow: current-fence validation, crash-before/after durable commit,
lost-response replay and wrong-ticket/stale/forged evidence negatives.
```

## Constraints and failure modes

- SQLite is the sole authority. Add only the minimal append-only binding needed
  for a new retry attempt; no parallel log-based workflow and no counter guessing.
- Immutable prompt changes affect new attempts, never rewrite stored inputs.
- The first repair boundary is pre-publication only. Any candidate/publication/
  CI history keeps the existing fail-closed disposition until a separately
  authenticated invalidation protocol is implemented. Do not reinterpret old
  `postbuild_command_failed` rows as repair permission.
- Carry the exact terminal command-result key through the failure type. The
  type is only a locator: Store must reload and bind it before any new entry.
  Signal/cancellation results do not become diagnostic retry evidence.
- A fresh Building entry must retire the predecessor from current result reuse,
  not mutate its completed artifact. Reauthenticate retained physical edits,
  including ignored-file refusal, before admitting a new writer. A registered
  worktree or allowed path alone is not provenance or edit permission.
- Real command output is untrusted and bounded/redacted before any diagnostic
  input. No command/URL execution derived from output.
- Four-ticket admission uses supported start/retry semantics, not a new implicit
  auto-start policy. With the one-refresh cap, stagger approvals/admission so the
  test does not silently require unlimited protected-base refreshes.
- Human onboarding availability is unresolved; do not fabricate a participant.
- Hardware/auth-native trials use the operator-approved isolated Mac fallback
  only when hosted execution cannot supply the required native authentication.

## Not in scope

Unrestricted Rails/Node dependency execution, new providers, automatic fallback,
autonomous merge, hosted credential copying, service installation, deletion of
old worktrees, a new dashboard, or a new release/update system. These do not
remove the observed ticket-recovery and first-use friction.

## Engineering review checkpoint

Architecture: reuse amendment and next-action authorities; exact postbuild retry
binding is implemented through immutable V61 repair entries. Code quality: keep projections separate from authority,
avoid duplicating status decisions. Tests: composition and model-eval gaps above
must be covered before claiming recovery. Performance: bounded attempts and
output; no provider calls on status reads and no transactions across execution.
The installed review skill lacks its referenced supplemental sections file;
this document is a working design, not a completed automated-review verdict.

## GSTACK REVIEW REPORT

| Review | Status | Findings |
| --- | --- | --- |
| Engineering scope/source inspection | In progress | Bounded repair and amendment composition implemented; restart acceptance pending |
| Independent review | Pending | Required before integration |
| Hosted verification | First slice passed | [34271491625](https://github.com/nysa-company/sf/actions/runs/34271491625), exact 1b1b7e4; prompt/status tests, existing readmission tests, native build only; not full repair acceptance |

VERDICT: implementation design in progress; not cleared for merge.

**UNRESOLVED DECISIONS:**
- Human unfamiliar-user availability; agent evidence must remain labeled.

## Execution checkpoint — bounded repair foundation

- Hosted run [34273314750](https://github.com/nysa-company/sf/actions/runs/34273314750)
  passed at b521934: strict failed-command evidence, prompt/status tests,
  first-use matrix (including honest unsupported/unprepared refusals), docs
  checks, and native candidate build. No local tests were run.
- Commit 41d8cf3 adds append-only V61 repair entries, exact consumed failure /
  Builder / verification / budget binding, replay-once tests, a normative
  Building-to-Building diagnosis entry, generic-entry refusal, and bounded
  retained-byte fingerprints. Hosted run
  [34274649199](https://github.com/nysa-company/sf/actions/runs/34274649199)
  passed those foundation tests, onboarding, and exact candidate build.
- Worker/phase admission, historical verification/restart bridges, and physical
  readmission are in progress. This foundation is not yet end-to-end recovery.
- Remaining acceptance: complete hosted composition/negative/restart checks,
  independent exact-head review, then four native Relay tickets at capacity two.
  Neither the four-ticket campaign nor an unfamiliar human setup trial has run.

## Execution checkpoint — independent amendment handoff

- Hosted [34275904089](https://github.com/nysa-company/sf/actions/runs/34275904089)
  passed at 378072a for connected bounded repair, Store recovery, admission,
  prompt/status and first-use checks. Its localruntime selection was incorrectly
  named and selected no tests; the next workflow fixes that selection explicitly.
- The next slice adds immutable amendment/physical-checkpoint receipts, retained
  implementation fingerprints, protected-only commits, and independent rejection
  disposition. Reviewer-written corrected tests must be authenticated after the
  Reviewer and command complete; the earlier Builder snapshot is not substituted.
- A rejected amendment retains the original proof and files, then stops with
  `postbuild_amendment_rejected`. It does not grant another Builder admission or
  silently restore files. Decision-before-block recovery must dispatch only this
  narrow disposition, not a provider or worktree mutation.
- Open acceptance blocker: protected checkpoint crash recovery must authenticate
  both ref and index completion. A prepared object alone does not prove index
  synchronization. Source work is not a passing hosted or native result.

## Execution checkpoint — checkpoint crash completion

- Commits through 21e7358 add the accepted/rejected real Store/Git workflow
  fixtures, immutable post-Reviewer checkpoint receipt, exact prepared-operation
  reclaim, and a dedicated resume path that cannot launch another Reviewer.
- Hosted runs 34279182024 and 34279850246 failed. The first exposed an incorrect
  late-result test expectation; the second exposed receipt authentication in the
  accepted fixture. Neither is counted as acceptance. Run 34280718207 also failed:
  snapshot fixtures passed raw instead of typed digests, retained-file fixtures
  did not advance their local bare remote, and accepted amendment reused its
  requesting Builder. The runtime package additionally exhausted its cumulative
  ten-minute timeout. The next run separates isolated hosted validation groups.
- Source review found ordinary startup commit observation could confirm HEAD
  before protected index synchronization. Startup now defers only an authenticated
  checkpoint intent to its dedicated completion path. Dangling receipts and
  malformed command evidence refuse generic fallback. New Store/daemon regression
  tests are authored but not yet executed.
- Still required: green hosted composition and full baseline on the final head,
  independent final review, four-ticket native campaign, and an honest first-use
  report. Existing live Relay data and other runtimes remain untouched.

## Execution checkpoint — isolated validation and review

- Run [34282752323](https://github.com/nysa-company/sf/actions/runs/34282752323)
  at e88323f passed Store, Git, admission, accepted/rejected runtime amendment,
  CLI/behavior and registration-onboarding groups. It failed the remaining
  runtime repair assertion (live versus historical projection) and restart's
  fresh-Builder qualification setup. No native artifact was released.
- Those fixture issues are corrected for the next run. Independent source
  review also found synced-but-unconfirmed checkpoints needed exact claim
  recovery, and failed-before-preparation retries needed an atomic retirement
  audit. Both fixes and regressions are authored; ordinary Git recovery remains
  unchanged. The previously alleged multiple-companion bug was retracted after
  tracing the schema-enforced correction budget.
- The onboarding fixtures now require a real compiled daemon to show an exact
  submitted Queued ticket with zero provider attempts. Prepared Python is an
  additional hosted group with explicitly enabled pinned public downloads.
  This is still not live model delivery or a human usability trial.

## Execution checkpoint — release validation

- At d955ef1, hosted focused run 34283758068 passed repair, amendment, Store,
  Git, admission and behavior. Onboarding failed to compile because its new
  helper depended on tagged tests; all three restart cases failed on the new
  assertion's `kind` column instead of `effect_kind`. Commits 8871045 and
  8619514 correct these test defects. No production assertion was weakened.
- The full baseline 34283760341 also contains these failures. It is diagnostic,
  not release acceptance. Focused run 34284505456 validates the corrected tree.
- Native failure/retry needs real authenticated eligibility. Ordinary pause /
  resume proves interruption, not retry. Prefer a genuine eligible exhaustion
  if encountered; never fabricate provider output or edit the runtime database
  to manufacture a passing campaign. Any deliberate interruptions, extra
  attempts, or undemonstrated cases must remain visible in the final report.

## Execution checkpoint — independently found finalization gaps

- d13db95 passes Go/Node and prepared-Python compiled submission/Queued checks.
  Its three prepared-checkpoint recovery cases all passed in run 34284912043,
  but accepted-amendment admission failed intermittently. The whole run failed.
- Diagnostic-only 42886f7 preserved closed stage names and typed error causes,
  without raw subprocess output or weaker checks. Run 34285851847 identified
  `final physical snapshot` plus `context deadline exceeded`: three separately
  bounded physical inspections competed for one shared 15-second deadline.
  The repair keeps individual inspection bounds/caller cancellation and gives
  the composed operation its own finite budget. It still needs hosted validation.
- Independent authorship-crossed review found amendment context could reject a
  recorded candidate before `build_pass`, and the earlier committed-but-not-
  recorded child lacked a dedicated replay witness. Exact authenticated handoff
  fixes and four real-flow crash regressions are in progress. Replay must issue
  no new model, command, or Git commit. Valid later CI/base-refresh authority must
  supersede the old amendment without permitting malformed-evidence fallback.
- Full run 34285053550 is diagnostic for d13db95, not the forthcoming fixes.
  Final exact-head full validation, independent re-review and native campaign
  remain required. No native campaign or unfamiliar-human trial has begun.
- The diagnostic restart group also exhausted its shared package deadline at
  470 seconds: two cases passed before the third inherited the remaining time.
  The three cases now have separate hosted jobs, each retaining the existing
  eight-minute bound. No individual Git inspection timeout was increased.
  Four separate commit-before-record/record-before-transition cases are added.

## Execution checkpoint — direct repair completion

- Exact c9d300b focused run34287852415 passed all 15 groups and built its native
  artifact. Full run34288028645 is still pending. It is not release clearance.
- Final independent cross-review found the direct implementation-repair path
  (without a test amendment) still rejects committed/persisted candidates after
  a crash. The accepted-amendment cases do not cover this distinct outcome.
  Add equivalent exact clean-candidate admission and historical command replay,
  plus four direct-repair crash cases. Never rerun a model/command/commit to
  compensate for a lost candidate response.
- Native acceptance remains gated. The campaign will explicitly distinguish
  deliberate Planner cancellation-exhaustion/retry from natural model failure;
  pause/resume alone does not prove provider retry.

## Execution checkpoint — native concurrency and ordinary resume

- Exact d9e9786 passed focused run34288973873 and full run34289176386,
  including every required acceptance lane. Independent Store and runtime
  source reviews passed. The hosted arm64 artifact began an isolated native
  four-ticket Relay campaign; no local portable test or build was run.
- Two real Planner attempts overlapped and a third start was refused at
  capacity two. One ticket reached a CI-green draft PR and WaitingApproval.
  No approval or merge has been dispatched yet.
- Two deliberate pause/resume interventions were recorded. The second pause
  crossed from Planner completion into verification, so it is not evidence of
  Planner exhaustion or the provider `retry` command. No additional forced
  interruptions are planned to manufacture eligibility.
- Native verification resume exposed a selector defect: an ordinary clean
  pause/resume was mistaken for a source-only Building takeover. Its valid
  resume committed, but scheduler admission refused before another provider
  launch. Add exact ordinary-control classification and a regression without
  weakening genuine takeover authentication. The repaired head needs fresh
  hosted checks and native continuation. The campaign remains incomplete.

- Focused34296598360 at fc2fbb3 confirmed the ordinary selector fix but failed
  the new restart assertion. Restoring an open runtime seals its exact resumed
  authority; pause recovery incorrectly required authority still at the stop.
  The next patch accepts only that exact resumed endpoint with the original
  phase/triplet/no-writer checks. An invented authority version still refuses.
  Full34296599899 was cancelled as superseded, not counted as passing. Native
  state remains unchanged while the new exact candidate is validated.

## Execution checkpoint — retry capacity and hosted integration

- Exact f7231a4 focused34297082600 passed all20groups. Full34297084568 failed
  normal workflowruntime integration on its cumulative30m package deadline,
  while the current test had run49seconds. Other normal packages passed;
  the race-other lane was still pending at this checkpoint. This is not green.
- Split the complete hosted normal workflowruntime inventory into four disjoint
  shards, retaining Test/Example/Fuzz and subtests, with the other integration
  packages unchanged. Every shard is required by the fail-closed aggregate.
  Full race coverage and per-package bounds remain unchanged. Source review
  passed; hosted execution of the partition changes is still pending.
- The fresh native trial used the exact focused-passing/reviewed artifact while
  remaining full CI ran in parallel. Merge remained gated on both. It recorded
  real overlap, two deliberate Planner cancellations/drains, typed exhaustion,
  and successful `sf retry`. However retry executed without global/project
  ticket-capacity leases while two other tickets retained them. Provider-level
  limits are not a substitute. This is a release blocker, not a campaign pass.
- Add one shared atomic capacity acquisition to every affected paused-to-active
  boundary, authenticated against the frozen ticket configuration. Refusal must
  roll back the transition and retry epoch; replay must not duplicate leases.
  Also project authenticated provider-retry disposition in status/show/list;
  a generic stopped message is not the actionable next-step acceptance.
- Native daemon is stopped with evidence retained. No PR approval or merge was
  dispatched. Earlier trial tickets are diagnostic and must not be combined
  with another binary to claim one final-candidate four-delivery campaign.
- First-use coverage is agent/hosted evidence, not an unfamiliar-human trial.
  Separate setup clocks were not precisely recorded, so under-ten-minute setup
  remains a target rather than a certified result. Narrow TypeScript has positive
  CLI registration coverage, not an exhaustive compiled delivery matrix.

## Execution checkpoint — startup composition and decision-quality eval

- Capacity and retry-status repairs are committed in the bc7bae5 candidate.
  Focused34301205015 passed every group; full34301206752 has passed all four
  normal runtime shards and eight Store race shards, with two broad jobs pending.
  This is not final full-CI or native acceptance.
- Cleanup startup found the production daemon did not supply the read-only
  prepared-commit observer before recovery. Its workflow Git runner is created
  later. Add lazy trusted-core resolution independent of provider qualification;
  missing or unsafe assets still refuse before runtime/socket exposure. Never
  create a replacement commit or manually repair durable evidence.
- Add the previously missing three-case decision-quality corpus and strict
  scorer: one genuine proof contradiction and two implementation-defect cases.
  Prompts bind the production amendment instruction; expected decisions stay
  outside the prompt. Portable scorer tests run on GitHub. Native authenticated
  Reviewer execution is separately bounded to one attempt per case and120s,
  with a requested (not enforced)2048-token response budget and16KiB scoring cap.
  External receipts and independent rationale review are required; this small
  synthetic eval is neither Store amendment authority nor end-to-end acceptance.
- All four final native deliveries, separate approvals, exact-head broad CI
  and final SF PR remain outstanding. Earlier runs stay diagnostic evidence.

- Exact bb99d86 focused34302552992 passed all groups, including real-Git
  recovery and scorer tests. Native cleanup reached the observer but rejected
  creation registration v2 against a legitimate Verifying commit claim v3.
  Authenticate the exact immutable Store claim/prepared facts independently
  from registration provenance; retain identity/base checks and final Store CAS.
  Add later-phase registration/commit coverage. Native state remains stopped.
- V1 decision eval ran once per case: decisions, selected proofs and commands
  were correct3/3, but strict scoring was2/3 because acceptance_preserved did
  not specify proposal versus selected proof. Keep that failed result. V2
  explicitly names selected_proof_preserves_acceptance, versions the schema,
  and freezes its scorer before a new single-attempt three-case run. This is
  an evaluation clarification, not a production policy change or retroactive pass.

## Execution checkpoint — retained retry after successful publication

- Exact ef3f6b4 focused34304070521 passed all groups. Full34304075207 has
  seventeen passing jobs; crash and race-other are still running. No final
  exact-head full-CI verdict is claimed.
- V2 selected-proof decision eval passed all three cases, one invocation each,
  with independent rationale review. The original v1 strict failure remains.
  Receipts disclose requested model, timing/tokens and nonfatal CLI skill errors;
  this synthetic test does not replace native delivery or amendment evidence.
- Native startup now confirms the existing prepared Job commit correctly, but
  refuses runner fencing for an earlier successful Planner retry carried through
  to waiting_approval. A sealed control retains exhaustion v9 while authority
  advances to v16 at the same runner/leader. The post-publication recovery reader
  wrongly demands a post-publication pause/resume sequence. Repair must bind the
  original retry epoch and normal publication/review chain, not ignore controls.
- The database remains unedited, no replacement commit was made, and all pilot
  worktrees remain retained. The final four-ticket campaign and merge are pending.

## Execution checkpoint — approval after native restart

- Exact eb0bf0b focused34306877416 passed all twenty groups and its artifact
  build. Full34306878975 remains pending its final race job; not a full pass.
- Fresh native tickets on that artifact proved capacity-two refusal, concurrent
  verification, two deliberate Planner cancellations with drain evidence, and
  the supported bounded retry with both capacity reservations reacquired.
  A separate ticket automatically retried one invalid Planner artifact.
- Both first-wave tickets reached waiting approval with passing PR checks and
  independent exact-head review. Same-binary restart preserved both heads and
  completed reviews, but approving the retry-controlled ticket refused without
  mutation. The runtime control remained sealed at the previous authority;
  recovery had advanced the ticket but had not opened decision admission.
- Repair must use the existing runtime drain/join and capability machinery,
  including explicit activity admission before the decision and authenticated
  crash-after-decision recovery. Do not permit sealed authority directly in the
  generic decision transaction. The displayed failure must distinguish admission
  failure from a genuinely changed reviewed head.
- No campaign approval or merge occurred. The two PRs/worktrees remain intact,
  and the other two tickets remain queued. This is not final native acceptance;
  the exact-final-candidate campaign requirement remains unchanged.

## Execution checkpoint — concurrent decision admission

- Exact283cec0 passed focused34309705107. Full34309826165 then exposed two
  regressions: compiled approval collided with a running scheduler observation,
  and provider retry selection rejected a valid semantic merge-budget retry.
  Full CI is not passing; these failures are retained, not rerun as flakes.
- The decision repair reserves the next exact-ticket activity and boundedly
  joins the existing poll, without cancelling it or resealing durable authority.
  It repeats current-fence/readiness checks before the unchanged Store decision.
  Stop and cancellation still win. The Store repair distinguishes a merge-budget
  stop only after authenticating its immutable merge/approval boundary.
- Focused validation now includes the actual compiled guarded/takeover paths
  and semantic merge retry. Positive and adversarial regressions are required.
  Native283 work remains retained, with one interrupted ticket paused, one at
  approval, and two queued. Further paid retries/approvals are held. The next
  final campaign waits for exact-head full CI as well as independent review.

## Execution checkpoint — complete race-suite scheduling

- Exact37c4a44 focused34311298818 passed all21groups, including both original
  compiled approval failures and semantic merge retry positive/tamper cases.
  Its full34311300533 remains running; no full pass is claimed.
- Older eb0 full34306878975 terminated at its cumulative80m race-other bound.
  All completed packages passed, including workflowruntime2357.921s, but the
  final worktree coordinator package had not finished. This is direct timeout
  evidence, not a speculative flake attribution or an individual test pass.
- Split the complete workflowruntime race package into its own required
  baseline lane. All other non-Store packages remain in race-other; the eight
  Store shards and full unpartitioned reference remain unchanged. Strict
  inventory tests must prove the partition is complete and disjoint. Keep
  individual package bounds and the fail-closed aggregate unchanged.
