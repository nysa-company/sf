# Relay serial repeatability and bounded recovery evidence

## Verdict and scope

The bounded repeatability goal passed: two additional Relay tickets reached
`done` serially on the same unchanged factory binary, with passing required
and post-merge CI. This is not a stable-v1, autonomous, concurrency, or load-test
verdict. The earlier failed trials below remain part of the result.

Frozen executable: `sf/0.1.0-dev.relay25-adb3ef5`, commit
`adb3ef5a43513c361d6073602ea090476e5b56b5`, dev leader 31. Builder
`gpt-5.6-luna` and Reviewer `gpt-5.5` were independently qualified. Capacity
was one; each ticket used guarded mode and a 4-hour/$100 ceiling. Stable,
Nysa, and the legacy factory were not changed.

All timestamps below are UTC on 2026-09-05.

## Delivered tickets

| Ticket | Actual start → SF done | Runtime | Provider attempts | Delivery |
| --- | --- | --- | --- | --- |
| Linked event, `SF-ebdc09995ba66b75b3b1b7dd1eed4767` | 03:27:30.983690 → 03:34:03.504857 | 6m 33s | One each: planning, verification, build, review | [PR #4](https://github.com/nysa-company/sf-v1-relay-pilot/pull/4) |
| Status counts, `SF-06ed6b48341efe3951f6931aef92b59a` | 03:35:03.850992 → 03:40:36.984335 | 5m 33s | One each: planning, verification, build, review | [PR #5](https://github.com/nysa-company/sf-v1-relay-pilot/pull/5) |

Linked event was submitted at 03:05:37.016289 but could not start until the
pre-start capacity repair. Its full submit-to-done time was 28m 26s, not 6m
33s. Status counts was submitted at 03:34:52.369541; submit-to-done was 5m 45s.

No SF changes, daemon replacement, provider retries, manual worktree edits,
database restoration, or cancel/resubmit occurred between the first actual
start and the second `done`. The operator steps were submit/start, read-only
inspection, and one guarded approval per exact candidate. Approvals were
applied by the assistant through SF under Sofia's standing delegated
authorization, not represented as new personal head-specific human reviews.
No direct GitHub merge, admin bypass, queue, or autonomous mode was used.

### Exact identity and CI

- PR #4 base: `add08cff903e8c9bbe46b1a4a97d5c6eabcea53b` (the prior
  delivered PR #3). Candidate: `d1d6838fd5e58df8268fdd9aade99b3adc88ec6b`.
  Approval: 03:33:44.685372. Merge: 03:33:58, OID
  `758db5ad679ad03bf72b94387a9463f751e5e190`.
  [Required CI](https://github.com/nysa-company/sf-v1-relay-pilot/actions/runs/33942107131)
  and [post-merge CI](https://github.com/nysa-company/sf-v1-relay-pilot/actions/runs/33942209879)
  passed. Only `app/tools/job-event.js` and its test changed.
- PR #5 automatically used PR #4's merge as its base. Candidate:
  `c4df97a03b7f6ab3aea5412c167142dbf6444ab6`.
  Approval: 03:40:16.578413. Merge: 03:40:30, OID
  `92b048aaa4d1c16eba2e7079d0eb078373876326`.
  [Required CI](https://github.com/nysa-company/sf-v1-relay-pilot/actions/runs/33942440009)
  and [post-merge CI](https://github.com/nysa-company/sf-v1-relay-pilot/actions/runs/33942509038)
  passed. Only `app/tools/job-status-count.js` and its test changed.

Both tickets ended at `done`, version 11, runner 1. Each retained the
reviewer's verification checkpoint before Builder implementation. The primary
checkout was not advanced to manufacture base freshness: worktrees obtained
their hosted bases through the factory's authenticated creation path.

SF recorded `usage_units=0` for all eight provider attempts. Per-ticket token
and billing breakdowns were not available in the durable evidence; this is
not a claim that the subscription or total engineering cost was zero.

## Failed trials and repairs before the final freeze

1. Inspect Job (`SF-78ef0960109701048a4491f8c8995992`) used stale local
   main after a hosted merge. Publication correctly refused. It remains
   paused with candidate/worktree intact; it was not rebased, cancelled, or
   resubmitted. Commit `3007aca` repairs fresh hosted base selection and pins
   each new worktree's base under its existing durable creation lease. A
   real temporary-remote regression proves both fresh next-ticket creation
   and unchanged existing-ticket identity after later remote movement.
2. An isolated blocked-provider restart failed historical leader validation.
   The same commit records authenticated prior/current leaders atomically
   with recovery while retaining the original attempt window. Regression
   tests include another restart and rejection of missing bridge evidence.
3. Inspect Approval (`SF-8114f97c7ef958c6d1a408da59377ff1`) planned
   successfully but asked about whitespace-only identifiers. It remains
   paused with its immutable question-bearing result. The CLI has no
   supported answer path; an ordinary resume would reuse that result.
4. That semantic pause retained capacity, and repeated `pause` incorrectly
   returned success without draining it. Commit `adb3ef5` requires a runtime
   join, sealed Store drain proof, and exact-fence capacity release for this
   case. The live supported pause then freed capacity without changing the
   ticket's paused version or rewriting evidence. The queued linked-event
   ticket subsequently started without resubmission.

The question/answer loop remains a usability limitation. It was not silently
fixed by editing a stored plan or treating the first suggested answer as
operator authority. Older draft PRs and failed trials were not deleted.

## Verification and isolated recovery matrix

Before final freeze, `go test -p 1 -timeout 30m ./...` passed, along with
focused daemon race tests, `go vet ./internal/daemon`, repository checks,
secret scan, and diff check. The base/blocked-leader repair also passed the
full fresh suite, `go vet ./...`, and focused Store/Git/worktree race tests.

After both live deliveries, an uncached anchored test run passed on the same
source for all of these cases:

- `TestProviderBlockedRecoveryAfterLeaderReplacement`
- `TestWorkerReconcilesLostCreateWithoutBlindReplay`
- `TestGuardedApprovalMergeRecoversLostProofResponse`
- `TestVerifyProtectedBranchReusesConfirmedProofAfterLostResponse`
- `TestDaemonProviderRetryRetainsProviderWrittenInvalidArtifactWorktree`
- `TestTicketBudgetExhaustionIsStoreAuthenticatedAndNonRecoverable`
- `TestWaitingCIWorkerBlocksWhenObserverIsUnavailable`
- `TestPauseDrainsQuestionPausedTicketBeforeReleasingCapacity`
- `TestPauseQuestionCapacityRefusesOutstandingEffect`

Package times: publication 9.793s, mergeproof 3.566s, daemon 6.063s, Store
0.727s, localruntime 0.505s. These are isolated deterministic fixtures, not
claims of live GitHub/provider chaos or hostile-process containment.

Final read-only dev checks found zero capacity leases, active/quarantined
provider attempts, repository-command leases, and Git mutation leases. The
dev foreground daemon remains on the frozen build; no additional work was
submitted after the two deliveries.

## Recommendation

Keep this executable as the pilot baseline. Do not start concurrency/load
testing yet. The next targeted factory improvement should be a bounded,
durable planner-answer workflow exposed by `show`/`status`, with an explicit
answer command and authenticated continuation. Then repeat a question-bearing
ticket end to end. Prefer a small number of realistic serial tickets and
failure-specific regressions over another broad hardening or rewrite cycle.
