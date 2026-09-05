# Relay concurrency campaign

Date: 2026-09-05. All timestamps below are UTC.

## Verdict

**The scoped concurrency/stress campaign is complete.** Final confirmation
[PR15](https://github.com/nysa-company/sf-v1-relay-pilot/pull/15) and
[PR16](https://github.com/nysa-company/sf-v1-relay-pilot/pull/16) both delivered,
with177.439 seconds of successful provider overlap at peak2. The second
delivery exercised same-PR base refresh and a fresh independent review.
See the final confirmation and completion audit below. This is not a stable-v1
or unattended-reliability certification.

The ten-ticket live stress run finished with four factory deliveries and six
cancelled trials. Capacity-two operation, overlapping provider execution,
guarded exact-head merges, protected-base refresh, and native restart recovery
were exercised. This is **not an unattended-reliability or stable-v1 verdict**.
The run exposed production contention/refresh bugs and generated-test defects.

The final read-only live inventory found zero active/quarantined providers,
Git leases, repository-command leases, admission leases, and executing/uncertain
effects. No live database rows or factory worktree files were manually repaired.
Stable, Nysa, prior pilot evidence, and the registered primary checkout were
preserved. Draft PR10 remains open as retained evidence, not an active run or a
delivered ticket. Earlier draft PR1/2 remain unchanged.

This report supersedes the delivery limitations in the
[isolated stress baseline](2026-09-05-concurrency-stress-baseline.md), while
retaining that report as historical evidence.

## Ten live outcomes

All tickets used guarded mode. The per-ticket ceiling was 90 minutes/$20;
duration begins at submission, not at start. The campaign included debugging
and test/deployment pauses, so elapsed times are not clean throughput benchmarks.

| Trial | Ticket | Outcome | Start to terminal | Evidence |
| --- | --- | --- | --- | --- |
| 1 event inspection | SF-b5001ad66009928dab7cc8bba8b990b7 | Cancelled | 59m53s | Reviewer assertion contradicted its required fixed error text; no manual test repair |
| 2 approval count | SF-b2c6074ad715180c0f6be5fc47c773fb | Cancelled | 1h56m30s | Initial worktree contention recovered; later unleased command blocked cancellation until supported startup recovery |
| 3 approval status counts | SF-f8314c3ea05eb4996319ca14bc54b984 | Done | 8m09s | [PR7](https://github.com/nysa-company/sf-v1-relay-pilot/pull/7) |
| 4 event type counts | SF-1d3cb7025538bdcf52686ecdf26545f4 | Cancelled | 6m38s | Test expected `constructor` before `__proto__`, contradicting required UTF-16 order; exact test passed 9/10 |
| 5 outbox count | SF-0242907f811be459e70ac76d7cd4a0e1 | Done | 16m51s | [PR8](https://github.com/nysa-company/sf-v1-relay-pilot/pull/8); one planner artifact retry; post-merge restart recovery |
| 6 outbox count by approval | SF-e39a12096b8695f18704065ce896cf48 | Done | 12m01s | [PR9](https://github.com/nysa-company/sf-v1-relay-pilot/pull/9); automatic protected-base refresh and fresh Builder |
| 7 jobs with errors | SF-c01293d040c4f763c5bdeb8ed0dcfc70 | Done | 10m53s | [PR11](https://github.com/nysa-company/sf-v1-relay-pilot/pull/11); automatic refresh and fresh Builder |
| 8 total job attempts | SF-4776c3e413b389d99b3367b6c4de6d6a | Cancelled | 15m38s | [Draft PR10](https://github.com/nysa-company/sf-v1-relay-pilot/pull/10) passed CI/review but stalled on old PR base snapshot; budget expired during diagnosis |
| 9 total job retries | SF-607fb8ea6364d5a11ef4696ee1378b86 | Cancelled | 2m05s | Typed `provider_result_indeterminate`; prescribed action was cancel/fresh submit |
| 10 attempts since retry | SF-1565de29879c0b38b94ebc255ddf6101 | Cancelled | 4m23s | Planner/Reviewer completed; Builder stopped at original ticket deadline, `ticket_budget_exhausted` |

All ten were submitted before they started. Trials4–10 were submitted at
12:00:25; the last two did not start until 13:24:50 and13:26:55. Their limited
remaining budgets partly reflect campaign orchestration, not model execution.
No budget was silently extended. No replacement tickets were added to improve
the success percentage.

## Delivery and protection evidence

Every delivered PR changed only its named tool and test file, passed required
GitHub `test`, and completed independent final review. The operator reviewed
the exact diff and fresh protection, then used `sf-dev approve`; SF performed
the merge and recorded `done`. There was no direct `gh pr merge` for factory
deliveries, no admin/queue/automerge bypass, and no repeated merge mutation.

| PR | Reviewed head | Merge commit | Merged at | Required CI run |
| --- | --- | --- | --- | --- |
| 7 | 7b46a061e5d4b3e5139afd417178371ca8c5c6a1 | 54d3c9514185a9434555ba0b51bdfdac8727d785 | 12:06:55 | 33965019481 |
| 8 | f69f6dfedaf3285360b9aaf148d0e70a6cc4b3d0 | 0fe4514343fe4afde2a60e2d3176d382630bf01a | 13:03:12 | 33967686836 |
| 9 | 22f6e9c207b1f8df70101dbcd955cee6b4f0989a | 61f9b6f29c5c829aa949c59ccf410725598d4052 | 13:13:39 | 33968008711 |
| 11 | 87d9a7eeac2a48fa9ef78a77d9644ae431cbe294 | 3eb86bb96d8d372656c7f8ea3c4e1293bb40e2b4 | 13:24:10 | 33968689738 |

Parent-inclusive ruleset inventory contained only active Repository rule22065613
for exact `refs/heads/main`: no bypass actors, current user bypass `never`,
strict required `test` integration15368, squash only, non-fast-forward and
deletion protection. SF independently rechecks this authority at its launch gate.
Latest main CI run33968794875 passed on merge3eb86bb9.

Setup-only PR6 changed capacity documentation and is excluded from delivery
counts. The authorized immutable dev configuration was generation3, capacity2;
production remains hard-capped at two. Default provider composition remains one,
with explicit `SF_CODEX_PROVIDER_CAPACITY=2` for this campaign.

## What “concurrent” means here

- Trials5 and6 had overlapping active lifetimes from13:02:19 to13:12:31 and
  both delivered. Trial6 refreshed after trial5 moved `main`.
- Trials6 and7 also overlapped from13:13:24 to13:14:19 and both delivered.
- Actual simultaneous released planner processes were observed for trials4/5
  starting12:55:47.968708 and12:55:49.723592; exactly two global, project and
  provider leases were present. Third admission refused without mutation.
- Trials7/8 had simultaneous Builder execution from13:20:13.886673 through
  13:20:57.177982. Their final reviewers also overlapped. Trial8 later cancelled;
  do not describe that particular pair as two successful deliveries.
- Initial Relay26/27 ticket concurrency did not imply provider overlap: the
  shared provider route defaulted to one. That distinction was measured and
  corrected explicitly, not inferred from ticket state.

The final independent timestamp audit found **zero provider-call overlaps
between two eventually delivered tickets** in this original ten-ticket batch.
Peak provider concurrency was two, but those pairs each included a cancelled
trial. A separate fresh confirmation pair was therefore required for the
stronger goal of two successful, simultaneously executing provider workflows.
The final confirmation below satisfies it, separately from the original
four-of-ten success rate.

Across the original ten tickets there were37 provider attempts and33 immutable
completed results: two invalid-artifact failures, one indeterminate result and
one budget exhaustion. Each delivered ticket has exactly one confirmed draft
creation, merge effect and merged observation; refreshed deliveries each have
two candidate generations but one PR. Native Git diffs for every delivered
generation match its two declared regular100644 files exactly. One sealed
control row retained for cancelled trial2 is historical evidence, not an active
lease or unresolved effect.

## Reproduced bugs and repairs

1. **Worktree creation contention/absence recovery.** A claim issued before
   an unavailable repository lease could strand creation. The native absence
   proof authenticates directory, branch, private ref and registration before
   recovery; no manual worktree/DB repair. Delivered in Relay27/031ba66.
2. **Command/commit contention.** Ordinary sibling access could leave an
   unleased executing command or strand a dirty completed Builder. Commit
   cf5fbf1 adds a typed pre-insert contention outcome and bounded same-call
   waits. Same-claim leases, quarantine and ambiguous responses remain errors.
   Repository commands share their caller/spec deadline; Git waits cap at2m.
3. **Protected merge-proof contention.** PR8 merged but its child proof became
   uncertain before acquisition. Existing recovery intentionally requires a
   restart; idle Relay29 restart recovered parentclaim2/proofclaim3 and reached
   done without another merge. Commit19acf19 adds the same pre-insert wait for
   `protected-ref-fetch`; PR9 subsequently completed at claim1 under contention
   without a restart. Uncertain external merges are not blindly retried.
4. **Old PR base snapshot.** GitHub kept PR10's authenticated original BaseOID
   after `main` advanced. Published refresh wrongly required it already equal
   the new tip. The final patch accepts only original published base or exact
   freshly observed remote base, rejects every third value, and retains all
   source, ancestry, reservation, prepare and CAS authority. Real Store/Git
   regression now retains old FakeGH PR base instead of masking the behavior.
   This fix is tested in isolation; the expired PR10 trial was not rerun live.

Generated-test contradictions in trials1/4 were not fixed by silently editing
Reviewer-owned tests. The factory correctly refused their resulting commands,
but this remains a product-quality failure. Provider indeterminacy and budget
exhaustion remained typed, bounded outcomes rather than fabricated success.

## Fault coverage and validation

The [baseline report](2026-09-05-concurrency-stress-baseline.md) records 200
seeded isolated cases: Store capacities2/4 and runtime two-worker/four-caller
tests under race. Four-caller stress does not enable a four-worker daemon.

The full suite includes these representative regressions:

| Fault | Regression |
| --- | --- |
| Targeted pause/drain leaves sibling running | `TestDaemonFactoryTwoWorkerPauseDrainsOnlyTargetAndResumeRearms` |
| Duplicate admission and reuse | `TestSeededLeaseAdmissionStress`, `TestEnsureConcurrentCallersCreateExactlyOneWorktree` |
| Lost creation response / unproven push refusal | `TestEnsureReconcilesCreationResponseLossAfterReopen`, `TestWorkerKeepsUnprovenPushUncertainAfterLostCommandResult` |
| Lost edit/merge response | `TestUpdateFactoryPullRequestReconcilesLostResponseWithoutSecondEdit`, `TestMergeLostResponseReconcilesFromOriginalBaseWitness` |
| Invalid artifact bounded repair | `TestInvalidArtifactFailureReasonsAreDurableAndBounded`, `TestFallbackInvalidArtifactExhaustionIsStableAcrossRestart` |
| Unavailable/malformed GitHub observation | `TestCreateFinalHandoffRefusesUnavailableOrMalformedBaseObservation` |
| Budget exhaustion | `TestTicketBudgetExhaustionIsStoreAuthenticatedAndNonRecoverable` |
| Cancellation after unleased command | `TestUnleasedRepositoryCommandCancellationCompletesAfterStartupRecovery` |
| Old PR base and unrelated base | `TestPublishedBaseRefreshLostApplyResponseRecoversToBuilding`, `TestPublishedBaseRefreshRejectsUnrelatedObservedPullRequestBase` |

Validation checkpoints:

- Full serialized normal suite46348 passed at cf5fbf1; targeted race91807
  passed Git2.307s, Store37.893s, repositoryexec1.585s, Codex4.601s.
- Full static gate passed: format, vet, repo-check, secret-scan, artifact-check,
  docs-smoke and release-build-smoke.
- Protected-proof extension race45003 passed Git2.366s.
- Final old-base/same-PR refresh race36372 passed publication45.998s. The
  localruntime package had no selected tests in that narrow invocation.
- Final full normal run1802 passed: Git180.873s, publication96.662s,
  Store121.218s, workflowruntime110.951s, worktreecoord128.909s. Compiled
  guarded/manual/takeover/channel-coexistence passed234.557s. All static gates,
  including release-build smoke, passed in the same run. The stronger live
  confirmation pair was still pending at that checkpoint and was not implied
  by those tests.

## Next priorities

### Fresh confirmation update, 14:11 UTC

Validated commit b0fac2f ran as Relay30, leader36, qualified pair59/60,
capacity2. Fresh submissions at13:59:32 did not inherit depleted queue budgets.
SF-3aa375b829a6e219034c1ccdac0075bb delivered through
[PR12](https://github.com/nysa-company/sf-v1-relay-pilot/pull/12): exact head
9fcc98da6f9c3404aa102013eecf72e791b07748, required CI33970876772 passed,
independent final review passed, fresh protection/diff checked, SF-approved,
merged14:11:01 to37bc539209819ed7b7075611c8dd79a8c8d4fc94, localdonev11.
Its prepared uncertain verification commit recovered automatically at14:05:56
without restart or manual repair.

Sibling SF-d6c21776cdeefd08aca00f7b1827d539 overlapped planning and verification,
but failed two Builder artifacts after rewriting its Reviewer-owned test.
Supported retry refused dirty state without mutation; supported cancel left
the worktree intact. Replacement SF-eb3bcc8b59745a87fb3410ac8e65b22b started
after the successful sibling's final provider call and returned an indeterminate
planner result after30s. Supported cancellation retained its evidence and
released admission. These extra
trials are separate from the original4/10 and do not prove successful provider
overlap. The goal remains open.

Coverage precision: the push-loss fixture returns before running Git; it proves
no blind replay, not composed recovery after an applied push loses its reply.
Creation/edit/merge tests independently cover post-mutation lost responses.
The daemon pause test directly checks sibling continuity after pause, but not a
second sibling snapshot after terminal cancellation. Neither stronger claim is
made here.

Next diagnostic repair: indeterminate provider results currently persist no
closed failure category beyond the generic outcome. Exit code and truncation
flags exist only transiently; raw provider output must remain unpersisted.
The recurring30s planner failure cannot be attributed to any particular cause
from present evidence. Bounded non-secret classification is now implemented;
full normal suite16271, targeted race (contracts1.467s, adapter1.573s,
coordinator7.837s, Store10.927s), and all static gates passed before rollout.
This does not retrospectively identify the old failures or authorize blind retry.

1. Keep capacity at two. Fix generated verification quality before another
   broad stress run: deterministic examples/property assertions must agree
   with acceptance, and independent amendment must be used for contradictions.
2. Improve queue-budget visibility and intake scheduling. Submit live jobs
   just in time for a throughput run; explicitly redesign queue-time budget
   semantics only through a reviewed contract, never by changing active rows.
3. Repeat a small same-PR stale-base acceptance run with the final repair,
   then a fresh unattended capacity-two batch. This run's40% delivery rate and
   repair restarts are not a reliability target.
4. Keep hosted CI for broad portable checks, and macOS local acceptance for
   native supervision, credential transport, sockets and stable/dev coexistence.

Dollar cost is unavailable from this evidence; subscription provider admission
and usage units are not an audited monetary bill. Explicit operator actions
included four exact-head approvals, six cancellations, qualification after
repair rollouts and one post-merge recovery restart. Report those interventions
rather than describing the campaign as unattended.

## Second confirmation and final-review refresh defect

Relay31 (`f267719`) started a fresh pair at14:39 UTC with full90-minute/$20
limits. SF-f0e41df7a1b66127a00a1068477cbd31 delivered
[PR13](https://github.com/nysa-company/sf-v1-relay-pilot/pull/13), merged at
14:48:06 to `3b2ff8479ad890496db846661123756e779f7807`; local state is donev11.
Its exact reviewed head was `0ef53b0cb7e5c60a843355538e6f4d7335cfd702`, with
required CI33972768370 and independent final review passing before SF approval.

Sibling SF-8f05243f19d6be0cc1592764490fcf3e retained
[PR14](https://github.com/nysa-company/sf-v1-relay-pilot/pull/14) through an
automatic protected-base refresh. Candidate2 head
`83474cf25a4a7568d7396d4085b52ac1f5066c6f` uses PR13's merge as its base;
CI33972962797 passed. At16:00 it remained reviewingv12, with no fresh review
attempt. It is **not counted as delivered**.

Independent timestamp audit proves135.939 seconds of successful cross-ticket
provider overlap, peak2. Native diffs for both sibling generations and PR13
contain only their two declared regular files. There was one PR14 creation and
one edit, not two PRs. These facts do not establish two completed workflows.

Read-only diagnosis proved current final-review authority and clean worktree
both pass. The reusable-result selector instead selects the old v7 review,
then rejects its attempt to traverse the refresh as a stale fence. Worker
launches a new review only on authenticated absence, so every tick stalls.
The narrow repair recognizes only the exact predecessor review consumed by
an authenticated completed refresh, validates current successor/CI authority,
and reports that a fresh review is required. It does not reuse the old review.
The new full-lifecycle Store regression passes with the repair and fails with
`ticket fence is stale` when the repair call is disabled. Unrelated historical
review evidence remains refused. Full normal validation (`go test -p 2 -count=1
-timeout=30m ./...`), the targeted race regression, and `make verify-static`
all passed in session34163. Store239.218s, Git429.559s, workflowruntime215.325s,
and the targeted race17.355s are recorded wall times, not throughput promises.
Relay31 has not been replaced and PR14 has not been approved. Its original
deadline passed at16:09:21. Supported SF cancellation completed at16:11 UTC,
leaving cancelledv14/runner2 and preserving draft PR14 and its worktree. This
second confirmation is one delivery and one cancellation, not a successful
concurrent-delivery pair. The regression/validation delay is an operator
intervention and is included in the failed trial's elapsed time.

## Final confirmation: two concurrent deliveries

Validated repair `c6913abbc34e2b3f26cec608f8f8889e840604ff` ran as Relay32,
dev leader38, independent qualified pair63/64, capacity2. The idle Relay31
stopped cleanly before rollout; no active leases existed. Two unchanged,
previously undelivered specs were submitted just in time at16:22:04 and started
back-to-back, each with its original90-minute/$20 ceiling.

| Ticket | Delivery | Submitted to done | Reviewed head | Merge |
| --- | --- | --- | --- | --- |
| SF-679b3a5bcb5c8e3e369ca065b4644c1e, approval count | PR15, donev11 at16:30:11.896 | 487.144s | 9890cff59469f6a4685e984fd42e171aabe8027d | a57886d38daff844f15eff041ca9859310e40bf7 at16:30:07 |
| SF-6a37a4f7028c8654529c98a7043db323, retry count | PR16, donev16 at16:34:00.407 | 715.622s | fe028f814c63e9fda2fb1698443f24223842cd3f | e65a0bc5438f66ca32db1436d73c1a226c0e0e4f at16:33:54 |

Required PR CI33977959697 and33978152528 passed. Protected-main CI33978261707
also passed on the final merge. Protection was freshly rechecked before both
SF approvals: exact active Repository ruleset22065613, strict test15368,
squash-only, no bypass, no additional parent/evaluate rulesets.

Both delivered workflows had successful overlapping provider calls:

| Calls | Exact UTC overlap | Seconds |
| --- | --- | --- |
| Both planners | 16:22:27.085057–16:23:07.329765 | 40.244708 |
| Approval verification / retry planner | 16:23:08.967183–16:23:15.171291 | 6.204108 |
| Both verification reviewers | 16:23:16.679741–16:25:04.580935 | 107.901194 |
| Both initial final reviewers | 16:28:51.115684–16:29:14.204300 | 23.088616 |

Union overlap177.438626s; peak2. Approval count used4/4 successful provider
calls. Retry count used6/6: its second Builder and final Reviewer belong to
new authenticated refresh phase entries, not failed-attempt retries. There
were zero provider failures/retries, no runtime restart, no cancellation, and
no worktree/DB repair during this final pair. Operator actions after start
were the two exact-head SF approvals following diff/protection checks.

The second candidate's initial head wasfa51540a34e88a7124e18027f8847e4d9536f82e
on base3b2ff8479ad890496db846661123756e779f7807. After PR15 merged, SF refreshed
to its merge base, created candidate2, updated the same PR16, and completed
fresh CI and review2 at16:32:45.841603 before approval. This reproduces and
verifies the repaired review-selection behavior in the real factory.

Independent read-only SQLite and native Git audit verified:

- All three candidate generations differ from their exact base only by the
  declared tool and test, each regular mode100644; refreshed tool/test blobs
  were preserved. No cross-ticket changes.
- Each ticket has exactly one draft creation, merge effect, guarded merged
  observation, and approval. PR16 has one edit and two generation-specific
  pushes but one distinct PR. Semantic keys are unique.
- Effect counts are10 and16. Each includes one expected failed prebuild
  command proving the implementation was missing, not a provider failure.
  Two pre-launch claim reclaims retain one semantic operation each.
- Final global active/quarantined providers0, Git leases0, command leases0,
  admission leases0, executing/uncertain effects0, external quarantines0.
- Draft PR1/2/10/14 and failed worktrees remain retained evidence. The primary
  local pilot checkout remains clean at2373b836; stable and Nysa were not
  modified. No manual live SQL or factory-file repair occurred.

Keep outcomes separate: original10 trials4done/6cancelled; first confirmation
3trials1done/2cancelled; second confirmation2trials1done/1cancelled; final
confirmation2trials2done. Overall8 deliveries across17 trials is not a clean
throughput or reliability benchmark because repair/validation pauses consumed
earlier ticket budgets. Failed trials have not been relabelled as successes.

## Completion audit against the authorized objective

| Requirement | Evidence and result |
| --- | --- |
| Two real concurrent Relay deliveries, cap2 | PR15/16 done; exact successful overlap above; peak2; no production cap4 |
| Ten-ticket campaign | Original ten immutable outcomes recorded, including all six cancellations |
| Repeatable isolated capacity2/4 stress | 200 seeded Store/runtime/Scheduler cases, baseline race run and current full normal suite; source asserts bounds, duplicate admission, drain and zero residual runs/leases |
| Shared-base collision and recovery | Private-bare refusal regression; same-PR refresh/lost-Apply tests; live PR16 refresh + new Builder/CI/review/approval |
| Restart, targeted pause/cancel, duplicate requests | Named fault regressions above; original live cancellation/startup recovery and prepared commit recovery; final pair needs no recovery intervention |
| Lost responses, invalid artifacts, unavailable GitHub, budgets | Named current regressions above plus retained live failures; no claim that pre-push failure tests prove applied-push lost-response recovery |
| Reproduced blockers fixed with regressions | Creation/command/proof contention, old observed PR base, and final-review cutover repairs; exact final regression fails before/passes after and live PR16 confirms it |
| No duplicate mutations, cross-ticket writes, leaked authority | Independent current DB/Git audit and exact final counts above |
| Preserve stable/Nysa/prior evidence; no manual repairs | Dev-only runtimes and SF controls; clean unchanged primary checkout; retained failed trials/PRs; no direct DB/worktree mutation |
| Record timings, retries, interventions | Original outcome table, separate confirmation cohorts, exact overlap and elapsed times, approvals/cancellations/rollouts; dollar cost explicitly unavailable |

Recommended next work is generated-test quality and predictable intake/budget
visibility, not a higher concurrency limit. The combined sequence of base
refresh followed by a further CI-repair generation remains unproven; this
repair authenticates the direct refreshed successor and fails closed outside
that case. Do not present the completed campaign as exhaustive edge-case
coverage or permission for unattended deployment.
