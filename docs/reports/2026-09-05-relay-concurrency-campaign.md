# Relay concurrency campaign

Date: 2026-09-05. All timestamps below are UTC.

## Verdict

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
trial. A separate fresh confirmation pair is therefore still required for the
stronger goal of two successful, simultaneously executing provider workflows.
It must not be folded into the original four-of-ten success rate.

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
| Lost creation/push response | `TestEnsureReconcilesCreationResponseLossAfterReopen`, `TestWorkerKeepsUnprovenPushUncertainAfterLostCommandResult` |
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
  confirmation pair remains pending and is not implied by these tests.

## Next priorities

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
