# Concurrent-ticket acceptance

Status: automated acceptance passed September 6, 2026. Baseline: `bfae1fd`.
Results: [concurrency scorecard](../reports/2026-09-06-concurrency-acceptance.md).

## Goal

Prove bounded, safe concurrency at capacity two through isolated, reproducible
ticket scenarios. Preserve SQLite authority, independent verification, exact-head
human approval and stable/dev isolation. No live database surgery, public release,
or implicit merge is part of this campaign.

## Acceptance matrix

1. Two tickets make overlapping progress; a third remains queued at capacity.
   After a supported start retry when capacity becomes available, it proceeds.
   Do not introduce automatic start merely to satisfy this test.
2. Cancel/drain one ticket while its sibling remains active. Capacity is retained
   until safe drain, then becomes available exactly once.
3. Restart with two occupied slots. Authenticate recovered identities, retain
   occupancy, reject stale callbacks, and resume through supported boundaries.
4. Lose a committed start/publication response. Reconciliation must not duplicate
   a ticket, provider launch, PR or other external mutation.
5. Contend on the same primary repository with distinct ticket worktrees. Provider,
   repository-command and Git writer exclusion must remain effective.
6. Repeat selected scenarios and run the race detector. Assert no false terminal
   success, wrong-ticket mutation, leaked writer or leaked capacity.

The composition gate uses the compiled development CLI/daemon with two fixture
provider routes at capacity two. Both tickets must traverse planning, independent
verification, build, native command proof and publication into distinct draft
PRs on one private bare repository. Assert overlapping pipeline timestamps,
distinct worktrees and no ready/merge action without approval. This is stronger
than scheduler stubs but remains controlled-provider acceptance, not live delivery.

## Execution

Inventory existing scheduler, daemon, Store and compiled workflow coverage first.
Reuse fixtures and add missing multi-ticket composition, rather than duplicating
already-covered isolated predicates. Use synchronization barriers for overlap and
fault timing; elapsed sleeps alone are not evidence of concurrency.

Run one test campaign at a time to avoid shared-host contention. Fix only
reproduced blockers and add a regression for each. Run the repository baseline
before declaring code complete. Save commands, results, skips and interventions
in a concurrency scorecard; failed trials remain in the record.

Controlled provider/GitHub processes and local bare repositories are automated
acceptance fixtures, not live-model or hosted delivery evidence. Ten-ticket
unassisted reliability and unfamiliar-user onboarding remain separate targets.
Do not substitute this campaign for those observations.

## Completion

All six scenarios have explicit passing evidence at the strongest exercised
boundary, repeated concurrency and focused race checks pass, baseline checks
pass, and a saved scorecard documents limitations. No safety invariant is relaxed
to make a scenario pass. Existing live runtimes and projects remain unchanged.
