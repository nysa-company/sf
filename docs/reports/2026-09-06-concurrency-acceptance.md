# Concurrent-ticket acceptance

Status: automated concurrency acceptance passed. Local baseline `bfae1fd`; no live runtime,
project, database, remote, approval or merge changed by this campaign.

## Reproduce

On the supported macOS host, run `make test-concurrency`. It runs fifteen
selected tests three times under the race detector, serializing packages, then
the compiled two-ticket workflow twice without the race detector.
Fixtures use disposable SQLite databases, Unix sockets and local Git repositories.
No provider credentials or GitHub mutation are required.

## Scenario coverage

| Scenario | Evidence boundary |
| --- | --- |
| Three concurrent run requests, capacity two | New `TestConcurrentCLIRunQueuesThirdWithoutDuplicatingStarts`: actual CLI execution over Unix socket into daemon/Store; exactly two starts, one durable queued ticket, exact input replay without duplicate starts |
| Cancellation retains capacity until drain | Same test holds a controlled drain false, refuses third admission, then completes drain and admits the queued ticket; sibling version/state/runner are unchanged |
| Runtime slot transfer | `TestDaemonFactoryTwoWorkerPauseDrainsOnlyTargetAndResumeRearms`: real scheduler/runtime/controller with controlled blocking workers; target pause/resume/cancel, sibling remains active, new ticket enters released slot |
| Two occupied slots across two restarts | New `TestConcurrentTicketCapacitySurvivesTwoDaemonRestarts`: real daemon close/start, durable recovery increments each runner/version, preserves exact lease identity/acquisition time, third start remains refused |
| Lost committed response | Real CLI/socket submit/start replay tests; publication fixtures cover lost create without blind replay and uncertain push retained rather than fabricated success |
| Same-repository exclusion and eventual admission | Real-Git `TestEnsureExcludesActiveRepositoryCommandWriter` now proves blocked sibling creates nothing, exact lease release permits creation, replay creates one worktree and holder identity stays unchanged |
| Contention and stale authority | Fifty-seed scheduler and Store tests exercise duplicate callers, bounded admission and slot reuse; terminal-ticket tests refuse new writers after reassignment |
| Publication under base movement/restarts | Private bare remote moves protected base; stale publication stays mutation-free; candidate-only publication recovers through two leader changes |
| Compiled two-ticket workflow | Two fresh trials passed: compiled CLI/daemon, qualified fixture providers, native red-to-green executor, independent worktrees and separate PRs; overlapping pipeline timestamps and no approval/merge |

The runtime workers in the overlap test are controlled fixtures, not Codex.
Store launch/drain claims in some tests are modeled, not OS process proof.
Real-Git tests execute Git, but remote publication uses controlled adapters.
These distinctions are intentional: this scorecard is not a claim that two
live-model tickets were delivered concurrently or that ten-ticket unassisted
reliability has passed. Native process containment and full compiled workflows
remain separate acceptance gates.

## Executed results and interventions

- Initial existing concurrency baseline, three repetitions: PASS (workflowruntime
  0.549s, daemon 2.430s, Store 1.101s).
- New concurrent CLI admission/replay test: ten repetitions PASS, then expanded
  cancellation/slot-transfer version ten repetitions PASS (2.540s).
- First race run: FAIL. The test-only deterministic ID counter had unsynchronized
  increments under concurrent socket requests. Production uses cryptographic
  randomness with local buffers. Added a mutex to the fixture; no production ID
  behavior changed.
- Restart fixture initially failed before admission because it supplied a
  test-only operator alias not configured after restart. The next diagnostic
  request lacked required scope parameters. Corrected fixture requests to use
  the authenticated peer and explicit channel/project; exact scenario then PASS.
- Corrected daemon/scheduler matrix, race count three: PASS (daemon 55.459s,
  workflowruntime 1.680s). The later runtime slot-transfer assertion's separate
  rerun is listed below; it is not included in that earlier result.
- Worktree contention matrix, race count three: PASS (57.034s).
- Store contention matrix, race count three: PASS (113.276s).
- Publication matrix, race count three: PASS (99.443s).
- Final runtime slot-transfer assertion, race count three: PASS (11.975s).
- Full baseline: PASS (`go test -p 1 ./...`, vet, repo-check, secret-scan,
  docs-smoke, artifact-check and diff-check; session 73293 exit zero). Native
  command package 63.694s, daemon 22.120s, workflowruntime 110.088s and
  worktreecoord 134.608s. Other package results include cached coverage.
- Compiled two-ticket trials: PASS twice (90.34s and 90.54s; session 46457,
  181.696s total, exit zero). Four fixture tickets reached four independent
  draft PRs across fresh environments, with native passing candidate proofs,
  no active provider/command leases, and zero ready/merge mutations. Tickets
  intentionally retain admission leases while waiting for CI, not terminal
  delivery. Named Make target recipes are dry-run checked; all constituent
  test selections were executed in the component runs recorded above.

No production behavior changed. The only defect repaired was the test fixture's
concurrent ID counter. The investigation traced the race to the injected fixture
before editing it; the production random-ID implementation was left unchanged.
Failures above remain recorded rather than counted as successful trials.

## Next decision

This gate supports a controlled live concurrency pilot, not a stable-release
claim. The next useful experiment is two small independent supported-stack
tickets with real providers on an explicitly selected test project, preserving
human approval and counting interventions. Do not widen concurrency beyond two
or add new runtimes based on this result alone.
