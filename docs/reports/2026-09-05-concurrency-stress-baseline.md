# Concurrency stress baseline

Scope: isolated tests on `feat/local-factory-v1`, starting at `1a0e032`.
This report does not claim concurrent live Relay delivery.

## Verified

- Store admission: 50 seeded cases at each of capacities two and four.
  Duplicate concurrent requests share admission; global/project limits are
  atomic; stale leaders cannot release; rejected tickets reuse released slots;
  each case finishes with no residual leases.
- Runtime: 50 seeded two-worker cases and 50 four-caller Scheduler cases.
  Duplicate source rows do not double-admit, targeted drain leaves the sibling
  running, cancellation joins all workers, and no active run remains.
  The four-caller test does not raise the production runtime maximum of two.
- Real private-bare Git regression: advance protected main after a candidate
  is built, then retry publication three times. All attempts refuse before any
  push or PR creation, and the candidate remains immutable.

Validation:

```text
go test -race -p 1 -count=1 ./internal/store ./internal/workflowruntime \
  -run '^(TestSeededLeaseAdmissionStress|TestRuntimePoolConcurrencyStress|TestSchedulerFourCallerConcurrencyStress)$'
PASS store 29.592s; workflowruntime 1.740s

go test -p 1 -count=1 ./internal/publication \
  -run '^TestSharedBaseMovementNeverPublishesStaleCandidateOnRetry$'
PASS 2.902s

go test -p 1 -timeout 30m ./...
PASS
scripts/repo-check: PASS
scripts/secret-scan: PASS (578 commits and working tree)
```

Tests ran on the authorized host with a private Go cache because the sandbox
cannot bind the Unix sockets needed by integration fixtures. Some packages in
the full suite were cached. Seeded inputs are reproducible; operating-system
goroutine scheduling is not claimed to be deterministic.

## Release gate still open

Protected-base drift is safe but not recoverable by the current publication
worker. The regression leaves the sibling in `publishing`: no Store-authorized
base refresh exists. Generic `base_or_candidate_head_changed` is explicitly
refused rather than granting authority from a caller's assertion.

Before same-repository concurrent deliveries can be called reliable, implement
and test a bounded authenticated refresh, including the case where both PRs
already exist. Preserve historical evidence and the same PR; rerun current-base
proof, CI, final review, and exact-head approval. Test crash replay and conflicts.
Do not replace the refusal with a permissive base-equality check.

Ten file-disjoint guarded Relay ticket drafts are prepared but not submitted.
No live runtime, machine capacity, pilot repository, or database was changed
for this baseline. Stable and Nysa were untouched.
