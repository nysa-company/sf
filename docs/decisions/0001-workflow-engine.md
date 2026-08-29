# 0001: workflow engine selection is gated by the DBOS SQLite spike

Status: decided — DBOS rejected for v1

## Context

sf needs one local durable workflow runtime backed by the channel SQLite file.
The runtime may checkpoint local work, but application tables remain the sole
authority for tickets, effects, approvals, and recovery. External Git and
GitHub effects stay fenced and reconcile before retry regardless of runtime.

## Decision gate

`spikes/dbos` pins DBOS Go `v1.2.0` and runs with the pure-Go SQLite driver.
It must pass with no external database or service:

```sh
cd spikes/dbos && go test ./...
```

The gate validates SQLite operation, persisted workflow IDs, completed-step
non-replay, deterministic recovery after `queued -> planning` is durable but
workflow ownership is absent, lost-response reconciliation, bounded SQLite
contention, stable/dev database separation, and process-group cancellation.

The remaining product-level tests (signals, engine-version migration, event
projection, leader locking, and all crash injection points) remain mandatory
in the main verification plan. This spike does not waive them.

## Acceptance rule

Choose DBOS only if the spike and every plan criterion pass on the supported
macOS environment within one working day. In particular, the transition to
durable workflow ownership must be atomic, or startup must safely adopt or
re-enqueue exactly one workflow using the persisted stable workflow ID. An
unknown result is a typed blocked state, never a silent retry.

## Fallback contract if DBOS fails

Use one small explicit Go engine over the existing SQLite schema. Its boundary
is deliberately narrow:

```text
StartOrAdopt(ticket, stableWorkflowID) -> durable phase ownership
Signal(ticket, command)                -> fenced state transition
Recover(channel)                       -> observe/adopt/block
```

It must preserve the normative state-machine JSON, ticket/leader/runner/effect
epochs, one transaction per state event, and the same effects reconciliation
protocol. It is the only production runtime in that outcome; DBOS is not
retained as a second path.

## Evidence and verdict

Run on 2026-08-29 with Go 1.25, DBOS Go `v1.2.0`, and the DBOS-registered
pure-Go `modernc.org/sqlite v1.54.0` driver:

```text
cd spikes/dbos && go test ./...
--- FAIL: TestSQLiteBusyHonorsContextDeadline (1.04s)
busy operation outlived bounded deadline: 1.034s
```

The test intentionally holds an exclusive SQLite writer, gives a competing
operation a 40 ms context deadline, and configures the database's busy timeout
to one second. The operation waited for the busy timeout instead of returning
at the context deadline. That violates the approved requirement that busy or
locked work produce a bounded typed result with no surviving retry.

**Decision: FAIL.** DBOS is not selected for v1. Do not add it as a production
dependency or run a second proof attempt. Implement the single custom engine
contract above and make its bounded SQLite behavior part of its first tests.
