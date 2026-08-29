# DBOS SQLite proof spike

Run this module without importing it from the production module:

```sh
cd spikes/dbos
go test ./...
```

The tests exercise the selected DBOS version with a pure-Go SQLite driver and
the application tables in the same SQLite file. They specifically probe stable
workflow IDs, completed-step replay, recovery from the state-transition to
workflow-ownership gap, lost external-effect responses, bounded SQLite locking,
channel isolation, and process-group cancellation.

This is decision evidence, not a fallback implementation. Production code must
remain dependency-free until `docs/decisions/0001-workflow-engine.md` records a
PASS. A failure of any critical test selects the one-runtime custom state engine
described in that decision record.
