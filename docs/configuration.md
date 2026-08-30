# Configuration

The factory has two configuration layers: an optional owner-local machine cap
and an optional committed project file. Both are strict TOML; unknown fields,
shell strings, symlinks, oversized files, unsafe identifiers, and values above
the machine cap fail closed.

Stable and development read separate machine files:

```text
~/Library/Application Support/sf/stable/machine.toml
~/Library/Application Support/sf/dev/machine.toml
```

An omitted machine file uses conservative defaults: two tickets, a 45-minute
phase, a four-hour ticket, a USD 100 ticket ceiling, and autonomous mode off.
A machine file can narrow those values:

```toml
max_concurrent_tickets = 1
max_phase_timeout = "30m"
max_ticket_timeout = "2h"
max_ticket_cost_usd = 25
allow_autonomous = false
```

The optional project file is `<repository>/.sf/config.toml`. When it is
omitted, `sf init` selects a conservative command pair from the repository:

- A regular `go.mod` selects `go test ./...` for verification and review.
- A regular `package.json` with both `scripts.test` and `scripts.build`
  selects `npm test` and `npm run build`.

Repositories that match neither shape, match both, contain malformed metadata,
or use symlinked marker files are refused with an actionable `sf config --help`
(or `sf-dev config --help`) next step. An explicit file may select another exact argv pair, still
without shell interpretation.

Example explicit configuration:

```toml
base_branch = "main"
merge_mode = "guarded"
merge_method = "squash"
max_concurrent_tickets = 2
phase_timeout = "30m"
ticket_timeout = "2h"
max_ticket_cost_usd = 20

[commands]
verify = ["make", "test-focused"]
review = ["make", "test"]

[providers]
planner = ["codex"]
builder = ["codex"]
reviewer = ["codex"]
```

Commands are exact argv arrays and are never evaluated by a shell. Repository
configuration cannot raise the channel's machine limits. Guarded is the
default merge mode: the factory may perform an immediate exact-head merge only
after current human approval and all current gates. Manual mode only observes
an operator's external merge. Autonomous eligibility requires a separate
passing execution-profile proof; configuration alone cannot establish it.

`sf init --project <name> --repo <absolute-path>` reads and resolves these
files, registers canonical bytes and a SHA-256 digest in the selected channel's
SQLite database, and never writes the repository. When a ticket starts, it
copies the exact registered generation into its durable row. Editing a config
file cannot alter an active ticket.
