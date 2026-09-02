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

- A dependency-free `go.mod`, or a checked-in compatible Go `vendor/`
  closure, selects the exact guarded `go test ./...` recipe.
- A regular dependency-free `package.json` selects the exact guarded
  `node --test` recipe. npm commands are not locally executable repository
  commands. Dependency-bearing Node repositories remain CI/operator takeover
  unless they explicitly select the narrow typed Nysa recipe below.

Repositories that match neither shape, match both, contain malformed or
unvendored Go metadata, or use symlinked marker files are refused with an
actionable `sf config --help` (or `sf-dev config --help`) next step. An
explicit file may record another exact argv pair without shell interpretation,
but that is CI/operator metadata: the local repository executor still refuses
everything except the exact dependency-closed Go recipe.

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
SQLite database, and normally never writes the repository. For a repository
that needs the narrow Nysa recipe, the explicit bootstrap form creates
`.sf/config.toml` only when it is absent:

```text
sf init --project nysa --repo /absolute/path/to/nysa-app \
  --profile nysa-api-pure-v1 \
  --test apps/api/tests/retrieval-fusion.test.ts
```

The profile command validates the entrypoint and its bounded relative import
closure before installing the file. It will not overwrite an existing config.
The generated profile also sets `phase_timeout = "60s"`, matching the
supervisor's one-minute admission bound for pure commands.
The first registration is immutable generation 1: rerunning the exact command
only confirms an identical registration, while a changed config, repository,
or profile is refused as a project conflict. To freeze a reviewed current
configuration for future starts, use the direct local command:

```text
sf-dev config apply --project nysa
```

It reads the registered repository's optional `.sf/config.toml` (or its
authenticated absence) and the selected channel's current `machine.toml`,
then atomically appends generation 2, 3, and so on. It never creates or edits
the repository config, never retargets the registered repository or base
branch, and preserves earlier generations. Repeating it with the current exact
snapshot is an observed replay. A queued ticket snapshots whichever generation
is current when it first enters planning; an already active ticket keeps its
previous frozen bytes. This is the supported way to narrow a channel to one
concurrent future ticket by setting `max_concurrent_tickets = 1` in the
channel-local machine file and applying the project configuration.

`init` and `config apply` coordinate through an owner-local advisory lock on
the authenticated repository root, followed by the optional `.sf` lock. This
serializes supported local sf operations; it does not claim to defeat a
hostile same-UID process outside the documented trusted-local boundary.
`machine.toml` is sampled and validated immediately before the generation
transaction. An edit made after that sample is intentionally not folded into
the in-flight snapshot; it is considered by the next `config apply`.

Configuration generations are forward-only in v1. `config apply` refuses an
older historical snapshot instead of rolling the project pointer backward. To
change policy again, make a new reviewed configuration generation; rollback is
not a supported local operation. A legacy registration that has generation
zero and no configuration history can be safely bootstrapped once to generation
one; contradictory partial history is refused.

For the Docker/Colima-free Nysa API compatibility slice, the only TypeScript
command shape is a typed recipe. Its path is frozen into the ticket
configuration and must be a canonical repository-relative `.test.ts` entrypoint.

```toml
[commands]
verify = ["node", "--sf-nysa-api-pure-v1", "apps/api/tests/retrieval-fusion.test.ts"]
review = ["node", "--sf-nysa-api-pure-v1", "apps/api/tests/retrieval-fusion.test.ts"]
```

sf resolves only the selected test's relative `.js` imports to same-path `.ts`
files below the authenticated worktree. It never invokes npm, reads
`node_modules`, resolves packages, or evaluates a shell. The test may initially
refer to a missing relative implementation so the prebuild verifier can record
the expected red result; that source is never substituted. Dynamic imports,
JSX, symlinks, escapes, and any other TypeScript topology remain CI or
operator-takeover work.

This recipe requires Node `>=22.8.0 <23`. On macOS sf accepts only the
authenticated resolved executable behind the supported Homebrew entrypoints
`/opt/homebrew/bin/node` (Apple Silicon) or `/usr/local/bin/node` (Intel), and
copies that runtime's non-system Mach-O closure into a private launch stage.
