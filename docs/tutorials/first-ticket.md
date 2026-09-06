# Run your first ticket

SF delegates implementation while you retain control of the reviewed merge.
The current beta is for trusted local macOS repositories. It is not yet a
general-purpose runner for every project or provider.

## Check whether your project fits

| Project | Current local execution |
|---|---|
| Go | Dependency-free module or compatible checked-in vendor closure |
| JavaScript | Dependency-free Node project using `node --test` |
| TypeScript | Only the configured bounded Nysa pure-test recipe |
| Python | Experimental pinned Python/pytest profile on Apple Silicon; no additional dependencies; automated workflow passes, live-model delivery pending |
| Ruby on Rails | Not yet supported locally |

See [configuration](../configuration.md) for exact constraints. An explicit
command in TOML does not grant permission to run an unsupported recipe.
Mixed-stack roots require an explicit supported verification/review recipe:
for example, a Rails `Gemfile` beside `package.json` is not automatically a
Node project. SF refuses to guess which tests represent the project.
The qualified live beta uses Codex for independent Builder/Reviewer model
families. Using Claude already does not itself establish an SF-qualified
Claude runtime. Provider expansion is tracked in the
[self-serve beta plan](../plans/2026-09-05-self-serve-cli-beta.md).

## Prepare once

Keep the project in a durable directory, such as your normal projects folder.
Use your normal HOME for a real run. If you deliberately isolate HOME, put that
directory in durable owner-only storage too. Do not use `/tmp`, `/private/tmp`,
or a test framework's temporary directory for a run that must survive reboot.
SF binds work to the registered checkout's filesystem identity; a clone or
file copy is not an authenticated replacement for a lost checkout. A SQLite
backup alone does not preserve the repository, linked worktrees, or runtime
snapshots needed to finish an in-flight ticket.

From the repository root, preview the local configuration without registering
anything:

```sh
sf-dev init --check
sf-dev init
```

The project name defaults to the directory name (normalized to a valid name).
Use `--project my-app` to override it and `--repo /absolute/path` to select a
different repository root. `--check` does not execute tests, contact providers
or GitHub, or prove full runtime readiness; those checks remain separate.
It currently previews existing configuration, not `--profile`/`--test` setup.

For the experimental Python profile, use the explicit setup instead of plain
`init`. Start with an existing dependency-free Python project and a `tests`
directory (or substitute an existing `.py` test path):

```sh
sf-dev runtimes prepare python
sf-dev runtimes prepare python --download
sf-dev init --profile python-pytest-v1 --test tests
sf-dev init --check
```

The first command previews without writing or downloading. `--download` fetches
pinned public runtime/pytest artifacts into the dev channel's private cache;
it does not install project dependencies, modify PATH, or execute project code.
Preparation and profile registration have compiled clean-environment coverage.
An isolated compiled Python workflow also passes through real test execution,
publication and terminal reconciliation using controlled provider/GitHub
fixtures; live-model Python delivery is still pending. Use the Go or
dependency-free Node path for the established first-ticket workflow. See
[Python configuration](../configuration.md) for the restricted recipe and
[the acceptance plan](../plans/2026-09-05-self-serve-cli-beta.md) for remaining
validation. Provider qualification and publication readiness remain separate.

Follow [source build and foreground setup](source-build-foreground.md) to build
the dev bundle, authenticate, register your repository, start the daemon in a
second terminal, qualify the provider pair, and run doctor. Public installation
and automatic updates are not shipped yet. A verified explicit
[local bundle install](local-bundle.md) is available from a clean source build.
Doctor's host/provider checks do
not prove your GitHub protection and required checks are merge-ready.
From the repository root, `sf-dev doctor --repo .` also previews the local
configuration/test recipe. A failed `repository_recipe` check points back to
`sf-dev init --check`; it does not modify your configuration. The report labels
its scope so a green host/provider verdict is not mistaken for launch approval.

Use a disposable supported project for your first run. Keep your real project
credentials out of ticket text. Do not change branch protection to bypass a
refusal. The initial supported path uses GitHub and guarded merge.

## Write one small ticket

Run `sf-dev ticket new ticket.md` in a terminal for guided creation and a full
preview before saving. Alternatively, use `sf-dev ticket template` and save its
output to a new `ticket.md`, then replace the sample requirements. Do not
overwrite an existing ticket. Creation does not submit anything.
For a dependency-free Node repository, this is a complete format example
(not an automatically submitted ticket):

```markdown
---
type: feature
merge: guarded
max_duration: 1h
max_cost_usd: 10
---
# Count items without modifying them

Add a named `countItems` export in `src/count-items.js` that returns the length
of an array. Use the existing project module format. Keep the implementation
dependency-free. Add verification in `test/count-items.test.js` using node:test.

## Acceptance
- An empty array returns 0.
- An array of three elements returns 3, including null and undefined elements.
- A non-array throws TypeError with message "items must be an array".
- The input array is not modified.
- No dependencies or unrelated files are changed.
```

Ticket duration starts at submission, not at execution. Submit when ready to
start; queue time consumes the same deadline. Costs are ceilings, not estimates.

Before submitting, check the format without starting that deadline:

```sh
sf-dev ticket validate ticket.md
```

This checks syntax and highlights omitted acceptance/budget fields; project
policy and runtime readiness are still checked at submission and execution.

## Submit, start, and observe

For the composed path, use:

```sh
sf-dev run ticket.md --project my-app --watch
```

This submits and starts the exact queued ticket, then follows its status.
Ctrl-C stops watching, not the ticket. If start is refused, submission still
exists and its deadline is running; follow the reported action. Paused/blocked
tickets are never implicitly resumed. The separate commands below remain
available when you want to inspect submission before starting.

From the SF source directory, replace `my-app` with the registered name:

```sh
./bin/sf-dev submit /absolute/path/to/ticket.md --project my-app
```

Submission does not start work. In a terminal, select the ticket by its title
and state instead of copying its full ID:

```text
./bin/sf-dev start --project my-app
./bin/sf-dev status --select --project my-app --watch
```

Find existing tickets by title and state with
`./bin/sf-dev tickets --project my-app`. In an interactive terminal,
`./bin/sf-dev start --project my-app` offers a numbered picker; `q` cancels.
The picker never chooses a ticket implicitly. Unique six-character hex ID
prefixes work too, for example `status 543bc4 --project my-app`. Scripts must
provide an unambiguous ID and never prompt. Approval/rejection use a separate
confirmation that displays and binds the complete reviewed head.

SF plans, writes independent verification, implements, runs proof, publishes a
draft PR, checks CI, and performs independent final review. When it requests
approval, inspect the actual PR diff and reviewed head before approving:

```text
./bin/sf-dev approve <ticket-id> --head <full-reviewed-commit>
```

Use the full commit ID from the PR you inspected, not an abbreviated hash.
Alternatively, `./bin/sf-dev approve --project my-app` opens a ticket picker
and displays its reviewed head. Inspect that commit, then type `approve` to
confirm it; any other answer cancels without a decision.
The supplied head must still match when the daemon records the decision.
Approval is not automatic. A changed candidate requires fresh evidence and
approval. Keep watching until SF records `done`, not merely until a PR exists.

## When something stops

Read the reported next action and use `logs <ticket-id>` for sanitized events.
`pause`, `take`, `resume`, `retry`, and `cancel` have different meanings; use
[the CLI reference](../cli.md) before modifying an interrupted worktree.
Do not edit the database or delete worktrees to unstick a ticket. Some failures
still require operator restoration or resubmission; the beta does not promise
unattended recovery from every failure.

Use `--help` on any command and `--json` for automation. Stop a foreground
daemon with Ctrl-C in its terminal. Stable and dev are separate channels;
always use the same binary/channel for a ticket.
