# Run your first ticket

SF delegates implementation while you retain control of the reviewed merge.
The current beta is for trusted local macOS repositories. It is not yet a
general-purpose runner for every project or provider.

## Check whether your project fits

| Project | Current local execution |
|---|---|
| Go | Dependency-free module or compatible checked-in vendor closure |
| JavaScript | Dependency-free Node project using `node --test`, with an existing discoverable JavaScript test |
| TypeScript | Only the configured bounded Nysa pure-test recipe |
| Python | Experimental pinned Python/pytest profile on Apple Silicon; no additional dependencies; automated workflow passes, live-model delivery pending |
| Ruby on Rails | Not yet supported locally |

See [configuration](../configuration.md) for exact constraints. An explicit
command in TOML does not grant permission to run an unsupported recipe.
Mixed-stack roots require an explicit supported verification/review recipe:
for example, a Rails `Gemfile` beside `package.json` is not automatically a
Node project. SF refuses to guess which tests represent the project.
Codex remains supported. The current multi-CLI source also supports qualified
Claude/Codex pairs in either direction; native model trials have passed with
local simulated GitHub. Login alone is not qualification. Cursor's experimental
trusted-hooks path passed local full-ticket tests in both Cursor/Claude role
directions, plus a two-ticket restart run with Cursor Luna Low Builder and
Claude Sonnet 5 Reviewer. Those runs used local GitHub fixtures, not hosted
Relay; use the tested Claude/Codex pair for this guide.
See the [provider setup and model picker](../cli.md#overview)
and [acceptance ledger](../plans/2026-09-06-multi-cli-acceptance.md) for exact
tested scope and remaining gates.

## Prepare once

Already configured? Use `sf home --project YOUR_PROJECT` to create a draft,
start a saved draft, inspect work or review an approval candidate. For direct
offline drafting, run `sf ticket new --multiline`, paste the description and
finish it with a line containing only `.`. Supply observable acceptance criteria,
review the suggested filename and full contents, then type `yes` to save.
Nothing starts until you explicitly submit/run it. Existing GitHub Issues can
be imported with `sf ticket import https://github.com/OWNER/REPO/issues/NUMBER`;
the issue is read-only reference material and is never changed by import.

### Get the CLI before running setup

Public installation and automatic updates are not shipped yet. From an SF
source checkout, build the development bundle and put that bundle on PATH in
**both terminals** used below. Replace the absolute path with your checkout:

```sh
cd /absolute/path/to/sf-source
make build-dev
export PATH="/absolute/path/to/sf-source/scripts:/absolute/path/to/sf-source/bin:$PATH"
sf version --json
sf auth status
sf auth login github
sf auth login claude
sf auth login codex
```

The `scripts/sf` launcher always invokes this checkout's `bin/sf-dev`; it keeps
all development state and helper identities unchanged. Keep both directories
on PATH so channel-explicit recovery commands still work. Help and recovery
output may say `sf-dev` to make that channel visible. An installed stable `sf`
remains separate: remove the source PATH prefix to use it. No alias or global
installation is modified automatically.

Login uses the official interactive provider flow; do not paste credentials
into tickets. These examples use a Claude Builder and Codex Reviewer. For
Codex-only use, choose `codex-codex` instead at registration and qualification
and omit Claude login and estimated-cost consent. See
[source-build details](source-build-foreground.md) for prerequisites and custom
authentication directories, or [local bundle install](local-bundle.md) for an
explicit verified install. Do not repeat that tutorial's example registration.

### Register a supported project

Keep the project in a durable directory, such as your normal projects folder.
Use your normal HOME for a real run. If you deliberately isolate HOME, put that
directory in durable owner-only storage too. Do not use `/tmp`, `/private/tmp`,
or a test framework's temporary directory for a run that must survive reboot.
SF binds work to the registered checkout's filesystem identity; a clone or
file copy is not an authenticated replacement for a lost checkout. A SQLite
backup alone does not preserve the repository, linked worktrees, or runtime
snapshots needed to finish an in-flight ticket.

Use a committed checkout with its configured base branch (normally `main`)
available locally and a GitHub origin for eventual publication. From the
product repository root, preview Go or dependency-free Node configuration,
then register it with the pair used in this guide:

For Node, commit a meaningful baseline test (for example `test/smoke.test.js`)
before setup; an empty package with no discoverable tests is refused. This
baseline does not replace the independent verification for your new ticket.

```sh
sf init --check
sf init --providers claude-codex
```

The project name defaults to the directory name (normalized to a valid name).
To choose a different pair by number, replace the registration command with
`sf init --providers select` to choose by number. Existing projects use
`sf config providers --project <name> --preset select`, followed by
`sf config apply --project <name>` for future tickets. Provider preferences
do not install, log in, or qualify models.
Use `--project my-app` to override it and `--repo /absolute/path` to select a
different repository root. `--check` does not execute tests, contact providers
or GitHub, or prove full runtime readiness; those checks remain separate.
It currently previews existing configuration, not `--profile`/`--test` setup.

For the narrow TypeScript recipe, use explicit setup instead of plain init:

```sh
sf init --profile nysa-api-pure-v1 --test path/to/existing.test.ts --providers claude-codex
sf init --check
```

Replace the test path with an existing entrypoint satisfying the bounded
relative-import closure. This is not general TypeScript/npm support. Consult
the [exact runtime and closure requirements](../configuration.md) first.
Rails users should stop at the compatibility refusal: changing providers or
adding an arbitrary command does not enable Rails execution.

For the experimental Python profile, use the explicit setup instead of plain
`init`. Start with an existing dependency-free Python project and a `tests`
directory (or substitute an existing `.py` test path):

```sh
sf runtimes prepare python
sf runtimes prepare python --download
sf init --profile python-pytest-v1 --test tests --providers claude-codex
sf init --check
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

Review any generated `.sf/config.toml` and commit intended configuration and
test sources to your project before starting: linked execution worktrees do
not inherit arbitrary uncommitted files from your checkout. Do not blindly add
other files or credentials. The examples below use `my-app`; replace it with
the registered project name printed by init.

### Start the daemon and qualify the same pair

In a second terminal, with the same bundle on PATH, HOME and authentication
directory settings, leave this running:

```sh
sf daemon run
```

Back in the first terminal, from your product repository:

```sh
sf providers qualify --preset claude-codex
sf doctor --repo .
```

Qualification may invoke paid models; login alone is not qualification.
Doctor's host/provider checks do
not prove your GitHub protection and required checks are merge-ready.
From the repository root, `sf doctor --repo .` also previews the local
configuration/test recipe. A failed `repository_recipe` check points back to
`sf init --check`; it does not modify your configuration. The report labels
its scope so a green host/provider verdict is not mistaken for launch approval.

Use a disposable supported project for your first run. Keep your real project
credentials out of ticket text. Do not change branch protection to bypass a
refusal. The initial supported path uses GitHub and guarded merge.

## Write one small ticket

Run `sf ticket new ticket.md` in a terminal for guided creation and a full
preview before saving. Alternatively, use `sf ticket template` and save its
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
start; queue time consumes the same deadline. Claude/Cursor tickets require explicit
`--accept-cost-estimates` when starting. Their reported estimates can stop
further launches but do not guarantee a hard-dollar ceiling on actual charges;
missing cost stays unknown. Read [estimated accounting](../cli.md#estimated-provider-accounting-multi-cli)
before opting in. Codex-only accounting remains unchanged.

Before submitting, check the format without starting that deadline:

```sh
sf ticket validate ticket.md
```

This checks syntax and highlights omitted acceptance/budget fields; project
policy and runtime readiness are still checked at submission and execution.

## Submit, start, and observe

For the composed path, use:

```sh
sf run ticket.md --project my-app --watch
# For a qualified Claude/Codex project, explicitly accept estimated accounting:
sf run ticket.md --project my-app --accept-cost-estimates --watch
```

This submits and starts the exact queued ticket, then follows its status.
Choose the appropriate command above, not both. Estimate consent is sent only
when starting a queued ticket; it cannot change an already active ticket.
Ctrl-C stops watching, not the ticket. If start is refused, submission still
exists and its deadline is running; follow the reported action. Paused/blocked
tickets are never implicitly resumed. The separate commands below remain
available when you want to inspect submission before starting.

From your product directory, replace `my-app` with the registered name:

```sh
sf submit /absolute/path/to/ticket.md --project my-app
```

Submission does not start work. In a terminal, select the ticket by its title
and state instead of copying its full ID:

```text
sf start --project my-app --accept-cost-estimates
sf status --select --project my-app --watch
```

Find existing tickets by title and state with
`sf tickets --project my-app`. In an interactive terminal,
`sf start --project my-app --accept-cost-estimates` offers a numbered picker;
`q` cancels. Omit estimated-cost consent for a Codex-only project.
The picker never chooses a ticket implicitly. Unique six-character hex ID
prefixes work too, for example `status 543bc4 --project my-app`. Scripts must
provide an unambiguous ID and never prompt. Approval/rejection use a separate
confirmation that displays and binds the complete reviewed head.

SF plans, writes independent verification, implements, runs proof, publishes a
draft PR, checks CI, and performs independent final review. When it requests
approval, inspect the actual PR diff and reviewed head before approving:

```text
sf approve <ticket-id> --head <full-reviewed-commit>
```

Use the full commit ID from the PR you inspected, not an abbreviated hash.
Alternatively, `sf approve --project my-app` opens a ticket picker
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

## Measure first use honestly

Record prerequisite time (build/install, login, downloads and qualification)
separately from hands-on setup and ticket drafting. Our target is under ten
minutes of hands-on setup once prerequisites are ready, not a promise of a
ten-minute delivered PR. Record model, local-proof and GitHub/CI waits separately.
After submission, queueing, pauses, remediation and approval waits all consume
the ticket's wall-clock deadline. Automated clean-environment tests and an
agent walkthrough are not evidence that an unfamiliar human met this target.
