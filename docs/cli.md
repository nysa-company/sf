# `sf` CLI

## Estimated provider accounting (multi-CLI)

`sf start <ticket> --accept-cost-estimates` explicitly opts a ticket into
reported cost estimates before its first provider attempt. Estimates are not
verified charges and do not guarantee a hard dollar cap. Missing cost remains
unknown. SF limits Claude/Cursor admission to 16 total ticket attempts and
45 minutes per invocation, also subject to the ticket deadline; these are SF
launch limits, not provider-internal API request limits. Reaching the reported
estimate ceiling stops another launch. Ordinary `start` does not opt in.

The composed `sf run ticket.md --project app --accept-cost-estimates --watch`
path forwards the same explicit consent only when starting its exact queued
ticket. It never changes accounting for an already active ticket. Omitting the
flag keeps the existing default and does not opt in.

This flag does not install, authenticate, or qualify a provider. Existing
in-flight tickets cannot change accounting policy after their first attempt.

## Overview

For a Claude/Codex pair, use `sf providers qualify --builder claude --reviewer
codex` (or reverse the roles). Claude defaults to Sonnet 5 and qualification
makes two small model-bearing CLI launches plus a cancelled startup; each CLI
may make several API requests, so it is not a free
login check. Pair selection changes only after both roles qualify. Cursor has
an experimental trusted-hooks path: ambient hooks are trusted dependencies,
not claimed to be disabled or contained. SF applies its own role filesystem
profile and requires a signed native qualification. Luna Low and Sonnet Low have
passed qualification; disposable full-ticket tests passed in both Cursor/Claude
role directions. A two-ticket Cursor Luna Low Builder/Claude Sonnet 5 Reviewer
run also passed restart, separate approvals, and delivery with local GitHub
fixtures. Hosted Relay acceptance remains a separate gate. The model picker offers
exact Cursor IDs and filters same-family reviewers across CLIs; each model still
requires its own qualification. Cursor qualification may invoke paid models and never treats
login as execution authority. New Claude/Cursor tickets
require the explicit estimate opt-in described above. Login status alone is
not execution readiness.

Qualification accepts the same pair presets as setup:

```sh
sf providers qualify --preset select       # numbered terminal picker
sf providers qualify --preset select --models select # choose pair and exact models
sf providers qualify --preset claude-codex # noninteractive, also supports --json
sf providers qualify --preset claude-codex --builder-model claude-sonnet-5 --reviewer-model gpt-5.6-luna
```

Do not combine `--preset` with `--builder` or `--reviewer`. Cancelling the
picker makes no request. Selecting a pair requests qualification and may invoke
paid models; it is not merely a preview. Use `sf doctor` to inspect the exact
qualified model/family and authentication state without qualifying again.

`--builder-model` and `--reviewer-model` pin exact supported IDs for this
qualification; Planner follows Builder. Unknown IDs, aliases, and a
same-family pair are refused rather than replaced. Omitted IDs keep the
daemon's existing role defaults. The selected identities remain channel-local
qualification authority, not portable project settings. Qualification is
refused while the workflow runtime is already active; do not interpret editing
project preferences as changing an in-flight model. A restarted daemon
reconstructs selected Codex models from their stored qualifications and still
requires current binary/auth/policy qualification before execution.

`--models select` prompts for both exact models before sending any request.
The Reviewer menu excludes the Builder's inference family, even when the two
CLI names differ. Cancelling either answer makes no qualification request.
The menu is a supported catalog, not an account-entitlement or readiness
verdict; the daemon must still qualify the selection. Do not combine it with
explicit model flags. JSON and nonterminal callers must use exact IDs instead.

Qualification does not overwrite a project's immutable configuration. Before
registering a new project, choose an initial preset without editing TOML:

```sh
sf init --project app --repo /absolute/project --providers claude-codex
# Or choose the initial pair by number in a terminal:
sf init --project app --repo /absolute/project --providers select
```

Presets are `codex-codex`, `claude-codex`, `codex-claude`, plus experimental
`cursor-codex`, `codex-cursor`, `cursor-claude`, `claude-cursor`, and
`cursor-cursor`. Planner follows Builder. Cursor is a transport, not a model
family: Claude through Cursor cannot independently review Claude Code.
Use `providers qualify --preset <pair> --models select` to select independent
exact models before qualification. The `select` picker uses stderr, offers cancellation without changes,
and is unavailable with JSON or piped input; scripts use an explicit preset.
This creates a missing `.sf/config.toml` under the existing config
lock and freezes its preferences in the initial project generation. It does
not qualify providers, opt into estimates, or select models. Existing files
are accepted only if the preset already matches; they are never overwritten.
The flag can accompany a supported `--profile`/`--test` recipe, but not
read-only `--check`. Registration failure rolls back only the generated file.

For an existing project, use the numbered editor, then explicitly apply the
new configuration for future tickets:

```sh
sf config providers --project app --preset select
sf config apply --project app
```

The editor preserves unrelated settings and creates a backup. It does not
qualify models or change in-flight tickets. Noninteractive callers can use
`--preset claude-codex` (or the reverse preset). The equivalent TOML is:

```toml
[providers]
planner = ["claude"]
builder = ["claude"]
reviewer = ["codex"]
```

Reverse these names for Codex Builder/Claude Reviewer. Use one provider per
role; fallback lists are not supported by this workflow. Existing tickets
retain their frozen configuration. A selected pair that does not match a
ticket's role refuses before launch rather than silently substituting a model.
Use `providers qualify --preset select --models select` for exact model
selection, separately from the project preference editor.

### Which model actually runs?

| CLI | Current factory selection | Readiness |
| --- | --- | --- |
| Claude Code | Defaults to `claude-sonnet-5`; explicit supported IDs through qualification | Native qualification required; subscription OAuth, estimated cost only |
| Codex | Builder defaults to `gpt-5.6-luna`; Reviewer defaults to `gpt-5.5` | Existing native qualification and subscription accounting |
| Cursor | Experimental exact-model qualifier; default Builder Luna Low, Reviewer Sonnet 5 Low | Trusted hooks; signed native qualification required. Luna Low and Sonnet Low passed native qualification; disposable Cursor/Claude ticket tests passed in both role directions. Two-ticket Cursor Builder/Claude Reviewer restart acceptance passed with local GitHub fixtures; hosted Relay remains separate |

The pinned Cursor CLI's explicit Luna Low selection reports 272K context,
despite its catalog's 1M label. Sonnet Low reports 300K with thinking disabled,
also despite the catalog's 1M label. SF binds those measured session identities;
it does not claim the catalog context or thinking capability. Other selections
still require their own native qualification; Grok Low has not passed it.

For Claude Builder with Codex Luna Reviewer, start the foreground daemon,
then qualify exact models from another terminal:

```sh
sf-dev daemon run
# In another terminal, using the same channel:
sf-dev providers qualify --preset claude-codex --builder-model claude-sonnet-5 --reviewer-model gpt-5.6-luna
```

Use `sf` instead of `sf-dev` for the stable channel. This does not start a
second daemon alongside an existing one: stop the existing foreground daemon
before changing its model environment. Qualification reports the exact selected
model and family. A Codex-only pair still requires different model families;
two role names pointing to Luna do not create independent review. No provider
or model is silently substituted if the requested choice is unavailable.

If Claude reports that authentication cannot cover the required launch window,
run `claude auth login` to renew the subscription login, then repeat the same
qualification command. SF requires more than 46 minutes of credential validity
before launch and does not refresh credentials, switch to API billing, or send
a prompt to work around an expired login. Never paste tokens into SF settings.

`sf` is a thin client for one channel-specific local daemon. Ticket lifecycle
commands never open SQLite or mutate workflow state in the CLI. The stable
binary uses the `stable` channel; a development build uses `dev`, so the two
channels have separate sockets and roots.

In v1, `sf` can observe a manual external merge or request a guarded
exact-head merge after a human approves the reviewed head. Autonomous ticket
selection and merge are deliberately unavailable pending a stronger native
containment proof and guarded pilot. Docker and Colima are not required and
are never installed silently.

## Primary verbs

```text
sf init [--project <name>] [--repo <path>] [--check]
sf bundle manifest <directory>
sf bundle verify <directory>
sf bundle install <directory> --to <new-directory>
sf ticket template
sf ticket new [ticket.md] [--multiline]
sf ticket import <github-issue-url> [--json]
sf home [--project <name>]
sf ticket validate <ticket.md>
sf submit <ticket.md> --project <name>
sf run <ticket.md> --project <name> [--watch]
sf tickets [--project <name>]
sf start <ticket>
sf status [ticket] [--watch]
sf show <ticket> [--json]
sf logs <ticket> [--follow] [--phase <name>]
sf pause <ticket> --operator <identity>
sf resume <ticket> --operator <identity>
sf recover <ticket> [--mode guarded] [--operator <identity>]
sf cancel <ticket> --operator <identity>
sf retry <ticket> [--operator <identity>]
sf take <ticket> --operator <identity>
sf approve <ticket> --operator <identity> [--head <full-reviewed-commit>]
sf reject <ticket> --operator <identity> --reason <text> [--head <full-reviewed-commit>]
sf doctor [--repo <path>]
sf daemon cleanup prepare
sf daemon cleanup recover
```

Use `--head` with the full lowercase 40- or 64-character commit ID you inspected
for approval or rejection. The daemon refuses a different current candidate;
it never substitutes that candidate for the supplied head. A matching head is
still subject to the existing review, ticket-version and authority checks.
Omitting the option preserves the older current-candidate decision behavior.
Older daemons reject the new parameter rather than silently ignoring it.

`--json` is available on every command. Human and JSON output are rendered from
the same versioned response envelope. The CLI never invents success: a command
that is not configured returns a typed error, exit code, and one executable
next action.

`State` is the current lifecycle status. A blocker reason retained after recovery
is labelled `Recorded blocker`, not `Blocker`; it is diagnostic history and does
not override the current state or authorize another recovery. JSON preserves
the stored `blocked_code` field alongside `state`.

Single-ticket status also shows `Recorded review` when the newest final-review
attempt has an authenticated completed result. Its decision, reviewed head,
source version and findings are historical provider evidence, not approval or
instructions to execute. Findings are credential-redacted and terminal-control
sanitized: JSON includes at most five 512-character summaries; human output
shortens each to 160 characters and reports truncation. This is pattern-based
redaction, not a guarantee that arbitrary source text contains no secrets.
A newer failed/incomplete attempt suppresses older findings. Authentication
failure reports the diagnostic unavailable without inferring a verdict or
hiding the ticket's durable state. No raw transcript or artifact is displayed.

`tickets` is a read-only list using the same status authority. It shows ticket
IDs, titles, states, and available next actions; `--project` filters the list.
`start`, `show`, `logs`, `pause`, `resume`, `recover`, `cancel`, `retry`, and
`take` offer numbered selection when the ID is omitted in an interactive
terminal. Enter `q` to cancel; no ticket is selected by default, even for a
single match. `status --select` opens the same picker; plain `status` still
lists tickets. `--project` scopes selection, and duplicate titles are shown
with distinct full IDs and projects.

These commands and `status` also accept a unique 6–31 character lowercase hex
ID prefix, with or without `SF-`. Ambiguous prefixes require an interactive
choice or a longer ID; incomplete inventories fail closed. Piped input and
`--json` never prompt. Full IDs keep the direct path. In a terminal, omitted
approval/rejection IDs open a picker followed by the complete reviewed head.
Type the command name (`approve` or `reject`) to confirm; anything else cancels.
`--select` requests confirmation even with a full ID. These interactive
decisions always send the displayed head. Scripts require a full ID and should
use `--head`; they never select or confirm interactively.
The daemon checks current state and authority after selection; a menu is not
permission to bypass those checks.

Status includes a submission-budget clock: age since submission, immutable
deadline, and remaining time for nonterminal tickets. Queue and pause time are
included. This is not execution duration and does not itself transition an
expired ticket. Unknown historical budgets are not guessed; completed tickets
do not show an active countdown.

Single-ticket status and `daemon status --json` also expose recent completed
scheduler observations when the composed runtime provides them. This is a
bounded process-local diagnostic history (latest observation per ticket,
at most 64), not SQLite lifecycle state. Each observation names its historical
version/fence/time; a readiness/worker failure does not authorize replay.
Idle/pool-contention ticks do not erase useful observations, and a subsequent
completed invocation replaces the previous failure. Restart clears this
history. No raw tool output or exception text is retained. Status does not
wait for runtime reconfiguration to finish: it reports diagnostics unavailable
during that handoff instead.

`init` defaults to the current repository root and a normalized directory-based
project name. Override these with `--repo` and `--project`. `init --check`
previews existing configuration and local recipe compatibility without creating
configuration, registering a project, invoking a provider, or contacting GitHub.
It reports runtime, provider, and publication checks separately; recipe support
is not full execution readiness. The preview currently does not combine with
the explicit `--profile`/`--test` configuration-creation flags. Python requires
the prepared `python-pytest-v1` profile described below. Rails and general
dependency-bearing Node/TypeScript local runtimes remain unsupported; writing
an arbitrary command in configuration does not enable them.

`ticket template` prints an editable Markdown template (`--json` returns it in
the response data). Save it to a new file, replace the example requirements,
then run `ticket validate <file>`. Validation uses the submission parser without
contacting the daemon, starting the ticket deadline, or submitting work. It
checks syntax only, not feasibility, project limits, provider qualification or
readiness. Missing acceptance criteria and omitted budget limits produce notes.
Files must be regular, non-symlink files of at most 1 MiB.

In a terminal, `ticket new [file]` collects a title, a one-line problem/scope,
and up to 32 acceptance criteria. It previews the complete guarded ticket with
explicit 1h/$10 ceilings, then saves only if you type `yes`. The output is a
new private file; existing files are never overwritten. Edit the saved Markdown
to change its limits or add detail. JSON and piped calls do not prompt or create
a file; use the template and validation commands instead. Creation never
submits work or starts the deadline.

Omit the filename to use a bounded title-derived name in the current directory.
The absolute destination is previewed before saving; a collision refuses rather
than overwriting or inventing another identity. Use `--multiline` to paste the
problem description, ending with a line containing only `.`. Descriptions are
limited to 256 lines/64 KiB; each input line is at most 16 KiB. Acceptance entries
remain one observable criterion per prompt. This wizard is offline, not an AI
feasibility assessment.

`ticket import https://github.com/OWNER/REPO/issues/NUMBER` reads that exact issue
using your installed, authenticated `gh` CLI (20-second deadline, 128 KiB response
limit). It quotes the issue body as untrusted reference material and asks for your
acceptance criteria before showing the full draft. Saving requires `yes`; the
source-named `github-owner-repo-number.md` file is private and never overwritten.
Repeated import to the same directory refuses an existing file. This is local
collision protection, not global deduplication across copied/renamed drafts.
No GitHub issue is created, edited or closed. PR URLs, URL queries/fragments,
response identity mismatches and control-bearing text refuse. `--json` is a
read-only preview, even if a local draft already exists; piped non-JSON input
refuses. Imported titles are limited to 512 bytes and bodies to 64 KiB, also
subject to the drafting line/count bounds.

`home --project app` opens a terminal-only menu to create a multiline draft,
start a saved draft, view project tickets, or enter the existing exact-head
approval picker. Without `--project`, project actions ask for the registered
name; SF does not guess registration from a directory name. Starting previews
the source and requires `run`; a changed draft refuses before submission.
Approval still requires selecting the ticket and confirming the exact reviewed
head. At the start confirmation, plain `run` retains verified-cost accounting;
type `run estimates` to explicitly accept estimated costs, equivalent to the
direct command's `--accept-cost-estimates`. Estimates are not a hard billing cap.
The menu never supplies that consent from project/provider defaults. `q`/EOF cancels prompts;
`--json` and nonterminal home calls refuse without dispatch. Existing explicit
commands remain available to scripts and agents.

`run` composes submission and start through the existing daemon. It starts only
the exact queued ticket returned for that source/project/channel. Repeating it
does not request a new identity: an active ticket is observed, while paused,
blocked, stopping or cancelling tickets require explicit operator action.
It never automatically resumes, retries or approves. An uncertain response
stops the command with an inspection action; no mutation is retried by the CLI.
After a refused start, the submitted ticket still exists and its deadline is
already running. `--watch` follows that exact ticket; Ctrl-C stops watching,
not the work. JSON watch output is newline-delimited response envelopes.

Production `start` checks the stored verification and review recipes before
entering planning. Unsupported recipes return `unsupported_repository_recipe`;
the ticket stays queued. Explicit configuration is not permission to execute
arbitrary commands. If configuration changes while readiness is checked,
`start_configuration_changed` refuses admission; explicitly run start again
to check the new generation. These checks do not certify dependency closures,
executable versions, provider qualification or publication readiness; those
remain separate runtime checks. No additional Python dependencies, Rails or
Claude execution support is implied by recognizing configuration/authentication.

For newly recorded indeterminate provider results, `sf logs <ticket> --json`
includes a `provider_result_diagnostic` event. Its closed `reason` distinguishes
`command_error`, `nonzero_exit`, `output_limit`, `protocol_invalid`,
`provider_terminal_failure`, `binding_or_usage`, and `adapter_error`.
The event contains authenticated attempt identity, not provider output or error
text. It is diagnostic only: it does not authorize retry, and historical failures
without this event cannot be assigned a more precise cause retroactively.

## Exit codes

The local [bundle workflow](tutorials/local-bundle.md) verifies an exact
matching-version helper distribution before installation. It does not publish
releases, change PATH, replace existing files or operate on running daemons.
Manifest hashes establish integrity, not publisher authenticity.

| Code | Meaning |
| ---: | --- |
| 0 | Requested operation succeeded |
| 2 | Invalid command or input |
| 3 | Operator action is required |
| 4 | Temporary daemon/provider/external wait |
| 5 | Policy or safety refusal |
| 6 | Protocol, schema, or version incompatibility |
| 7 | Internal invariant failure |

## Response grammar

Human errors use this stable shape:

```text
Error: <code>: <message>
Mutation: none
Next: sf-dev daemon run
```

`Next:` appears at most once and is generated from executable argv, not a shell
string. JSON preserves the same `ok`, `mutation`, `error`, and `next_action`
semantics. A next action always contains a non-empty argv.

## Direct setup and diagnostics

`doctor`, `auth status`, `auth login`, `init`, and `config apply` are direct local
setup/diagnostic commands. `providers qualify` is an authenticated local-daemon
operation because a passing result must carry the current supervisor's
signature. `init` is implemented: it validates an
absolute Git worktree root and its configured base branch, reads optional
strict `.sf/config.toml`, creates only the selected channel's owner-only local
state, and idempotently registers the canonical repository in SQLite. The
normal form never writes into the repository or contacts a remote. An explicit
`--profile nysa-api-pure-v1 --test <repo-relative .test.ts>` form safely creates
the missing `.sf/config.toml` for one selected bounded pure-kernel test; it
never overwrites an existing config. See
[`configuration.md`](configuration.md).

`runtimes prepare python` previews the pinned Python 3.13.15/pytest 8.4.2
environment without network or filesystem changes. `--download` explicitly
allows verified public downloads into this channel's private runtime cache;
it does not install Python on PATH, run pip/project code, register a project,
open a database or restart a daemon. Existing exact snapshots are reused;
corrupt snapshots are refused, never overwritten. Normal output shows the
runtime and next action; `--json` also includes the exact environment/lock
digests. Preparation currently supports Apple Silicon macOS only. Afterwards,
run `init --profile python-pytest-v1 --test tests` (or select an existing `.py`
test file). Init authenticates the prepared runtime before registration and
creates only an absent config; replay never overwrites one. The profile admits
standard-library/local modules and bundled pytest, not additional dependencies
or arbitrary pytest options. Preparation success is not a provider or
publication-readiness verdict. Start rechecks the frozen runtime identity.

For an existing registered project with `.sf/config.toml`, use
`config providers --project <name> --preset select` for a numbered terminal
picker, or specify `claude-codex`, `codex-claude`, `codex-codex`, or an
experimental Cursor pair such as `cursor-claude` directly
(also supported with `--json`). The first provider plans/builds; the second
verifies/reviews. The command edits only provider preferences, preserves
unrelated configuration, and retains an original-byte backup whose path is
reported. Concurrent source/directory changes are refused. Repeating an
already-selected preset does not rewrite the file or create another backup.
This command does not qualify providers, call models, change the stored
configuration generation, or affect active tickets. Review the edited file,
then explicitly run `config apply`. For a missing config, use
`init --providers <preset>` instead.

`config apply --project <name>` freezes one next immutable configuration
generation from the registered repository's current optional config source and
the selected channel's machine policy. It is local-only, does not require a
daemon, never writes `.sf/config.toml`, and affects queued work only when that
work later starts; active tickets retain their existing frozen snapshot. v1
generations are forward-only: applying an older snapshot is refused rather
than repointing the project to historical configuration.

The pure TypeScript recipe requires Node `>=22.8.0 <23` from the supported
Homebrew entrypoint for the host: `/opt/homebrew/bin/node` on Apple Silicon or
`/usr/local/bin/node` on Intel. sf authenticates and stages the resolved
runtime; it does not trust `PATH`, NVM, or another ambient Node installation.

Doctor performs read-only checks for the
channel root, socket, disk space, Git/gh executables, and an optional
repository worktree. `doctor --repo .` resolves the current directory and
previews its working-tree configuration/test closure through the same read-only
checks as `init --check`. This preview does not register a project or replace
its stored configuration. A refused preview prevents a green guarded
eligibility report. The report explicitly labels its scope: host/provider
qualification and optional recipe preview are not ticket execution or merge
approval, and do not certify the runtime dependency/executable launch checks.
Doctor also reads the channel database's persistent external-process cleanup
quarantine. A quarantine, unavailable database, or unconfigured inspection
prevents a green guarded-eligibility report. The `external_mutation_recovery`
check does not clear the latch. Its next action prepares a host-recovery
checkpoint without reopening the gate. Repeated ticket `recover` calls or
daemon restarts are not proof that the previous process drained.
When an owner-only socket exists it also performs a
read-only `daemon.status` handshake. A missing socket points to `daemon run`;
a present but unhealthy socket points to `daemon status`, so Doctor does not
loop back into the same failed probe. Human Doctor output includes every
check and its executable action, bounded authentication status/reason, the
selected builder/reviewer identity and qualification, guarded/autonomous
eligibility, and `credentials_stored_by_sf=false`; it never prints paths,
digests, raw outputs, transcripts, or credential bytes. It reports typed check
records, keeps `autonomous_eligible` false, and never treats missing Docker or
Colima as an error. `auth status` probes only the four allowlisted official CLIs (`gh`,
`cursor-agent`, `claude`, and `codex`) with bounded, discarded output. `auth
login <provider>` delegates to that CLI's official interactive flow and then
re-probes status; sf never accepts, captures, or stores a credential byte. The
human and JSON authentication reports explicitly assess login only—not runtime
qualification, independent models, billing limits, or ticket readiness. After
successful login, run the channel's `sf doctor` (or `sf-dev doctor`) to inspect
readiness. A successful Claude or Cursor login does not enable that runtime.
The
Codex qualification is a foreground-daemon operation: it admits only the
exact local `Logged in using ChatGPT` subscription status, binds that bounded
mode into the supervisor attestation, and performs no model call. API-key,
metered, and unknown login statuses fail before an invocation; tokens remain
observability while the trusted incremental subscription charge is zero.

### Recovering external-process cleanup quarantine

Use the same channel executable and HOME/database throughout. For development,
replace `sf` with `sf-dev`. These commands require the owner-only daemon socket;
they do not accept caller-provided machine or boot identities.

1. Run `sf daemon cleanup prepare` while the quarantined daemon is available.
   This records an immutable checkpoint for that exact quarantine and leaves
   the gate sealed. Repeating it returns the same checkpoint.
2. Before rebooting, verify that HOME, the database, registered repository,
   linked worktrees, and required runtime snapshots are in durable storage,
   not `/tmp` or `/private/tmp`. A database backup alone cannot restore a lost
   checkout's registered filesystem identity. If any required path is temporary,
   stop here: do not move it and assume the old identity remains valid.
   Save your work, stop the foreground daemon normally, and reboot the host
   when safe. SF never initiates this reboot. A daemon-only restart is not
   sufficient; copying the database to another machine does not qualify.
3. Start the same channel daemon, then run `sf daemon cleanup recover`.
   Recovery requires an OS-observed different boot on the same checkpointed
   machine and current daemon authority. Missing, changed or malformed
   evidence refuses without clearing quarantine.
4. Inspect `sf status` and `sf doctor`. Follow any provider-qualification
   prerequisite shown by the daemon. Recovery does not confirm a merge or
   retry an uncertain mutation; normal exact remote reconciliation still runs.

The checkpoint and retirement audit remain in SQLite. This procedure is only
for the persistent GitHub cleanup latch; it does not clear provider or
repository-command leases, discard worktrees, or bypass ticket controls.
It assumes the database has remained on the host where the quarantine arose.
Moving an uncheckpointed legacy database to another machine is unsupported:
rebooting that other machine cannot prove the original host's processes died.

`daemon run` is the foreground entry point for development and tests (for
example, `sf-dev daemon run`). Its socket-backed lifecycle commands use the
channel-specific owner-only socket.

The foreground daemon enables `take`, `resume`, `retry`, and `recover` in
addition to the basic lifecycle commands. `take` follows the same fenced
stop/drain authority as pause, then returns the authenticated absolute
worktree path, branch, repository, base, and head. It never opens an editor or
GUI. If an active ticket is stopped before a worktree exists, the human view
says so and prints the channel-correct `resume` command instead of inventing a
path. After completed drain, a repeated `take` is read-only and returns the
same retained handoff. A semantic pause (such as Planner questions) may still
hold capacity: `pause` or `take` first joins that runtime and proves all
writers/effects drained before releasing its slot. It preserves the paused
ticket and its evidence; it does not answer Planner questions or resume work.

`resume` reauthenticates the registered worktree, branch, remote candidate,
protected base, and filesystem identity. A clean checkout at an existing
factory checkpoint resumes its exact stored state. Uncommitted operator source
edits are retained but refused with `source_commit_required`: the operator
must create one clean commit on the displayed ticket branch first. sf then
accepts that commit only when its single parent is the retained verification
checkpoint, its complete A/M/D path set is inside the Planner's approved
scope, it changes no verification-owned path, and the candidate/base remote
tuple is byte-for-byte unchanged from the post-drain take observation.

An accepted operator commit resumes into a fresh Reviewer verification, never
directly into Builder. The new verification checkpoint must parent the
operator commit and may change only verification-owned files; the operator
source paths are protected from Reviewer mutation. Builder runs only after
that fresh checkpoint and must still produce new provider, repository-command,
candidate, checks, and final-review evidence before publication.

A merge commit, a commit with the wrong parent, an out-of-plan edit, a remote
ref change, or a change to verification-owned files is preserved and refused
with an actionable takeover blocker. The idempotent `sf take <ticket>` prints
the absolute path, branch, local and remote heads, retained proof/policy
digests, and the channel-correct next action. sf never overwrites edits or
treats them as Builder/proof authority. Verification-file changes require the
separate authenticated verification-amendment flow; they are never silently
routed into a source resume.

Provider retry has three distinct cases:

- Automatic artifact repair permits one additional attempt after a safely
  drained invalid artifact, on the same role and runtime binding. It is not
  an API/network retry and does not add a separate hidden launch budget.
- Automatic server-rejection retry is narrowly supported for Claude under a
  freshly qualified streaming policy. The supervisor must observe a complete
  server-error-only rejection, prove process and stream completion, and sign
  the exact attempt plus an independently inspected clean checkpoint. Store
  authenticates that receipt and persists a bounded backoff; the next attempt
  rechecks the physical checkout before launch and uses the same role, model,
  authentication and runtime binding. It shares the artifact-repair attempt
  budget, rather than adding another retry allowance. Restart does not reset
  that budget or backoff. This is not enabled by login alone; older Claude
  qualifications must be renewed for the changed policy.
- Operator `retry` applies only to an eligible durable exhaustion pause and
  requires the physical-worktree and Store checks below. It never changes
  provider/model as a fallback.

An arbitrary API/network error, retry hint, timeout, missing final response,
or partial write is **not** eligible for automatic retry. Without the exact
signed rejection and checkpoint proof, sf preserves the uncertain outcome
instead of blindly relaunching. Cursor automatic API retry is not yet enabled.
In pinned Cursor CLI `2026.09.02-c22c1a3`, the print-mode error handler emits
an error string and exits rather than producing an authenticated terminal
server-rejection record. Text containing `503` is therefore insufficient to
authorize another launch. Complete artifact-validation failures still follow
the bounded same-role repair path; this limitation concerns uncertain API errors.
Failed requests with no reported cost remain explicitly
unknown, not free; the ticket's request/time limits still apply.

`retry` applies only to the durable retry/correction-exhaustion pause and
re-enters its exact stored resume state. If a prior interrupted control action
left a sealed runtime admission, retry performs its one fenced rearm before a
new attempt can run. Before reopening an exhausted provider phase, SQLite
derives the exact expected commit from its registered worktree and confirmed
commit-intent chain. A read-only existing-worktree boundary then reauthenticates
that retained checkout, including ignored files; it never allocates, replaces,
or cleans a worktree. Provider-written changes or a clean foreign commit
therefore produce `provider_retry_worktree_unready` before the ticket version
or one bounded retry window changes. The executable `take` next action shows
the retained checkout for inspection; sf never deletes or blesses those files.
After the operator restores the exact clean reviewed checkout, `take` points
back to the still-available `retry`; a fully consumed provider retry points to
`cancel` and resubmission. `recover` accepts only a typed blocked ticket after a
fresh drain; `--mode guarded` is further narrowed to the
`autonomy_ineligible` blocker and a frozen project configuration whose maximum
mode is `guarded` or `autonomous` (a `manual` project is refused). It then
atomically changes that ticket's durable merge mode to guarded and starts a
fresh guarded candidate cycle. Pause, take, and cancel invalidate the runner
fence before draining. Capacity is released only by a final durable `paused`,
`cancelled`, or terminal workflow transition after Store proves that provider,
repository-command, Git-mutation, and uncertain-effect writers are gone.

Provider retries cannot reuse a retained checkout whose phase entry crossed an
operator source-resume or a pending verification-amendment boundary: neither
lineage has one Store-derived physical HEAD that can be safely replayed. sf
returns `provider_retry_resubmit_required` without consuming the retry and
points to the executable `cancel` action. Cancel that ticket and submit a fresh
ticket; repeating `retry` cannot make the ambiguous lineage valid.

An authenticated nonzero post-build check may enter one budgeted diagnosis
entry when the Store can authenticate the completed Builder, failed command,
original proof, retained files, and absence of active writers. This is not a
blind provider retry: only an independent Reviewer can approve a proposed test
amendment, and the original acceptance and command remain frozen. If that
authority is unavailable, `postbuild_command_failed` retains the failed command
and completed Builder without publishing a candidate. Follow the channel-correct
`cancel` action shown by `status`; `resume`, `recover`, and `retry` cannot invent
the missing authority.

`postbuild_amendment_rejected` means the independent Reviewer refused the
proposed test correction. SF retains the original proof and local implementation
and stops without launching another Builder or silently restoring files. Use
the reported `cancel <ticket>` action, then submit clarified acceptance if needed.
Status is not a writer-drain proof or permission to edit retained files.

`verification_amendment_invalid` is likewise nonrecoverable. A malformed
Builder amendment request or independent Reviewer response is not an
authenticated acceptance or rejection, so `recover`, `resume`, and `retry`
must not reinterpret it. `status` and `show` point to the channel-correct
`cancel <ticket>` action; submit a fresh ticket after preserving any work that
still needs inspection.

`legacy_candidate_repair_recovery_unverifiable` is also nonrecoverable. It
means an existing repair lineage predates the signed recovery-prefix evidence
required by this version. Preserve any needed local work, cancel the ticket,
and submit a fresh ticket; `recover`, `resume`, and `retry` cannot make the
legacy lineage valid.

`status` and `show` expose durable ticket/evidence metadata. Human output uses
product labels when those fields are present; `--json` remains the versioned
response envelope. `logs` reads bounded redacted durable events and `--follow`
polls with an event cursor. Provider transcripts and credentials are never
returned by the logs API.

Single-ticket `status` and `show` authenticate durable plan, verification,
candidate, worktree, phase-attempt, and operator-decision checkpoints before
displaying their bounded metadata. They include the socket-authenticated
operator and current runner epoch, including the plan/checkpoint revisions,
candidate generation/head, worktree branch, phase outcomes, and invalidated
operator decisions. Absolute worktree paths are omitted from human output.
Blocker codes are shown when present; a `next_action` is shown only when the
API emits one. Raw provider transcripts, proof bodies,
credential material, and worktree identity bytes are never returned; corrupt
evidence fails closed as `evidence_conflict` instead of disappearing from the
view.

## Operator identity

The daemon authenticates the socket peer. An omitted operator is resolved by
the daemon to the authenticated macOS user; a supplied label must match that
user or a configured local alias. The CLI forwards the label as request data
but cannot elevate or spoof it.
