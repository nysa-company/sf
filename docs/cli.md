# `sf` CLI

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
sf ticket new <ticket.md>
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
sf approve <ticket> --operator <identity>
sf reject <ticket> --operator <identity> --reason <text>
sf doctor [--repo <path>]
```

`--json` is available on every command. Human and JSON output are rendered from
the same versioned response envelope. The CLI never invents success: a command
that is not configured returns a typed error, exit code, and one executable
next action.

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
`--json` never prompt. Full IDs keep the direct path. Approval and rejection
still require full IDs pending candidate-bound interactive confirmation.
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
the explicit `--profile`/`--test` configuration-creation flags. Python/Rails and
general dependency-bearing Node/TypeScript local runtimes remain unsupported;
writing an arbitrary command in configuration does not enable them.

`ticket template` prints an editable Markdown template (`--json` returns it in
the response data). Save it to a new file, replace the example requirements,
then run `ticket validate <file>`. Validation uses the submission parser without
contacting the daemon, starting the ticket deadline, or submitting work. It
checks syntax only, not feasibility, project limits, provider qualification or
readiness. Missing acceptance criteria and omitted budget limits produce notes.
Files must be regular, non-symlink files of at most 1 MiB.

In a terminal, `ticket new <file>` collects a title, a one-line problem/scope,
and up to 32 acceptance criteria. It previews the complete guarded ticket with
explicit 1h/$10 ceilings, then saves only if you type `yes`. The output is a
new private file; existing files are never overwritten. Edit the saved Markdown
to change its limits or add detail. JSON and piped calls do not prompt or create
a file; use the template and validation commands instead. Creation never
submits work or starts the deadline.

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
remain separate runtime checks. No Python/Rails/Claude execution support is
implied by recognizing their configuration or authentication.

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
Codex qualification is a foreground-daemon operation: it admits only the
exact local `Logged in using ChatGPT` subscription status, binds that bounded
mode into the supervisor attestation, and performs no model call. API-key,
metered, and unknown login statuses fail before an invocation; tokens remain
observability while the trusted incremental subscription charge is zero.

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

An authenticated nonzero post-build check produces `postbuild_command_failed`.
The failed command and completed Builder result are retained, but no candidate
is published. This is not a provider retry window: `resume`, `recover`, and
`retry` cannot repair the same frozen proof. Use the channel-correct `cancel`
action shown by `status`, then submit a fresh ticket with clarified acceptance
criteria. sf does not silently edit Reviewer-owned tests or invent an amendment.

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
