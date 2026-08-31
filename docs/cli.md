# `sf` CLI

`sf` is a thin client for one channel-specific local daemon. Ticket lifecycle
commands never open SQLite or mutate workflow state in the CLI. The stable
binary uses the `stable` channel; a development build uses `dev`, so the two
channels have separate sockets and roots.

## Primary verbs

```text
sf submit <ticket.md> --project <name>
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

## Exit codes

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

`doctor`, `auth status`, `auth login`, and `init` are direct local
setup/diagnostic commands. `providers qualify` is an authenticated local-daemon
operation because a passing result must carry the current supervisor's
signature. `init` is implemented: it validates an
absolute Git worktree root and its configured base branch, reads optional
strict `.sf/config.toml`, creates only the selected channel's owner-only local
state, and idempotently registers the canonical repository in SQLite. It never
writes into the repository or contacts a remote. See
[`configuration.md`](configuration.md).

Doctor performs read-only checks for the
channel root, socket, disk space, Git/gh executables, and an optional
repository worktree. When an owner-only socket exists it also performs a
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
GUI. A repeated `take` is read-only and returns the same retained handoff.

`resume` reauthenticates the registered worktree, branch, remotes, base, and
filesystem identity. A clean checkout resumes its exact stored state. A
bounded set of uncommitted source edits may instead resume into a fresh
Builder cycle, but only when the checkout is still at the durable verification
checkpoint, every changed path is inside the Planner's approved paths, and no
verification-owned file changed. Those edits remain in the worktree for the
Builder; they are not adopted directly as a candidate. The Builder must still
produce a new result, repository-command proof, candidate, checks, and review
before publication can continue.

An operator commit with an unrecognized head, an out-of-plan edit, or a change
to verification-owned files is preserved and refused with an actionable
takeover blocker. The idempotent `sf take <ticket>` prints the authenticated
handoff path again. sf never overwrites edits or treats them as Builder/proof
authority. Verification-file changes require the separate authenticated
verification-amendment flow; they are never silently routed into a source
resume.

`retry` applies only to the durable retry/correction-exhaustion pause and
re-enters its exact stored resume state. If a prior interrupted control action
left a sealed runtime admission, retry performs its one fenced rearm before a
new attempt can run. `recover` accepts only a typed blocked ticket after a
fresh drain; `--mode guarded` is further narrowed to the
`autonomy_ineligible` blocker and a frozen project configuration whose maximum
mode is `guarded` or `autonomous` (a `manual` project is refused). It then
atomically changes that ticket's durable merge mode to guarded and starts a
fresh guarded candidate cycle. Pause, take, and cancel invalidate the runner fence before
draining; capacity is released only by the final durable
`paused`/`cancelled` transition.

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
