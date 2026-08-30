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
repository worktree. It reports typed check records, keeps
`autonomous_eligible` false, and never treats missing Docker or Colima as an
error. `auth status` probes only the four allowlisted official CLIs (`gh`,
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

The public Cobra surface includes every primary verb above, but the foreground
daemon currently enables only `submit`, `start`, `status`, `show`, `logs`,
`pause`, and `cancel`. Pause and cancel first commit the authenticated control
intent and invalidate the runner fence, then wait for the injected runtime to
prove writers drained; cancellation also observes external merge state before
and after that drain. Capacity is released only with the final durable
`paused`/`cancelled` transition. `take`, `resume`, `recover`, `retry`, approval,
and rejection fail closed with `not_ready` until their complete worktree/effect
evidence and workflow handlers are wired. Listing a command is therefore the
stable CLI surface, not a claim that its lifecycle backend is enabled.

`status` and `show` expose durable ticket/evidence metadata. Human output uses
product labels when those fields are present; `--json` remains the versioned
response envelope. `logs` reads bounded redacted durable events and `--follow`
polls with an event cursor. Provider transcripts and credentials are never
returned by the logs API.

Single-ticket `status` and `show` authenticate durable plan, verification,
candidate, worktree, phase-attempt, and operator-decision checkpoints before
displaying their bounded metadata. They include the socket-authenticated
operator and current runner epoch. Raw provider transcripts, proof bodies,
credential material, and worktree identity bytes are never returned; corrupt
evidence fails closed as `evidence_conflict` instead of disappearing from the
view.

## Operator identity

The daemon authenticates the socket peer. An omitted operator is resolved by
the daemon to the authenticated macOS user; a supplied label must match that
user or a configured local alias. The CLI forwards the label as request data
but cannot elevate or spoof it.
