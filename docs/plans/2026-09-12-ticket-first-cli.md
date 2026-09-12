# Ticket-first CLI, AI authoring and live activity

Status: implementation approved on 2026-09-12; work started on `feat/ticket-first-cli`. Source baseline: main `f632e81`. This is not a shipped-capability statement.

## Goal and decisions

Developers should describe a change, review an AI-authored draft, explicitly start a ticket, and understand observable activity without memorizing unrelated root verbs. Three independent Astra source reviews corroborated the design. The user explicitly chose draft first, then a separate Start offer, and requested delegated implementation with Luna for research/test/easy tasks and coding by sub-sub-agents.

Use `factory` for process operations and `ticket` for ticket operations. Preserve current aliases, API envelopes, exit codes, selection behavior and authority. SQLite remains the only lifecycle authority; telemetry, drafts and AI proposals cannot authorize mutations. No autonomous merge or new background service management.

## Canonical commands

```text
sf factory run
sf factory status
sf factory cleanup prepare|recover
sf ticket new [--no-ai]
sf ticket new --status SESSION --turn KEY
sf ticket import <issue-url>
sf ticket template
sf ticket validate <file>
sf ticket submit <file> --project P
sf ticket start [id]
sf ticket start --file <file> --project P
sf ticket list [--project P]
sf ticket view [id] [--section plan]
sf ticket watch [id]
sf ticket logs [id] [--follow]
sf ticket pause|resume|cancel|retry|recover|take [id]
sf ticket approve|reject [id]
sf home
```

IDs and `start --file` are mutually exclusive. Reuse exact submit/start composition, including partial success and uncertain responses; never silently resume a paused ticket. General unique prefixes and interactive selection remain supported; approval/rejection retain stricter full-ID and exact-head confirmation. Old root `run` and `start` keep ticket semantics, never factory startup. Old `status` with no ID still lists tickets. `daemon` remains compatible. Setup commands remain unchanged. Advertise neither background `factory start/stop/restart` nor other unimplemented operations.

Update help, completion, canonical next-action argv and home dispatch. Nested commands must not lose root-only selection/decision decorators. Avoid changing legacy machine output while improving canonical human output.

## Ticket view and observations

View combines ticket source/state, exact next action, available plan/acceptance, verified evidence and PR links through owner-only read APIs. Unavailable artifacts are explicit; provider content is untrusted and bounded/safely rendered.

Introduce a bounded read-only activity contract keyed by channel/project/ticket, phase/attempt and daemon epoch/sequence. Distinguish process observations, provider-reported tool categories and SF-verified results. Track output arrival, last validated event, last completed tool and monitoring heartbeat independently. Do not infer progress, reasoning or death from a heartbeat or silence. Show quiet durations and gaps rather than fabricated percentages.

Supervisor stdout/stderr currently return only after process completion. New observation must operate before exit without blocking stream drain or cancellation. Decode complete bounded frames; project allowlisted categories/counters rather than raw command arguments, transcripts or arbitrary text. No private reasoning, signatures or redacted-thinking content. Provider capability differences must remain visible. Final result/session/usage/artifact authentication is independent and unchanged; existing output limits cannot simply be removed.

Bound queues/retention, coalesce noisy deltas and expose dropped-event/restart markers. Slow or disconnected viewers cannot prevent execution or drain. Watching Ctrl-C detaches only. Diagnostic persistence, if needed, belongs in SQLite and cannot create lifecycle transitions.

## AI authoring and home

`ticket new` defaults to an AI-assisted interactive conversation. Show the selected provider/model and limits before an explicitly consented inference; no silent provider fallback. Prefer an explicit authoring preference, otherwise ask. Reuse supported existing account capabilities, not mandatory new subscriptions. Manual `--no-ai`, template and validate remain available offline. Noninteractive calls must never prompt or unexpectedly spend.

Use a dedicated bounded pre-ticket authoring request/result, not a fabricated PhaseInput/planning ticket. A strict schema returns questions or title/problem/scope/acceptance/assumptions; SF renders canonical Markdown. Context is opt-in, selected and bounded: snapshot approved regular files, exclude credentials/unrelated projects and treat content as data. Enforce read-only snapshot and tool restrictions through the qualified supervisor; prompts or Cursor ask-mode alone are insufficient. Providers lacking this operation boundary are unavailable, not silently less restricted.

Review complete draft, refine, save a new private file/copy, then offer explicit Start. Authenticate the exact reviewed bytes at submission. No deadline begins before submission. Do not confuse authoring limits with execution limits or claim estimated billing is a hard cap. Separate request/time/turn/output accounting and cancellation; explicit retries require prior drain. Interrupted requests never auto-resend after restart. Any durable authoring bookkeeping stays in SQLite, without draft lifecycle duplication.

Natural-language home reuses the same bounded inference capability and proposes only a closed typed action. Deterministic code resolves project/ticket and constructs the public command. Read-only actions may run after unambiguous resolution; mutations require exact-action confirmation and fresh state/head checks. No shell, arbitrary argv, bulk cancellation or model-created approval. Preserve the numbered menu and explicit commands. Home disconnection does not cancel tickets.

## Work packages and verification

1. Grammar and rich view: namespace/compatibility, selectors, decisions, exact start composition, safe artifact display and complete help/next-action migration.
2. Live activity: contract, provider decoders, nonblocking supervisor observation, owner-only snapshot/watch API and renderer.
3. AI authoring: dedicated authenticated production capability, bounded context/accounting, structured conversation, preview/save/Start and manual fallback.
4. Natural-language home: typed intent schema, exact-target confirmation, read-only navigation and safe public-handler dispatch.

Each package needs source tests and production wiring, not only mocks. Required regressions include old/new request parity; nested selector/head confirmation; malformed/ambiguous IDs; exact preview/file swaps; cancelled/failed/uncertain submission; delayed activity before exit without state change; fragmented JSON/UTF-8/secrets/ANSI; flooding/backpressure; reconnect/restart/stale attempts; no-authority telemetry; authoring denied tools/context escape; unavailable auth; budget/cancel/retry; malformed model output; prompt injection and stale action confirmations.

Automated tests/builds run in GitHub under the existing user policy. Full Go suite, race, repository/docs/secret checks and applicable compiled acceptance must pass before claiming delivery. Live provider acceptance, if needed, requires an approved account/budget and environment; fixtures alone do not prove a installed provider works. No pushes/merges, installations, daemon restarts or live DB mutations without applicable authorization.

## Orchestration and completion

Root owns scope, contracts, integration, evidence and review. Implementation lead delegates production coding to child agents. Luna handles bounded research and tests. Shared four-agent capacity requires explicit file ownership: after Luna completes a bounded task, the free slot can host a second coding child for an independent integration surface. Formatting/static diff checks only locally.

Implementation checkpoint: all four packages have connected source and regression tests. Activity tests cover observation before exit, recorder refusal, bounded buffers/frames, stale attempts, reconnect/gaps and viewer detachment. Authoring includes async daemon integration, real-gate lifecycle fixtures, a Darwin native sandbox fixture, migration/reopen coverage, and a read-only interrupted-turn inspection command printed before inference. Home uses a closed seven-action schema and existing deterministic public handlers. Luna supplied independent content/context, activity, sandbox and home tests plus documentation. Root and the delivery lead reviewed source; this is not execution evidence. No automated tests or builds have run, and the implementation is not installed or released. Push/draft-PR authorization and GitHub validation remain pending.

Authoring uses separate v62 session/turn bookkeeping, immutable purpose/context/runtime binding, at most four consented SF turns per session, one undrained authoring launch per channel and a 90-second turn deadline. These are not provider API-request or hard billing caps. The initial safe operation is Claude-only, independently prepared without requiring execution-role qualification. Other providers remain explicitly unavailable for authoring until an equivalent boundary is implemented. Valid but undrainable authoring recovery retains quarantine and blocks further authoring, not ordinary ticket startup; malformed durable evidence still fails closed. Creation performs no inference, context stays in bounded daemon memory, and restart never automatically resends a turn.

Completion requires connected canonical commands, safe useful live activity, AI draft-first flow through a real qualified operation, typed natural-language home, compatibility and regression evidence, and matching documentation. An unavailable-only AI stub or mock-only demo does not complete this goal. Measure a five-task user journey for correct command choice, draft creation, selection, start and quiet/active/disconnected interpretation; do not invent timing or usability scores.
