# Cursor execution compatibility gate

Status: Luna Low signed native qualification PASS (Sept7, 59.82s); production
composition wiring is under regression, and full-ticket acceptance is pending.
On Sept7 the user explicitly instructed us to assume hooks do not
interfere and continue. The all-hooks-isolated requirement below is historical,
superseded for this operator's trusted local/manual/guarded mode. Do not remove
managed hooks or claim they are contained. Native file permissions, process
drain, exact role/model bindings, and independent review still require proof.

First native probe under the revised assumption passed (19.223s): staged
browser-auth Cursor `gpt-5.6-luna-low` accepts a stdin prompt and returns one
bounded valid JSON artifact. This used one CLI launch against the additional
$100 allowance; actual charge is unknown. It is not role qualification or
delivery. Exec-free invocation/permission proposals and unit tests are added;
production admission remains disabled until the native role tests pass.

## Evidence rechecked on 2026-09-06

Sept7 native role follow-up: Cursor's allow entries did not prevent an
out-of-scope file write, with or without `--force`. A proposed SF-owned outer
macOS filesystem profile passes credential-free authorized/forbidden and
read-only write tests. A native Builder passed under it; native Reviewer
also passed (19.326s) after permitting the pinned CLI's same-session
`thinking` delta/completed notifications. Those notifications are never
logged or used as artifacts; only a valid terminal result is accepted. These are
compatibility experiments, not signed qualification or production admission.
The fixture uses a short private home (the pinned CLI otherwise falls back to
`/tmp/.cursor`) and disables only the nested Cursor sandbox while retaining
SF's outer profile. The canonical invocation and Supervisor now implement this
mode under a distinct trusted-hooks policy. Runtime observation checks the
pinned bundle, browser authentication, and exact model catalog; an exec-free
adapter authenticates the result stream. These components do not yet enable
Cursor in production composition or prove qualification.

Native qualification found that Cursor leaves a local worker after its main
print process exits. An SF-owned wrapper now retains the launch leader until
the pinned CLI's private local-worker cleanup completes; native drain then
passed. Qualification next isolated a real catalog-versus-session context
label mismatch: explicit `gpt-5.6-luna-low` reports `GPT-5.6 Luna 272K Low`,
while its catalog entry is `GPT-5.6 Luna 1M Low`. Installed source confirms
parameterized selection reconstructs session display names separately. A
narrow code-owned mapping now requires that exact catalog entry and binds the
272K session label; changed labels refuse. No generic label normalization is
used. Do not claim 1M context support from the catalog or accept a result's
label as its own authority. The complete signed qualification, cancellation,
composition, and delivery remain gates until their tests pass.

The gated native run subsequently passed model/terminal validation and exposed
two tool-stream assumptions in the fixture validator. Pinned protobuf tool
calls include optional IDs/timestamps/hook metadata beside exactly one tool;
completion events may omit args. The validator now consumes only documented
metadata, pairs starts/completions by ID and tool, carries the starting read
path, and rejects a changed completion path. These are parser corrections,
not permission relaxations; denied outside reads and unchanged forbidden files
must still be observed independently before signing.

The outside-read failure was a schema mismatch: the stream contains
`ReadToolResult.error.errorMessage`, not lower-level `ReadResult.error.error`.
A reproducing regression failed before the fix and passes afterward. The
validator now accepts only the pinned error-message shape with an OS denial
and the paired starting path, never generic failures or the wrong schema.
Native73465 PASS59.82s then signed the complete role fixture: Builder write,
outside-read refusal, unchanged forbidden file, read-only Reviewer and launch
cancellation/drain. An earlier inventory failure did not recur; retain its
fixed diagnostics as a reliability concern, not a repaired claim. Running-
model cancellation, Store/coordinator full-ticket acceptance, and automatic
Cursor API retry remain separate requirements. No live channel was changed.

Installed CLI: `2026.09.02-c22c1a3`. Both `agent` and `cursor-agent` resolve to
the same versioned bundle. A read-only `agent --help` lists print, read-only
plan/ask modes, sandbox enabled/disabled, and force/yolo. Print mode explicitly
has file-write and shell tools. No all-hooks-off option is exposed in this help.

The installed `190.index.js` hook loader defaults `loadProjectHooks` to true,
loads separate configuration sources, and has command-hook execution. A bounded
search of installed JavaScript found no `disableHooks`, `disableAllHooks`, or
`--disable-…hook…` switch. This search is supporting evidence, not proof that no
possible mechanism exists. Earlier bundle inspection identified enterprise,
team, user, project, and compatible Claude hook sources.

Current official [hooks documentation](https://prod.cursor.com/docs/hooks)
describes hooks as spawned processes and includes session, tool, and file-edit
events. [CLI permissions](https://prod.cursor.com/docs/cli/reference/permissions)
and [sandbox configuration](https://prod.cursor.com/docs/reference/sandbox)
describe useful controls, but do not establish that all hook execution is
disabled or constrained to SF's role policy. A sandbox flag by itself is not
that proof. No paid calls, authentication changes, or installed-file edits were
used in this recheck.

## Required evidence to reopen execution

Sept7 resumed investigation: the user authorized an additional $100 for Cursor
testing. New-window spend remains $0. A deterministic offline probe executes
the installed HookConfigLoader class with a fake filesystem reporting no files.
With `loadProjectHooks=true` it checks all seven configured source paths; with
`false` it still checks enterprise, team, user, and Claude-user paths. This is
file-selection evidence only, not a live execution or containment test. The
unmodified class digest is
`0b7e60bf9d8642df918dd8a789dc16b6a953975ffc944d120f3e8f8adbff87c5`.
Scratch reproduction is `.context/cursor-hook-path-probe.cjs`.

Installed `4347.index.js` also handles `teamHooksResultPromise` by merging its
returned team hooks into the executor using `updateConfig`. That path means a
proposal to block only hook-file reads cannot by itself prove that no hooks
execute. No enterprise policy was stripped, no credentials were read, and no
paid request was made. This narrows the missing proof; it does not establish
that every supported integration strategy is impossible.

A metadata-only local check found `authInfo.teamId` present in this account's
CLI configuration. Its value and other account fields were not printed. This
does not prove any team hook is configured or that the account is enterprise-
managed, but it rules out assuming that the team-hook path is irrelevant to
this operator. Do not remove that binding to make a probe pass.

Sept7 alternative-path audit: the documented ACP CLI integration is not a
verified hook-isolation alternative. Installed `5421.index.js` constructs its
hook executor with `promptHookClient`, merges asynchronous `teamHooks` into
both its config provider and executor, and wraps session resources with that
executor. ACP permission requests do not by themselves prove that these
separate paths are mediated. No ACP session or model request was launched.
The [ACP documentation](https://cursor.com/docs/cli/acp) establishes the
integration interface, not an all-hook isolation guarantee.

The [sandbox reference](https://cursor.com/docs/reference/sandbox) documents
filesystem/network policy and policy merging, including unioned extra paths.
It does not establish a fixed model/cost policy for in-process prompt hooks.
Therefore neither ACP nor a sandbox configuration file currently supplies the
missing qualification evidence. This is a bounded failed compatibility spike,
not a claim that Cursor is generally unsafe or impossible to integrate.

Sept7 follow-up: both installed symlinks still resolve to the same
2026.09.02-c22c1a3 bundle. Fresh official
[parameters](https://prod.cursor.com/docs/cli/reference/parameters) and
[configuration](https://prod.cursor.com/docs/cli/reference/configuration)
checks did not establish an all-hooks-off execution contract. `CURSOR_CONFIG_DIR`
is documented as a configuration-location override, not proof of disabling
enterprise/team/project hooks. No installation, credentials, config, or paid
execution changed. This leaves the existing gate unresolved, not proof that
every possible supported integration is impossible.

2026-09-07 bounded alternative check: denying native child creation alone is
insufficient. The official [prompt-hook documentation](https://prod.cursor.com/docs/hooks#prompt-based-hooks)
describes LLM-evaluated hooks with an optional model override. The unchanged
installed `190.index.js` dispatches prompt hooks separately from command hooks
via `promptHookClient.evaluatePromptHook`; `index.js` forwards that operation
to an `EvaluatePromptHook` RPC. This is source evidence, not a live hook test.
An OS process-fork denial would not establish control over that in-process
path, its selected model, or its billing. No native policy prototype or paid
request was launched. An acceptable isolation proof must cover these hooks
and MCP paths as well as spawned command hooks; do not equate no children
with no ambient execution.

1. A pinned official CLI mechanism that prevents ambient hook/plugin/MCP
   execution, or independently enforced policy covering both in-process
   evaluation and every spawned child. Do not modify or silently strip
   enterprise policy to bypass it.
2. Isolated browser authentication without exposing credentials to project
   tools; exact executable, model, family and policy binding.
3. Native fixtures proving allowed verification writes, read-only final review,
   refusal of shell/Git/GitHub mutations, cancellation/drain, and retained-pipe
   recovery. Every successful mode needs its own signed qualification.
4. Only after those gates: bounded real Cursor Builder and Reviewer delivery
   tests, with independent model families and explicit estimate accounting.

Until then, SF refuses Cursor qualification before invoking a model or changing
the selected pair. The supervisor exposes no Cursor policy and refuses attempts
to reuse Claude/Codex policies, including known Sonnet/Luna/Grok catalog models.
`--force`, `--yolo`, login success, and JSON output are not substitutes.

This does not prevent using the verified Claude/Codex combinations. The $100
total Cursor testing authorization remains a ceiling; no additional Cursor
spend was incurred by this inspection and actual earlier spend is still unknown.
