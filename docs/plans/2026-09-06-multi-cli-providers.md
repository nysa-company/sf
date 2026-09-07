# Cursor and Claude Code execution alongside Codex

Status: approved for execution; compatibility preflight started. Prepared
2026-09-06 against `6f12229`. User requested a plan corroborated by independent
agents and subsequently authorized implementation and an explicit goal.

2026-09-07 user decision: assume existing Cursor hooks will not interfere and
continue implementation. This supersedes the all-ambient-hooks-disabled gate
for the operator's trusted local/manual/guarded integration. Treat hooks as
trusted dependencies, not proven isolated; do not remove managed policies or
claim hostile-hook containment. Exact model selection, independent families,
role file permissions, process ownership/drain, durable retry authority, and
native acceptance remain required. Additional Cursor testing budget is $100;
track new paid launches separately from the earlier allowance and unknown
charges. Qualification under this assumption needs its own distinct policy
identity; it must not inherit a stronger isolation verdict.

2026-09-06 update: user explicitly authorized installation of both CLIs and
up to $100 total Cursor testing spend, then chose normal Cursor browser login
instead of an API key. Both logins and four minimal live requests passed:
Claude Sonnet 5; Cursor Sonnet 5, GPT-5.6 Luna, and Grok 4.6. Prefer these exact
model families without automatic fallback. Browser-auth qualification must
replace the API-key assumption for this acceptance path. Cursor terminal JSON
reports tokens but not dollar cost; live spend accounting remains unresolved.
Basic access does not qualify either runtime for production SF execution.

Billing decision (2026-09-06): user approved explicitly labeled cost estimates
with time/request limits, not a hard-dollar guarantee. Keep reported estimates
separate from verified charges; absent pricing remains unknown. Persist the
chosen accounting policy and reserve request capacity before execution so
restart and retry cannot reset limits. Do not enable production execution just
by marking estimates UsageTrusted. The existing $100 total Cursor testing
ceiling is unchanged; token-only responses do not establish remaining dollars.

Initial code-owned request guard: at most 16 total SF provider attempts on a
ticket admitting Claude/Cursor (all phases/providers/outcomes count), at most
45 minutes per invocation, also bounded by the ticket deadline. Store checks
the durable attempt count in the admission transaction. These are SF launch
limits, not provider-internal API request limits; one CLI process may make
multiple API calls. Codex-only billing compatibility remains unchanged. The
estimated accounting policy is still required before enabling these providers.

Store policy checkpoint: append-only migration v59 stores explicit
`reported_estimate_v1` consent and the ticket's estimate/request/time limits.
No legacy ticket is backfilled. Opt-in requires the current planning fence and
zero prior attempts; exact replay remains idempotent. Claude/Cursor admission
requires this row. `start --accept-cost-estimates` now records consent before
starting the ticket. Initial project setup supports `init --providers select`
and explicit `claude-codex`, `codex-claude`, or `codex-codex` presets;
existing-project model editing remains pending. The row alone does not
qualify a runtime. Drained terminal estimates persist separately from verified
charges, including explicit unknown observations. Completion authenticates
that observation against the opt-in policy without setting UsageTrusted.

Native acceptance checkpoint (2026-09-06): Claude Sonnet 5 and Codex Luna have
both qualified as an independent pair. The first trial stopped on Claude's
rejection of the draft-2020-12 schema declaration. The adapter now projects only a validated dialect-common subset
to draft-07; original PhaseInput and SF artifact validation remain unchanged.
Unknown/dialect-specific keywords refuse, and the changed policy requires
fresh qualification. The reproducing native schema probe passes. The compiled
Claude Planner/Builder + Codex Luna verifier/final-reviewer trial then passed
through guarded Done in 216 seconds: four completed attempts, one draft PR,
one ready/merge action, and no retained leases. Models and SF were real; GitHub,
hosted Git, and approval were disposable local fixtures, not a hosted delivery.
The reverse-role trial also passed in 231 seconds with exact persisted role/model
assertions. Both trials used local publication fixtures. The two-ticket real-model
concurrency trial passed in 219 seconds: overlapping pipelines, separate worktrees
and draft PRs, exact Claude Sonnet 5/Codex Luna roles, successful command proofs,
and no active provider/command leases. It stops at waiting CI with fake GitHub;
it is not a hosted delivery or concurrent merge verdict.
Qualification now itself exercises the
schema projection and carries a new fixture digest. Qualification transport is bounded to four minutes
after owner/request authentication; ordinary requests retain thirty seconds.

## Outcome and scope

A developer with an existing supported CLI login can select Claude Code,
Cursor, or Codex for SF roles without managing internal provider IDs. SF
delivers tickets through the existing verification, build, publication,
independent final review, and human approval workflow. Provider selection must
not create another workflow engine or weaken durable authority.

First slice: Claude Code plus Codex. Second slice: Cursor. Keep Codex-only
projects working throughout. Initially use explicit assignments, not automatic
fallback or model benchmarking. Retain the trusted local macOS/repository
boundary and manual/guarded merge modes. No autonomous merge, API billing
fallback, provider installation, or live project changes are implicit in this
plan. User confirmed paid Claude and Codex accounts and Cursor API credits.
Support those explicit billing modes; do not treat Cursor credits as free or
assume that they are usable by the CLI without a compatibility check. Direct
API adapters are not implicitly added. User confirmed fallback stays deferred,
but same-provider retries of a failed role are required.

## Verified starting point

- `internal/auth/auth.go` and the friendly CLI already recognize Claude and
  Cursor authentication. Authentication is not execution qualification.
- `internal/contracts/provider.go` and `internal/providercoord/coordinator.go`
  expose an exec-free adapter interface, immutable claims, and logical roles.
- `internal/codexprovider/composer.go` only composes/qualifies Codex in
  production. `internal/processsupervisor/supervisor.go` hard-codes Codex
  runtime staging, environment, and subscription authentication.
- `internal/store/qualification.go` requires signatures specifically for
  Codex and has a permissive non-Codex current-qualification path. New
  production adapters must not use that as an admission shortcut.
- Configuration already names provider preferences, but preferences must be
  resolved to qualified, pinned role identities before execution.
- Legacy `software-factory/scripts/adapters/{claude-code,cursor-agent}.sh`
  and the family-specific Cursor helpers are reference material only. Do not
  port legacy shell execution, unrestricted flags, or state journals. Old
  pinned flags and versions require fresh verification.

## Delivery sequence

### 1. Bounded compatibility spike

Inventory installed executable identities without reading/displaying secrets.
Confirm current official CLI behavior and pin a tested compatibility range:
headless invocation, final artifact framing, actual model identity, auth mode,
private runtime/home support, permission controls, cancellation, and usage.
Exercise only disposable fixtures once implementation/live-test scope is
approved. Time-box each adapter spike to one working day; publish pass/fail and
the exact missing capability instead of growing a parallel security project.

Gate: a supported authenticated CLI can produce one bounded structured result
under SF-owned supervision and can be stopped without losing process ownership.
If it cannot, leave that provider unavailable with a concrete explanation.

### 2. Shared admission, without a broad rewrite

Extract the smallest production composition/runtime strategy boundary needed
by the existing supervisor. Keep process launch, drain, quarantine, immutable
attempt evidence, and recovery owned by current SF components. Explicitly
allowlist supported provider strategies; unknown providers fail closed.

Require supervisor-signed qualification for every production adapter at
issuance, composition, and attempt admission. Bind executable/runtime,
model/family, policy, auth class, and qualification evidence. Audit Store SQL
and historical loaders, not only the public interface. Use append-only
migrations if persisted changes are necessary; do not rewrite shipped
migrations or old canonical inputs. Preserve historical Codex replay.

Resolve the current channel-wide selected pair versus project role preferences
into one Store-authenticated ticket configuration. Reject unsupported fallback
lists rather than silently ignoring them. Keep local executable/auth references
out of portable project settings; show the actually resolved roles in doctor.

### 3. Claude Code vertical slice

Add an adapter with argv-only invocation, strict bounded output parsing,
normalized failure/usage reporting, and provider-specific auth/runtime policy.
Keep raw provider output and credentials out of user-facing diagnostics and
durable authority. Prove both verification-authoring and Builder execution;
final review is a fresh invocation, not a shared conversation.

First real delivery preset: Claude Builder/Planner + existing Codex Reviewer.
Then exercise Claude verification/final review using Codex Builder.

### 4. Cursor vertical slice

Add the same boundaries for Cursor. Resolve the installed `agent` versus
legacy `cursor-agent` explicitly; never execute an arbitrary binary called
`agent` without identity qualification. Pin an explicit available model;
reject `auto` or an unverifiable model identity for independent-review roles.
Do not assume Cursor exposes the same artifact, usage, auth, or sandbox
semantics as Claude or Codex.

### 5. Friendly configuration

Extend existing setup/auth/qualification flows rather than creating another
setup command family. Proposed interaction:

1. Show installed, authenticated, compatible, and qualified as distinct states.
2. Offer a recommended qualified Builder/Reviewer preset.
3. Default Planner to Builder; expose separate Planner selection as advanced.
4. Show the selected CLI, exact model/family, auth/billing class, and capacity.
5. Save through the existing immutable project configuration mechanism.

Keep noninteractive flags and JSON output for automation. Existing commands
and Codex configuration remain compatible. Unknown billing is not zero cost;
missing auth never silently switches to API billing. Configuration changes
apply to future tickets, not in-flight attempts. No copying long ticket IDs
or hand-editing internal qualification IDs should be necessary.

Independence is based on authenticated model family, not CLI names. Cursor
using Claude plus Claude Code using the same family is not an independent
pair. Do not infer stronger vendor independence than the recorded evidence.

### 6. Acceptance, then release decision

- Contract tests for bounded/malformed/multiple/missing final results, stdout
  versus stderr, exit errors, unsupported versions, auth expiry, model drift,
  quota failure, unknown usage, and secret redaction.
- Every new provider must reject unsigned/stale qualification and wrong
  binary/auth/policy/model bindings. Preserve historical Codex recovery.
- Process tests: cancel during launch/streaming, daemon restart, ambiguous
  descendants, retained pipes, cleanup only after proved drain, and no blind
  restart of a provider-native conversation.
- Permission tests: verification may author only its allowed files; final
  review is read-only; providers cannot perform SF Git/GitHub mutations.
  Audit hooks, MCP, plugins, shell tools, inherited settings and credentials.
- Capacity tests: two tickets using different providers; same-account shared
  capacity across profiles; third queues; cancellation frees only the correct
  slot; no duplicate attempts or external effects.
- Setup tests: no CLI, installed-but-logged-out, account model unavailable,
  unsupported pair, upgrade requiring requalification, interactive and JSON.
- Live disposable delivery matrix: Claude Builder/Codex Reviewer; Codex
  Builder/Claude Reviewer; Cursor Builder/independent Reviewer; independent
  Builder/Cursor Reviewer. Reuse a ticket for the same adapter's verification
  and fresh final-review coverage, but do not label mock evidence as live.
- At least one live two-ticket cross-provider run, within an explicit budget;
  wait through ordinary CI and exact human approval. No manual DB edits,
  controller patches during a passing trial, duplicate PRs, or leaked leases.
- Run focused tests/race first, then full Go and repository baseline checks.
  Hosted CI may offload portable tests; native macOS process/auth tests remain
  mandatory and must not be marked passed by Linux-only results.

Done means both adapters deliver through the normal workflow, setup explains
every unavailable state, independent review is enforced, and existing Codex
acceptance remains green. A CLI returning JSON alone is not completion.

## Estimates and scope control

Planning estimate, not a delivery promise: first Claude/Codex vertical slice
2–4 focused working days; both adapters plus setup, recovery/concurrency and
live acceptance 5–8 focused working days. Auth isolation and runtime packaging
are the largest unknowns. Re-estimate after the bounded spikes. Do not block a
passing Claude slice on a failing Cursor spike; label Cursor unavailable and
report the specific prerequisite. No automatic fallback in this milestone.

## Same-role retry requirement (user clarification)

Retry is distinct from fallback: retain the exact phase, provider/model and
authenticated input, preserving completed earlier phase evidence. Do not rerun
the whole ticket or change providers silently.

Current SF has one same-binding repair for a clean, trusted invalid artifact;
two failed attempts exhaust the entry, and an eligible operator retry opens
one further two-attempt window. This is not a general network/API retry loop.
Post-launch command/protocol/usage uncertainty is durably indeterminate and
does not admit another paid attempt. Retry requires physical worktree proof;
retained writes are not automatically discarded or adopted.

Add provider-specific, authenticated failure classification before expanding
automatic retries. Proposed initial policy: at most two total launches per
normal role entry, shared with artifact repair rather than an additive hidden
budget. A proven transient rejection/pre-execution failure may use the remaining
attempt on the same binding, with bounded backoff/jitter and Retry-After when
available. Honor ticket time/cost limits; persist attempt/backoff state so
daemon restart cannot reset it. CLI-internal HTTP retries must be documented
separately from SF launches. Unknown partial execution, missing final response,
dirty/ambiguous worktree, authentication failure and exhausted credits must not
enter a blind retry loop. Preserve current indeterminate behavior until an
explicit reconciliation boundary can prove another launch safe.

Tests must cover safe transient retry, second failure pause, no new model or
provider, no repeated earlier phases, durable counters across restart, no
double repair/retry allowance, and uncertain/partial-write refusal. Runtime
behavior is unchanged by this planning document.

### Implementation boundary checkpoint (2026-09-07; not yet wired)

The complete-stream observer now exists, but it is deliberately not authority.
Source inspection identifies these required integration points:

1. `Supervisor.Run` currently converts nonzero exits to `ExitCode=-1` plus an
   error, and Coordinator skips adapter parsing on a Run error. Preserve that
   conservative public behavior for existing policies. A newly qualified
   streaming policy must retain the real exit and validated rejection metadata
   internally; do not reconstruct them later from an adapter-supplied result.
2. Existing `Drain` removes the run after proving it gone. Use an optional
   supervisor-owned drain-and-rejection operation to return both attestations
   together, without widening every fixture/provider interface. Sign only
   after complete process/I/O drain, with the exact full DrainRequest, qualified
   policy, stream digest and closed rejection classification bound under a
   distinct signature domain. A concurrent control drain that wins first must
   leave no usable rejection attestation. Never accept a drain signature as an
   API-error signature or let adapters submit raw output to a signing endpoint.
3. Initial eligible candidates should be all-server-error transcripts only.
   Rate-limit/quota, authentication, billing, unknown, cancelled, partial,
   truncated and mixed histories remain ineligible. Even an all-server-error
   observation must be paired with a Store-derived checkpoint and independent
   physical HEAD/identity/cleanliness verification while the current claim
   still excludes other writers. Do not discard or adopt retained changes.
4. `ProviderRetryWorktreeProof` intentionally authenticates an exhausted
   operator-retry epoch; it cannot authorize the first automatic retry. Add a
   separate exact-attempt checkpoint authority using the existing phase/head
   evidence derivation, not a fake exhaustion event or caller-supplied HEAD.
   Production composition must supply the trusted physical inspector; missing
   inspection remains fail-closed. Keep verification/candidate/refresh lineage
   checks and the existing paused retry API unchanged.
5. An append-only migration after v59 must record the signed rejection,
   checkpoint identity, and immutable not-before time atomically with the
   failed attempt/phase and exact lease release. Backoff derives once from a
   bounded code-owned policy plus available validated delay information;
   restart cannot recalculate it. Do not claim that the last internal-retry
   delay is a final HTTP Retry-After header when none was emitted.
6. Admission must recognize only that authenticated predecessor and remaining
   position in the existing two-attempt window (including the single operator
   retry window). API retry and artifact repair share the same counter, not
   two independent allowances. Preserve exact role/runtime/auth/input lineage,
   current recovery fencing, prior completed phases, ticket/request budgets,
   and no provider fallback. A changed binding cannot consume the pending retry.

Required crash tests cover before receipt persistence (no new launch), after
atomic finish but before backoff (same persisted deadline), after restart
(exact signed recovery and same remaining budget), cancellation/control races,
dirty checkpoint refusal, forged/stale/wrong-domain signatures, and both mixed
failure orders: API then invalid artifact, invalid artifact then API. Streaming
qualification and a fresh live subscription trial remain required; the local
synthetic API probe does not substitute for them. This checkpoint is an
implementation design, not a claim that these boundaries exist yet.

## User decisions pending

No further billing decision is pending: the approved estimate policy and
$100 total Cursor test ceiling are recorded above. Actual Cursor spending
remains unknown, not zero. Claude-first remains the recommendation.

### Current validation checkpoint

Mixed Claude/Codex production qualification and composition are implemented in
source; Cursor remains unavailable until role permissions/hooks are qualified.
The installed Cursor build loads enterprise, team, user, and project hooks
separately from `.cursor/cli.json`; `--disable-project-configs` alone is not an
isolation proof. Do not enable unrestricted execution as a substitute.
Claude qualification uses two model-bearing CLI launches plus one cancelled
startup, not a guarantee of two underlying API requests. Ticket status now
labels estimated accounting and actual-total uncertainty explicitly.
Compiled production-gate acceptance and full feature delivery remain separate
from protocol smoke tests; the complete multi-provider goal is not yet met.

## Independent review

Three independent agents reviewed architecture, official-CLI feasibility, and
adversarial acceptance. They agreed on the staged approach; their findings
expanded the plan beyond adapters to signed admission, runtime dependency
staging, one resolved role configuration, account-level capacity and verified
billing classification. This is corroboration of a proposed plan, not a
runtime qualification or release verdict. The primary agent also checked the
legacy adapters and current official headless/output documentation.

The architecture reviewer recommended retaining the existing Provider interface
initially and extracting only production composition. The feasibility reviewer
recommended Claude first because of its structured-output support; Cursor's
failed run may have no final JSON, so absence must never mean success. The
adversarial reviewer highlighted subscription overages and the channel-pair
versus project-role configuration conflict. All are incorporated above.

## Official feasibility evidence and remaining gates

- [Claude headless](https://code.claude.com/docs/en/headless) documents print
  mode, JSON/schema output and SIGTERM behavior. Its bare mode suppresses
  discovered configuration but does not use subscription OAuth/keychain
  credentials. Therefore subscription-safe isolation is a spike gate, not an
  assumed combination of flags. Documented signal behavior still needs native
  supervisor testing. Reported cost is an estimate, not billing authority.
- [Claude authentication](https://code.claude.com/docs/en/authentication) and
  [CLI reference](https://code.claude.com/docs/en/cli-usage) guide explicit auth
  mode and capability checks. Do not accidentally inherit API-key billing.
- [Cursor parameters](https://cursor.com/docs/cli/reference/parameters) document
  print mode, model selection and sandbox options. Current executable naming
  differs from SF's legacy authentication detection and must be reconciled.
- [Cursor output](https://cursor.com/docs/cli/reference/output-format) documents
  JSON/stream framing and failure cases. Use a bounded final-result protocol;
  do not enable partial-output mode initially.
- [Cursor authentication](https://cursor.com/docs/cli/reference/authentication)
  and [permissions](https://cursor.com/docs/cli/reference/permissions) are the
  starting point for runtime qualification. Account entitlements, private auth
  portability, actual model identity, and print-mode child cleanup remain
  unproved. Do not substitute ACP/session continuation in the first milestone.

No implementation, installs, model calls, or tests were performed while
preparing this plan. Documentation/source review does not prove installed
runtime compatibility.

## Execution checkpoint

The multi-CLI goal is active. Initial executable discovery found Codex in
PATH, but neither `claude`, `agent`, nor `cursor-agent`. The usual private
CLI installation locations and Homebrew executable paths checked so far did
not expose either new CLI. This does not prove that the accounts are absent
or that the CLIs are not installed elsewhere. Real runtime qualification
requires locating them or explicitly authorizing installation; the approved
scope above does not implicitly install providers. No credentials were read,
models called, or live runtime state modified.

Source implementation has begun: atomic three-role selection, credential-bearing
signed qualification/admission for the three explicit auth classes, and a
bounded terminal JSON decoder. Full focused Store/Codex/coordinator suites pass
after these changes. The new Claude invocation proposal has role-specific file
tools, explicit models, and restricted/safe modes; focused proposal tests pass.
It is not yet registered or executable through production composition. Existing
credential-free qualification fixtures are not production runtime admission.

The current [Claude CLI reference](https://code.claude.com/docs/en/cli-reference)
documents safe mode retaining authentication and restricted mode limiting
tools/configuration. These are the proposed qualification target instead of
bare mode, which excludes subscription authentication. Installed capability,
runtime dependency isolation, and real subscription behavior were initially
unverified. The following checkpoint supersedes that initial state.

### Installed-runtime checkpoint (2026-09-06)

- Official Claude 2.1.263 and Cursor 2026.09.02-c22c1a3 are installed. User
  browser logins work, including tiny explicit Sonnet 5/Luna/Grok smoke tests.
- Claude schema output works with restricted/safe mode. Its reported usage
  includes auxiliary Haiku alongside the selected Sonnet model; reported
  list-price costs are not asserted to be subscription charges.
- Bounded runtime snapshots authenticate and copy the actual installations:
  one Claude executable and Cursor's complete 570-member file/directory tree.
- Supervisor-owned, fixed-service Keychain handoff authenticates both CLIs
  with disposable private homes. Status-only native tests pass without copying
  user settings/history/MCP/hooks. Secrets stay out of argv, Store and logs.
- Production registration, role execution qualification, billing enforcement,
  friendly setup, retry composition and full live acceptance remain pending.
  Browser authentication does not prove zero incremental cost. Do not enable
  these runtimes through the credential-free fixture path.
