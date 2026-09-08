# Multi-CLI acceptance ledger

Scope: the approved multi-CLI plan, not just the passing Claude slice.
Verdict: **implementation and validation complete with declared capability
limits**. See `2026-09-07-multi-cli-completion-audit.md`. Not a stable release
or a claim that every catalog model or uncertain API failure is supported.

## Current checkpoint

Final full multiCLI87903 TERMINAL PASS exit0: cmd/sf106.491s,
daemon22.745s, GitHub64.534s, supervisor74.006s, workflowruntime113.036s;
other packages passed, some cached. Paid/live opt-ins were off. Full vet,
repo/docs/diff checks pass; secret83523 passes. Live28572 already passed the
original concurrency/restart/separate-approval scenario. Local development
candidate10576 and version/help checks pass. Final requirement audit records
Cursor unstructured API-error retry and Grok qualification as unavailable,
not bypassed. MultiCLI source is local/uncommitted and excluded from PR #2.
All older pending/running statements below are historical.

Live Cursor concurrency28572 TERMINAL PASS752.36s (package752.998s), exit0.
Real Cursor Luna Low + Claude Sonnet5, two tickets, native daemon restart and
requalification, separate approvals, fresh sibling build/review on moved base,
both Done. Git/GitHub/approvals remain disposable fixtures, not hosted Relay.
This confirms the corrected original test scenario; earlier failures remain
historical evidence. Full post-assertion multiCLI baseline refresh and final
requirements audit remain before a completion claim.

Corrected live concurrency28572 is running with the original 25-minute bound,
after isolated beta suite91548 finished PASS. That beta suite is not the
multiCLI baseline;86750 remains the latest full multiCLI run, followed by
test-only correction65374/race87126 and exact Store/Coordinator57908 PASS.
Current repo/secret checks83523 PASS. No live rerun success is claimed yet.

Live55528 TERMINAL FAIL476.08s: test validator rejected a durable
failed/invocation_failed prior attempt. Store records this only before any
supervisor launch with empty process identity. Test-only regression80021
failed red;65374 passed green0.865s after allowing that exact outcome with
the existing same-binding/fence/two-attempt limits. Quarantined, indeterminate,
cancelled and unknown outcomes still reject. Original live rerun is pending.
The separate committed Codex beta is now PR #2 at exact6f12229; none of this
unfinished multiCLI work is in that PR. Older running statements below are
historical and superseded by this checkpoint.

Baseline86750 TERMINAL PASS (paid/live opt-ins off): cmd/sf91.465s,
daemon22.507s, GitHub63.993s, supervisor79.492s, Store159.858s,
workflowruntime171.412s; other packages passed, some cached. Full vet exit0,
repo-check/secret-scan84871 PASS, diff-check clean. Claude native tool-disabled
Sonnet request57959 PASS refreshed browser authentication automatically;
expiry-only check now479 minutes, clearing the prior lifetime blocker without
changing SF policy or user action. Bounded live concurrency rerun launched next.

Joined synthetic native process/Store retry63066 PASS33.254s (all five
receipt scenarios, race count3). Real Store admission/launch/finish, database
reopen, durable backoff, real Git checkpoint inspection, same-binding second
process launch and second rejection persistence are joined. CLI, credentials,
qualification and gate remain synthetic; not an installed-vendor forced failure
or full Coordinator test. Full supervisor race29469 PASS79.264s before this
joined fixture. Full current-tree baseline86750 is running after these additions.
Fresh native auth status reports Claude signed in, but expiry-only Keychain
check reports four minutes remaining, below the required 46-minute launch
window. No paid rerun or token disclosure. Live concurrency remains pending.

Native-process synthetic rejection coverage41349 PASS5.960s/count3/race:
actual Supervisor Run capture→process completion→drain→exact single-use receipt;
partial/false-success/stderr-only output refused. CLI, gate helper, credential
and checkpoint inspector are synthetic; not installed-Claude or combined
Store retry proof. Public Run still selects fixed Keychain lookup; only a
private package test seam supplies synthetic credentials. Full supervisor race
29469 active; full67096 below predates this refactor/test addition.

Full current-tree67096 PASS exit0, paid/live opt-ins off (some unchanged
packages cached): cmd/sf91.778s, supervisor65.283s, providercoord37.695s,
publication91.274s, Store127.843s, workflowruntime109.769s,
worktreecoord132.015s. Current full vet, repo-check/secret-scan59473 and
diff-check PASS. All handles terminal. Live Cursor restart confirmation and
combined native retry coverage remain open; no goal completion claim.

Live58623 FAILED67.99s at initial qualification: Cursor qualified, Claude
credential lifetime could not cover the launch window. This does not exercise
or falsify the startup fix. Paid reruns are stopped pending credential renewal;
current-tree nonpaid full baseline launched next.

Startup composition regression62917 proved stale leader qualifications still
triggered runtime inspection before eventual refusal. Added the same existing
qualification checks before any inspection; no timeout increase or admission
relaxation. Positive current-qualification inspection and stale idle behavior
pass race38839 (4.684s); daemon/providercoord/multiprovider81138 PASS.
Git recovery race count5 also PASS188.017s. Corrected paid concurrency rerun
launched for causal confirmation; prior restart failure is not yet declared fixed.

Corrected live concurrency69217 FAILED365.56s during daemon restart:
`recover stranded git mutations: sqlite write deadline exceeded: context deadline exceeded`.
Prior bounded diagnostics included verification CHECKPOINT=commit failures.
The identity assertion is no longer the blocker. No further paid run launched;
startup recovery must be investigated locally. This is not a passing
concurrency/restart result and no cause is yet established.

Concurrent2248 FAIL336.65s at a test-only hardcoded Claude/Codex expectation
after observing the correct Cursor Luna identity. The helper now receives the
selected role pair; exact model/family checks remain. Nonpaid regression15497
failed red for Cursor,26550 passed. Compiled nonpaid concurrency82480
PASS160.88s including restart, separate approvals and base refresh. Corrected
paid concurrency rerun launched next; still no passing Cursor concurrency claim.

Reverse live80038 PASS308.16s (package308.684s), Claude Builder/Cursor
Reviewer. Both Cursor/Claude directions now have disposable full-ticket
acceptance. Cursor two-ticket restart/separate-approval acceptance launched
next with a 25-minute bound; no hosted Relay mutation.

Post-repair Store/workflowruntime/workflowworker35189 PASS
(129.582s/113.814s/1.527s). Repo-check and secret-scan35781 PASS.
Reverse live pairing rerun80038 is active with a 15-minute test bound;
disposable local Git/FakeGH, no hosted Relay mutation. Older active/pending
statements below are historical and superseded by this checkpoint.

Newest: Cursor Sonnet Low signed native qualification63082 PASS61.67s after
measuring catalog1M Low versus session300K Low No Thinking (probes52675/65781).
Exact mapping is fixture-bound; cursorprovider race89083 PASS. No 1M/thinking
claim. Grok remains unqualified. Full98977 PASS before repair change below.

Deterministic compiled final-review verification repair77973 reproduced commit
failure after genuine failing proof. Materializer incorrectly used original
base instead of reviewed candidate. Store now authenticates that candidate at
the exact repair endpoint; Git preserves inherited implementation paths.
Store53392 and68209 PASS; compiled1793 PASS75.89s through fresh verification to
Building, no ready/merge mutation. Broader regressions and paid reverse rerun
remain pending. This new test is not full repair delivery/concurrency evidence.

Latest reverse pairing66737 FAILED756.13s: all initial phases completed,
then independent review requested repair; the second Cursor verification wrote
an invalid Go test (duplicate package declaration), leaving verifying v8.
Local fixture draft1, ready0, merge0. No approval or merge bypass occurred.
Checkpoint-stage diagnostic83445 passes; exact persistence rejection remains
to reproduce. Cursor concurrency is held, not passed. Grok low74121 refused
qualification (terminal_artifact); Sonnet low27813 refused (session model
identity differs from catalog). No aliases guessed or requirements weakened.
Backoff elapsed-deadline fix passed deterministic red/green, count5 budget/
checkout cases40638 and rejection race32893. Full post-fix baseline pending.

**First Cursor full ticket PASS:** compiled77965 Cursor Builder / Claude
Reviewer passed328.87s (package329.480s) after the explicit Planner proof-kind
binding. Real models and compiled SF/Store; disposable local Git/FakeGH only.
Reverse pairing66737 is active. Prompt/artifact race13336, vet, repo-check and
secret-scan37181 passed. Cursor-specific concurrent restart/two-approval test
is added but not yet run. Goal remains incomplete pending remaining acceptance.

Latest Sept7: full non-paid baseline16725 passed. Compiled98403 failed after
Cursor planning completed: Claude verification returned result_indeterminate.
Compiled95843 passed qualification then failed two bounded Planner attempts
with the closed proof_kind validation diagnostic. Prompt omitted the required
ticket-type mapping while schema offered all six. Controller-derived Planner
OUTPUT_BINDING now uses the validator's canonical mapping; regression failed
red then workflowprompt/phaseartifact49482 passed. Validator remains strict.
Compiled77965 is active against this fix. Neither previous compiled run is a
passing delivery; Claude parser-stage diagnosis remains pending reproduction.
Known Cursor model-capable CLI launches before77965:54 plus up to5 unverified;
actual API calls/dollar charges unknown. No hosted/live daemon mutation.

Sept7 latest: native Cursor Luna Low qualification73465 PASS59.82s
(package60.288s), signed by the Supervisor after Builder/outside-read,
read-only Reviewer, owned worker cleanup and launch-cancellation drain.
The exact session uses 272K context despite the catalog's 1M label; an explicit
pinned mapping binds that fact. The stream uses ReadToolError.errorMessage,
not ReadError.error; a red/green regression proves the repair. A preceding
file-inventory failure did not recur and remains a reliability observation.
Mixed-provider qualifier/composition and daemon routing now include Cursor
only through current signed qualification; cross-provider same-family pairs
refuse before paid work. Focused route tests5987 pass; qualification/observer
race12708 passes. Full host baseline37400 PASS. Cursor model-picker/preset
integration is implemented; focused62567 PASS. Native after-write cancellation
58825 PASS28.95s with signed drain and byte-preserved marker. Its first two
fixtures failed before any tool events; canonicalizing the macOS worktree path
as production does resolved startup. Same-role automatic API retry,
full-ticket and concurrency acceptance remain incomplete. No hosted/live
channel mutation or full-goal completion is claimed.

Compiled Cursor Builder/Claude Reviewer90145 failed620.76s: both qualifiers
passed, worktree registered, but zero provider attempts/PR mutations. Source
root was workflowruntime.configuredProvider's obsolete codex/claude-only gate.
Regression39103 failed red; cursor route fix75231 passed; full workflowruntime
64018 PASS114.924s. Corrected compiled16903 failed40.34s at qualification:
Builder did not create required result.txt, so admission was correctly refused.
One unchanged bounded repeat10478 failed37.09s with the same missing write.
Counts-only write diagnostics98353 and race18228 PASS. Standalone native
qualification57426 PASS57.65s unchanged; compiled diagnostic98403 is active.
Pinned CLI normalizes write paths before permission checks. Omitted versus
refused writes is not yet distinguished. Inventory variability remains
a reliability concern; no passing full Cursor ticket yet. This is not an
excuse to bypass current signed qualification, which remains mandatory.

The preceding continuation added at most thirteen model-capable Cursor invocations
(including cancelled startup) to the earlier29 known plus up tofive unverified
attempts. These are CLI attempts, not internal API counts. Actual dollar
charges remain unknown; the historical $0 statements below predate these runs.
This continuation adds three native qualification launches plus the active
compiled run; count the latter from terminal evidence.

Latest Sept7 decision supersedes the hook blocker: the user assumes existing
Cursor hooks do not interfere. Implement a distinctly identified trusted-hooks
policy, without claiming isolation or weakening the remaining role/drain/Store
gates. First bounded native Cursor browser-auth stdin/framing probe98677
PASS19.223s (one CLI launch, exact requested Luna ID, disposable environment,
unknown dollar cost). This is not native role qualification. New exec-free
invocation/permissions proposal tests78154 PASS0.554s; Supervisor admission and
composition are not yet wired. Full baseline below predates these additions.

Sept7 resumption: additional Cursor test authorization is $100; spend against
this new allowance is currently $0. Cursor isolation remains an implementation
gate, not an authentication failure. The combined native Claude forced-server-
rejection test below is a coverage gap owned by engineering, not a user login
or API-key prerequisite. It must not block the independently passing Claude/
Codex slice solely because a production server failure cannot be induced.
The original plan's fresh native qualification and live delivery requirements
have passing evidence below. Full Cursor delivery remains incomplete.

Latest test-only retry increment: Store/coordinator now has joined real-Git
registration and physical inspection coverage. Clean retry completes/replays
without an extra launch; tracked, untracked and ignored writes after receipt
refuse the second launch and preserve changes. Targeted race94831 PASS58.638s,
including original synthetic budget/recheck tests. Full providercoord40.761s,
vet, repo-check, secret-scan (644 commits + working tree, no leaks), diff-check
12026 passed. Provider process and receipt signer remain fixture-owned;
this does not close native Supervisor.Run capture/receipt acceptance. No
credentials, endpoint policy, production behavior or paid calls changed.
Post-addition full baseline23540 PASS, exit0 (`go test -p 1 ./...`, paid/live
opt-ins disabled, some unchanged packages cached): cmd/sf90.214s,
Git183.626s, supervisor65.448s, providercoord37.529s, Store127.310s,
workflowruntime111.178s, worktreecoord131.928s; other packages passed.
repo-check/secret-scan40522 and diff-check passed, no leaks. The prior baseline
is retained as historical evidence, not substituted for the new test additions.

Fresh renewed-login qualification29331 PASS (22.639s). Investigation reproduced
native stream variants missing from the first parser: bounded `thinking_tokens`,
pending `permission_denied` events, subscription `rate_limit_event` metadata, and
exact disabled-tool results for unavailable read-only-role Write/Edit attempts.
The parser accepts only validated metadata and exact paired unavailable-tool
responses, never an ordinary failed or successful forbidden write. Thinking and
quota metadata are not billing or retry evidence. Runtime tools, independent
physical inventory checks and cancellation requirements are unchanged. The
policy now binds `complete-stream-json-v2`; older qualifications cannot authorize
it. Temporary fingerprint/lookahead diagnostics are removed; fixed stage/index
errors remain. Regression tests reproduced each parser gap before its fix.
Full Claude adapter tests pass; race16087 passed for Claude/providerjson/
supervisor (supervisor79.418s, exit0) with only local synthetic protocol opt-ins.
Disposable real-model Claude/Codex ticket59836 PASS (213.094s): compiled SF,
real providers, local Git/GitHub fixtures, guarded delivery. This is not a hosted
Relay delivery. Focused vet, repo-check and secret-scan89527 PASS (644 commits
plus working tree, no leaks). Full baseline36092 below predates this fix and
needs refreshing. Reverse Codex/Claude45777 PASS (231.612s), also disposable
real models with local publication. Both role directions now pass current
policy. Full vet73699 PASS. Restart/concurrency15849 FAIL (86.871s) before
publication: Planner exhausted two schema-invalid artifacts; clean checkout,
zero PR/ready/merge mutations. Root cause is not yet established. Added bounded
sf_e2e-only Planner categories to diagnose without printing provider output;
diagnostic tests61509 PASS0.560s. Instrumented repeat92204 PASS391.931s: two
real-model tickets, restart/requalification, separate approvals, and fresh
sibling build/review after base refresh. The first failure did not reproduce;
this is an unresolved nondeterministic Planner reliability observation, not a
claimed fix or erased failure. No parser/admission limits were relaxed.

Fresh full native baseline49283 PASS, exit0, with all paid/live opt-ins off:
cmd/sf102.714s, Git275.181s, GitHub86.995s, supervisor82.616s,
providercoord14.927s, publication96.612s, Store130.428s,
workflowruntime113.608s, worktreecoord136.795s; all other packages passed.
Full vet79999, repo-check, diff-check and secret-scan58422 PASS (644 commits
plus working tree, no leaks). No active handles, live channel changes, Cursor
paid calls, commits, or remote mutations. Combined native rejection/retry
acceptance and Cursor execution remain open; this is not full-goal completion.

### Earlier baseline (before native framing repair)

Claude invocation now requests complete `stream-json` with `--verbose` under
policy v4 and fixture v3. The bounded success parser validates one session and
model, message identities, role-allowed tool/result pairs, internal retry
notices and a final success; only its final envelope enters the existing
artifact/accounting parser. Missing/truncated/foreign/partial streams cannot
become artifact repair. Qualification checks canary disclosure across the
entire stream before extracting the terminal envelope. Existing qualification
digests cannot authorize this changed invocation; Codex policy is unchanged.

Installed Claude 2.1.263 passed a localhost synthetic API success and a
503→internal retry→success probe. Full Claude/providerjson race15972 PASS
(6.012s/1.293s), including native local success/rejection and forbidden-endpoint
checks. These probes use synthetic API auth/private homes, not subscription
qualification. Targeted parser/qualification race39582, full vet70077,
repo-check and secret-scan6322 PASS (644 commits and working tree, no leaks).
Full repository native test36092 PASS, exit0, with live/paid opt-ins off:
`go test -p 1 -count=1 ./...`; Store130.050s, supervisor66.747s,
workflowruntime112.092s and worktreecoord135.098s. All test handles are terminal.
Fresh status-only Keychain check found expired Claude OAuth; login renewal was
requested. Fresh policy-v4 subscription qualification/delivery remains pending.
No paid request or live factory/project mutation occurred in this checkpoint.

Acceptance gates recorded at that earlier baseline (current progress above supersedes):

1. Repository baseline is complete. Preserve that evidence; do not relabel
   opt-in skips as passing live tests or launch a duplicate unchanged baseline.
2. Renewed Claude login must pass the credential-lifetime check, then the
   installed CLI must earn a fresh policy-v4 signed qualification. A new login
   or the earlier policy-v3 delivery does not satisfy this gate.
3. Repeat disposable native delivery in both Claude/Codex role directions and
   the concurrency/restart path under that new qualification. Do not touch the
   live Relay channel or call local publication fixtures hosted delivery.
4. Close the combined native retry evidence gap: current coverage composes real
   Store/Coordinator with synthetic process/filesystem observations, separately
   tests the native CLI protocol, and separately tests physical checkpoints.
   It does not yet prove one end-to-end native supervised failed invocation,
   physical reinspection, signed receipt, durable retry and successful result.
5. Cursor remains unavailable until its documented execution-isolation and
   remaining-spend gates have evidence. Do not repeat paid probes or switch
   models/billing to make the matrix appear complete.

That earlier read-only host credential recheck reported expired Claude OAuth.
Only fixed status booleans were emitted; no token or raw provider output was
printed. The renewed-login qualification reported above resolves that auth block.

### Retry orchestration checkpoint

Coordinator now consumes optional supervisor-signed server-rejection/drain
proofs, records unknown cost separately, atomically finishes via Store, waits
only on the authenticated persisted deadline, and retries the same binding.
Pending rejection lookup pins that binding before provider probing, including
restart. The new active attempt requires another Store-derived physical
checkpoint inspection before Run, with a repeated authority check; missing,
changed or revoked inspection refuses before launch and cannot use fallback.
Ordinary errors/cancellation remain on conservative drain/indeterminate paths.
Unavailable receipt is not guessed; an invalid returned receipt quarantines.

Store restart/exact-finish replay tests pass (normal49910 1.162s, race22680
16.120s). Full providercoord/Store20360 PASS (6.094s/130.765s). Combined real
Store + Coordinator with explicitly synthetic process/filesystem observations
covers server-error→success, two-server-error exhaustion, changed checkout
refusing a second launch, and replay with no extra launch: targeted28027 PASS
9.164s, targeted race45165 providercoord20.163s/Store16.439s. Those fixtures
are not native filesystem or permission qualification. Full providercoord
race47126 passes (152.953s), including final cancellation/missing-route
hardening. Focused vet, repo-check and diff-check pass; all handles terminal.
Qualified streaming policy/parser, native combined retry acceptance and a new
full-repository baseline remain pending. No paid/live/remote state changed.

## Earlier implementation checkpoints (chronological claims may be superseded)

Atomic signed rejection finalization and receipt-aware admission now exist in
Store. The dedicated `server_rejected` outcome cannot be minted by generic
Finish; checkpoint/signature validation, receipt insertion, failed attempt/phase
and exact lease release share one transaction. Admission authenticates missing/
altered receipt failures, same binding/input and stored deadline under the
existing shared attempt count. Both mixed API/artifact orders consume the same
window; exhaustion authenticates rejection evidence. Tests include a forced
late phase-write failure proving rollback and missing-receipt refusal.
Focused race26922 PASS (15.744s); full Store55745 PASS (130.572s), including
reverse mixed-order exhaustion. Store vet, repo-check and diff-check PASS.
Coordinator use, restart/finish replay, physical retry reinspection, qualified
streaming policy and live acceptance remain pending; automatic production
retry is not enabled. No full-repository post-change result is claimed.

Receipt encoding/backoff validation is implemented but not exposed as a writer.
It authenticates the exact signed claim, canonical bytes, digest and duplicated
timestamps. A deterministic 2.0–2.9-second delay derives from signed observation
time, never from replay/load time; CLI internal delay is not treated as a final
Retry-After header. Store and the physical inspector now share the unchanged
versioned checkpoint digest encoder. Encoding/checkpoint/schema race80796 PASS
(34.693s); focused Store/worktree vet and diff-check PASS. Atomic failure/receipt
commit, receipt-aware admission/exhaustion and coordinator retry remain pending.

Append-only migration v60 reserves immutable server-rejection receipts and
not-before timestamps with an exact composite attempt foreign key, bounded
canonical JSON/digest and deadline constraints. Schema version/checksum,
backup migration dispatch and required-schema validation are wired; no legacy
failure backfill or write/admission API is enabled. Schema9496 PASS (1.054s);
schema/backup/history/legacy-v1/v10 migration race55993 PASS (26.054s); Store
vet/diff-check PASS. These migrations ran only in disposable databases.
Atomic finish/receipt persistence and matching backoff admission must be
implemented together; durable retry behavior is not yet delivered.

Trusted inspector composition is now wired through localruntime, coordinator
and the supervisor's idle-only configuration method. Store's supervisor-facing
lookup authenticates the full persisted claim against every DrainRequest field;
the worktree inspector repeats that lookup around physical inspection. Store
request-auth race37805 PASS (30.944s). Native configuration/drain/Factory/Git
race8889 PASS (supervisor1.524s, localruntime23.065s, worktreecoord27.150s).
Focused six-package vet and diff-check PASS. The initial broader sandbox regex
54825 passed Factory but hit existing child-identity gating in a Git fixture;
it was not a passing combined run. Receipt persistence/admission, backoff/shared
attempt limits, streaming qualification and live acceptance remain incomplete.

Supervisor-private rejection capture and optional `DrainServerRejection` are
implemented. Public Run error semantics remain unchanged. Receipt issuance
requires complete Run/process/I/O, trusted checkpoint inspection and a final
ownership/control check; ordinary control Drain clears eligibility immediately.
Wrong requests, duplicate consumption, missing/dirty evidence, cancellation and
control-during-inspection refuse. Seeded private-state lifecycle race32927 PASS
(1.536s); full native supervisor suite8088 PASS (68.467s); focused vet/diff-check
PASS. These tests do not qualify streaming execution. Current terminal-JSON
policy, absent inspector composition and unwired Store retry handling still
prevent automatic retries. No paid invocation or live project changed.

Physical checkpoint composition now sandwiches the existing strict registered
Git clean-HEAD inspection between two identical exact-active-attempt Store
lookups. Dirty/ignored files, registration movement, revocation or cancellation
produce no inspection result. Its digest is metadata only, not retry admission.
Sequencing tests use callbacks; Store authority and physical Git are tested
separately, so a combined active-attempt/supervisor end-to-end test is still due.
Sandbox race37694 failed in existing Git fixture process-identity gating before
inspection. Identical native-host race51037 PASS (26.666s), including existing
real-Git dirty/ignored/foreign-head/replacement/cancel coverage. Focused vet and
diff-check PASS. Supervisor receipt and durable retry wiring remain pending.

Store now exposes an exact-active-attempt checkpoint lookup, separate from the
operator-exhaustion retry proof. It rehydrates the full claim and derives the
registered, verification or candidate HEAD through authenticated phase evidence
in one read transaction. Current fences, active phase/attempt, exact provider
lease, phase-entry linkage and registered creation/commit identity are required.
It neither inspects the filesystem nor authorizes retry. Tests cover all four
roles, altered claims, lost lease/leader/phase, registration tampering and
terminal attempts. Expanded focused race32214 PASS (27.925s); focused
Store/contracts vet and diff-check PASS. Physical inspection and the atomic
retry terminal/admission paths still need wiring. No broad post-change baseline
has yet run.

The separate signed rejection contract now exists in
`internal/contracts/provider_rejection.go`. Signing requires the same
supervisor signer's exact drain proof; its versioned signature domain is
distinct from ordinary drain evidence. Tests mutate every claim/evidence field,
substitute keys and signature domains, exercise persistence roundtrip, and
reject malformed or mixed-failure metadata. Focused contracts test92885 PASS
(0.356s); full contracts race3874 PASS (1.422s); diff-check PASS.
This is a cryptographic primitive, not production retry admission: independent
physical checkpoint inspection, supervisor-owned capture, Store persistence,
backoff and the shared attempt budget remain unwired. The full49030 baseline
below predates this addition; no new full-suite result is claimed.

Complete-stream observer implemented in `internal/claudeprovider/rejection.go`.
It accepts only pinned-model init, bounded contiguous retry notices, one typed
API-error assistant event with text-only content, and a final error result.
Session/UUID consistency, duplicate keys, output/event/delay limits, malformed
or trailing events and prior model/tool/hook activity fail closed. It returns
only a closed category, digest, retry counters and whether **all** failures were
server errors; it does not return text/session IDs or grant retry authority.
Mixed quota/auth history cannot be hidden by a final server error.

Unit30856 and initial race6081 PASS. Native32534/96731/26944 exposed a fixture
expectation error: the pinned CLI classifies the synthetic400 as `unknown`, not
`invalid_request`; the observer correctly rejected it. Temporary shape-only
diagnostics were removed. Native46650 PASS3.231s now covers that refusal plus
known server-exhaustion recognition. Final native+unit race63167 PASS
(Claude4.407s, providerjson1.313s). A five-second fuzz smoke32519 passed but
executed only3 mutations; this is not substantial fuzz coverage.

Full49030 PASS after the observer addition (cmd/sf98.885s, GitHub64.453s,
supervisor67.789s, workflowruntime113.920s; remaining packages passed, some cached).
Prior full43603 remains the completed pre-observer baseline. Production invocation is unchanged and
the observer is not used to admit attempts. Required next work is qualified
streaming execution plus signed rejection/worktree evidence and Store-owned
backoff sharing the artifact-repair limit. Login renewal and Cursor isolation
remain unresolved; no paid or live-project mutation occurred.

Live mixed-provider restart attempt36505 failed at initial Claude qualification
(6.286s), before ticket submission. Status-only observation and a boolean-only
Keychain check isolated an expired OAuth credential; installed version remains
2.1.263. No token or account data was exposed. This is not evidence of a restart
regression and not a passing live restart test. No blind paid retry was made.
Credential-renewal guidance now survives the qualification boundary; tests
reproduced the missing guidance (9215) and pass after repair (35271). Native
status-only55826 confirms refusal now says `run claude auth login` without
weakening the46-minute validity requirement. Full non-paid baseline58282 PASS,
vet8778 PASS, repo-check PASS, secret-scan3179 PASS (644 commits and working
tree). Focused synthetic credential/observer race29262 PASS1.861s. The earlier
race result was unavailable after context recovery; no process remained, so
29262 was run to obtain a verifiable result. Live acceptance needs login renewal.

### Non-paid native retry protocol probe

Pinned Claude2.1.263 was run with synthetic API credentials, a disposable HOME,
no tools, bare/restricted/safe modes, and Seatbelt denying network except the
local mock server. No subscription login, actual API, model, or billing was used.
This is a protocol investigation, **not** qualification of SF's subscription
invocation or a production direct-API adapter. Scratch reproducer:
`.context/claude-retry-probe.py` (ignored; do not promote credentials or raw output).

- Mock429 followed by401: six requests and six `system/api_retry` events in
  35seconds, reported maximum10retries; the bounded probe killed its process
  group before a terminal result. The retry delays increased. Do not infer that
  a subscription401 behaves identically or that an auth error permits SF retry.
- Mock429 followed by400: two requests, one retry event, one terminal result,
  exit1, no tool-use messages, no timeout. Both runs stayed within output bounds.
- Initial mock startup refusal was Seatbelt's required `localhost:port` syntax,
  corrected without enabling external network. No production policy changed.

The installed CLI does expose documented internal retry progress, but this does
not prove another SF launch is safe. A whole transcript, authenticated runtime,
drain and physical worktree evidence would still be needed, with the existing
shared attempt budget and durable backoff. Production output remains terminal
JSON; no new automatic SF retry or billing fallback is enabled. In particular,
the sixteen-SF-launch cap must not be presented as sixteen underlying API calls.

The probe is now reproducible as the opt-in Darwin test
`SF_TEST_CLAUDE_LOCAL_RETRY=1 go test ./internal/claudeprovider -run '^TestInstalledClaudeLocalRetryProtocol$'`.
It verifies that an unlisted loopback endpoint is denied before launching the
CLI, pins the installed version, targets only `/v1/messages` with the transient
response, bounds request/output/process lifetime, validates session/model/retry
framing and terminal ordering, and asserts SF classifies the failure indeterminate.
It does not read real credentials or run an actual model. The initial test
failed because the first HTTP request was not necessarily a messages request;
endpoint-specific injection fixed the fixture (55285 PASS1.996s). This reinforces
why total HTTP counts are not model-request or billable-call counts.

Remaining retry implementation gates are explicit: subscription-mode stream
qualification (the current policy uses terminal JSON); authenticated complete
pre-execution rejection evidence rather than message-count heuristics; fresh
physical worktree reconciliation; and a Store-owned backoff/attempt transition
sharing the artifact-repair limit and surviving restart. The local fixture is
evidence for that work, not a replacement for it or a claim of automatic retry.

Further primary-source evidence identifies the next bounded probe rather than
an execution-policy decision: the [SDK message reference](https://code.claude.com/docs/en/agent-sdk/typescript)
defines `assistant.error` as a closed API error category. The
[official changelog](https://raw.githubusercontent.com/anthropics/claude-code/main/CHANGELOG.md)
documents `CLAUDE_CODE_MAX_RETRIES` and separately the watchdog override. Test
an exhausted all-429 mock with a small explicit retry cap, inspect only the
typed category and complete event ordering, and keep watchdog disabled. Neither
the flag's exact installed semantics nor safe SF relaunch is proven by docs;
this experiment must follow the current serialized full-suite run. Do not parse
human-readable error text or equate a final rate-limit category with absence of
earlier tool effects.

That bounded experiment is now complete on pinned2.1.263: all429 (40639) and
all503 (98482), with `CLAUDE_CODE_MAX_RETRIES=1`, each made two messages requests,
emitted one retry event reporting max1/delay1000, and exited1 with one complete
error result. The single assistant event carried typed `error=rate_limit` or
`error=server_error` respectively. Model/session matched; no tool-use messages,
truncation or timeout. No real account/API/model usage. This verifies the mock
protocol and retry-cap behavior, not absence of earlier effects in an arbitrary
run. Next implementation evidence can use a strict complete-stream validator;
Store/supervisor authentication and physical worktree proof remain mandatory.

Latest baseline after the optional native regression: full43603 PASS, including
Store126.727s, workflowruntime113.214s and worktreecoord135.839s; fresh vet PASS;
repo-check and secret-scan63423 PASS. Native local regression race15519 passed
three runs. No process remains active. All changes remain uncommitted.

The restart regression exposed two history gaps: CI/review before a base
refresh, and a pending CI poll before the first daemon restart. The latter
explains why the initial Store reproduction passed while the compiled test
still failed. A matching Store fixture reproduced it (34254, FAIL0.855s).
The repair now anchors the original publication lifecycle and authenticates
the exact recovery row within the complete historical CI/review chain.
Focused36061 PASS2.907s covers no pending poll, pending after restart, pending
before restart, six evidence-tamper cases, and wrong runner refusal.
Temporary diagnostic instrumentation is removed. Fixture-only compiled80111
PASS162.672s: both concurrent tickets delivered across restart, requalification,
sibling base refresh, fresh build/review, and separate approvals with exact
history/PR/head assertions. This uses FakeCodex/FakeGH/local bare—not hosted
GitHub or paid models. Fresh full baseline10444 PASS (Store127.161s,
workflowruntime113.632s, worktreecoord134.856s), vet76172 PASS, repo-check PASS,
and secret-scan61161 PASS644commits plus working tree. Focused refresh/recovery
race36110 PASS123.115s. No test job remains active. Changes are uncommitted.
No additional Cursor or paid model calls were made for this investigation.

## Historical checkpoints (not active handles)

Retry investigation update: official [headless CLI documentation](https://code.claude.com/docs/en/headless#handle-api-retries)
describes `system/api_retry` events with status/category/delay and attempt bounds.
These report CLI-internal retries, not a pre-execution receipt for another SF
launch. Next safe evidence step: pinned installed CLI against a local mock
transport, synthetic credentials, private HOME, no real API/billing or Store
authority; verify complete stream framing and prior tool-use/error semantics.
No retry implementation or policy change follows merely from this documentation.

Store root cause confirmed by84834/93831: the live fence passes, but the
generic prefix cannot cross authenticated green-CI/review-pass before refresh.
Repair adds a dedicated bounded historical publication/CI/reviewer bridge,
leaving generic ledger rules unchanged. Direct current-result/materializer
proofs now pass; five tamper cases and wrong source runner fail closed44808
(1.965s). The proof remains valid after the newer generation has CI history.
Original compiled restart reproduction20385 is running; no paid call involved.

Diagnostic rerun83551 FAILED249.829s with the same deterministic restart +
sibling-refresh stall. Closed worker diagnostics include stale evidence after
restart. Temporary instrumentation was removed; no production fix applied.
Next is a direct Store regression isolating the recovered CI/review-to-refresh
authority bridge, not another paid-model run. No test currently running.

New restart matrix74089 FAILED248.757s: restart/requalification preserved both
waiting-CI tickets, exact runner/version advances and immutable provider/PR
history; first ticket delivered. After sibling base refresh, Builder2 completed
but candidate materialization/transition stalled at building v10/r2. Checkout
was clean and local tests passed. No duplicate PR/ready/merge appeared. Root
boundary is under investigation in fixture-only83551 with closed diagnostics;
no production repair or passing restart-delivery claim yet.

Final-policy full non-paid baseline69479 PASSED, including Store124.847s,
workflowruntime112.096s and worktreecoord134.908s. Vet25154, repo-check and
secret-scan60039 also passed. Subsequent changes are tagged test scaffolding:
compiled fixture-only restart74089 is active. It stops the private daemon at
two waiting-CI tickets, restarts/requalifies the same pair, checks exact fence
advance and unchanged completed attempts/PRs, then continues separate approvals.
This is graceful daemon restart, not an in-flight SIGKILL or paid live trial.

Final-policy live70537 PASSED368.284s. Claude-only invocation policy v3,
unchanged shared Codex prompt, and current exact binding/history assertions
delivered both tickets with separate approvals, fresh sibling build/review on
the merged base, same two PRs and exact final protected head. All initial
attempts and fresh Builder attempt2 completed without repair. This replaces
98997 as the current-source live verdict; GitHub/approval remain simulated.
Final non-paid source validation is now underway. Cursor and transient API
retry availability are unchanged.

Latest native lifecycle evidence: after-write Claude cancellation81911 passed
in9.292 seconds, with a real Sonnet5 file effect, bounded cancellation, signed
drain, and exact retained bytes. This uses a fixture recorder, not full Store
restart recovery. Retained-pipe/private-home cleanup and prior-policy refusal
also passed scoped race37943 (supervisor3.284s; Claude1.472s). The final v3
two-delivery trial has been launched; no new Cursor request was made.

Compatibility correction after baseline70197 PASS: the inventory clarification
is now Claude-only invocation policy v3, not a shared workflow prompt change.
Original shared prompt bytes and Codex policy are restored; persisted inputs
are unchanged. Claude guidance applies only to the sf.builder/v1 build schema;
permission/cancellation qualification fixtures and other roles are untouched.
Old Claude v2 registrations refuse and require fresh qualification. Scoped
tests25393 passed; race37943 is pending. Live98997 proved the earlier placement,
so an exact v3 live rerun remains required before carrying that verdict forward.

Post-fix baseline70197 is active, including Git188.799s/GitHub64.619s passed;
not yet an overall PASS. Repo-check and secret-scan79604 passed. A new
retained-pipe regression assertion checks real Supervisor.Run private-home and
copied fixture-credential retention/cleanup alongside staged executable lifetime.
It uses a native synthetic Codex process, not a paid Claude or Cursor run;
explicit scoped execution is pending after the baseline clears.

Latest diagnostic trial24517 failed in336 seconds after first delivery. Both
fresh sibling Builder artifacts declared protected verification paths without
an amendment (closed test-only category); checkout was clean and tests passed.
This identifies the artifact rejection, not an actual protected-file mutation.
Builder prompt clarification now distinguishes implementation inventory from
the whole branch diff and explains preserved files after base refresh. The
prompt regression failed before the change; full workflowprompt2571 (1.420s)
and phaseartifact2571 (0.274s) passed afterward. The ordinary-prompt golden
fixture was updated for this intentional instruction change; nil CI repair
still adds no repair context. Persisted historical payloads are not rewritten.
No Store/artifact permission checks were relaxed. Original live rerun98997
then PASSED in401.319 seconds: both Done, fresh sibling Builder/review on the
new base, separate approvals, same two PRs, exact final protected head. The
fresh Builder completed on attempt2 without repair. Initial Planners each used
one permitted repair. This is real Claude/Codex with simulated GitHub, not
hosted delivery. Full post-prompt regression validation remains pending.

## Requirement evidence

| Requirement | Current evidence | Disposition |
| --- | --- | --- |
| Retain Codex behavior and historical authority | Earlier full baselines passed. The Planner prompt explicitly binds the validator-derived proof kind; historical input encoding is unchanged. Post-repair Store/workflowruntime/workflowworker35189 and full baseline86750 passed. | Current nonpaid baseline proven; live opt-ins are separate |
| Claude Builder/Planner and Codex Reviewer | Current streaming-policy trial59836 passed213.094s through guarded Done, following fresh native qualification29331. | Proven against local fake GitHub, not hosted delivery |
| Codex Builder/Planner and Claude Reviewer | Current streaming-policy reverse trial45777 passed231.612s through Done with exact phase provider/model/family assertions. | Proven against local fake GitHub |
| Cursor role execution in both directions | Native Luna Low and Sonnet Low qualifications passed. Cursor Builder/Claude Reviewer77965 passed328.87s; Claude Builder/Cursor Reviewer80038 passed308.16s. Exact model/family assertions; disposable compiled SF/Store and local GitHub. | Both single-ticket directions proven; not hosted delivery. Grok Low remains unqualified |
| Exact provider/model/auth/policy and independent review | Store attested qualification, current signature checks, exact composition and frozen role preferences; native trials assert identities. | Implemented and tested; not proof of untrusted-repository containment |
| Explicit billing mode and durable limits | V59 estimate consent/observations; signed-drain estimate recording, unknown distinct from zero, 16 SF launches and 45-minute invocation limit. | Implemented; estimates never guarantee actual charges or internal API-call count |
| Bounded same-role artifact repair | Estimated Claude fixture repairs once, exhausts at two, preserves exact binding and outcomes after reopen/fresh coordinator; race 83033 passed. | Proven for artifact repair |
| Safe transient API retry with durable delay and no fallback | V60 signed receipt, atomic Store completion, persisted backoff and shared attempt budget; layered Coordinator tests pass. Joined native-process test63066 passes receipt→Store reopen→delay→real Git inspection→same-binding second process→persisted rejection, race count3. | Joined synthetic-CLI/native-process coverage proven; installed-vendor forced rejection is separate protocol evidence, not a hosted outage test. Ordinary ambiguous errors remain indeterminate |
| Partial-write/uncertain failure safety | Estimated fixture retains partial writes, refuses relaunch after reopen; supervisor drain remains mandatory. | Proven at protocol/Store boundary, not all native crash windows |
| Shared-account capacity across profiles | Signed two-profile Store test: third refuses without consuming attempt; reopen preserves capacity; cancel frees only exact slot. Race 83033 passed. | Proven Store boundary; not live runtime profile swapping |
| Two live tickets | Trial 6701 passed: Claude/Codex pipelines overlap; independent worktrees and draft PRs; exact roles; no active leases. | Proven to waiting CI with fake GitHub |
| Concurrent live delivery through CI and approval | Final Claude-only v3 policy trial70537 passed368.284s: both Done, fresh sibling Builder/review on the new base, separate approvals, exact history/PR/mutation/head. Shared Codex prompt and policy unchanged. | Live two-delivery and approval isolation proven with simulated GitHub; no hosted-delivery claim |
| Restart before separate concurrent approvals | Current-policy real Claude/Codex trial92204 passed391.931s: both tickets Done after restart/requalification, fresh sibling build/review and separate approvals. Earlier15849 paused before publication after two Planner schema failures; not reproduced and not claimed fixed. | Real-model/native daemon/Store composition proven with local GitHub; nondeterministic Planner reliability caveat retained |
| Cursor concurrent restart and separate approvals | Corrected original live28572 passed752.36s: both Done, restart/requalification, separate approvals and fresh sibling build/review after protected-base refresh. Earlier55528 assertion failure repaired with red80021/green65374/race87126. | Proven with real models/native SF and disposable local Git/GitHub; not hosted delivery |
| Friendly new-project setup and qualification | `init --providers select` and `providers qualify --preset select`; explicit flags/JSON; no request on cancel; no config overwrite. `run --accept-cost-estimates` forwards consent only to queued start. Full CLI 11119 and Run race 34824 passed. | Implemented initial-pair and one-command run workflow |
| Friendly existing-project edits and exact model selection | `config providers --project p --preset select` edits with backup and separate apply; full config/CLI 70559 passed. Exact model flags preserve retry guidance and reconstruct selected models. `--models select` offers both numbered models and filters same-family review; cancellation sends no request. Picker normal 36779/race 66562 passed; native exact-flag 58549 passed. | Preset/model selection implemented and validated |
| Diagnostics distinguish login from qualification | Auth/doctor show installed/authenticated/qualified facts and model/family; Cursor refusal gives capability reason; error guidance retains selected pair. | Implemented; further end-user acceptance pending |
| Permission and lifecycle native gates | Claude qualification and native role/read-only/cancel fixtures exist; current permission verdict is for pinned native version. Generic cleanup tests require Run and Wait completion. | Existing native evidence; exhaustive multi-CLI daemon-crash/retained-pipe matrix not yet proven |
| Repository checks | Full vet exit0, repo-check and secret-scan84871 passed; diff-check clean. Full Go baseline86750 passed with paid/live opt-ins disabled. | Current-tree baseline passed; opt-in live gates remain separate |

## Next implementation priorities

2026-09-07 supplemental concurrency evidence: compiled regression 1170 passed
in 127 seconds with fake providers and local GitHub. After the first approved
merge, the sibling's effective worktree adopted that exact protected base with
advanced authority and a changed head, without a second ready/merge mutation.
This proves the refresh boundary, not a fresh second build/review/delivery.

Extended compiled fake-provider trial 86446 failed after 255 seconds: sibling
fresh Builder completed, tests passed and a new candidate was recorded, but it
remained publishing at version 10. No artifact failures, duplicate PR or second
merge occurred. Second-generation publication versus fake GitHub observation
needs diagnosis; this is not yet attributed to production or the fixture.

Resolved by diagnostic 84541: repeated refusal at draft reconciliation. The
fake's existing PR retained its original source/base after Git push. An explicit
test-only synchronization reads actual private bare refs without changing PR
text, approval, or mutation history; ordinary stale-snapshot fixtures retain
their old behavior. Original compiled test 42045 then passed in 168 seconds:
both tickets Done with separate approvals, same two PRs, two ready/two merge
actions, exact final protected head. Unit 18000 and race 38099 passed. Production
publication code is unchanged and temporary diagnostics are removed. This is
fake-provider/fake-GitHub delivery evidence, not live-model or hosted acceptance.

The live two-delivery entrypoint 48636 failed after delivering only the first
ticket; fresh sibling Builder attempts 2/3 failed schema validation. Its strengthened
history assertions require immutable earlier rows, fresh Builder/review on the
new base, unchanged runtime bindings and durable bounded attempt sequences.
Pure assertion regression 41503 and tagged race 16461 passed. Full baseline
76915 finished before live acceptance was launched, avoiding native contention.

Baseline 76915 subsequently finished PASS. Latest one-command onboarding fix
adds explicit `run --accept-cost-estimates` forwarding only for the exact queued
start; regression first reproduced the unknown flag, then Run race 34824 and
full CLI 11119 passed. CLI vet, repo-check and secret-scan 92897 passed. This
does not change Store accounting policy or default consent. The next bounded
live two-delivery trial failed as recorded above. Test-only closed validation
categories and prompt pause detection are being added before any diagnostic rerun;
these are observability changes, not a claim to fix the Builder failure.

1. Preserve the passing regression evidence and validate each remaining change
   in proportion to its scope; do not duplicate active test runs.
2. Preserve the passing native exact-model and two-delivery approval acceptance;
   catalog selection never substitutes for qualification. Validate the restart
   repair with the full baseline and focused race tests before more paid trials.
3. Establish an authenticated pre-execution transient-rejection signal before
   implementing paid automatic API retries. If the CLI cannot supply one,
   report that specific capability as unavailable rather than infer safety.
4. Run the missing native restart/cancellation and end-to-end acceptance paths
   against disposable state. Existing test coverage is not a hosted-delivery
   claim. Do not mutate the live Relay channel to satisfy a test.
5. Reopen Cursor only on new isolation evidence described in its gate document;
   repeating catalog/login probes does not advance role qualification.

All implementation changes remain uncommitted at this checkpoint. Session
identifiers refer to recorded local test runs, not durable application authority.
Tests with fake providers or fake GitHub are labeled as such; they must not be
presented as real vendor/hosted evidence.
