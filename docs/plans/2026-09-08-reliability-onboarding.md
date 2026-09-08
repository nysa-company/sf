# Reliable ticket recovery and first use

Status: approved outcomes; implementation design and validation in progress.
Baseline: main b79fd7a37a23eb38cd0aa40d08adb9ae091ba35e.

## Goal

An operator can start a bounded ticket, understand a failure, and use a supported
recovery path without database edits, lost work, or help interpreting internal
state. Deliver all four approved recommendations: generated-test recovery,
actionable status, first-use validation, and hosted regression/concurrency tests.

## Acceptance

1. A contradictory independent test has a bounded diagnosis and independent
   amendment path. Original acceptance, commands, protected files and old proof
   remain authoritative until Store accepts the exact reviewed amendment.
   Rejection preserves the old proof. Exhaustion stops with one actionable next
   step. Never assume a failing test is wrong merely because Builder says so.
2. Human and JSON status explain the cause, retained-work disposition, whether a
   writer has been proven drained, and the exact supported next action. Unknown
   evidence says unknown; retained does not mean safe to edit. No shell injection,
   secrets, implicit retry, or guessed approval authority.
3. The first-ticket guide and hosted clean-environment checks cover Go, Node/TS,
   Python and Rails compatibility decisions. Supported paths reach a visible
   ticket; unsupported recipes fail before a paid launch and explain why.
   Target less than ten minutes of operator setup, excluding tool installation,
   login, model runtime and CI waits. Record these clocks separately. A human
   unfamiliar with SF is the preferred usability check; agent-only evidence is
   explicitly labeled and is not a claim of external-user success.
4. All changed behavior has hosted positive, negative and crash/replay tests.
   Then run four bounded Relay tickets at capacity two on one exact final
   candidate: prove overlap, refusal/queue behavior, safe cancellation/retry,
   restart, same-PR correction, protected-base movement, separate exact-head
   approvals and all intended deliveries Done. Report every retry/intervention.
   No duplicate mutations, lost work, manual database repair or guard bypass.

## What already exists

- Store verification amendments authenticate a completed Builder request, prior
  revision, frozen command, correction budget and new independent Reviewer.
- Worker supports accepted/rejected amendments and recovered immutable results.
- Post-build command failure currently becomes `postbuild_command_failed`; a
  completed result cannot simply be replayed as a new diagnosis attempt.
- CLI already renders next-action argv, ticket selectors, setup preview, doctor,
  short IDs and composed run. Reuse these instead of introducing a new wizard.
- Hosted baseline has 15 lanes and a fail-closed required aggregate. Existing
  compiled concurrency fixtures test capacity two and a third refused start.
- Existing local recipes are deliberately restricted. Rails is not locally
  supported; this sprint does not silently expand execution authority.

## Implementation order

1. Trace the exact completed-Builder/postbuild failure boundary. Add a failing
   hosted composition regression for contradictory tests and stale/replayed
   callbacks. Design the smallest Store-owned retry context that can reuse the
   amendment flow; do not relabel a completed provider result or mutate it.
2. Add bounded proof-consistency instructions to verification, Builder and
   amendment Reviewer prompts. Include eval cases for true contradiction versus
   legitimate implementation failure; prompt text alone is not repair authority.
3. Add diagnosis/recovery projections to status from authenticated Store facts,
   then human/JSON golden tests and executable-next-action tests.
4. Audit the clean first-use commands for all four stack categories; improve
   documentation and refusal guidance. Run hosted compiled clean-HOME fixtures.
5. Run hosted focused and full/race/upgrade/security/compiled checks. Independently
   review the exact candidate before its native campaign and protected merge.
6. Execute and record the four-ticket campaign with the hosted artifact. Preserve
   other runtimes and all evidence. No broad local tests/builds.

## Recovery test map

```text
postbuild failure (authenticated command result)
  -> bounded diagnosis, no test-writing authority
     -> implementation defect -> bounded Builder correction -> fresh proof
     -> alleged contradiction -> existing amendment request
        -> fresh independent Reviewer rejects -> original proof remains
        -> fresh independent Reviewer accepts exact change -> new checkpoint
           -> fresh Builder/proof/CI/review/approval
     -> ambiguous / budget exhausted / unsafe writer -> stop + exact next action

Every arrow: current-fence validation, crash-before/after durable commit,
lost-response replay and wrong-ticket/stale/forged evidence negatives.
```

## Constraints and failure modes

- SQLite is the sole authority. Add only the minimal append-only binding needed
  for a new retry attempt; no parallel log-based workflow and no counter guessing.
- Immutable prompt changes affect new attempts, never rewrite stored inputs.
- The first repair boundary is pre-publication only. Any candidate/publication/
  CI history keeps the existing fail-closed disposition until a separately
  authenticated invalidation protocol is implemented. Do not reinterpret old
  `postbuild_command_failed` rows as repair permission.
- Carry the exact terminal command-result key through the failure type. The
  type is only a locator: Store must reload and bind it before any new entry.
  Signal/cancellation results do not become diagnostic retry evidence.
- A fresh Building entry must retire the predecessor from current result reuse,
  not mutate its completed artifact. Reauthenticate retained physical edits,
  including ignored-file refusal, before admitting a new writer. A registered
  worktree or allowed path alone is not provenance or edit permission.
- Real command output is untrusted and bounded/redacted before any diagnostic
  input. No command/URL execution derived from output.
- Four-ticket admission uses supported start/retry semantics, not a new implicit
  auto-start policy. With the one-refresh cap, stagger approvals/admission so the
  test does not silently require unlimited protected-base refreshes.
- Human onboarding availability is unresolved; do not fabricate a participant.
- Hardware/auth-native trials use the operator-approved isolated Mac fallback
  only when hosted execution cannot supply the required native authentication.

## Not in scope

Unrestricted Rails/Node dependency execution, new providers, automatic fallback,
autonomous merge, hosted credential copying, service installation, deletion of
old worktrees, a new dashboard, or a new release/update system. These do not
remove the observed ticket-recovery and first-use friction.

## Engineering review checkpoint

Architecture: reuse amendment and next-action authorities; exact postbuild retry
binding still to design. Code quality: keep projections separate from authority,
avoid duplicating status decisions. Tests: composition and model-eval gaps above
must be covered before claiming recovery. Performance: bounded attempts and
output; no provider calls on status reads and no transactions across execution.
The installed review skill lacks its referenced supplemental sections file;
this document is a working design, not a completed automated-review verdict.

## GSTACK REVIEW REPORT

| Review | Status | Findings |
| --- | --- | --- |
| Engineering scope/source inspection | In progress | Existing amendment reusable; postbuild retry authority missing |
| Independent review | Pending | Required before integration |
| Hosted verification | First slice passed | [34271491625](https://github.com/nysa-company/sf/actions/runs/34271491625), exact 1b1b7e4; prompt/status tests, existing readmission tests, native build only; not full repair acceptance |

VERDICT: implementation design in progress; not cleared for merge.

**UNRESOLVED DECISIONS:**
- Exact Store-bound diagnosis attempt and immutable failure evidence design.
- Human unfamiliar-user availability; agent evidence must remain labeled.

## Execution checkpoint — bounded repair foundation

- Hosted run [34273314750](https://github.com/nysa-company/sf/actions/runs/34273314750)
  passed at b521934: strict failed-command evidence, prompt/status tests,
  first-use matrix (including honest unsupported/unprepared refusals), docs
  checks, and native candidate build. No local tests were run.
- Commit 41d8cf3 adds append-only V61 repair entries, exact consumed failure /
  Builder / verification / budget binding, replay-once tests, a normative
  Building-to-Building diagnosis entry, generic-entry refusal, and bounded
  retained-byte fingerprints. Hosted run
  [34274649199](https://github.com/nysa-company/sf/actions/runs/34274649199)
  passed those foundation tests, onboarding, and exact candidate build.
- Worker/phase admission, historical verification/restart bridges, and physical
  readmission are in progress. This foundation is not yet end-to-end recovery.
- Remaining acceptance: complete hosted composition/negative/restart checks,
  independent exact-head review, then four native Relay tickets at capacity two.
  Neither the four-ticket campaign nor an unfamiliar human setup trial has run.
