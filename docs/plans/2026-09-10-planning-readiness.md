# Actionable planning readiness

Status: approved implementation; independently corroborated; verification pending.

## Goal

A ticket must not silently remain in planning: report a bounded, specific
admission cause and an actionable prerequisite, without inventing lifecycle
authority or exposing credentials. Doctor must distinguish historical selection
from current daemon qualification, and credential retrieval from repository
access. Diagnose the affected Mac's exact pre-planner refusal rather than
assuming that missing vendor files explain zero provider attempts.

## Corroboration

Independent admission and readiness reviews confirmed that coordinator refusals
lose their cause through PlannerRunner and scheduler classification, and Doctor
does not check current-leader qualification. Both reviews reject weakening
attestation or copying uncommitted dependencies as a remedy. Missing vendor
closure is a later command-readiness risk, not a proven planning admission cause.

## Ordered implementation

1. Preserve closed code-owned admission reasons from provider coordination and
   Git preflight through scheduler/status. Unknown errors remain generic; never
   print raw errors, provider output, credentials, or command stderr.
2. Check all three selected roles using current Store attestation authority in
   Doctor. Expose unavailable runtime composition and a requalification action
   in daemon/ticket status. A diagnostic remains an observation, not a ticket
   transition or permission to retry an uncertain mutation.
3. Add a bounded read-only repository-base probe using the actual packaged Git
   transport. Do not equate successful helper credential retrieval with access.
   Retain private Git HOME, pinned helpers and explicit credential capabilities.
4. Report execution-base dependency readiness separately from primary-checkout
   preview. No dependency installation, implicit copying, or requirement for a
   worktree config file when configuration is already frozen in SQLite.
5. Keep restart qualification explicit through the existing signed qualification
   path. Automatic re-attestation is gated: no reuse of old signatures/epochs,
   silent paid work, or new authority inferred from historical selection.
6. Diagnose redundant executable-path separators without relaxing ownership,
   symlink, executable, or helper identity authentication. Add sanitized shutdown
   context only where the actual exit cause is available; unexplained exits on
   another Mac remain unverified until evidence is collected.

## Acceptance

- Regression coverage for pre-attempt refusal with zero launches/attempts;
  exact safe reason survives the composed scheduler/status path.
- Unknown/secret-bearing errors cannot enter JSON or human diagnostics.
- Stale planner, builder or reviewer attestation cannot produce Doctor PASS;
  fresh qualification remains the activation path after restart.
- Helper-success/remote-failure, missing base, cancellation and malformed
  response remain distinct; probes perform no fetch, push or ticket mutation.
- Current-fence checks, completed result reuse, uncertain effects and provider
  retry budgets remain unchanged.
- Required Go suite, repository check and secret scan run in GitHub, with
  focused/race coverage for changed packages. Local automated tests are deferred
  under the user's CI-first policy, not represented as passing.
- An instrumented run on the affected Mac is needed before claiming its exact
  worker refusal or unexplained foreground exit resolved.

## Delivery

Branch: `fix/planning-readiness`, based on main `808f8aa` (PR #9).
No schema or state-machine changes planned. No live database, login, daemon,
or execution worktree mutation. On September 10 the user explicitly approved
push, PR creation and merge after GitHub validation, plus balancing the slow
CI lanes. Completion requires verification, not only source changes.

## CI balancing

Hosted baseline run `34416329428` took 48m16s for runtime race, 44m50s
for the other race packages and 26m15s for crash tests. Split the long lanes
using exhaustive, disjoint test/package inventories, with every shard required
by the acceptance gate. Preserve all tests and runtime safety checks. Compare
fresh hosted timings before claiming a speedup; no local tests or builds.
