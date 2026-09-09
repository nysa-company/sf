# Ticket entry experience

## Goal and acceptance

Deliver three CLI entry improvements for developers with a configured project:
offline drafting, interactive navigation, and read-only GitHub Issue import.
The configured-project scripted draft/validate journey should finish within two
minutes. This is not a claim about human onboarding, installation, qualification,
model latency, or end-to-end delivery time.

1. `ticket new [file]`: optional safe suggested filename; opt-in multiline
   description; concrete acceptance/scope guidance; complete preview; explicit
   save; exclusive private creation. Existing single-line scripted test fixtures
   and explicit-path behavior remain compatible. No daemon/model calls.
2. `home [--project name]`: terminal-only entry menu for creating a draft,
   starting a saved draft, viewing work, and reviewing an approval candidate.
   Reuse existing run, status, and candidate-bound approval commands. Require an
   explicit project where needed, show it before dispatch, and never infer a
   project solely from a directory basename. No new lifecycle authority.
   The start confirmation offers plain `run` (verified costs) or explicit
   `run estimates` (existing estimated-cost consent, never a hard billing cap).
3. `ticket import <https://github.com/owner/repo/issues/number>`: bounded
   authenticated read through the installed `gh` CLI; validate returned URL and
   number; reject PRs, malformed URLs and oversized/control-bearing content.
   Preserve source as quoted reference material, collect operator-owned
   acceptance criteria, preview, and save only. Deterministic source-based
   filename prevents repeated import to the same destination; no tracker
   synchronization, GitHub writes, submission or deadline start.

## Safety and scope

Drafts are local files, never a second application-state database. All starts
still use the daemon. Menus are not approval authority. Budgets remain visible;
estimated provider costs are not hard billing caps. Cancellation/EOF/output
failure/context cancellation must not submit, approve, or overwrite a file.
Noninteractive and JSON callers retain explicit commands; import can expose a
read-only JSON preview, but cannot silently save. Imported issue text is
untrusted data, never executable configuration or permission.

AI drafting, new providers, runtime changes, installers, issue creation/sync,
automatic execution, and live Relay mutations are out of scope.

## Delivery sequence

- Implement drafting and regression tests.
- Implement navigation and command-dispatch regression tests.
- Implement import boundary and fake-transport regression tests.
- Update CLI/tutorial documentation and run static formatting/diff checks.
- Push a short-lived branch and validate on GitHub: focused CLI tests/race,
  repository checks, and full acceptance. No local test/build runs.
- Review the exact diff and record actual results and remaining limitations.

Regression matrix includes existing-file and preview races, duplicate titles,
multiline paste, invalid/control input, cancelled/EOF prompts, JSON/nonterminal
refusals, ambiguous project selection, exact approval confirmation, malformed
issue/PR URLs, response identity mismatch, oversized output, timeout, duplicate
import, and interrupted submission without automatic replay.

Human usability trials remain a follow-up requiring external participants; do
not manufacture timings or claim the scripted journey is a human trial.
