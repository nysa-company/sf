# Multi-CLI completion audit

Status: implementation and validation complete, with the explicit unavailable
capabilities below. This is not a stable-release claim. Evidence refers to the
validated multi-CLI tree. PR #2 is being expanded to consolidate this tree
with the previously committed Codex beta under the user's merge authorization.

## Requirements and evidence

| Requirement | Verified scope and sources |
| --- | --- |
| Preserve Codex | Historical canonical input compatibility remains unchanged; `cli_registration_test.go` checks Codex policy separation. Full baseline86750 passed before the last test-only assertion correction; refresh87903 passed after it. |
| Qualified Claude and Cursor roles | `multi_provider_qualification_test.go` exercises unsigned, changed-auth, changed-mode and old-leader refusal. `multiprovider/local_test.go` verifies current independent qualifications and exact model selection. Live Claude/Codex role directions59836/45777 and Cursor/Claude directions77965/80038 passed with local publication fixtures. |
| Runtime identity and permissions | `cli_snapshot_test.go` verifies staged Claude/Cursor bundles and helper tamper refusal. Native signed qualification tests exercise role permissions and drain; native after-write cancellation81911 (Claude) and58825 (Cursor) passed. Shared supervisor retained-pipe/snapshot lifecycle coverage supplements, rather than replaces, these native probes. |
| Billing and limits | `provider_accounting_test.go` verifies explicit opt-in, immutable unknown/estimated observations, exact signed drain, and refusal at the estimate ceiling. Actual charges are not inferred from estimates. Auth modes cannot switch through qualification replay. |
| Friendly configuration | Provider/model picker tests cover cancellation before requests, noninteractive/JSON refusal of interactive prompts, exact model IDs, paid disclosure and family filtering. `config_providers_test.go` proves backed-up source editing, no implicit application, idempotent replay and explicit next-generation apply. Init tests distinguish configuration from qualification. |
| Durable same-role retry, no fallback | `provider_rejection_test.go` in Store proves atomic receipt/attempt/backoff, rollback and restart budget. Coordinator tests prove success after rejection, second-error exhaustion, changed-checkout refusal and no extra replay launch. Joined native-process/Store63066 passes real Git inspection, reopen and same-binding second process using a synthetic CLI. Installed Claude local protocol probes are separate evidence; no hosted outage is claimed. |
| Capacity and recovery | `estimated_capacity_test.go` covers account capacity across profiles, reopen and cancellation of only the exact slot. Live28572 passed752.36s with two real Cursor/Claude tickets, daemon restart/requalification, separate approvals, fresh sibling build/review after the first merge, and both Done. Git/GitHub/approvals are disposable fixtures. |
| Baseline and focused gates | Latest focused test-only fix: red80021, green65374, race87126, exact Store/Coordinator57908. Full vet passes; repo/secret83523 pass. Full multiCLI87903 passed exit0, paid opt-ins off: cmd/sf106.491s, daemon22.745s, GitHub64.534s, supervisor74.006s, workflowruntime113.036s; other packages passed, some cached. Isolated beta91548 also passes but is not substituted for multiCLI validation. |

## Explicit unavailable capabilities and limits

- Cursor automatic API-error retry is unavailable in pinned CLI
  `2026.09.02-c22c1a3`: its print-mode failure is an error string/exit, not the
  complete rejection evidence needed to prove another launch safe. See
  `docs/cli.md` provider retry contract. Artifact repair and eligible operator
  retry remain separate. No text-only `503` inference is allowed.
- Grok Low has not passed native qualification. Catalog presence does not
  authorize execution; no model alias is substituted to bypass that result.
- Cursor uses the user-approved trusted-hooks boundary, not a claim of
  arbitrary hostile local configuration or same-UID containment.
- Browser authentication and estimated/unknown accounting are explicit. SF
  launch/time bounds are not a guarantee of actual vendor charges or the
  vendor CLI's internal HTTP request count. No silent API billing fallback.
- Acceptance uses the supported macOS host. It does not establish Linux
  native sandbox support, every vendor-version permutation, or hosted Relay
  delivery with the new providers.

## Delivery boundary

The full local validation gate is complete. The user authorized consolidating
this change set and Codex beta6f12229 through PR #2 into main. GitHub requires
an approving review; publication of a branch is not proof of a completed merge.
No live daemon, stable installation or hosted Relay project was replaced during
this closeout. Consult the PR's current state for the authoritative merge result.

## Local candidate

Build10576 passed with the private Go cache. The complete development binaries,
known-hosts file and license are in `.context/multicli-candidate.9H4KK6/bin`.
`sf-dev version --json` reports `0.1.0-dev.multicli-local`, development channel,
and the base commit with an explicit `-dirty` suffix. Qualification/configuration
help smoke checks pass. This is a local testing build, not an immutable release
bundle, installed daemon replacement, or code published by PR #2.
