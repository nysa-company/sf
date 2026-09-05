# Self-serve CLI beta

Status: active implementation goal. Audience approved by the operator:
open-source developers already using Claude/Codex, working in Go,
Node/TypeScript, Python, and Ruby on Rails.

## Outcome

A developer unfamiliar with SF can install a macOS beta, add a supported
trusted project, submit and run a ticket, understand waits, and approve the
exact reviewed head without help from the author or database/worktree surgery.
Support must be explicit for each provider and stack; authentication support
alone is not a qualified execution adapter. Python, Rails, and general
dependency-bearing Node/TypeScript are expansion work, not current capabilities.

## Sequence

1. CLI foundations: every command explains its purpose; errors link to the
   relevant command; linked quickstart with a complete ticket example.
2. Guided setup: safe cwd/project defaults, read-only compatibility preview,
   authentication/provider/runtime/recipe/publication readiness shown separately.
   Missing capabilities must be reported before provider work starts.
3. Ticket facade: local template/validation and safe submit/start/watch
   composition with replay tests; retain low-level commands and merge approval.
   Add a ticket list and interactive selection by title/state/short ID, so
   operators do not copy long IDs. Missing ticket arguments may open a picker
   only in an interactive terminal; noninteractive and JSON calls stay
   deterministic. Resolve short IDs only when unique within the selected
   channel/project; disambiguate before any mutation and pass the full resolved
   identity to existing authority. Guided ticket creation previews the complete
   spec before submission; Markdown remains supported. Explicitly requested by
   the operator during the first checkpoint.
4. Operational clarity: expose sanitized readiness errors, last progress,
   queue/runtime duration, remaining deadline, and exact corrective actions.
5. Distribution: reproducible checksummed helper bundle, clean-install test,
   explicit upgrades with backup/compatibility checks and channel isolation.
   Public publication and service installation are not implicit in this plan.
6. Stack/provider expansion: evaluate dependency provisioning and isolated
   execution for Node/TS, Python, and Rails, plus actual Claude runtime
   composition. Define narrow supported recipes with integration fixtures;
   never enable arbitrary shell commands as a substitute for this work.
7. Acceptance: fresh CLI-led end-to-end delivery, clean environment tests,
   restart/outage/dirty-worktree/duplicate-request tests, then external beta.

## Gates

- Each checkpoint has tests and truthful docs; broad verification at integrated
  checkpoints before declaring implementation complete.
- Supported combinations have executable integration coverage. Unsupported
  combinations are rejected with an explanation and safe next step.
- Preserve SQLite lifecycle authority, independent verification, exact-head
  human approval, stable/dev isolation, and production capacity two.
- No manual live DB writes or factory worktree repair to make acceptance pass.
- Selection tests cover duplicate titles, ambiguous prefixes, empty lists,
  cancelled selection, changed state between list/action, channel/project
  separation, and piped stdin/JSON. Never choose a mutating target implicitly.
- Target: <=10 minutes from authenticated supported prerequisites to a first
  started ticket, measured separately from clean-machine installation time.
- External beta target: three unfamiliar users without author assistance;
  report this as pending unless actually observed.
- Reliability target: ten preselected small tickets, >=9 delivered without
  code/DB/worktree repair and with intended approvals only; zero leaked writers,
  duplicate mutations, or safety violations. Do not hide failed trials.

## Estimate and constraints

Planning estimate: 2–3 working days for onboarding on today's supported runtime;
1–2 weeks for the broader four-stack beta, contingent on dependency/runtime
constraints. Re-estimate after compatibility and provider feasibility checks.
This is not an unattended multi-platform stable-v1 delivery promise.

## Current checkpoint

Started CLI descriptions/examples, contextual help on input errors, and a
discoverable first-ticket guide. Added read-only project-scoped ticket listing,
titles in daemon status projection, interactive numbered selection, and unique
6–31 character hex prefixes for most lifecycle commands. Incomplete or
ambiguous inventories refuse noninteractive dispatch. Approval/rejection retain
explicit full IDs pending candidate-bound confirmation. No schema, provider,
or execution-policy changes in this checkpoint. Full normal Go suite, CLI race,
vet/repository/secret/artifact/docs/release checks passed on 2026-09-05. Compiled
interactive acceptance and the overall beta gates remain pending. Next:
current-directory defaults and a non-mutating compatibility preview.
