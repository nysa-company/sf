# Product brief

## Product

`sf` is a friendly local CLI and daemon for delegating a software ticket while
retaining safe operator control. Its normal path is Markdown ticket, plan,
independent verification before implementation, build, draft pull request,
checks, fresh independent review, merge policy, and reconciliation.

v1 implements two explicit merge outcomes: a manual external merge, observed
after the human merges the pull request, and a guarded exact-head merge that
`sf` requests only after a human approves the exact reviewed head. Autonomous
selection and merge remain deliberately unavailable pending a stronger native
containment proof and the guarded pilot.

## v1 user and self-serve beta

The initial operator is Sofia building Nysa from a trusted local macOS machine.
The next audience is open-source developers already using Claude or Codex who
want to delegate a bounded ticket through the CLI. They should be able to
preview project compatibility, create and select tickets without copying long
IDs, and understand the next action without author assistance.

The [self-serve beta plan](plans/2026-09-05-self-serve-cli-beta.md) targets Go,
Node/TypeScript, Python, and Rails. This is an expansion target, not a claim
that all four stacks or both provider runtimes work today. The
[first-ticket guide](tutorials/first-ticket.md) lists the actual supported
combinations. External beta validation remains pending. SF does not claim
safe execution of untrusted repositories.

## Success

- A ticket advances without controller repair or state-file surgery.
- Every wait or stop explains what changed and prints one executable next
  action or one concrete host prerequisite.
- `sf take` hands over a safe worktree and `sf resume` adopts valid operator
  work without weakening verification.
- Guarded merge is the default: a human approves the exact reviewed head and
  `sf` requests its immediate protected merge.
- Autonomous selection and merge are unavailable in v1; enabling them requires
  the guarded pilot and a stronger native execution-profile proof.

## Not v1

Remote workers, laptop-off execution, containers as a prerequisite, issue
trackers, generic forge support, deployment, a web dashboard, legacy workflow
migration, autonomous selection/merge, or untrusted repository execution.

Docker and Colima are not prerequisites and `sf` does not install them
silently. The supported boundary is a trusted local macOS repository; v1 does
not claim containment for an untrusted repository.
