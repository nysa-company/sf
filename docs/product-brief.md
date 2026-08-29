# Product brief

## Product

`sf` is a friendly local CLI and daemon for delegating a software ticket while
retaining safe operator control. Its normal path is Markdown ticket, plan,
independent verification before implementation, build, draft pull request,
checks, fresh independent review, merge policy, and reconciliation.

## v1 user

Sofia building Nysa from a trusted local macOS machine. The project is intended
to become open source, but v1 does not claim safe execution of untrusted
repositories.

## Success

- A ticket advances without controller repair or state-file surgery.
- Every wait or stop explains what changed and prints one executable next
  action or one concrete host prerequisite.
- `sf take` hands over a safe worktree and `sf resume` adopts valid operator
  work without weakening verification.
- Guarded merge is the default: a human approves the exact reviewed head and
  `sf` requests its immediate protected merge.
- Autonomous merge is available only after the guarded pilot and a stronger
  native execution-profile proof.

## Not v1

Remote workers, laptop-off execution, containers as a prerequisite, issue
trackers, generic forge support, deployment, a web dashboard, legacy workflow
migration, or untrusted repository execution.
