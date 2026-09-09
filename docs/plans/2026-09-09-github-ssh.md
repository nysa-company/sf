# First-class GitHub SSH transport

Status: implementation in progress; approved by Sofia on 2026-09-09.

## Outcome

A developer can register a trusted GitHub SSH checkout and run the existing
ticket workflow without changing its origin to HTTPS. SSH authenticates Git;
the official GitHub CLI still authenticates API operations (PRs, checks, merge).
HTTPS remains supported. No lifecycle or database migration is required.

## Corroboration and scope

Independent source review confirmed that the existing fixed-host SSH helper is
not wired into production composition. Publication and Store repository parsers
also disagree on supported SSH spellings. All three must agree before release.

- Accept exact `git@github.com:OWNER/REPO.git`,
  `ssh://git@github.com/OWNER/REPO.git`, the explicit port-22 equivalent, and
  existing `ssh://git@ssh.github.com:443/OWNER/REPO.git`.
- Preserve certified origin spelling and repository configuration. The packaged
  helper connects only to the pinned `ssh.github.com:443` endpoint regardless
  of the accepted input spelling. No aliases, user SSH config, proxies, or
  arbitrary hosts/options are admitted.
- Add explicit GitHub login protocol selection, keeping existing login APIs
  compatible. Explain gh's host-wide preference change before invoking it.
  Skip automatic key creation/upload; credentials remain owned by gh/ssh-agent.
- Resolve exact channel-specific sibling SSH helper and pinned host keys only
  when composing SSH capability. Pass the explicitly captured agent socket only
  to factory Git transport, never to provider/test environments.
- Report API and SSH-agent readiness separately. Missing keys/agents must name
  a host prerequisite; local readiness is not proof of remote write access.
- Do not rewrite existing registered origins, change live tickets, install a
  daemon, or alter branch protection as part of implementation.

## Acceptance

1. Strict URL and helper argv positives/negatives; HTTPS compatibility.
2. Exact-channel bundle tests and production composition tests.
3. CLI protocol validation, already-authenticated explicit selection, and no
   implicit key generation/upload or model credential exposure.
4. GitHub CI: focused tests, broad suite, race/security and repository checks.
5. Authorized private-repository SSH acceptance; report actual observed scope
   honestly, including any host-specific gate that CI cannot establish.
6. Documentation gives a short existing-key setup and recovery instructions.

Tests run in GitHub where possible. A local test is reserved for a host-specific
SSH/agent check that hosted CI cannot perform. Completion requires recorded
evidence, not merely a successful login or source review.
