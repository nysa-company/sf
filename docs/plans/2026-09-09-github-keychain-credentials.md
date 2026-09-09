# GitHub Keychain credential bridge and honest readiness

Status: implementation approved; independently corroborated; source/tests
implemented, validation pending.

## Goal

A normal macOS Keychain-backed `gh auth login` works for factory-owned HTTPS
Git without plaintext credential storage. A failed bridge is visible in Doctor
and repository preflight failures cannot masquerade as stale ticket fences.

## Design

1. Carry the existing authenticated operator home from runtime composition to
   the HTTPS helper as an explicit, validated capability. Only its trusted gh
   child receives that HOME. Git retains its private HOME. Provider and test
   environments, including Node's `/var/empty`, remain unchanged.
2. Reject missing, unsafe, writable or substituted home paths. Do not introduce
   a repository-configurable HOME override, ambient token inheritance, general
   environment inheritance, or `gh auth token` calls.
3. Doctor invokes the exact channel-matched packaged credential helper with an
   authenticated gh snapshot and the same environment as execution. Bound the
   probe, suppress stderr, validate credential presence without displaying or
   persisting credential bytes. Distinguish API login, credential retrieval,
   and actual repository access (the latter remains unproven by this probe).
4. Repository-base preflight failures receive a closed safe diagnostic distinct
   from stale Store fences and worktree identity failures. Status provides a
   Doctor next action without leaking subprocess output or changing lifecycle.
5. Document Keychain support, locked/unavailable credential-store recovery, and
   the reason plaintext storage is not the recommended workaround.

## Acceptance

- Unit/production-composition regressions: explicit owner home, exact helper
  and gh identities, hostile ambient variables, unsafe paths, missing/empty/
  malformed/oversized credential response, failure and cancellation.
- Doctor cannot show guarded eligibility for a selected HTTPS repository when
  the configured credential probe fails or was not run. SSH-only inspection
  does not require HTTPS credentials; mixed fetch/push URLs inspect both.
- Real coordinator preflight failure invokes no provider and reaches sanitized
  status output as a repository error; actual stale fences remain stale.
- GitHub focused/full acceptance includes `go test ./...`, repository check,
  secret scan and relevant race tests. No local broad tests.
- A real macOS Keychain check, if needed, is read-only, uses the CI-built helper,
  and reports only pass/fail. No token output, login change, push, live ticket,
  daemon restart, installation or database edits.

## Delivery boundaries

Branch `fix/github-keychain-credentials` starts at main `ab1d760`. The user
explicitly approved pushing the fix branch and running GitHub CI. Merge
and installed-binary rollout are not implied. No schema or state-machine change.

## Corroboration

- Security reviewer: the existing GitHub API path already uses OwnerHome;
  restrict the correction to the HTTPS helper, retaining isolated Git HOME.
- Diagnostics reviewer: ErrAuthentication currently represents identity as well
  as wrapped preflight failures. Do not rename every such error to a credential
  failure; split repository preflight, identity, and stale-fence outcomes.
