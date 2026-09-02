# Versions and local release channels

`sf` and `sf-dev` are separate local runtime channels even when built from the
same source tree. The channel is compiled into the binary and selects a
different application root, socket, SQLite database, logs, events, backups,
worktrees, and Git branch namespace. A command-line flag cannot switch it.
Both daemons are launched in the foreground in v1; service-manager packaging is
deferred.

## Development build

```sh
make build-dev
./bin/sf-dev version --json
```

The build embeds the exact source commit, the `dev` channel, and a development
version. `DEV_VERSION=<semver>` may label a local candidate without affecting
stable state.

## Stable candidate build

```sh
make build VERSION=1.2.3
./bin/sf version --json
```

`VERSION` is mandatory and must be SemVer. This produces a local stable-channel
candidate only; it does not install it, start a service, copy a database,
contact GitHub, or change Nysa. Public release checksums, installation, and
stable promotion remain explicit later gates.

`scripts/release-build-smoke` builds both channels in a temporary directory
and verifies the embedded version, commit, protocol, and channel. It also
proves an omitted or non-SemVer stable version is refused. This is a build
identity check, not proof that both daemons have been run concurrently; that
remains a required release acceptance test. The local gate also does not claim
the future 1,000-ticket/100-repetition randomized stress or license gates;
those remain promotion prerequisites.

## Credential-free local acceptance versus stable promotion

Local v1 acceptance is deliberately free of application credentials and live
service qualification. Run the documented local gates (including `make
test-all`, `scripts/release-build-smoke`, and `scripts/docs-smoke`) against the
candidate source/build. These gates use fakes where an external boundary is
needed; they do not authenticate a real provider, contact a GitHub repository,
create a pull request, or qualify a release for stable promotion.

The secret scan uses the pinned Gitleaks v8.30.1 artifact. If that artifact is
not already available in `.context/tools/gitleaks/8.30.1/` and `GITLEAKS_BIN`
is not set, `scripts/secret-scan` may download it from the public GitHub
release URL. That bootstrap is checksum-verified, capped at 32 MiB, and
subject to a hard 30-second end-to-end deadline; a failed or incomplete
download fails closed. Set `GITLEAKS_BIN` or pre-populate the ignored cache
when a fully offline local run is required. The scan never contacts a
repository, provider, or Nysa service.

A passing local result is necessary acceptance evidence, not a promotion
decision.

Stable promotion is a separate, manual operator decision. It remains blocked
until the following checklist has been completed and its redacted evidence has
been reviewed. None of the live steps below has been run merely by documenting
it here.

1. Record the candidate version, commit, checksums, and the passing
   credential-free local-gate results.
2. On a disposable trusted fixture checkout, with only the approved isolated
   Codex provider home available, authenticate and start the candidate's
   foreground daemon:

   ```text
   ./bin/sf auth login codex
   ./bin/sf daemon run
   ```

   In a second terminal, run the daemon-signed qualification and diagnostic:

   ```text
   ./bin/sf providers qualify --builder codex --reviewer codex
   ./bin/sf doctor --repo /absolute/path/to/disposable-fixture
   ```

   `providers qualify` first authenticates the resolved `codex` executable and
   its exact executable `codex-code-mode-host` sibling as one release bundle,
   then runs the bounded, credential-free local Codex probes
   (configuration parsing, guarded read/write, network denial, and credential
   isolation) and records guarded-qualified only when those probes pass. It
   does not execute `testdata/hostile-repository` and does not make a model
   call. The automated `make test-all` security/redaction targets likewise use
   fake credential-free fixtures; neither command is real-provider hostile-
   fixture evidence. Real-provider hostile fixtures for each supported version
   remain a separate manual release gate. Do not expose Nysa, GitHub,
   deployment, SSH-agent, or unrelated provider credentials to this fixture.
3. Obtain explicit operator authorization for the exact disposable repository;
   this checklist does not grant it. The separately initiated guarded flow may
   run only against that dedicated, non-production GitHub repository. It must
   have real `gh` authentication, branch protection and required checks
   matching supported behavior, and no deployment secrets. It must not be Nysa
   or any production repository. After the operator has verified and approved
   that scope, use the implemented lifecycle commands with the repository's
   actual project, ticket ID, and authenticated operator:

   ```text
   ./bin/sf auth login github
   ./bin/sf init --project <project> --repo /absolute/path/to/disposable-repository
   ./bin/sf submit <guarded-ticket.md> --project <project>
   ./bin/sf start <ticket-id>
   ./bin/sf status <ticket-id> --watch
   ./bin/sf approve <ticket-id> --operator <identity>
   ```

   Exercise and retain evidence for the guarded happy path, stale-head
   refusal, a red required check, lost-read retry, one takeover, and one
   daemon crash after a remote mutation. Approval must be a human action on
   the exact reviewed head; autonomous merge is not a v1 qualification path.
4. Complete the ten-ticket guarded Nysa pilot under separately approved Nysa
   scope, then have the release operator review all retained redacted
   evidence and explicitly decide whether to promote. This checklist creates
   no authorization to install, update, or roll back a stable runtime.

## Promotion and rollback implementation status

The CLI reserves `update` and `rollback`, but both currently fail closed as
`not_configured`. Before either becomes active, stable promotion must install
an already-built checksummed artifact, create an online SQLite backup before a
schema change, leave the dev database untouched, and refuse incompatible active
workflows. Rollback must bind the previous binary to its matching database
backup. No non-development background service is installed by the current
repository.
