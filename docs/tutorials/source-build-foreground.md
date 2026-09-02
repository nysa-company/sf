# Source build and foreground first ticket

This is the development path for a fresh macOS checkout. It is intentionally
local and credential-safe: the tutorial never copies a token into `sf`, and
the foreground daemon is the only process that owns ticket mutations.

## Prerequisites

- macOS with Git, Go, and the official `gh` CLI installed.
- `gh` authenticated interactively to the disposable or trusted GitHub
  account you intend to use.
- One qualified Builder provider and one independently qualified Reviewer
  provider. The local beta pair uses separately qualified Codex model families.
- A supported Codex installation whose resolved `codex` executable has the
  exact executable sibling `codex-code-mode-host` from the same release.
- A trusted local product checkout with a GitHub origin.

Docker, Colima, root access, deployment credentials, and copied GitHub tokens
are not prerequisites. Do not paste credentials into a command or ticket.

## Build and configure locally

Replace only the two absolute paths below with local paths. This tutorial does
not invent a clone URL or run a network download.

```text
cd /absolute/path/to/sf-source
make build-dev

./bin/sf-dev auth status
./bin/sf-dev auth login github
./bin/sf-dev auth login codex

./bin/sf-dev init --project nysa --repo /absolute/path/to/nysa-app
```

`init` reads an optional `.sf/config.toml` but the normal form does not create
or modify that file. It canonicalizes the Git worktree root, verifies the
configured local base branch, creates only the dev channel's owner-only state
directory, and records a durable configuration generation. Running the exact
command again is an idempotent observation. A different repository or changed
configuration under the same project name is refused rather than silently
replacing the registration.

For the dependency-bearing Nysa monorepo, explicitly select the bounded pure
API test recipe. This is the only `init` form that may create the missing
project config, and it never overwrites an existing file:

```text
./bin/sf-dev init --project nysa --repo /absolute/path/to/nysa-app \
  --profile nysa-api-pure-v1 \
  --test apps/api/tests/retrieval-fusion.test.ts
```

The selected entrypoint and its relative `.js`-to-`.ts` closure are validated
before the config is installed. The resulting command uses no npm, package
scripts, network, or `node_modules`.

`auth login` starts the official interactive login flow in your terminal. It
does not send a displayed token to the daemon. `auth status` reports only a
safe account label, credential state, and remediation.

## Run the foreground daemon

Leave this command running in a second terminal:

```text
cd /absolute/path/to/sf-source
./bin/sf-dev daemon run
```

Return to the first terminal and qualify the two Codex-backed logical roles,
then run the read-only diagnostic:

```text
cd /absolute/path/to/sf-source
./bin/sf-dev providers qualify --builder codex --reviewer codex
./bin/sf-dev doctor --repo /absolute/path/to/nysa-app
```

Qualification is served by the running foreground daemon: its current local
supervisor signs the exact provider/version/family, two-file Codex runtime
bundle, non-secret
authentication identity, and bounded probe digests before SQLite admits the
guarded result. It invokes no model. On macOS it fails closed unless the outer
Seatbelt probe denies loopback/external sockets and CODEX_HOME metadata access.
Doctor never prints provider output or credential bytes.

## Submit and watch one ticket

```text
cd /absolute/path/to/sf-source
./bin/sf-dev submit ./tickets/fix-duplicate-reminders.md --project nysa
```

The command prints a channel-scoped ID and confirms that no work has started,
for example `Submitted SF-00000001 to dev/nysa. No work has started.` Start the
ticket explicitly, then follow its durable status:

```text
./bin/sf-dev start SF-00000001
./bin/sf-dev status SF-00000001 --watch
```

Every pause or error explains what changed and prints exactly one executable
next action. Use that action as the next command; do not edit the database or
event files. The stable and dev channels have separate roots and may each
contain the same generated ticket ID safely.

## Hermetic documentation check

From the source checkout, run:

```text
scripts/docs-smoke
```

This check reads the tutorial and verifies the approved command order and
safety language. It does not invoke `gh`, a provider, a daemon, a network, or
any credential store.
