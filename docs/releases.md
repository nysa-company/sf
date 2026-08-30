# Versions and local release channels

`sf` and `sf-dev` are separate products at runtime even when built from the
same source tree. The channel is compiled into the binary and selects a
different application root, socket, SQLite database, logs, events, backups,
worktrees, and Git branch namespace. A command-line flag cannot switch it.

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
proves an omitted or non-SemVer stable version is refused.

## Promotion and rollback status

The CLI reserves `update` and `rollback`, but both currently fail closed as
`not_configured`. Before either becomes active, stable promotion must install
an already-built checksummed artifact, create an online SQLite backup before a
schema change, leave the dev database untouched, and refuse incompatible active
workflows. Rollback must bind the previous binary to its matching database
backup. No non-development background service is installed by the current
repository.
