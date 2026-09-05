# Build and install a local macOS bundle

This is an explicit local distribution path, not a published release or an
automatic updater. Use trusted source. A checksum manifest detects changes
relative to that manifest; it does not authenticate the publisher. Signing,
notarization and public release distribution remain pending beta gates.

## Build from a clean checkout

With the repository's Go toolchain and macOS prerequisites installed, choose
a new output directory. Do not reuse a directory containing another channel.

```sh
make bundle-dev DEV_VERSION=0.1.0-dev.local BIN_DIR=.context/bundle-dev
.context/bundle-dev/sf-dev bundle verify "$(pwd)/.context/bundle-dev"
```

The build records the checkout commit. The bundle contains `sf-dev`,
`sf-ssh-dev`, `sf-git-exec-dev`, `sf-git-credential-dev`, `github_known_hosts`
and `sf-bundle.json`. Stable builds use `make bundle VERSION=<semver>` and
unsuffixed executables. These commands do not publish a release.

Verification checks the complete inventory, permissions, SHA-256 hashes,
platform, and embedded executable version/commit/channel. It never executes
the payloads. Current verification requires the unstripped Mach-O metadata
produced by these build targets; other toolchain representations fail closed.
Helpers expose the same identity through the read-only `--sf-build-info` flag.

## Install without replacing anything

Choose a new private directory beneath an existing canonical parent:

```sh
mkdir -p "$HOME/.local/sf"
.context/bundle-dev/sf-dev bundle install "$(pwd)/.context/bundle-dev" \
  --to "$HOME/.local/sf/dev-0.1.0-local"
"$HOME/.local/sf/dev-0.1.0-local/sf-dev" version --json
"$HOME/.local/sf/dev-0.1.0-local/sf-dev" --help
```

Installation verifies before copying and verifies the installed copy again.
It refuses an existing destination, including symlinks, and does not modify
PATH, shell startup files, services, databases, or running daemons. Use the
absolute executable path or add this directory to PATH yourself. Keep stable
and dev bundles separate; an installer executable refuses another channel.

A failed copy may leave a clearly reported partial destination. Do not execute
it. Inspect it and choose a new destination; retry never overwrites it. No
automatic cleanup, rollback or migration is implied.

Continue with [your first ticket](first-ticket.md). Installation does not
qualify providers, make unsupported languages executable, or establish GitHub
publication readiness.
