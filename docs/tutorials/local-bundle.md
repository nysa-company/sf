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
`sf-ssh-dev`, `sf-git-exec-dev`, `sf-git-credential-dev`, `github_known_hosts`, `LICENSE`
and `sf-bundle.json`. Stable builds use `make bundle VERSION=<semver>` and
unsuffixed executables. These commands do not publish a release.

Verification checks the complete inventory, permissions, SHA-256 hashes,
platform, and embedded executable version/commit/channel. It never executes
the payloads. Current verification requires the unstripped Mach-O metadata
produced by these build targets; other toolchain representations fail closed.
Helpers expose the same identity through the read-only `--sf-build-info` flag.
The SF MIT license is part of the verified inventory and is copied on install.
Older internal five-payload bundles lack this notice; rebuild from the current
source rather than editing their manifests. Third-party distribution notices
remain part of public-release review, not a claim made by bundle verification.

## Install without replacing anything

Choose a new private directory beneath an existing canonical parent:

Every ancestor must be owned by you or root and must not be group/world-writable.
A private leaf beneath `/tmp` or `/private/tmp` is not a valid runtime install
location. Installation checks this before creating files and checks the runtime
helpers again after copying. It does not loosen the runtime's path policy.

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

## Replace a bundle deliberately

There is no automatic updater or general rollback command. Install a verified
new bundle into a different private directory first; keep the old bundle.
Check its version and channel before pointing any daemon at existing state.

Use the old channel's `status` to inspect in-flight work. For the simplest beta
upgrade, wait for tickets to finish. Stop that foreground daemon with Ctrl-C
and wait for successful shutdown before starting its replacement. A cleanup
or drain error is not permission to launch a second writer; follow its reported
recovery action instead. Do not stop or replace the other channel.

Keep HOME, registered repositories, linked worktrees and runtime snapshots in
their original durable locations. Preserve a trusted backup before a stateful
upgrade; copying a live SQLite file or restoring only a database is not a
supported recovery procedure. Stable-channel startup makes an owner-only
database backup before a recognized schema migration. Development-channel
startup can migrate without that automatic backup. Do not assume dev has
stable's backup policy or that an older executable can read an upgraded schema.
Future/foreign schemas and incompatible migration history are refused rather
than forced; retain the evidence and follow the compatibility error.

Start the replacement with the same HOME and channel using `daemon run`, then
inspect `status` and `doctor`. Re-qualify providers if requested. A restart may
require explicit recovery/rearm; do not resubmit or reapprove a ticket merely
because the daemon changed. This is a deliberate local replacement procedure,
not a claim of unattended upgrades or universal rollback support.
