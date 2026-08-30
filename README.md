# sf

`sf` is a local, operator-controlled software factory. It turns a Markdown
ticket into a planned, independently verified implementation and a reviewed
GitHub pull request, then follows the configured manual, guarded, or autonomous
exact-head merge policy.

The first implementation target is Sofia building Nysa on a trusted macOS
machine. Docker and Colima are not required.

## Development

Prerequisites: Go 1.25 or newer, Git, the official `gh` CLI, and supported AI
provider CLIs.

```sh
make build-dev
./bin/sf-dev version
go test ./...
```

`make build-dev` embeds the development channel and exact source commit.
Stable artifacts are intentionally explicit: `make build VERSION=<semver>`
embeds the stable channel and refuses to create an unversioned stable binary.
Neither command copies state between the isolated channels.

The approved product, architecture, verification, state-machine, and task
contracts live in [`docs/plans/`](docs/plans/). Implementation has no authority
to create a remote, mutate Nysa, enable autonomous Nysa merge, retire the
legacy factory, or install non-development services without a later explicit
approval.
