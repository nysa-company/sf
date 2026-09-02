.PHONY: build build-dev test test-full test-race test-integration test-crash test-security test-upgrade test-compiled test-compiled-e2e test-all fmt-check test-native-profile verify-static check

VERSION ?=
DEV_VERSION ?= 0.0.0-dev
COMMIT ?= $(shell git rev-parse --verify HEAD 2>/dev/null || printf unknown)
VERSION_PACKAGE = github.com/nysa-company/sf/internal/version
BIN_DIR ?= bin

build:
	@test -n "$(VERSION)" || { echo "VERSION=<semver> is required for a stable build" >&2; exit 2; }
	@./scripts/semver-check "$(VERSION)"
	mkdir -p "$(BIN_DIR)"
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=stable" -o "$(BIN_DIR)/sf" ./cmd/sf
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=stable" -o "$(BIN_DIR)/sf-ssh" ./cmd/sf-ssh
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=stable" -o "$(BIN_DIR)/sf-git-exec" ./cmd/sf-git-exec
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=stable" -o "$(BIN_DIR)/sf-git-credential" ./cmd/sf-git-credential
	cp internal/gitssh/github_known_hosts "$(BIN_DIR)/github_known_hosts"
	chmod 0644 "$(BIN_DIR)/github_known_hosts"

build-dev:
	mkdir -p "$(BIN_DIR)"
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(DEV_VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=dev" -o "$(BIN_DIR)/sf-dev" ./cmd/sf
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(DEV_VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=dev" -o "$(BIN_DIR)/sf-ssh-dev" ./cmd/sf-ssh
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(DEV_VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=dev" -o "$(BIN_DIR)/sf-git-exec-dev" ./cmd/sf-git-exec
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(DEV_VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=dev" -o "$(BIN_DIR)/sf-git-credential-dev" ./cmd/sf-git-credential
	cp internal/gitssh/github_known_hosts "$(BIN_DIR)/github_known_hosts"
	chmod 0644 "$(BIN_DIR)/github_known_hosts"

test:
	go test -count=1 -shuffle=off -timeout 30m ./...

# Race runs the complete credential-free Go suite once, serializing package
# execution because several durable SQLite tests intentionally contend.
test-race:
	go test -race -count=1 -shuffle=off -p 1 -timeout 30m ./...

# Friendly explicit alias for the complete serialized race suite.
test-full: test-race

test-integration:
	go test -count=1 -shuffle=off -p 1 -timeout 30m ./cmd/sf ./internal/daemon ./internal/github ./internal/localruntime ./internal/publication ./internal/workflowruntime ./internal/workflowworker

test-crash:
	go test -count=1 -shuffle=off -p 1 -timeout 30m ./... -run '(^Test.*Crash|Crash|Recovery|Recover|Rearm|Quarantine)'

test-security:
	go test -count=1 -shuffle=off -p 1 -timeout 30m ./... -run '(Hostile|Security|Redact|Credential|Secret|Symlink|Escape|Sandbox|Origin)'

test-upgrade:
	go test -count=1 -shuffle=off -p 1 -timeout 30m ./... -run '(Migration|Upgrade|Channel|Schema|Compatibility|Backup)'

# The compiled walking skeleton is tagged because it builds and exercises the
# Darwin guarded runtime. The test itself skips on non-Darwin hosts.
test-compiled-e2e:
	go test -count=1 -shuffle=off -p 1 -timeout 30m -tags sf_e2e ./cmd/sf -run '^(TestCompiledDev(GuardedWalkingSkeleton|ManualWalkingSkeleton|FriendlyOperatorTakeover)|TestCompiledStableAndDevDaemonsCoexist)$$'

test-compiled: test-compiled-e2e

# test-race covers the complete suite; the remaining targets add named,
# readable verification gates without running that complete suite again.
# Static/repository/release checks are part of the same claimed final gate.
# The outer timeout bounds the complete local path; each Go target has its own
# 30-minute package timeout as well.
test-all:
	python3 scripts/run-bounded --timeout 90m -- $(MAKE) --no-print-directory -j1 test-race test-integration test-crash test-security test-upgrade test-compiled-e2e verify-static

fmt-check:
	@files="$$(git ls-files -co --exclude-standard -- '*.go')"; \
	test -n "$$files" || exit 0; \
	unformatted="$$(gofmt -l $$files)"; \
	test -z "$$unformatted" || { echo "gofmt required for:"; echo "$$unformatted"; exit 1; }

test-native-profile:
	spikes/native-profile/run.sh

verify-static:
	@$(MAKE) --no-print-directory fmt-check
	go vet ./...
	./scripts/repo-check
	./scripts/secret-scan
	./scripts/artifact-check --working-tree
	./scripts/docs-smoke
	./scripts/release-build-smoke

check: test verify-static
