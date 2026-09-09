.PHONY: build build-dev bundle bundle-dev test test-full test-race test-integration test-crash test-security test-upgrade test-compiled test-compiled-e2e test-all fmt-check test-native-profile verify-static check

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
	cp LICENSE "$(BIN_DIR)/LICENSE"
	chmod 0644 "$(BIN_DIR)/LICENSE"

build-dev:
	mkdir -p "$(BIN_DIR)"
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(DEV_VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=dev" -o "$(BIN_DIR)/sf-dev" ./cmd/sf
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(DEV_VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=dev" -o "$(BIN_DIR)/sf-ssh-dev" ./cmd/sf-ssh
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(DEV_VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=dev" -o "$(BIN_DIR)/sf-git-exec-dev" ./cmd/sf-git-exec
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(DEV_VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=dev" -o "$(BIN_DIR)/sf-git-credential-dev" ./cmd/sf-git-credential
	cp internal/gitssh/github_known_hosts "$(BIN_DIR)/github_known_hosts"
	chmod 0644 "$(BIN_DIR)/github_known_hosts"
	cp LICENSE "$(BIN_DIR)/LICENSE"
	chmod 0644 "$(BIN_DIR)/LICENSE"

test:
	go test -count=1 -shuffle=off -timeout 30m ./...

# Choose a fresh BIN_DIR. Manifest creation refuses extra payloads or overwrite.
# These targets produce local artifacts, never publish or install them.
bundle:
	@git diff --quiet HEAD && test -z "$$(git ls-files --others --exclude-standard)" || { echo "bundle requires a clean committed source tree" >&2; exit 2; }
	$(MAKE) build
	"$(BIN_DIR)/sf" bundle manifest "$(abspath $(BIN_DIR))" --json

bundle-dev:
	@git diff --quiet HEAD && test -z "$$(git ls-files --others --exclude-standard)" || { echo "bundle requires a clean committed source tree" >&2; exit 2; }
	$(MAKE) build-dev
	"$(BIN_DIR)/sf-dev" bundle manifest "$(abspath $(BIN_DIR))" --json

# Race runs the complete credential-free Go suite once, serializing package
# execution because several durable SQLite tests intentionally contend.
test-race:
	go test -race -count=1 -shuffle=off -p 1 -timeout 60m ./...

# Friendly explicit alias for the complete serialized race suite.
test-full: test-race

# Credential-free capacity-two/fault campaign. Fixtures include real sockets,
# SQLite and local Git, not live model or hosted delivery evidence.
.PHONY: test-concurrency
test-concurrency:
	@test "$$(uname -s)" = Darwin || { echo "Concurrency acceptance requires the supported macOS host" >&2; exit 2; }
	go test -race -count=3 -shuffle=off -p 1 -timeout 15m ./internal/daemon ./internal/workflowruntime ./internal/worktreecoord ./internal/store ./internal/publication -run '^Test(ConcurrentCLIRunQueuesThirdWithoutDuplicatingStarts|ConcurrentTicketCapacitySurvivesTwoDaemonRestarts|RuntimePoolConcurrencyStress|SchedulerFourCallerConcurrencyStress|DaemonFactoryTwoWorkerPauseDrainsOnlyTargetAndResumeRearms|CLIRunRealDaemonLostResponseDoesNotDuplicateWork|EnsureExcludesActiveRepositoryCommandWriter|EnsureConcurrentCallersCreateExactlyOneWorktree|SeededLeaseAdmissionStress|RepositoryCommandAcquireRetriesOnlyDifferentActiveCommand|TerminalTicketCannotMintWritersAfterCapacityIsReassigned|SharedBaseMovementNeverPublishesStaleCandidateOnRetry|WorkerReconcilesLostCreateWithoutBlindReplay|WorkerKeepsUnprovenPushUncertainAfterLostCommandResult|RecoveredPublishingRuntimePublishesAfterTwoPrePublicationCrashes)$$'
	go test -count=2 -shuffle=off -p 1 -timeout 15m -tags sf_e2e ./cmd/sf -run '^TestCompiledDevConcurrentTicketsReachIndependentPRs$$'

test-integration:
	go test -count=1 -shuffle=off -p 1 -timeout 30m ./cmd/sf ./internal/daemon ./internal/github ./internal/localruntime ./internal/publication ./internal/workflowruntime ./internal/workflowworker

# Hosted integration runs every workflowruntime test in disjoint shards so
# cumulative real-Git fixtures do not exhaust one package's 30-minute bound.
.PHONY: test-integration-other
test-integration-other:
	go test -count=1 -shuffle=off -p 1 -timeout 30m ./cmd/sf ./internal/daemon ./internal/github ./internal/localruntime ./internal/publication ./internal/workflowworker

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

# Explicit public-download acceptance; never silently pass by skipping on CI.
.PHONY: test-python-e2e
test-python-e2e:
	@test "$$(uname -s)/$$(uname -m)" = Darwin/arm64 || { echo "Python E2E requires macOS ARM64" >&2; exit 2; }
	@test "$$SF_TEST_PYTHON_CLI_DOWNLOAD" = 1 || { echo "Set SF_TEST_PYTHON_CLI_DOWNLOAD=1 to allow pinned public downloads into disposable test homes" >&2; exit 2; }
	go test -count=1 -shuffle=off -p 1 -timeout 15m -tags sf_e2e ./cmd/sf -run '^(TestCompiledPythonPreparationAndInit|TestCompiledPythonGuardedWalkingSkeleton)$$'

test-compiled: test-compiled-e2e

# test-race covers the complete suite; the remaining targets add named,
# readable verification gates without running that complete suite again.
# Static/repository/release checks are part of the same claimed final gate.
# Without SF_CI_LANE this is the complete serialized local path. Hosted CI
# distributes the same gates across isolated runners, including all eight
# disjoint Store race shards and the complete workflowruntime race package;
# one lane alone is NOT complete acceptance.
# Each lane keeps the existing per-package bounds.
test-all:
	@case "$${SF_CI_LANE:-}" in \
	  '') python3 scripts/run-bounded --timeout 120m -- $(MAKE) --no-print-directory -j1 test-race test-integration test-crash test-security test-upgrade test-compiled-e2e verify-static ;; \
	  race-other) python3 scripts/run-bounded --timeout 80m -- python3 scripts/ci-race.py other ;; \
	  runtime-race) python3 scripts/run-bounded --timeout 65m -- python3 scripts/ci-race.py runtime-race ;; \
	  store-race) python3 scripts/run-bounded --timeout 65m -- python3 scripts/ci-race.py store --index "$$SHARD" --count 8 ;; \
	  runtime-integration) python3 scripts/run-bounded --timeout 40m -- python3 scripts/ci-race.py runtime-integration --index "$$SHARD" --count 4 ;; \
	  test-integration|test-integration-other|test-crash|test-security|test-upgrade|test-compiled-e2e|verify-static) python3 scripts/run-bounded --timeout 80m -- $(MAKE) --no-print-directory "$$SF_CI_LANE" ;; \
	  *) echo 'unknown CI acceptance lane' >&2; exit 2 ;; \
	esac

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
