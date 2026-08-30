.PHONY: build build-dev test test-native-profile check

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

build-dev:
	mkdir -p "$(BIN_DIR)"
	go build -trimpath -buildvcs=false -ldflags "-X $(VERSION_PACKAGE).Version=$(DEV_VERSION) -X $(VERSION_PACKAGE).Commit=$(COMMIT) -X $(VERSION_PACKAGE).Channel=dev" -o "$(BIN_DIR)/sf-dev" ./cmd/sf

test:
	go test ./...

test-native-profile:
	spikes/native-profile/run.sh

check: test
	./scripts/repo-check
	./scripts/secret-scan
	./scripts/artifact-check --working-tree
	./scripts/release-build-smoke
