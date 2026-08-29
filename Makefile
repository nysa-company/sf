.PHONY: build build-dev test check

build:
	mkdir -p bin
	go build -trimpath -o bin/sf ./cmd/sf

build-dev:
	mkdir -p bin
	go build -trimpath -ldflags "-X github.com/nysa-company/sf/internal/version.Channel=dev" -o bin/sf-dev ./cmd/sf

test:
	go test ./...

check: test
	./scripts/repo-check
	./scripts/secret-scan
	./scripts/artifact-check --working-tree
