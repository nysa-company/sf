.PHONY: build build-dev test test-native-profile check

build:
	mkdir -p bin
	go build -trimpath -o bin/sf ./cmd/sf
	go build -trimpath -o bin/sf-ssh ./cmd/sf-ssh
	go build -trimpath -o bin/sf-git-exec ./cmd/sf-git-exec
	cp internal/gitssh/github_known_hosts bin/github_known_hosts
	chmod 0644 bin/github_known_hosts

build-dev:
	mkdir -p bin
	go build -trimpath -ldflags "-X github.com/nysa-company/sf/internal/version.Channel=dev" -o bin/sf-dev ./cmd/sf
	go build -trimpath -ldflags "-X github.com/nysa-company/sf/internal/version.Channel=dev" -o bin/sf-ssh-dev ./cmd/sf-ssh
	go build -trimpath -ldflags "-X github.com/nysa-company/sf/internal/version.Channel=dev" -o bin/sf-git-exec-dev ./cmd/sf-git-exec
	cp internal/gitssh/github_known_hosts bin/github_known_hosts
	chmod 0644 bin/github_known_hosts

test:
	go test ./...

test-native-profile:
	spikes/native-profile/run.sh

check: test build build-dev
	./scripts/repo-check
	./scripts/secret-scan
	./scripts/artifact-check --working-tree
