package executionpolicy

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func qualification() GuardedQualification {
	return GuardedQualification{
		Provider:               domain.ProviderIdentity{Provider: "fixture", Model: "m", Family: "test", Version: "1"},
		AuthenticationIsolated: true, EnvironmentScrubbed: true, ProcessSupervised: true,
		GitIdentityAuthenticated: true, HostileFixturePassed: true,
	}
}

func TestGuardedQualificationAndAutonomyGate(t *testing.T) {
	profile, err := SelectProfile(domain.MergeGuarded, qualification())
	if err != nil || profile != contracts.ProfileGuarded {
		t.Fatalf("profile=%q err=%v", profile, err)
	}
	if _, err := SelectProfile(domain.MergeAutonomous, qualification()); !errors.Is(err, ErrAutonomyUnavailable) {
		t.Fatalf("autonomy error=%v", err)
	}
	failed := qualification()
	failed.HostileFixturePassed = false
	if _, err := SelectProfile(domain.MergeManual, failed); err == nil {
		t.Fatal("failed fixture did not disable provider in manual mode")
	}
}

func TestMinimalEnvironmentDoesNotInheritCredentials(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "home")
	temporary := filepath.Join(root, "tmp")
	for _, path := range []string{home, temporary} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("GH_TOKEN", "must-not-leak")
	t.Setenv("SSH_AUTH_SOCK", "/secret/agent")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "must-not-leak")
	environment, err := MinimalEnvironment(home, temporary)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(environment, "\n")
	for _, forbidden := range []string{"GH_TOKEN", "must-not-leak", "SSH_AUTH_SOCK", "/secret/agent", "AWS_SECRET_ACCESS_KEY"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("environment leaked %q: %s", forbidden, joined)
		}
	}
	if !strings.Contains(joined, "HOME="+home) || !strings.Contains(joined, "TMPDIR="+temporary) {
		t.Fatalf("minimal environment=%q", joined)
	}
}

func TestMinimalEnvironmentRejectsSymlinkAndSharedDirectory(t *testing.T) {
	root := t.TempDir()
	secure := filepath.Join(root, "secure")
	if err := os.Mkdir(secure, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(secure, link); err != nil {
		t.Fatal(err)
	}
	if _, err := MinimalEnvironment(link, secure); err == nil {
		t.Fatal("symlink home accepted")
	}
	shared := filepath.Join(root, "shared")
	if err := os.Mkdir(shared, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := MinimalEnvironment(secure, shared); err == nil {
		t.Fatal("shared temporary directory accepted")
	}
}

func TestRepositoryCommandPolicy(t *testing.T) {
	tests := []struct {
		argv    []string
		allowed bool
	}{
		{[]string{"go", "test", "./..."}, true},
		{[]string{"go", "vet", "./..."}, false},
		{[]string{"go", "build", "./..."}, false},
		{[]string{"go", "list", "./..."}, false},
		{[]string{"npm", "test"}, true},
		{[]string{"npm", "run", "build"}, true},
		{[]string{"npm", "run", "ci:local"}, false},
		{[]string{"npm", "test", "--", "--runInBand"}, false},
		{[]string{"npm", "--workspace", "app", "test"}, false},
		{[]string{"/bin/sh", "-c", "true"}, false},
		{[]string{"nice", "go", "test", "./..."}, false},
		{[]string{"xargs", "sh"}, false},
		{[]string{"git", "status", "--porcelain=v1"}, true},
		{[]string{"git", "diff", "--no-ext-diff", "--no-textconv", "--exit-code"}, true},
		{[]string{"git", "diff", "--exit-code"}, false},
		{[]string{"git", "push", "origin", "main"}, false},
		{[]string{"git", "-C", "../other", "status"}, false},
		{[]string{"git", "diff", "--ext-diff"}, false},
		{[]string{"gh", "pr", "merge", "1"}, false},
		{[]string{"curl", "https://example.invalid"}, false},
		{[]string{"npm", "install"}, false},
		{[]string{"npx", "unknown-package"}, false},
		{[]string{"go", "env", "-w", "GOPROXY=evil"}, false},
		{[]string{"go", "test", "-exec=/tmp/wrapper", "./..."}, false},
		{[]string{"go", "test", "-p=64", "./..."}, false},
		{[]string{"go", "test", "-count=0", "./..."}, false},
		{[]string{"go", "test", "-c", "-o", ".git/config", "./..."}, false},
		{[]string{"go", "build", "-o", ".git/config", "./..."}, false},
		{[]string{"go", "test", "-mod=mod", "./..."}, false},
		{[]string{"go", "test", "-modfile=/tmp/go.mod", "./..."}, false},
		{[]string{"go", "test", "-overlay=/tmp/overlay.json", "./..."}, false},
		{[]string{"go", "vet", "-vettool=/tmp/evil", "./..."}, false},
		{[]string{"go", "test", "-ldflags=-linkmode=external -extld=/tmp/evil", "./..."}, false},
		{[]string{"go", "test", "-gcflags=all=-N", "./..."}, false},
		{[]string{"go", "test", "-tags=unsafe", "./..."}, false},
		{[]string{"launchctl", "load", "job.plist"}, false},
		{[]string{"make", "deploy"}, false},
	}
	for _, test := range tests {
		decision := EvaluateRepositoryCommand(test.argv)
		if decision.Allowed != test.allowed {
			t.Errorf("argv=%q allowed=%v decision=%+v", test.argv, test.allowed, decision)
		}
	}
}

func TestImmutableCommandSnapshot(t *testing.T) {
	snapshot, err := NewCommandSnapshot([]string{"go", "test", "./..."}, []string{"git", "status", "--porcelain=v1"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(snapshot.Digest(), "sha256:") {
		t.Fatalf("digest=%q", snapshot.Digest())
	}
	if err := snapshot.Authorize([]string{"go", "test", "./..."}); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Authorize([]string{"go", "test", "./internal/..."}); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("changed command error=%v", err)
	}
	if _, err := NewCommandSnapshot([]string{"gh", "auth", "token"}); !errors.Is(err, ErrCommandDenied) {
		t.Fatalf("forbidden snapshot error=%v", err)
	}
}
