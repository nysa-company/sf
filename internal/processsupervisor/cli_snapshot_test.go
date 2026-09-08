package processsupervisor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/cliruntime"
)

func TestCLISnapshotUsesFullBundleForCachedValidation(t *testing.T) {
	for _, kind := range []string{"claude", "cursor"} {
		t.Run(kind, func(t *testing.T) {
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			entry := "claude"
			files := []string{entry}
			if kind == "cursor" {
				entry = "cursor-agent"
				files = []string{entry, "node", "index.js", "helper.node"}
			}
			for _, name := range files {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("fixture"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			bundle, err := cliruntime.Resolve(context.Background(), kind, filepath.Join(dir, entry))
			if err != nil {
				t.Fatal(err)
			}
			trusted := trustedExecutable{path: bundle.Executable(), digest: bundle.Digest(), cliBundle: &bundle}
			if err := trusted.stage(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(trusted.stagedDir) })
			if !stagedRuntimeMatches(trusted.snapshot, trusted.digest) {
				t.Fatal("new snapshot refused")
			}
			member := entry
			if kind == "cursor" {
				member = "helper.node"
			}
			// Source replacement cannot alter a running immutable snapshot.
			if err := os.WriteFile(filepath.Join(dir, member), []byte("new"), 0700); err != nil {
				t.Fatal(err)
			}
			if !stagedRuntimeMatches(trusted.snapshot, trusted.digest) {
				t.Fatal("source change invalidated staged bytes")
			}
			// A cached helper mutation must refuse reuse even with an intact launcher.
			target := filepath.Join(trusted.stagedDir, member)
			if err := os.Chmod(target, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(target, []byte("tampered"), 0700); err != nil {
				t.Fatal(err)
			}
			if stagedRuntimeMatches(trusted.snapshot, trusted.digest) {
				t.Fatal("modified stage accepted")
			}
		})
	}
}

func TestCLIBundleCannotChangeUnderSameRuntime(t *testing.T) {
	if !cliBundleMatches(nil, nil) {
		t.Fatal("legacy runtime mismatch")
	}
	var empty cliruntime.Bundle
	if cliBundleMatches(&empty, nil) || cliBundleMatches(nil, &empty) {
		t.Fatal("CLI and legacy runtimes conflated")
	}
	trusted := trustedExecutable{path: "/private/fixture", digest: "not-qualified", cliBundle: &empty}
	if trusted.stage() == nil {
		t.Fatal("unbound runtime staged")
	}
}
