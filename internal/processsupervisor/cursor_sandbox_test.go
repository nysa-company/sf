package processsupervisor

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestCursorOwnedLifecyclePreservesArgvAndExit(t *testing.T) {
	root := credentialHome(t)
	shim, logPath := filepath.Join(root, "sandbox-shim"), filepath.Join(root, "events")
	script := `#!/bin/sh
test "$1" = -p || exit 90
test "$2" = profile || exit 91
test "$3" = executable || exit 92
shift 3
if [ "$1" = local-worker ]; then
  test "$2" = kill || exit 93
  printf 'cleanup\n' >> "$SF_FIXTURE_LOG"
  exit "$SF_FIXTURE_CLEANUP"
fi
test "$1" = 'literal;not-code' || exit 94
test "$2" = '$(not-a-command)' || exit 95
printf 'main\n' >> "$SF_FIXTURE_LOG"
exit 17
`
	if os.WriteFile(shim, []byte(script), 0700) != nil {
		t.Fatal("fixture unavailable")
	}
	for _, cleanup := range []string{"0", "1"} {
		if os.WriteFile(logPath, nil, 0600) != nil {
			t.Fatal("fixture unavailable")
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		cmd := exec.CommandContext(ctx, "/bin/bash", "-c", cursorOwnedLifecycleScript, "sf-cursor-owned", shim, "profile", "executable", "literal;not-code", "$(not-a-command)")
		cmd.Env = []string{"PATH=/usr/bin:/bin", "SF_FIXTURE_LOG=" + logPath, "SF_FIXTURE_CLEANUP=" + cleanup}
		err := cmd.Run()
		cancel()
		var exit *exec.ExitError
		want := 17
		if cleanup == "1" {
			want = 125
		}
		if !errors.As(err, &exit) || exit.ExitCode() != want {
			t.Fatal("lifecycle exit mismatch", err)
		}
		data, err := os.ReadFile(logPath)
		if err != nil || string(data) != "main\ncleanup\n" {
			t.Fatal("cleanup ordering mismatch")
		}
	}
}

// No provider credentials or model calls: prove the physical write boundary
// independently of the model following its prompt or tool permission policy.
func TestCursorRoleSandboxPhysicalWrites(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native macOS profile")
	}
	root := credentialHome(t)
	worktree, home, stage := filepath.Join(root, "worktree"), filepath.Join(root, "home"), filepath.Join(root, "stage")
	for _, p := range []string{worktree, home, stage} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	allowed, forbidden := filepath.Join(worktree, "result.txt"), filepath.Join(worktree, "forbidden.txt")
	for _, p := range []string{allowed, forbidden} {
		if err := os.WriteFile(p, []byte("BASELINE"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, phase := range []domain.Phase{domain.PhaseBuild, domain.PhaseReview} {
		profile, err := cursorRoleSandboxProfile(contracts.PhaseInput{Phase: phase, Profile: contracts.ProfileGuarded, Worktree: worktree, AllowedPaths: []string{"result.txt"}}, stage, home)
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{allowed, forbidden} {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			cmd := exec.CommandContext(ctx, repositorySandboxExec, "-p", profile, "/bin/bash", "-c", `printf CHANGED > "$1"`, "sf-fixture", path)
			cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + home, "TMPDIR=" + home}
			output, err := cmd.CombinedOutput()
			cancel()
			wantWrite := phase == domain.PhaseBuild && path == allowed
			if (err == nil) != wantWrite {
				t.Fatalf("phase=%s allowed_target=%t write_succeeded=%t error=%v diagnostic=%s", phase, path == allowed, err == nil, err, output)
			}
			data, readErr := os.ReadFile(path)
			want := "BASELINE"
			if path == allowed {
				want = "CHANGED"
			}
			if readErr != nil || string(data) != want {
				t.Fatal("physical file invariant failed")
			}
		}
	}
}
