package workflowruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/config"
)

func TestExecutionBaseDoesNotUsePrimaryDependencies(t *testing.T) {
	primary, worktree := t.TempDir(), t.TempDir()
	for _, dir := range []string{primary, worktree} {
		if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.test/app\ngo 1.24\nrequire example.test/lib v1.0.0\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(primary, "vendor"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(primary, "vendor", "modules.txt"), []byte("# example.test/lib v1.0.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("p", primary), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckExecutionBaseDependencies(context.Background(), effective, primary); err != nil {
		t.Fatal(err)
	}
	if err := CheckExecutionBaseDependencies(context.Background(), effective, worktree); !errors.Is(err, ErrExecutionBaseDependencies) || diagnosticReason(err) != "execution_base_dependencies" {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(worktree, "vendor")); !os.IsNotExist(err) {
		t.Fatal("worktree was modified")
	}
}

func TestPlannerChecksExecutionBaseBeforeProvider(t *testing.T) {
	request, evidence, coordinator := plannerFixture(t)
	checked := false
	runner := PlannerRunner{Store: evidence, Coordinator: coordinator, ExecutionBaseReadiness: func(_ context.Context, _ config.Effective, path string) error {
		checked = path == request.Worktree.Path
		return diagnosticError{code: "execution_base_dependencies", err: ErrExecutionBaseDependencies}
	}}
	if _, err := runner.RunArtifact(context.Background(), request); !errors.Is(err, ErrExecutionBaseDependencies) || !checked || coordinator.calls != 0 {
		t.Fatalf("err=%v checked=%v calls=%d", err, checked, coordinator.calls)
	}
}
