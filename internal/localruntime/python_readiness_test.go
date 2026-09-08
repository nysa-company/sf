package localruntime

import (
	"errors"
	"os"
	"runtime"
	"testing"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/pythonprepare"
	"github.com/nysa-company/sf/internal/store"
)

func TestProjectStartPythonRequiresPreparedCurrentCatalog(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS runtime readiness")
	}
	argv, err := pythonprepare.RecipeArgv("tests")
	if err != nil {
		t.Fatal(err)
	}
	project := config.DefaultProject("python", "/tmp/python")
	project.Commands.Verify.Argv = argv
	project.Commands.Review.Argv = append([]string(nil), argv...)
	effective, err := config.Resolve(config.DefaultMachineLimits(), project, config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	payload, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	stored := store.Project{Channel: domain.ChannelDev, ID: "python", ConfigGeneration: 1, ConfigSnapshot: payload, ConfigDigest: digest}
	if err := CheckProjectStart(t.Context(), stored); !errors.Is(err, daemon.ErrStartRecipeUnsupported) {
		t.Fatal("missing explicit composition accepted", err)
	}
	if err := ProjectStartChecker(t.TempDir())(t.Context(), stored); !errors.Is(err, daemon.ErrStartRecipeUnsupported) {
		t.Fatal("empty directory accepted", err)
	}
	root := os.Getenv("SF_TEST_PYTHON_PREPARED")
	if root == "" {
		t.Skip("positive readiness requires explicit prepared cache")
	}
	if err := ProjectStartChecker(root)(t.Context(), stored); err != nil {
		t.Fatal("verified catalog refused", err)
	}
	effective.Commands.Review.Argv = append([]string(nil), argv...)
	effective.Commands.Review.Argv[2] = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	stored.ConfigSnapshot, stored.ConfigDigest, err = config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := ProjectStartChecker(root)(t.Context(), stored); !errors.Is(err, daemon.ErrStartRecipeUnsupported) {
		t.Fatal("mismatched review recipe accepted", err)
	}
}
