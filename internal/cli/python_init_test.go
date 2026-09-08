package cli

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/pythonprepare"
	"github.com/nysa-company/sf/internal/store"
)

func pythonInitFixture(t *testing.T) (string, string) {
	t.Helper()
	repo := initializedRepository(t)
	if err := os.Mkdir(filepath.Join(repo, "tests"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "tests/test_ok.py"), []byte("def test_ok(): assert 1 == 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return repo, t.TempDir()
}

func TestPythonInitMissingRuntimeDoesNotRegisterOrWriteConfig(t *testing.T) {
	repo, home := pythonInitFixture(t)
	response := RunInit(t.Context(), InitRequest{Channel: domain.ChannelDev, Project: "python", Repo: repo, Home: home, Profile: config.PythonPytestV1Profile, TestPath: "tests"})
	if response.OK || response.Mutation.Attempted || response.Error == nil || response.Error.Code != "not_ready" {
		t.Fatal(response)
	}
	if _, err := os.Lstat(filepath.Join(repo, ".sf")); !os.IsNotExist(err) {
		t.Fatal("wrote config before readiness", err)
	}
	entries, err := os.ReadDir(home)
	if err != nil || len(entries) != 0 {
		t.Fatal("created state before readiness", err)
	}
}

func TestPythonInitWithPreparedRuntimeRegistersAndReplays(t *testing.T) {
	source := os.Getenv("SF_TEST_PYTHON_PREPARED")
	if source == "" {
		t.Skip("requires explicitly prepared Python cache; skip is not onboarding acceptance")
	}
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		t.Skip("pinned macOS ARM64 profile")
	}
	repo, home := pythonInitFixture(t)
	argv, _ := pythonprepare.RecipeArgv("tests")
	if err := pythonprepare.CheckRecipe(t.Context(), source, argv); err != nil {
		t.Fatal("source fixture", err)
	}
	paths, _ := config.PathsFor(home, domain.ChannelDev)
	cache, err := config.PreparePythonSnapshots(paths)
	if err != nil {
		t.Fatal(err)
	}
	catalog, _ := pythonprepare.DefaultCatalog(runtime.GOOS, runtime.GOARCH)
	if err := exec.CommandContext(t.Context(), "/bin/cp", "-Rp", filepath.Join(source, catalog.EnvironmentDigest[7:]), cache).Run(); err != nil {
		t.Fatal("copy authenticated fixture", err)
	}
	request := InitRequest{Channel: domain.ChannelDev, Project: "python", Repo: repo, Home: home, Profile: config.PythonPytestV1Profile, TestPath: "tests"}
	response := RunInit(t.Context(), request)
	if !response.OK {
		t.Fatal(response)
	}
	var result initResult
	if err := json.Unmarshal(response.Data, &result); err != nil || !result.ConfigCreated {
		t.Fatal(result, err)
	}
	db, err := store.Open(t.Context(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	project, err := db.Project(t.Context(), domain.ChannelDev, "python")
	db.Close()
	if err != nil || project.ConfigGeneration != 1 {
		t.Fatal(project, err)
	}
	frozen, err := config.DecodeSnapshot(project.ConfigSnapshot, project.ConfigDigest)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(frozen.Commands.Verify.Argv, "\x00") != strings.Join(argv, "\x00") {
		t.Fatal("wrong frozen recipe")
	}
	if replay := RunInit(t.Context(), request); !replay.OK || !replay.Mutation.Observed {
		t.Fatal(replay)
	}
	preview := RunInitCheck(t.Context(), InitRequest{Channel: domain.ChannelDev, Project: "python", Repo: repo, Home: home})
	if !preview.OK || preview.Mutation.Attempted || !strings.Contains(string(preview.Data), "Python prepared") {
		t.Fatal(preview)
	}
	if err := os.Rename(filepath.Join(cache, catalog.EnvironmentDigest[7:]), filepath.Join(cache, "retained")); err != nil {
		t.Fatal(err)
	}
	if refused := RunInit(t.Context(), request); refused.OK || refused.Error == nil || refused.Error.Code != "not_ready" {
		t.Fatal(refused)
	}
}
