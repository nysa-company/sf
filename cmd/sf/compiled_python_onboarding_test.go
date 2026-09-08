package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/pythonprepare"
	"github.com/nysa-company/sf/internal/store"
)

// Explicit opt-in performs pinned public downloads in a disposable clean HOME.
// This proves setup, not provider execution, PR delivery or human approval.
func TestCompiledPythonPreparationAndInit(t *testing.T) {
	if os.Getenv("SF_TEST_PYTHON_CLI_DOWNLOAD") != "1" {
		t.Skip("requires explicit credential-free public-download acceptance")
	}
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" {
		t.Skip("pinned macOS ARM64 profile")
	}
	binary := buildDevRuntimeBundle(t)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home, repo := filepath.Join(root, "home"), filepath.Join(root, "python-app")
	for _, p := range []string{home, repo, filepath.Join(repo, "tests")} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "pyproject.toml"), []byte("[project]\nname='python-app'\nversion='0.1.0'\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "tests/test_setup.py"), []byte("raise RuntimeError('setup must not execute project code')\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env := []string{"HOME=" + home, "PATH=/usr/bin:/bin:/usr/sbin:/sbin", "TMPDIR=" + root, "LANG=C", "CODEX_HOME=" + filepath.Join(root, "no-codex"), "GH_CONFIG_DIR=" + filepath.Join(root, "no-gh"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"}
	run := func(binary string, args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Dir = repo
		cmd.Env = env
		return cmd.CombinedOutput()
	}
	for _, args := range [][]string{{"init", "-b", "main"}, {"add", "."}, {"-c", "user.name=SF Test", "-c", "user.email=sf@example.invalid", "commit", "-m", "fixture"}} {
		if output, err := run("/usr/bin/git", args...); err != nil {
			t.Fatalf("git %v: %v %s", args, err, output)
		}
	}
	request := func(args ...string) api.Response {
		output, err := run(binary, append(args, "--json")...)
		var response api.Response
		if json.Unmarshal(output, &response) != nil || (err == nil) != response.OK {
			t.Fatalf("command %v: %v %s", args, err, output)
		}
		return response
	}
	preview := request("runtimes", "prepare", "python")
	if !preview.OK || preview.Mutation.Attempted {
		t.Fatal(preview)
	}
	refused := request("init", "--profile", "python-pytest-v1", "--test", "tests")
	if refused.OK || refused.Mutation.Attempted {
		t.Fatal("registered without prepared runtime", refused)
	}
	if entries, err := os.ReadDir(home); err != nil || len(entries) != 0 {
		t.Fatal("preview/unready init changed HOME", err)
	}
	prepared := request("runtimes", "prepare", "python", "--download")
	if !prepared.OK || !prepared.Mutation.Attempted {
		t.Fatal(prepared)
	}
	paths, _ := config.PathsFor(home, domain.ChannelDev)
	if _, err := os.Stat(paths.Database); !os.IsNotExist(err) {
		t.Fatal("preparation opened a database", err)
	}
	argv, _ := pythonprepare.RecipeArgv("tests")
	if err := pythonprepare.CheckRecipe(t.Context(), config.PythonSnapshotsPath(paths), argv); err != nil {
		t.Fatal("compiled preparation integrity", err)
	}
	if response := request("init", "--profile", "python-pytest-v1", "--test", "tests"); !response.OK {
		t.Fatal(response)
	}
	if response := request("init", "--profile", "python-pytest-v1", "--test", "tests"); !response.OK || !response.Mutation.Observed {
		t.Fatal("registration replay", response)
	}
	if response := request("init", "--check"); !response.OK || response.Mutation.Attempted || !strings.Contains(string(response.Data), "Python prepared") {
		t.Fatal("readiness preview", response)
	}
	if response := request("runtimes", "prepare", "python", "--download"); !response.OK || !strings.Contains(string(response.Data), "already prepared") {
		t.Fatal("preparation replay", response)
	}
	db, err := store.OpenReadOnly(t.Context(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	project, err := db.Project(t.Context(), domain.ChannelDev, "python-app")
	if err != nil || project.ConfigGeneration != 1 {
		t.Fatal(project, err)
	}
	frozen, err := config.DecodeSnapshot(project.ConfigSnapshot, project.ConfigDigest)
	if err != nil || strings.Join(frozen.Commands.Verify.Argv, "\x00") != strings.Join(argv, "\x00") {
		t.Fatal("wrong frozen command", err)
	}
	stable, _ := config.PathsFor(home, domain.ChannelStable)
	if _, err := os.Lstat(stable.Root); !os.IsNotExist(err) {
		t.Fatal("stable channel changed", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	compiledOnboardingVisibleQueuedTicket(t, binary, home, repo, "python-app", env)
}
