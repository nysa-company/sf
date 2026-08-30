package cli

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestRunInitRegistersCanonicalProjectInSelectedChannelOnly(t *testing.T) {
	repository := initializedRepository(t)
	home := t.TempDir()
	response := RunInit(context.Background(), InitRequest{Channel: domain.ChannelDev, Project: "nysa", Repo: repository, Home: home})
	if !response.OK || !response.Mutation.Attempted || response.Mutation.Kind != "project.register" || response.Mutation.Observed {
		t.Fatalf("response=%+v", response)
	}
	var result initResult
	if err := json.Unmarshal(response.Data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Channel != domain.ChannelDev || result.Project != "nysa" || result.Repository != repository || result.BaseBranch != "main" || result.MergeMode != domain.MergeGuarded || !result.Created || len(result.ConfigDigest) != 64 {
		t.Fatalf("result=%+v", result)
	}
	paths, _ := config.PathsFor(home, domain.ChannelDev)
	database, err := store.Open(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	project, err := database.Project(context.Background(), domain.ChannelDev, "nysa")
	_ = database.Close()
	if err != nil || project.Path != repository || project.ConfigGeneration != 1 || project.ConfigDigest != result.ConfigDigest || len(project.ConfigSnapshot) == 0 {
		t.Fatalf("project=%+v err=%v", project, err)
	}
	stable, _ := config.PathsFor(home, domain.ChannelStable)
	if _, err := os.Lstat(stable.Root); !os.IsNotExist(err) {
		t.Fatalf("stable state was touched: %v", err)
	}

	replay := RunInit(context.Background(), InitRequest{Channel: domain.ChannelDev, Project: "nysa", Repo: repository, Home: home})
	if !replay.OK || !replay.Mutation.Observed {
		t.Fatalf("replay=%+v", replay)
	}
	if err := json.Unmarshal(replay.Data, &result); err != nil || result.Created {
		t.Fatalf("replay result=%+v err=%v", result, err)
	}
}

func TestRunInitFailsClosedOnConflictAndUnsafeConfiguration(t *testing.T) {
	home := t.TempDir()
	first := initializedRepository(t)
	second := initializedRepository(t)
	if response := RunInit(context.Background(), InitRequest{Channel: domain.ChannelDev, Project: "nysa", Repo: first, Home: home}); !response.OK {
		t.Fatalf("first=%+v", response)
	}
	conflict := RunInit(context.Background(), InitRequest{Channel: domain.ChannelDev, Project: "nysa", Repo: second, Home: home})
	assertInitFailure(t, conflict, "project_conflict", "sf-dev")

	unsafe := initializedRepository(t)
	if err := os.Mkdir(filepath.Join(unsafe, ".sf"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unsafe, ".sf", "config.toml"), []byte("unrestricted = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	invalid := RunInit(context.Background(), InitRequest{Channel: domain.ChannelDev, Project: "unsafe", Repo: unsafe, Home: home})
	assertInitFailure(t, invalid, "invalid_configuration", "sf-dev")
}

func TestRunInitRejectsSubdirectoryAndMissingBase(t *testing.T) {
	home := t.TempDir()
	repository := initializedRepository(t)
	subdirectory := filepath.Join(repository, "nested")
	if err := os.Mkdir(subdirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	response := RunInit(context.Background(), InitRequest{Channel: domain.ChannelStable, Project: "nysa", Repo: subdirectory, Home: home})
	assertInitFailure(t, response, "invalid_repository", "sf")

	if err := os.Mkdir(filepath.Join(repository, ".sf"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, ".sf", "config.toml"), []byte("base_branch = \"missing\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	response = RunInit(context.Background(), InitRequest{Channel: domain.ChannelStable, Project: "nysa", Repo: repository, Home: home})
	assertInitFailure(t, response, "invalid_repository", "sf")
}

func TestRunInitMachinePolicyNarrowsDefaultProject(t *testing.T) {
	repository := initializedRepository(t)
	home := t.TempDir()
	paths, _ := config.PathsFor(home, domain.ChannelDev)
	if err := config.PrepareChannel(paths); err != nil {
		t.Fatal(err)
	}
	policy := "max_concurrent_tickets = 1\nmax_phase_timeout = \"10m\"\nmax_ticket_timeout = \"20m\"\nmax_ticket_cost_usd = 5\nallow_autonomous = false\n"
	if err := os.WriteFile(paths.Machine, []byte(policy), 0o600); err != nil {
		t.Fatal(err)
	}
	response := RunInit(context.Background(), InitRequest{Channel: domain.ChannelDev, Project: "nysa", Repo: repository, Home: home})
	if !response.OK {
		t.Fatalf("response=%+v", response)
	}
}

func TestRunInitReportsActionableConfigForUnsupportedRepository(t *testing.T) {
	repository := initializedRepository(t)
	if err := os.Remove(filepath.Join(repository, "go.mod")); err != nil {
		t.Fatal(err)
	}
	response := RunInit(context.Background(), InitRequest{Channel: domain.ChannelDev, Project: "unsupported", Repo: repository, Home: t.TempDir()})
	if response.OK || response.Error == nil || response.Error.Code != "invalid_configuration" || response.NextAction == nil || len(response.NextAction.Argv) != 3 || response.NextAction.Argv[0] != "sf-dev" || response.NextAction.Argv[1] != "config" || response.NextAction.Argv[2] != "--help" {
		t.Fatalf("response=%+v", response)
	}
}

func assertInitFailure(t *testing.T, response api.Response, code, binary string) {
	t.Helper()
	if response.OK || response.Error == nil || response.Error.Code != code || response.NextAction == nil || len(response.NextAction.Argv) == 0 || response.NextAction.Argv[0] != binary {
		t.Fatalf("response=%+v", response)
	}
	if err := validateCLIResponse(response); err != nil {
		t.Fatalf("invalid failure response: %v", err)
	}
}

func initializedRepository(t *testing.T) string {
	t.Helper()
	repository := t.TempDir()
	runGitTest(t, repository, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repository, "go.mod"), []byte("module example.test\n\ngo 1.25\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGitTest(t, repository, "add", "README.md")
	runGitTest(t, repository, "-c", "user.name=SF Test", "-c", "user.email=sf@example.invalid", "commit", "-m", "fixture")
	resolved, err := filepath.EvalSymlinks(repository)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(resolved)
}

func runGitTest(t *testing.T, repository string, arguments ...string) {
	t.Helper()
	argv := append([]string{"-C", repository}, arguments...)
	command := exec.Command("git", argv...)
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", arguments, err, output)
	}
}
