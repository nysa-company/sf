package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

type InitRequest struct {
	Channel domain.Channel
	Project string
	Repo    string
	Home    string
	Paths   config.ChannelPaths
}

type initResult struct {
	Channel      domain.Channel   `json:"channel"`
	Project      string           `json:"project"`
	Repository   string           `json:"repository"`
	BaseBranch   string           `json:"base_branch"`
	MergeMode    domain.MergeMode `json:"merge_mode"`
	ConfigDigest string           `json:"config_digest"`
	Created      bool             `json:"created"`
}

// RunInit is the direct local setup path. It never contacts the daemon or a
// remote and never writes into the registered repository.
func RunInit(ctx context.Context, request InitRequest) api.Response {
	binary := binaryForChannel(request.Channel)
	initHelp := []string{binary, "init", "--help"}
	if !request.Channel.Valid() || request.Project == "" || request.Repo == "" {
		return failure("invalid_argument", "channel, project, and repository are required", initHelp)
	}
	repository, err := canonicalGitRepository(ctx, request.Repo)
	if err != nil {
		return failure("invalid_repository", "repository registration failed: "+err.Error(), []string{binary, "doctor", "--repo", request.Repo})
	}
	paths := request.Paths
	if paths.Root == "" {
		home := request.Home
		if home == "" {
			home, err = os.UserHomeDir()
			if err != nil {
				return failure("init_failed", "current-user home directory is unavailable", initHelp)
			}
		}
		paths, err = config.PathsFor(home, request.Channel)
		if err != nil {
			return failure("init_failed", "channel paths could not be resolved", initHelp)
		}
	}
	if err := config.PrepareChannel(paths); err != nil {
		return initFailure("init_failed", "channel state could not be prepared: "+err.Error(), []string{binary, "doctor"}, false)
	}
	machine, err := config.LoadMachine(paths.Machine)
	if err != nil {
		return initFailure("invalid_configuration", err.Error(), initHelp, false)
	}
	effective, snapshot, digest, err := config.LoadProject(repository, request.Project, machine)
	if err != nil {
		return initFailure("invalid_configuration", err.Error(), initHelp, false)
	}
	if err := verifyBaseRef(ctx, repository, effective.BaseBranch); err != nil {
		return initFailure("invalid_repository", "configured base branch is unavailable in the local repository", []string{binary, "doctor", "--repo", repository}, false)
	}
	database, err := store.OpenChannel(ctx, paths.Database, paths.Backups, request.Channel)
	if err != nil {
		return initFailure("init_failed", "local state could not be opened", []string{binary, "doctor"}, true)
	}
	defer database.Close()
	created, err := database.RegisterProject(ctx, store.Project{
		Channel: request.Channel, ID: domain.ProjectID(request.Project), Path: repository, BaseRef: effective.BaseBranch,
		ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: snapshot,
	})
	if err != nil {
		if errors.Is(err, store.ErrProjectConflict) {
			return initFailure("project_conflict", "project name already has a different durable registration", []string{binary, "config", "--help"}, true)
		}
		if errors.Is(err, store.ErrBusy) {
			return initFailure("store_busy", "local state is busy; registration was not confirmed", []string{binary, "init", "--project", request.Project, "--repo", repository}, true)
		}
		return initFailure("init_failed", "project registration was not committed", []string{binary, "doctor"}, true)
	}
	data, err := json.Marshal(initResult{Channel: request.Channel, Project: request.Project, Repository: repository, BaseBranch: effective.BaseBranch, MergeMode: effective.MergeMode, ConfigDigest: digest, Created: created})
	if err != nil {
		return initFailure("init_failed", "registration succeeded but its response could not be encoded", []string{binary, "doctor"}, true)
	}
	return api.Response{
		Version: api.Version, RequestID: requestID(), OK: true,
		Mutation: api.Mutation{Attempted: true, Kind: "project.register", Identity: string(request.Channel) + "/" + request.Project, Observed: !created},
		Data:     data,
	}
}

func initFailure(code, message string, argv []string, attempted bool) api.Response {
	response := failure(code, message, argv)
	response.Mutation = api.Mutation{Attempted: attempted, Kind: "project.register"}
	return response
}

func binaryForChannel(channel domain.Channel) string {
	if channel == domain.ChannelDev {
		return "sf-dev"
	}
	return "sf"
}

func canonicalGitRepository(ctx context.Context, input string) (string, error) {
	if !filepath.IsAbs(input) || filepath.Clean(input) != input || strings.ContainsAny(input, "\x00\r\n\t") {
		return "", errors.New("repository path must be absolute and clean")
	}
	canonical, err := filepath.EvalSymlinks(input)
	if err != nil {
		return "", errors.New("repository path is unavailable")
	}
	canonical, err = filepath.Abs(canonical)
	if err != nil {
		return "", errors.New("repository path cannot be canonicalized")
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.IsDir() {
		return "", errors.New("repository path is not a directory")
	}
	output, err := boundedGit(ctx, canonical, "rev-parse", "--path-format=absolute", "--show-toplevel")
	if err != nil {
		return "", errors.New("path is not a Git worktree")
	}
	top := strings.TrimSpace(string(output))
	resolvedTop, err := filepath.EvalSymlinks(top)
	if err != nil || filepath.Clean(resolvedTop) != filepath.Clean(canonical) {
		return "", errors.New("path must name the Git worktree root")
	}
	return filepath.Clean(canonical), nil
}

func verifyBaseRef(ctx context.Context, repository, base string) error {
	for _, reference := range []string{"refs/heads/" + base + "^{commit}", "refs/remotes/origin/" + base + "^{commit}"} {
		if _, err := boundedGit(ctx, repository, "rev-parse", "--verify", "--quiet", reference); err == nil {
			return nil
		}
	}
	return errors.New("base ref not found")
}

func boundedGit(parent context.Context, repository string, arguments ...string) ([]byte, error) {
	git, err := exec.LookPath("git")
	if err != nil {
		return nil, err
	}
	git, err = filepath.Abs(git)
	if err != nil {
		return nil, err
	}
	git, err = filepath.EvalSymlinks(git)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(git)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("git executable is not a protected regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 && stat.Uid != uint32(os.Getuid()) {
		return nil, errors.New("git executable has an unexpected owner")
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	argv := append([]string{"-c", "core.hooksPath=/dev/null", "-C", repository}, arguments...)
	command := exec.CommandContext(ctx, git, argv...)
	command.Env = []string{
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
		"LC_ALL=C", "LANG=C", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0",
	}
	output, err := command.Output()
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("git probe deadline: %w", ctx.Err())
		}
		return nil, err
	}
	if len(output) > 16*1024 {
		return nil, errors.New("git probe output exceeds limit")
	}
	return output, nil
}
