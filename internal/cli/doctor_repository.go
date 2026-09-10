package cli

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	localauth "github.com/nysa-company/sf/internal/auth"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/ghrunner"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/gitcredential"
	"github.com/nysa-company/sf/internal/runtimeassets"
	"github.com/nysa-company/sf/internal/store"
)

var errDoctorRepositoryAccess = errors.New("repository base read unavailable")

func checkDoctorRepositoryAccess(ctx context.Context, deps DoctorDeps, report *DoctorReport) {
	check := DoctorCheck{ID: "repository_access", Status: CheckNotRun, Summary: "repository base read was not tested; read/write permissions are not established by local transport checks"}
	defer func() { report.Checks = append(report.Checks, check) }()
	if deps.Repo == "" || deps.RepositoryBase == nil || deps.RepositoryAccess == nil || !doctorChecksPass(*report, "repository_worktree", "git_transport") {
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	base, err := deps.RepositoryBase(probeCtx, deps.Repo)
	if errors.Is(err, store.ErrNotFound) || (err == nil && base == "") {
		check.Summary = "no registered project base is available; repository read was not tested"
		return
	}
	if err != nil {
		check = failedCheck("repository_access", "registered project base could not be authenticated; inspect project registration before retrying Doctor", deps.Binary, "init", "--help")
		return
	}
	if err := deps.RepositoryAccess(probeCtx, deps.Repo, base); err != nil {
		check = failedCheck("repository_access", "production Git transport could not read the registered repository base; verify GitHub repository access and the configured base, then retry Doctor", deps.Binary, "doctor", "--help")
		var diagnostic interface{ RuntimeDiagnosticCode() string }
		if errors.As(err, &diagnostic) {
			code := diagnostic.RuntimeDiagnosticCode()
			if summary := contracts.RuntimeDiagnosticSummary(code); strings.HasPrefix(code, "git_") && summary != "" {
				check.Summary = summary
			}
		}
		return
	}
	check.Status = CheckPass
	check.Summary = "production Git transport read the registered repository base without fetching; write permission and later ticket execution remain unverified"
}

func productionDoctorRepositoryBase(channel domain.Channel, databasePath string) func(context.Context, string) (string, error) {
	return func(ctx context.Context, repository string) (string, error) {
		canonical, err := filepath.EvalSymlinks(repository)
		if err != nil {
			return "", errDoctorRepositoryAccess
		}
		db, err := store.OpenReadOnly(ctx, databasePath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return "", store.ErrNotFound
			}
			return "", errDoctorRepositoryAccess
		}
		defer db.Close()
		projects, err := db.Projects(ctx, channel)
		if err != nil {
			return "", errDoctorRepositoryAccess
		}
		return doctorRegisteredBase(projects, channel, canonical)
	}
}

func doctorRegisteredBase(projects []store.Project, channel domain.Channel, repository string) (string, error) {
	base := ""
	for _, project := range projects {
		if project.Path != repository {
			continue
		}
		frozen, err := config.DecodeSnapshot(project.ConfigSnapshot, project.ConfigDigest)
		if err != nil || project.Channel != channel || project.ConfigGeneration == 0 || frozen.Repository != repository || frozen.BaseBranch != project.BaseRef || project.BaseRef == "" || base != "" {
			return "", errDoctorRepositoryAccess
		}
		base = project.BaseRef
	}
	if base == "" {
		return "", store.ErrNotFound
	}
	return base, nil
}

// This probe composes the same pinned Git/gh/credential capabilities as the
// local runtime. No mutation authority is supplied; ObserveRepositoryBase only
// inspects the primary checkout and performs a remote ref read.
func productionDoctorRepositoryAccess(channel domain.Channel) func(context.Context, string, string) error {
	return func(ctx context.Context, repository, base string) (resultErr error) {
		probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if probeCtx.Err() != nil {
			return errDoctorRepositoryAccess
		}
		core, err := runtimeassets.CurrentCore(channel)
		if err != nil {
			return errDoctorRepositoryAccess
		}
		publication, err := runtimeassets.ResolvePublication(channel, core.Executable)
		if err != nil {
			return errDoctorRepositoryAccess
		}
		home, err := os.UserHomeDir()
		if err != nil || !gitcredential.TrustedHome(home) {
			return errDoctorRepositoryAccess
		}
		configDir, err := localauth.ExistingGitHubConfigDirectory(home, os.Getenv("GH_CONFIG_DIR"), os.Getenv("XDG_CONFIG_HOME"))
		if err != nil || configDir == "" {
			return errDoctorRepositoryAccess
		}
		binary, err := exec.LookPath("gh")
		if err != nil {
			return errDoctorRepositoryAccess
		}
		gh, err := ghrunner.New(binary)
		if err != nil {
			return errDoctorRepositoryAccess
		}
		defer func() {
			if gh.Close() != nil {
				resultErr = errDoctorRepositoryAccess
			}
		}()
		capability, err := gh.CredentialCapability()
		if err != nil {
			return errDoctorRepositoryAccess
		}
		privateHome, err := os.MkdirTemp("", "sf-doctor-git-")
		if err != nil {
			return errDoctorRepositoryAccess
		}
		defer os.RemoveAll(privateHome)
		// macOS temp paths may pass through /var's symlink. Git requires the
		// canonical private directory, never the operator's Git HOME.
		privateHome, err = filepath.EvalSymlinks(privateHome)
		if err != nil {
			return errDoctorRepositoryAccess
		}
		runner := git.Runner{Binary: "/usr/bin/git", Home: privateHome, ExecHelper: core.GitExec,
			CredentialHelper: publication.CredentialHelper, GHHome: home, GHConfigDir: configDir,
			GHBinary: capability.Path, GHBinaryDigest: capability.Digest}
		if socket := os.Getenv("SSH_AUTH_SOCK"); socket != "" {
			name := "sf-ssh"
			if channel == domain.ChannelDev {
				name += "-dev"
			}
			runner.SSHHelper = filepath.Join(filepath.Dir(core.Executable), name)
			runner.SSHKnownHosts = filepath.Join(filepath.Dir(core.Executable), "github_known_hosts")
			runner.SSHBinary, runner.SSHAgentSock = "/usr/bin/ssh", socket
		}
		if _, _, err := runner.ObserveRepositoryBase(probeCtx, repository, base); err != nil {
			return err
		}
		return nil
	}
}
