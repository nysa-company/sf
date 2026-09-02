package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/cli"
	"github.com/nysa-company/sf/internal/codexprovider"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/ghrunner"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/localruntime"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/version"
)

func main() {
	if len(os.Args) >= 3 && os.Args[1] == "__provider_gate" {
		// FD 3 is held by the supervisor until the launch PID/PGID is durably
		// recorded. EOF means the parent died before authority was published.
		gate := os.NewFile(uintptr(3), "provider-launch-gate")
		if gate == nil {
			os.Exit(125)
		}
		var one [1]byte
		if _, err := gate.Read(one[:]); err != nil {
			os.Exit(125)
		}
		if err := syscall.Exec(os.Args[2], os.Args[2:], os.Environ()); err != nil {
			os.Exit(126)
		}
		return
	}
	if len(os.Args) >= 3 && os.Args[1] == "__repository_command_gate" {
		gate := os.NewFile(uintptr(3), "repository-command-launch-gate")
		if gate == nil {
			os.Exit(125)
		}
		var one [1]byte
		if _, err := gate.Read(one[:]); err != nil {
			os.Exit(125)
		}
		if err := syscall.Fchdir(4); err != nil {
			os.Exit(125)
		}
		if err := syscall.Exec(os.Args[2], os.Args[2:], os.Environ()); err != nil {
			os.Exit(126)
		}
		return
	}
	if len(os.Args) >= 3 && os.Args[1] == "__repository_node_command_gate" {
		// FD 3 carries durable Store authority and FD 4 is the authenticated
		// launch root: the worktree for the ordinary recipe or the private staged
		// TypeScript closure for nysa_api_pure_v1. This gate is deliberately
		// separate from Go's test-wrapper gate: Node itself is the only executable
		// after Seatbelt starts.
		gate := os.NewFile(uintptr(3), "repository-node-launch-gate")
		if gate == nil {
			os.Exit(125)
		}
		var one [1]byte
		if _, err := gate.Read(one[:]); err != nil {
			os.Exit(125)
		}
		if err := syscall.Fchdir(4); err != nil {
			os.Exit(125)
		}
		target, err := filepath.EvalSymlinks(os.Args[2])
		if err != nil {
			os.Exit(125)
		}
		worktree, closure := os.Getenv("SF_REPOSITORY_NODE_WORKTREE"), os.Getenv("SF_REPOSITORY_NODE_CLOSURE")
		var profile string
		var profileErr error
		if rawFiles := os.Getenv("SF_REPOSITORY_NODE_FILES"); rawFiles != "" {
			var files []string
			if err := json.Unmarshal([]byte(rawFiles), &files); err != nil {
				os.Exit(125)
			}
			if files == nil {
				os.Exit(125)
			}
			profile, profileErr = processsupervisor.RepositoryNodeSandboxProfileForFiles(worktree, closure, target, files)
		} else {
			profile, profileErr = processsupervisor.RepositoryNodeSandboxProfile(worktree, closure, target)
		}
		if profileErr != nil || processsupervisor.ApplyRepositoryTestSandbox(profile) != nil {
			os.Exit(125)
		}
		syscall.CloseOnExec(3)
		syscall.CloseOnExec(4)
		if err := syscall.Exec(target, append([]string{target}, os.Args[3:]...), os.Environ()); err != nil {
			os.Exit(126)
		}
		return
	}
	if len(os.Args) >= 3 && os.Args[1] == "__repository_command_test_gate" {
		// Go's trusted driver invokes this wrapper for every test binary. Make
		// the binary its own process-group leader before the strict Seatbelt
		// profile starts it: a group leader cannot call setsid, and the profile
		// denies fork/exec. FDs 5/6 form a durable parent acknowledgement; the
		// target never inherits them.
		if err := syscall.Setpgid(0, 0); err != nil {
			os.Exit(125)
		}
		report, ack := os.NewFile(uintptr(5), "repository-test-group-report"), os.NewFile(uintptr(6), "repository-test-group-ack")
		if report == nil || ack == nil {
			os.Exit(125)
		}
		if _, err := fmt.Fprintf(report, "%d\n", os.Getpid()); err != nil {
			os.Exit(125)
		}
		var one [1]byte
		if _, err := ack.Read(one[:]); err != nil {
			os.Exit(125)
		}
		syscall.CloseOnExec(5)
		syscall.CloseOnExec(6)
		target, err := filepath.EvalSymlinks(os.Args[2])
		if err != nil {
			os.Exit(125)
		}
		profile, err := processsupervisor.RepositoryTestSandboxProfile(os.Getenv("SF_REPOSITORY_SANDBOX_REPOSITORY"), target)
		if err != nil {
			os.Exit(125)
		}
		if err := processsupervisor.ApplyRepositoryTestSandbox(profile); err != nil {
			os.Exit(125)
		}
		if err := syscall.Exec(target, append([]string{target}, os.Args[3:]...), os.Environ()); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "repository test sandbox exec:", err)
			os.Exit(126)
		}
		return
	}
	home, err := os.UserHomeDir()
	if err != nil {
		home = ""
	}
	channel := domain.Channel(version.Channel)
	if !channel.Valid() {
		channel = domain.ChannelStable
	}
	paths, _ := config.PathsFor(home, channel)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	runDaemon := func(runCtx context.Context) error {
		ownerHome, ghBinary, ghConfigDir, prePublishingOnly, err := publicationCapability(home, channel)
		if err != nil {
			return err
		}
		supervisor, err := processsupervisor.New(nil)
		if err != nil {
			return err
		}
		runErr := daemon.Run(runCtx, daemon.Config{
			Channel: channel, Paths: paths,
			DaemonIdentity:           fmt.Sprintf("sf/%s/%s", version.Version, version.Commit),
			RecoveryAuthorityKey:     supervisor.PublicKey(),
			ProviderSupervisor:       supervisor,
			RecoveryDrainer:          supervisor,
			GitMutationDrainer:       git.MutationDrainer{},
			RepositoryCommandDrainer: processsupervisor.RepositoryCommandDrainer{},
			ProviderCoordinatorFactory: func(database *store.Store, process contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
				return codexprovider.Compose(context.Background(), channel, database, process)
			},
			ProviderQualifier: func(qualifyCtx context.Context, database *store.Store, value domain.Channel, builder, reviewer string) (any, error) {
				return codexprovider.QualifyLocalPair(qualifyCtx, database, value, builder, reviewer, supervisor)
			},
			WorkflowRuntimeFactory: localruntime.Factory(localruntime.Config{
				Channel:           channel,
				GitHome:           filepath.Join(filepath.Dir(paths.Socket), "git-home"),
				OwnerHome:         ownerHome,
				GHConfigDir:       ghConfigDir,
				GHBinary:          ghBinary,
				GHAuthenticated:   !prePublishingOnly,
				PrePublishingOnly: prePublishingOnly,
				Workers:           2,
			}),
		})
		return errors.Join(runErr, supervisor.Close())
	}
	os.Exit(cli.ExecuteWithDaemon(ctx, os.Args[1:], os.Stdout, os.Stderr, cli.SocketClient{Path: paths.Socket}, runDaemon))
}

func existingDirectory(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.Mode()&os.ModeSymlink == 0 && info.IsDir()
}

// publicationCapability is intentionally evaluated only when daemon/run is
// requested. Direct commands such as doctor, version, and init must remain
// usable on a host without gh or GitHub authentication.
func publicationCapability(home string, channel domain.Channel) (ownerHome, ghBinary, ghConfigDir string, prePublishingOnly bool, err error) {
	ghBinary = ""
	if resolved, lookErr := exec.LookPath("gh"); lookErr == nil {
		ghBinary = resolved
	}
	ghConfigDir = filepath.Join(home, ".config", "gh")
	prePublishingOnly = home == "" || ghBinary == "" || !existingDirectory(ghConfigDir)
	if !prePublishingOnly {
		var authenticated bool
		authenticated, err = githubAuthenticated(ghBinary, home, ghConfigDir)
		if err != nil {
			return "", "", "", false, fmt.Errorf("GitHub capability preflight failed safely; run sf-%s doctor, install/repair gh, and authenticate GitHub: %w", channel, err)
		}
		prePublishingOnly = !authenticated
	}
	ownerHome = home
	if prePublishingOnly {
		// Qualification and local planning remain available when publication's
		// explicit gh/auth capability is absent. The runtime disables publishing
		// admission, rather than repeatedly reporting provider requalification.
		ownerHome, ghBinary, ghConfigDir = "", "", ""
	}
	return ownerHome, ghBinary, ghConfigDir, prePublishingOnly, nil
}

// githubAuthenticated is a read-only local capability probe. Its output is
// discarded and it never invokes login or writes the gh config. A missing or
// inactive login selects the explicit pre-publishing runtime so qualification
// can continue without admitting a ticket that would strand at publishing.
func githubAuthenticated(binary, home, configDir string) (bool, error) {
	runner, err := ghrunner.New(binary)
	if err != nil {
		return false, err
	}
	env := []string{
		"HOME=" + home,
		"GH_CONFIG_DIR=" + configDir,
		"GH_PROMPT_DISABLED=1",
		"GIT_TERMINAL_PROMPT=0",
		"NO_COLOR=1",
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, runErr := runner.Run(ctx, runner.Path(), []string{"auth", "status", "--active", "--hostname", "github.com"}, env)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cleanupCancel()
	proof, cleanupErr := runner.Cleanup(cleanupCtx)
	closeErr := runner.Close()
	if cleanupErr != nil || !proof.Drained || proof.Quarantined || closeErr != nil {
		cause := errors.Join(cleanupErr, closeErr)
		if cause == nil {
			cause = errors.New("invalid drain proof")
		}
		return false, fmt.Errorf("supervised gh auth cleanup was not proven: %w", cause)
	}
	return runErr == nil, nil
}
