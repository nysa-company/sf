package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/nysa-company/sf/internal/cli"
	"github.com/nysa-company/sf/internal/codexprovider"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/domain"
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
		// worktree. This gate is deliberately separate from Go's test-wrapper
		// gate: Node itself is the only executable after Seatbelt starts.
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
		profile, err := processsupervisor.RepositoryNodeSandboxProfile(os.Getenv("SF_REPOSITORY_NODE_WORKTREE"), os.Getenv("SF_REPOSITORY_NODE_CLOSURE"), target)
		if err != nil || processsupervisor.ApplyRepositoryTestSandbox(profile) != nil {
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
				Channel: channel,
				GitHome: filepath.Join(filepath.Dir(paths.Socket), "git-home"),
				Workers: 2,
			}),
		})
		return errors.Join(runErr, supervisor.Close())
	}
	os.Exit(cli.ExecuteWithDaemon(ctx, os.Args[1:], os.Stdout, os.Stderr, cli.SocketClient{Path: paths.Socket}, runDaemon))
}
