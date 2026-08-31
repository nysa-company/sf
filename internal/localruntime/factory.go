// Package localruntime composes the pre-publication local workflow runtime.
// It is intentionally a leaf package: daemon owns lifecycle and Store owns
// authority, while this package only connects already-audited boundaries.
package localruntime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/daemon/runtimecontrol"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/repositoryexec"
	"github.com/nysa-company/sf/internal/runtimeassets"
	"github.com/nysa-company/sf/internal/workflowruntime"
	"github.com/nysa-company/sf/internal/workflowworker"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

const defaultInterval = 250 * time.Millisecond
const defaultWorkers = 2

// Config contains only process-local composition choices. Ticket limits,
// commands, providers, and merge policy remain frozen Store configuration.
type Config struct {
	Channel  domain.Channel
	GitHome  string
	Interval time.Duration
	Workers  int
}

type coreResolver func(domain.Channel) (runtimeassets.Core, error)

// Factory returns the daemon's atomic runtime/control factory. An unqualified
// coordinator deliberately produces the nil/nil idle bundle so doctor and
// provider qualification remain available without granting execution.
func Factory(configuration Config) daemon.WorkflowRuntimeFactory {
	return factory(configuration, runtimeassets.CurrentCore)
}

func factory(configuration Config, resolve coreResolver) daemon.WorkflowRuntimeFactory {
	return func(dependencies daemon.RuntimeDependencies) (daemon.WorkflowRuntimeComponents, error) {
		coordinator := dependencies.ProviderCoordinator
		if coordinator == nil {
			return daemon.WorkflowRuntimeComponents{}, nil
		}
		if err := coordinator.ReadyForPrePublishing(); err != nil {
			if errors.Is(err, providercoord.ErrPrePublishingNotReady) {
				return daemon.WorkflowRuntimeComponents{}, nil
			}
			return daemon.WorkflowRuntimeComponents{}, fmt.Errorf("provider runtime readiness: %w", err)
		}
		if dependencies.Store == nil || dependencies.Engine == nil || !configuration.Channel.Valid() || resolve == nil {
			return daemon.WorkflowRuntimeComponents{}, errors.New("local runtime requires channel, Store, Engine, and asset resolver")
		}
		if err := securePrivateDirectory(configuration.GitHome); err != nil {
			return daemon.WorkflowRuntimeComponents{}, fmt.Errorf("prepare isolated Git HOME: %w", err)
		}
		core, err := resolve(configuration.Channel)
		if err != nil {
			return daemon.WorkflowRuntimeComponents{}, fmt.Errorf("resolve local runtime assets: %w", err)
		}
		gitRunner := git.Runner{
			Binary:            "/usr/bin/git",
			Home:              configuration.GitHome,
			ExecHelper:        core.GitExec,
			MutationAuthority: dependencies.Store,
		}
		repositorySupervisor := processsupervisor.RepositoryCommandSupervisor{
			Executable: core.Executable,
			GitRunner:  gitRunner,
			SoftDrain:  2 * time.Second,
			HardDrain:  2 * time.Second,
		}
		materializer := workflowruntime.RepositoryMaterializer{
			Store: dependencies.Store,
			Git:   gitRunner,
			Executor: repositoryexec.Executor{
				Authority:  dependencies.Store,
				Supervisor: repositorySupervisor,
			},
		}
		phaseRunner := workflowruntime.NewPhaseRunner(dependencies.Store, coordinator)
		worker := workflowworker.Worker{
			Evidence:               dependencies.Store,
			Engine:                 dependencies.Engine,
			Runner:                 phaseRunner,
			Checkpoint:             materializer,
			Candidate:              materializer,
			CheckpointMaterializer: materializer,
			CandidateMaterializer:  materializer,
		}
		scheduler := workflowruntime.NewScheduler(
			configuration.Channel,
			workflowruntime.StoreTicketSource{Store: dependencies.Store},
			worktreecoord.Coordinator{Store: dependencies.Store, Git: gitRunner},
			worker,
		)
		interval := configuration.Interval
		if interval == 0 {
			interval = defaultInterval
		}
		workers := configuration.Workers
		if workers == 0 {
			workers = defaultWorkers
		}
		runtime, err := workflowruntime.NewRuntimeWithConfig(scheduler, workflowruntime.RuntimeConfig{Interval: interval, Workers: workers})
		if err != nil {
			return daemon.WorkflowRuntimeComponents{}, fmt.Errorf("compose workflow runtime: %w", err)
		}
		controller, err := runtimecontrol.New(dependencies.Store, runtime.ControlBundle(), nil)
		if err != nil {
			_ = runtime.Close()
			return daemon.WorkflowRuntimeComponents{}, fmt.Errorf("compose runtime controller: %w", err)
		}
		return daemon.WorkflowRuntimeComponents{Runtime: runtime, Controller: controller}, nil
	}
}

func securePrivateDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return errors.New("directory must be a clean absolute non-root path")
	}
	parent := filepath.Dir(path)
	parentInfo, err := os.Lstat(parent)
	if err != nil || parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() || parentInfo.Mode().Perm()&0o077 != 0 || !ownedByCurrentOrRoot(parentInfo) {
		return errors.New("directory parent must be a real owner-only directory")
	}
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o077 != 0 || !ownedByCurrentOrRoot(info) {
		return errors.New("directory must be a real owner-only directory")
	}
	return nil
}

func ownedByCurrentOrRoot(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (int(stat.Uid) == os.Getuid() || stat.Uid == 0)
}
