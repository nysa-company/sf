// Package localruntime composes the local workflow and publication runtime.
// It is intentionally a leaf package: daemon owns lifecycle and Store owns
// authority, while this package only connects already-audited boundaries.
package localruntime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/daemon"
	"github.com/nysa-company/sf/internal/daemon/runtimecontrol"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/ghrunner"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/github"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/publication"
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
	Channel     domain.Channel
	GitHome     string
	OwnerHome   string
	GHConfigDir string
	GHBinary    string
	// GHAuthenticated is a sanitized result of the explicit, read-only
	// `gh auth status` preflight performed by cmd/sf. It is never a credential.
	GHAuthenticated bool
	// PrePublishingOnly is an explicit test/qualification composition. It
	// admits no remote publication capability; a publishing ticket is instead
	// durably blocked with a channel-specific doctor/install/auth action.
	PrePublishingOnly bool
	Interval          time.Duration
	Workers           int
}

type coreResolver func(domain.Channel) (runtimeassets.Core, error)
type publicationResolver func(domain.Channel, string) (runtimeassets.Publication, error)

// Factory returns the daemon's atomic runtime/control factory. An unqualified
// coordinator deliberately produces the nil/nil idle bundle so doctor and
// provider qualification remain available without granting execution.
func Factory(configuration Config) daemon.WorkflowRuntimeFactory {
	return factoryWithResolvers(configuration, runtimeassets.CurrentCore, runtimeassets.ResolvePublication)
}

func factory(configuration Config, resolve coreResolver) daemon.WorkflowRuntimeFactory {
	return factoryWithResolvers(configuration, resolve, runtimeassets.ResolvePublication)
}

func factoryWithResolvers(configuration Config, resolve coreResolver, resolvePublication publicationResolver) daemon.WorkflowRuntimeFactory {
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
		publicationAny := configuration.OwnerHome != "" || configuration.GHConfigDir != "" || configuration.GHBinary != ""
		publicationEnabled := configuration.OwnerHome != "" && configuration.GHConfigDir != "" && configuration.GHBinary != "" && configuration.GHAuthenticated
		if configuration.PrePublishingOnly {
			if publicationAny {
				return daemon.WorkflowRuntimeComponents{}, errors.New("pre-publishing runtime cannot configure publication capability")
			}
		} else if !publicationEnabled {
			return daemon.WorkflowRuntimeComponents{}, errors.New("local runtime publication capability is unavailable: owner HOME, gh config, gh executable, and active GitHub authentication are required; run the channel doctor, install gh, and authenticate GitHub")
		}
		var publicationAssets runtimeassets.Publication
		if publicationEnabled {
			if err := validateExistingDirectory(configuration.OwnerHome, "owner HOME"); err != nil {
				return daemon.WorkflowRuntimeComponents{}, err
			}
			if err := validateExistingDirectory(configuration.GHConfigDir, "gh config directory"); err != nil {
				return daemon.WorkflowRuntimeComponents{}, err
			}
			if resolvePublication == nil {
				return daemon.WorkflowRuntimeComponents{}, errors.New("local runtime requires publication asset resolver")
			}
			publicationAssets, err = resolvePublication(configuration.Channel, core.Executable)
			if err != nil {
				return daemon.WorkflowRuntimeComponents{}, fmt.Errorf("resolve publication runtime assets: %w", err)
			}
		}
		gitRunner := git.Runner{
			Binary:            "/usr/bin/git",
			Home:              configuration.GitHome,
			ExecHelper:        core.GitExec,
			MutationAuthority: dependencies.Store,
		}
		if publicationEnabled {
			gitRunner.CredentialHelper = publicationAssets.CredentialHelper
			gitRunner.GHConfigDir = configuration.GHConfigDir
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
		var runtimeWorker workflowruntime.Worker = Worker{
			Store: dependencies.Store, Engine: dependencies.Engine, Workflow: worker,
			PublicationEnabled: false,
		}
		var gh *ghrunner.Runner
		var mergeObserver runtimecontrol.MergeObserver
		if publicationEnabled {
			gh, err = ghrunner.New(configuration.GHBinary)
			if err != nil {
				return daemon.WorkflowRuntimeComponents{}, fmt.Errorf("compose gh runner: %w", err)
			}
			capability, capabilityErr := gh.CredentialCapability()
			if capabilityErr != nil {
				return daemon.WorkflowRuntimeComponents{}, fmt.Errorf("compose gh credential capability: %w", errors.Join(capabilityErr, gh.Close()))
			}
			// Git's helper receives only the authenticated, private snapshot;
			// GitHub API calls continue to pass the configured path to ghrunner,
			// which validates it and executes its own snapshot.
			gitRunner.GHBinary = capability.Path
			gitRunner.GHBinaryDigest = capability.Digest
			repositorySupervisor.GitRunner = gitRunner
			materializer.Git = gitRunner
			githubClient, clientErr := github.NewStoreClient(configuration.GHBinary, configuration.OwnerHome, configuration.GHConfigDir, gh, dependencies.Store, unavailableMergeVerifier{})
			if clientErr != nil {
				return daemon.WorkflowRuntimeComponents{}, fmt.Errorf("compose GitHub client: %w", errors.Join(clientErr, gh.Close()))
			}
			runtimeWorker = Worker{Store: dependencies.Store, Engine: dependencies.Engine, Workflow: worker, Publication: publication.Worker{Store: dependencies.Store, Git: gitRunner, GitHub: githubClient}, CI: CIWorker{Store: dependencies.Store, Observer: githubClient}, PublicationEnabled: true}
			mergeObserver = publishedMergeObserver{Store: dependencies.Store, GitHub: githubClient}
		}
		scheduler := workflowruntime.NewScheduler(
			configuration.Channel,
			workflowruntime.StoreTicketSource{Store: dependencies.Store},
			worktreecoord.Coordinator{Store: dependencies.Store, Git: gitRunner},
			runtimeWorker,
		)
		scheduler.AdmitPublishing = publicationEnabled
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
			if gh != nil {
				err = errors.Join(err, gh.Close())
			}
			return daemon.WorkflowRuntimeComponents{}, fmt.Errorf("compose workflow runtime: %w", err)
		}
		managed := &managedRuntime{runtime: runtime, gh: gh}
		controller, err := runtimecontrol.New(dependencies.Store, runtime.ControlBundle(), mergeObserver)
		if err != nil {
			return daemon.WorkflowRuntimeComponents{}, fmt.Errorf("compose runtime controller: %w", errors.Join(err, managed.Close()))
		}
		return daemon.WorkflowRuntimeComponents{Runtime: managed, Controller: controller}, nil
	}
}

// managedRuntime owns the exact construction pair. Runtime.Close joins all
// workflow activity before the gh runner is closed, ensuring no publication
// caller can race teardown of its process snapshot.
type managedRuntime struct {
	runtime *workflowruntime.Runtime
	gh      *ghrunner.Runner
}

func (r *managedRuntime) Start(ctx context.Context, fence domain.Fence) error {
	if r == nil || r.runtime == nil {
		return workflowruntime.ErrInvalidScheduler
	}
	return r.runtime.Start(ctx, fence)
}

func (r *managedRuntime) Close() error {
	if r == nil {
		return nil
	}
	var err error
	if r.runtime != nil {
		err = errors.Join(err, r.runtime.Close())
	}
	if r.gh != nil {
		err = errors.Join(err, r.gh.Close())
	}
	return err
}

type unavailableMergeVerifier struct{}

func (unavailableMergeVerifier) VerifyProtectedBranch(context.Context, contracts.RepositoryIdentity, string, string, string) (contracts.ProtectedBranchObservation, error) {
	return contracts.ProtectedBranchObservation{}, errors.New("protected-branch merge verification is not available in local publication runtime")
}

func validateExistingDirectory(path, label string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
		return fmt.Errorf("%s must be a clean absolute non-root path", label)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || !ownedByCurrentOrRoot(info) || info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%s must be an existing owner-controlled directory", label)
	}
	return nil
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
