// Package daemon owns the foreground, single-owner local daemon boundary. It
// coordinates durable recovery and admits provider or repository capabilities
// only through explicitly supplied, fail-closed runtime components.
package daemon

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/engine"
	"github.com/nysa-company/sf/internal/events"
	"github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/leader"
	"github.com/nysa-company/sf/internal/operator"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/redact"
	"github.com/nysa-company/sf/internal/statemachine"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/ticket"
	"github.com/nysa-company/sf/internal/transport"
)

// TicketIDGenerator permits deterministic test identities without weakening
// production's cryptographic random source.
type TicketIDGenerator interface {
	NewTicketID(channel domain.Channel) (domain.TicketID, error)
}

type RandomTicketIDs struct{ Reader io.Reader }

func (generator RandomTicketIDs) NewTicketID(channel domain.Channel) (domain.TicketID, error) {
	reader := generator.Reader
	if reader == nil {
		reader = rand.Reader
	}
	var bytes [16]byte
	if _, err := io.ReadFull(reader, bytes[:]); err != nil {
		return "", fmt.Errorf("read ticket randomness: %w", err)
	}
	// The SF prefix is the stable operator-facing identity; channel separation
	// is provided by the per-channel store and is part of the durable reference.
	_ = channel // channel separation is provided by the per-channel store.
	return domain.TicketID("SF-" + hex.EncodeToString(bytes[:])), nil
}

type Clock interface{ Now() time.Time }
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC() }

// RuntimeController is the narrow operator-control handoff to the process and
// remote-effect runtime. Drain must return true only after every writer owned
// by the ticket is gone or quarantined in a way that forbids continuation.
// MergeObserved is read-only and is checked both before cancellation and after
// draining so a concurrent external merge cannot be mislabeled cancelled.
type RuntimeController interface {
	Drain(context.Context, domain.TicketRef) (bool, error)
	MergeObserved(context.Context, domain.TicketRef) (bool, error)
}

type RuntimeRearmController interface {
	Rearm(context.Context, domain.TicketRef) error
}

// RuntimeRearmStateController lets a concrete runtime expose the sole safe
// retry window for a resume whose durable transition committed before runtime
// admission was installed.
type RuntimeRearmStateController interface {
	RuntimeRearmNeeded(context.Context, domain.TicketRef) (bool, error)
}

// RuntimeMergeRetryReplayController proves that an already-active merge or
// reconciliation ticket is the exact durable result of a prior semantic operator_retry. It is
// deliberately separate from RuntimeRearmStateController: an unrelated
// sealed pause/resume must never be replayed by the retry command.
type RuntimeMergeRetryReplayController interface {
	GuardedMergeRetryReplay(context.Context, domain.TicketRef) (store.GuardedMergeRetryReplayState, error)
}

// RuntimeRetirementController reclaims terminal-only runtime control state.
// It is separate from RuntimeController so an intentionally no-runtime
// composition need not pretend to own workflow admission state.
type RuntimeRetirementController interface {
	Retire(context.Context, domain.TicketRef) error
}

// RuntimeTakeoverInspector is the read-only authenticated Git/worktree
// boundary used for operator handoff and resume classification.
type RuntimeTakeoverInspector interface {
	InspectTakeover(context.Context, domain.TicketRef) (contracts.TakeoverInspection, error)
}

// RuntimeProviderRetryWorktreePreflight is the local, read-only filesystem
// proof required before an exhausted provider phase is reopened. A failed
// provider may have written files before returning an invalid artifact; the
// retry transition must not be consumed until the retained registered
// worktree reauthenticates as pristine.
type RuntimeProviderRetryWorktreePreflight interface {
	AuthenticateProviderRetryWorktree(context.Context, domain.TicketRef, uint64, domain.Fence) (bool, error)
}

// RuntimeProviderRetryRearmController is the sealed, provider-specific
// admission handoff. It repeats the active checkout proof and redeems Store's
// exact retry capability; generic pre/post-publication rearm authority cannot
// substitute for this boundary.
type RuntimeProviderRetryRearmController interface {
	RearmProviderRetry(context.Context, domain.TicketRef, uint64, domain.Fence) (bool, error)
}

// WorkflowRuntime is the daemon lifecycle boundary for a composed workflow
// runtime. Implementations own their goroutines and must not return from
// Close until those goroutines have stopped. The fence is supplied by the
// foreground daemon, never invented by the runtime.
type WorkflowRuntime interface {
	Start(context.Context, domain.Fence) error
	Close() error
}

// RuntimeDependencies are the already-open, authenticated authorities made
// available to a future runtime composition. The Store is an API boundary;
// this type deliberately exposes no database handle or SQL connection.
type RuntimeDependencies struct {
	Store               *store.Store
	Engine              *engine.Engine
	ProviderCoordinator *providercoord.Coordinator
}

// WorkflowRuntimeComponents is the atomic execution handoff. A factory must
// return both Runtime and Controller or neither. The Controller is built for
// Runtime's exact control bundle; the daemon never falls back to an idle
// controller for an executable Runtime.
type WorkflowRuntimeComponents struct {
	Runtime    WorkflowRuntime
	Controller RuntimeController
}

// WorkflowRuntimeFactory is called only after durable recovery and event
// projection have completed, and before the owner socket is exposed.
type WorkflowRuntimeFactory func(RuntimeDependencies) (WorkflowRuntimeComponents, error)

// ErrAlreadyServing keeps the foreground lifecycle single-owner. A second
// Serve call must not be allowed to outlive the call that joins the runtime.
var ErrAlreadyServing = errors.New("daemon serve lifecycle already started")

// idleRuntimeController is safe only for the current composition, which has
// no provider/Git/GitHub runner. A later composition that can launch work must
// inject its real supervisor and effect observer.
type idleRuntimeController struct{}

func (idleRuntimeController) Drain(context.Context, domain.TicketRef) (bool, error) {
	return true, nil
}
func (idleRuntimeController) MergeObserved(context.Context, domain.TicketRef) (bool, error) {
	return false, nil
}

type Config struct {
	Channel          domain.Channel
	Paths            config.ChannelPaths
	StateMachinePath string
	DaemonIdentity   string
	Projects         []store.Project
	TicketIDs        TicketIDGenerator
	Clock            Clock
	Operator         operator.Authenticator
	Controller       RuntimeController
	// Doctor is an injected local preflight. A nil doctor means only the
	// daemon's local storage/leader checks are required; no provider or Git
	// integration is implied by that default.
	Doctor func(context.Context, store.Project) error
	// StartupTimeout bounds migrations, recovery, and all startup-only SQLite
	// operations even when the process context is long-lived.
	StartupTimeout time.Duration
	// RecoverProvider must drain the exact provider process group represented by
	// a durable attempt. A nil callback fails closed when claims exist.
	RecoverProvider      func(context.Context, store.ProviderAttempt, uint64) error
	RecoveryAuthorityKey []byte
	// ProviderSupervisor is the production process-group supervisor. Start
	// installs its durable launch recorder before recovery or socket exposure;
	// the coordinator may install the same recorder when it is composed on its
	// own in tests or future worker entrypoints.
	ProviderSupervisor contracts.ProcessSupervisor
	RecoveryDrainer    interface {
		DrainPersisted(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) (contracts.DrainProof, error)
	}
	// GitMutationDrainer is the startup-only verifier for persisted Git helper
	// process groups. A missing verifier is acceptable only when no Git lease
	// exists; otherwise recovery refuses before socket/listener exposure.
	GitMutationDrainer contracts.GitMutationDrainer
	// PreparedCommitObserver is a read-only adapter used after Git lease and
	// effect recovery. It must authenticate a registered worktree before
	// observing HEAD; a missing adapter leaves a prepared commit uncertain.
	PreparedCommitObserver contracts.PreparedCommitObserver
	// GitRunner supplies the read-only Runner used by the default registered
	// worktree adapter. A nil runner is acceptable only when there are no
	// uncertain prepared commits or an explicit observer is supplied.
	GitRunner *git.Runner
	// RepositoryCommandDrainer proves persisted credential-free command
	// identities before effects, runners, or the socket are exposed.
	RepositoryCommandDrainer contracts.RepositoryCommandDrainer
	// ProviderCoordinatorFactory composes configured, qualified adapters after
	// the daemon opens the authoritative Store. A nil factory leaves provider
	// execution unavailable rather than inventing an adapter.
	ProviderCoordinatorFactory func(*store.Store, contracts.ProcessSupervisor) (*providercoord.Coordinator, error)
	// ProviderQualifier is invoked only through this authenticated foreground
	// daemon, after its supervisor key is current in SQLite.
	ProviderQualifier func(context.Context, *store.Store, domain.Channel, string, string) (any, error)
	// WorkflowRuntimeFactory atomically composes an executable runtime and its
	// exact controller after Store, Engine, and the optional provider
	// coordinator exist. Nil intentionally means runtime execution is
	// unavailable for this composition. Controller and factory are mutually
	// exclusive so a real runtime cannot be paired with unrelated control.
	WorkflowRuntimeFactory WorkflowRuntimeFactory
}

type Daemon struct {
	channel         domain.Channel
	paths           config.ChannelPaths
	lease           *leader.Lease
	store           *store.Store
	engine          *engine.Engine
	spec            statemachine.Spec
	doctor          func(context.Context, store.Project) error
	server          *transport.Server
	epoch           uint64
	clock           Clock
	ids             TicketIDGenerator
	auth            operator.Authenticator
	control         RuntimeController
	recoverProvider func(context.Context, store.ProviderAttempt, uint64) error
	recoveryDrainer interface {
		DrainPersisted(context.Context, contracts.DrainRequest, contracts.ProviderLaunch) (contracts.DrainProof, error)
	}
	gitMutationDrainer       contracts.GitMutationDrainer
	preparedCommitObserver   contracts.PreparedCommitObserver
	repositoryCommandDrainer contracts.RepositoryCommandDrainer
	providerCoordinator      *providercoord.Coordinator
	// closeProviderCoordinator is a narrow lifecycle adapter. Production leaves
	// it nil and uses Coordinator.Close directly; keeping the operation here
	// makes the composition ownership order testable without exposing a Store or
	// coordinator implementation detail through Config.
	closeProviderCoordinator   func(*providercoord.Coordinator) error
	providerCoordinatorFactory func(*store.Store, contracts.ProcessSupervisor) (*providercoord.Coordinator, error)
	providerSupervisor         contracts.ProcessSupervisor
	providerQualifier          func(context.Context, *store.Store, domain.Channel, string, string) (any, error)
	runtimeFactory             WorkflowRuntimeFactory
	runtime                    WorkflowRuntime
	// mu protects daemon process state and handler admission only. It is never
	// held while waiting for socket handlers, runtime shutdown, Store close, or
	// any other external I/O.
	mu        sync.Mutex
	handlers  sync.WaitGroup
	closeDone chan struct{}
	closeErr  error
	closed    bool
	serving   bool
	// runtimeMu serializes the exact runtime/controller/coordinator bundle and
	// operator control transitions. Its lock order is independent: Close first
	// seals handler admission under mu, releases it, waits handlers, then takes
	// runtimeMu to detach the bundle.
	runtimeMu      sync.Mutex
	runtimeStopped bool
	runtimeContext context.Context
	// runtimeCloseDone is created exactly once when a published runtime begins
	// closing. Every lifecycle caller joins it before closing its coordinator or
	// Store, so shutdown cannot race runtime-owned goroutines.
	runtimeCloseDone chan struct{}
	runtimeCloseErr  error

	projectionMu      sync.Mutex
	projector         events.Projector
	projectionPath    string
	projectionPending bool
}

// Start acquires the operating-system lease before it changes the durable
// leader epoch. Recovery completes before the owner socket is exposed.
func Start(ctx context.Context, configuration Config) (*Daemon, error) {
	if err := validateConfig(configuration); err != nil {
		return nil, err
	}
	startupCtx, cancel := boundedContext(ctx, configuration.StartupTimeout)
	defer cancel()
	if err := secureChannelPaths(configuration.Paths); err != nil {
		return nil, err
	}
	if err := validateDatabasePath(configuration.Paths.Database); err != nil {
		return nil, err
	}
	if err := secureDatabaseFiles(configuration.Paths.Database); err != nil {
		return nil, err
	}
	specification, err := loadSpecification(configuration.StateMachinePath)
	if err != nil {
		return nil, err
	}

	lease, err := leader.Acquire(filepath.Join(configuration.Paths.Root, "run", "leader.lock"), configuration.Channel, configuration.DaemonIdentity)
	if err != nil {
		return nil, err
	}
	var coordinator *providercoord.Coordinator
	fail := func(cause error) (*Daemon, error) {
		cause = joinCloseError(cause, "close leader lease", lease.Close)
		return nil, cause
	}
	if err := lease.Validate(); err != nil {
		return fail(err)
	}
	database, err := store.OpenChannel(startupCtx, configuration.Paths.Database, configuration.Paths.Backups, configuration.Channel)
	if err != nil {
		return fail(err)
	}
	failStore := func(cause error) (*Daemon, error) {
		// Keep the ownership order explicit: no coordinator may outlive the
		// Store it was composed against, and neither may outlive the lease.
		if coordinator != nil {
			cause = joinCloseError(cause, "close provider coordinator", coordinator.Close)
		}
		cause = joinCloseError(cause, "close store", database.Close)
		return fail(cause)
	}
	if err := secureDatabaseFiles(configuration.Paths.Database); err != nil {
		return failStore(err)
	}
	if err := lease.Validate(); err != nil {
		return failStore(err)
	}
	epoch, err := database.AcquireLeader(startupCtx, configuration.Channel, configuration.DaemonIdentity)
	if err != nil {
		return failStore(fmt.Errorf("acquire durable leader epoch: %w", err))
	}
	if len(configuration.RecoveryAuthorityKey) > 0 {
		if err := database.SetRecoveryAuthority(startupCtx, configuration.Channel, epoch, configuration.RecoveryAuthorityKey); err != nil {
			return failStore(fmt.Errorf("set recovery authority: %w", err))
		}
	}
	if configuration.ProviderSupervisor != nil {
		setter, ok := configuration.ProviderSupervisor.(contracts.LaunchRecorderSetter)
		if !ok {
			return failStore(errors.New("provider supervisor does not support durable launch recording"))
		}
		setter.SetLaunchRecorder(func(recordCtx context.Context, request contracts.DrainRequest, launch contracts.ProviderLaunch) error {
			claim := store.ProviderAttemptClaim{ID: request.ClaimID, Ref: request.Ref, Phase: request.Phase, Role: request.Role, Attempt: request.Attempt, Binding: contracts.RuntimeBinding{Identity: request.Identity, BinaryDigest: request.BinaryDigest, PolicyDigest: request.PolicyDigest, AuthDigest: request.AuthDigest, AuthMode: request.AuthMode}, LeaseKey: request.LeaseKey, BindingDigest: request.BindingDigest, LeaderEpoch: request.LeaderEpoch, RunnerEpoch: request.RunnerEpoch, ExpectedVersion: request.ExpectedVersion, Repository: request.Repository, Worktree: request.Worktree, WorktreeIdentity: request.WorktreeIdentity, BaseSHA: request.BaseSHA, RequestDigest: request.RequestDigest}
			return database.RecordProviderLaunch(recordCtx, claim, launch)
		})
	}
	for _, project := range configuration.Projects {
		if project.Channel != configuration.Channel {
			return failStore(fmt.Errorf("configured project %q belongs to another channel", project.ID))
		}
		durable, err := database.Project(startupCtx, project.Channel, project.ID)
		if errors.Is(err, store.ErrNotFound) {
			if err := database.CreateProject(startupCtx, project); err != nil {
				return failStore(fmt.Errorf("register project %q: %w", project.ID, err))
			}
			continue
		}
		if err != nil {
			return failStore(fmt.Errorf("read durable project %q: %w", project.ID, err))
		}
		if durable.Path != project.Path || durable.BaseRef != project.BaseRef {
			return failStore(fmt.Errorf("configured project %q does not match durable registration", project.ID))
		}
	}

	if configuration.ProviderCoordinatorFactory != nil {
		coordinator, err = configuration.ProviderCoordinatorFactory(database, configuration.ProviderSupervisor)
		if err != nil {
			if coordinator != nil {
				if closeErr := coordinator.Close(); closeErr != nil {
					err = errors.Join(err, closeErr)
				}
				coordinator = nil
			}
			return failStore(fmt.Errorf("compose provider coordinator: %w", err))
		}
	}
	preparedCommitObserver := configuration.PreparedCommitObserver
	if preparedCommitObserver == nil && configuration.GitRunner != nil {
		preparedCommitObserver = git.PreparedCommitObserver{Runner: *configuration.GitRunner, Resolve: registeredWorktreeResolver(database)}
	}
	instance := &Daemon{channel: configuration.Channel, paths: configuration.Paths, lease: lease, store: database,
		engine: engine.New(database, specification), spec: specification, doctor: configuration.Doctor, epoch: epoch, clock: configuration.Clock, ids: configuration.TicketIDs, auth: configuration.Operator, control: configuration.Controller, recoverProvider: configuration.RecoverProvider, recoveryDrainer: configuration.RecoveryDrainer, gitMutationDrainer: configuration.GitMutationDrainer, preparedCommitObserver: preparedCommitObserver, repositoryCommandDrainer: configuration.RepositoryCommandDrainer, providerCoordinatorFactory: configuration.ProviderCoordinatorFactory, providerSupervisor: configuration.ProviderSupervisor, providerQualifier: configuration.ProviderQualifier, runtimeFactory: configuration.WorkflowRuntimeFactory, runtimeContext: ctx}
	home, _ := os.UserHomeDir()
	instance.projector = events.Projector{Policy: redact.NewPolicy(home, map[string]string{
		configuration.Paths.Root:      "$CHANNEL_ROOT",
		configuration.Paths.Worktrees: "$WORKTREE_ROOT",
	})}
	instance.projectionPath = filepath.Join(configuration.Paths.Events, "events.ndjson")
	if instance.auth.ExpectedUID == 0 {
		instance.auth.ExpectedUID = uint32(os.Getuid())
	}
	if instance.clock == nil {
		instance.clock = wallClock{}
	}
	if instance.ids == nil {
		instance.ids = RandomTicketIDs{}
	}
	if instance.control == nil {
		instance.control = idleRuntimeController{}
	}
	if err := instance.Recover(startupCtx); err != nil {
		return failStore(fmt.Errorf("recover durable state: %w", err))
	}
	if err := instance.projectEvents(startupCtx); err != nil {
		return failStore(fmt.Errorf("rebuild event projection: %w", err))
	}
	// Keep the startup timeout scoped to recovery/projection. A long-lived
	// runtime follows the caller's process context instead of inheriting the
	// bounded startup deadline.
	if configuration.WorkflowRuntimeFactory != nil {
		components, runtimeErr := configuration.WorkflowRuntimeFactory(RuntimeDependencies{
			Store: database, Engine: instance.engine, ProviderCoordinator: coordinator,
		})
		if runtimeErr != nil {
			if components.Runtime != nil {
				if closeErr := components.Runtime.Close(); closeErr != nil {
					runtimeErr = errors.Join(runtimeErr, closeErr)
				}
			}
			return failStore(fmt.Errorf("compose workflow runtime: %w", runtimeErr))
		}
		if (components.Runtime == nil) != (components.Controller == nil) {
			bundleErr := errors.New("compose workflow runtime: factory returned an incomplete runtime/control bundle")
			if components.Runtime != nil {
				bundleErr = errors.Join(bundleErr, components.Runtime.Close())
			}
			return failStore(bundleErr)
		}
		if components.Runtime != nil {
			if _, ok := components.Controller.(RuntimeRearmController); !ok {
				runtimeErr = errors.New("compose workflow runtime: controller does not support runtime rearm")
			} else if _, ok := components.Controller.(RuntimeRetirementController); !ok {
				runtimeErr = errors.New("compose workflow runtime: controller does not support runtime retirement")
			}
			if runtimeErr != nil {
				if closeErr := components.Runtime.Close(); closeErr != nil {
					runtimeErr = errors.Join(runtimeErr, closeErr)
				}
				return failStore(runtimeErr)
			}
			// Nothing can expose the socket until this exact pair has started.
			// Do not publish a partially-started runtime to later lifecycle work.
			if runtimeErr := components.Runtime.Start(ctx, domain.Fence{LeaderEpoch: epoch}); runtimeErr != nil {
				if closeErr := components.Runtime.Close(); closeErr != nil {
					runtimeErr = errors.Join(runtimeErr, closeErr)
				}
				return failStore(fmt.Errorf("start workflow runtime: %w", runtimeErr))
			}
			instance.runtime, instance.control, instance.providerCoordinator = components.Runtime, components.Controller, coordinator
		}
	}
	if instance.runtime == nil && coordinator != nil {
		if err := coordinator.Close(); err != nil {
			coordinator = nil
			return failStore(fmt.Errorf("close idle provider coordinator: %w", err))
		}
		coordinator = nil
	}
	server, err := transport.ListenWithExecutable(configuration.Paths.Socket, uint32(os.Getuid()), instance, instance.executable())
	if err != nil {
		if instance.runtime != nil {
			if closeErr := instance.runtime.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			instance.runtime = nil
		}
		return failStore(err)
	}
	instance.server = server
	return instance, nil
}

func (daemon *Daemon) Serve(ctx context.Context) error {
	daemon.mu.Lock()
	if daemon.closed || daemon.serving {
		daemon.mu.Unlock()
		return ErrAlreadyServing
	}
	// A daemon has one foreground serving lifetime. Keep this set after Serve
	// returns so a caller cannot reopen the listener after its runtime has
	// already been joined.
	daemon.serving = true
	server := daemon.server
	daemon.mu.Unlock()

	if server == nil {
		return errors.New("daemon socket is unavailable")
	}
	err := server.Serve(ctx)
	runtimeErr := daemon.closeRuntime()
	if runtimeErr != nil {
		if err != nil {
			return errors.Join(err, runtimeErr)
		}
		return runtimeErr
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// Epoch exposes the durable leader fence to the production composition and
// integration harnesses without exposing the Store itself.
func (daemon *Daemon) Epoch() uint64 { return daemon.epoch }

func (daemon *Daemon) Recover(ctx context.Context) error {
	if err := daemon.lease.Validate(); err != nil {
		return err
	}
	// Git helper children can have crossed a mutable repository boundary. Drain
	// their exact persisted identity before effects are reconciled/fenced and
	// before any later composition can admit provider or repository writers.
	if err := daemon.store.RecoverGitMutationLeases(ctx, daemon.channel, daemon.epoch, daemon.gitMutationDrainer); err != nil {
		return fmt.Errorf("recover stranded git mutations: %w", err)
	}
	// This runs before generic effect reconciliation. An issued repository
	// command with no lease could not have crossed the closed launch gate, so
	// it can be retired for a future retry rather than becoming permanently
	// uncertain after a lost issue response.
	if err := daemon.store.RecoverUnleasedRepositoryCommands(ctx, daemon.channel, daemon.epoch); err != nil {
		return fmt.Errorf("recover unleased repository commands: %w", err)
	}
	if err := daemon.store.RecoverRepositoryCommandLeases(ctx, daemon.channel, daemon.epoch, daemon.repositoryCommandDrainer); err != nil {
		return fmt.Errorf("recover stranded repository commands: %w", err)
	}
	uncertainEffects, err := daemon.store.ReconcileEffects(ctx, daemon.channel, daemon.epoch)
	if err != nil {
		return fmt.Errorf("reconcile stranded effects: %w", err)
	}
	if err := daemon.reconcilePreparedCommits(ctx, uncertainEffects); err != nil {
		return fmt.Errorf("reconcile prepared git commits: %w", err)
	}
	if _, err := daemon.store.FenceRecoveredRunners(ctx, daemon.channel, daemon.epoch); err != nil {
		return fmt.Errorf("invalidate recovered runners: %w", err)
	}
	if err := daemon.store.RebindRecoveredPublishedCandidates(ctx, daemon.channel, daemon.epoch); err != nil {
		return fmt.Errorf("rebind recovered publication witnesses: %w", err)
	}
	claims, err := daemon.store.ActiveProviderAttempts(ctx, daemon.channel)
	if err != nil {
		return fmt.Errorf("read provider recovery claims: %w", err)
	}
	for _, claim := range claims {
		if daemon.recoveryDrainer != nil {
			launch, identityErr := daemon.store.ProviderLaunchIdentity(ctx, claim.ProviderAttemptClaim)
			if identityErr != nil {
				if err := daemon.store.QuarantineRecoveredProviderAttemptClaim(ctx, claim, daemon.epoch, daemon.clock.Now()); err != nil {
					return err
				}
				return fmt.Errorf("quarantined provider attempt %d without a provable launch identity: %w", claim.ID, store.ErrProviderDrain)
			}
			req := drainRequestForProviderClaim(claim.ProviderAttemptClaim)
			proof, drainErr := daemon.recoveryDrainer.DrainPersisted(ctx, req, launch)
			if drainErr != nil {
				if err := daemon.store.QuarantineRecoveredProviderAttemptClaim(ctx, claim, daemon.epoch, daemon.clock.Now()); err != nil {
					return err
				}
				return fmt.Errorf("quarantined provider attempt %d after identity verification failed: %w", claim.ID, store.ErrProviderDrain)
			}
			if err := daemon.store.RecoverProviderAttemptClaimWithProof(ctx, claim, daemon.epoch, proof, daemon.clock.Now()); err != nil {
				return err
			}
			continue
		}
		if daemon.recoverProvider == nil {
			return store.ErrProviderDrain
		}
		if err := daemon.recoverProvider(ctx, claim, daemon.epoch); err != nil {
			return fmt.Errorf("recover provider attempt %d: %w", claim.ID, err)
		}
	}
	// Every provider and repository writer has now been reconciled, fenced, and
	// drained. Retain admitted global/project capacity across the runner-fence
	// change by moving each exact stale ownership record to its replacement
	// runner. StaleLeases is ordered, making the recovery side effects stable
	// and ensuring any ambiguous group stops startup before socket exposure.
	stale, err := daemon.store.StaleLeases(ctx, daemon.channel, daemon.epoch)
	if err != nil {
		return fmt.Errorf("read invalidated lease ownership: %w", err)
	}
	for index := 0; index < len(stale); {
		group := stale[index]
		next := index + 1
		for next < len(stale) && stale[next].Ref == group.Ref && stale[next].RunnerEpoch == group.RunnerEpoch {
			next++
		}
		if _, err := daemon.store.AdoptInvalidatedLeases(ctx, group.Ref, group.RunnerEpoch, daemon.epoch); err != nil {
			// A quarantined repository-command lease retains its own writer
			// exclusion. Do not turn that operator-visible uncertainty into a
			// daemon/socket availability outage; a later exact recovery proof can
			// retire it and normal capacity adoption can then replay.
			if errors.Is(err, store.ErrRepositoryCommandLease) {
				index = next
				continue
			}
			return fmt.Errorf("adopt invalidated leases for %s/%s runner %d: %w", group.Ref.Project, group.Ref.Ticket, group.RunnerEpoch, err)
		}
		index = next
	}
	return daemon.engine.RecoverChannel(ctx, daemon.channel, daemon.epoch)
}

// reconcilePreparedCommits is deliberately between generic effect recovery
// and runner fencing. ReconcileEffects gives each stranded effect its current
// recovery leader/claim, while the ticket still carries the exact pre-fence
// runner identity needed to prove the prepared commit's expected parent.
func (daemon *Daemon) reconcilePreparedCommits(ctx context.Context, effects []store.Effect) error {
	for _, effect := range effects {
		if effect.Kind != "git/commit" {
			continue
		}
		facts, err := daemon.store.GitMutationIntentFacts(ctx, effect.SemanticKey)
		if err != nil {
			return errors.Join(store.ErrPreparedCommitRecovery, err)
		}
		if facts.Claim.Operation != "commit" || facts.Effect.State != store.EffectUncertain || facts.PreparedCommitOID == "" || facts.PreparedTreeOID == "" {
			return store.ErrPreparedCommitRecovery
		}
		if daemon.preparedCommitObserver == nil {
			return errors.Join(store.ErrPreparedCommitRecovery, errors.New("prepared commit observer is not configured"))
		}
		observation, err := daemon.preparedCommitObserver.ObservePreparedCommit(ctx, facts.Claim)
		if err != nil {
			return errors.Join(store.ErrPreparedCommitRecovery, err)
		}
		if _, err := daemon.store.ConfirmRecoveredPreparedCommit(ctx, facts.Claim, observation); err != nil {
			return err
		}
	}
	return nil
}

func drainRequestForProviderClaim(claim store.ProviderAttemptClaim) contracts.DrainRequest {
	return contracts.DrainRequest{ClaimID: claim.ID, Identity: claim.Binding.Identity, Ref: claim.Ref, Phase: claim.Phase, Role: claim.Role, Attempt: claim.Attempt, LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ExpectedVersion: claim.ExpectedVersion, LeaseKey: claim.LeaseKey, BindingDigest: claim.BindingDigest, BinaryDigest: claim.Binding.BinaryDigest, PolicyDigest: claim.Binding.PolicyDigest, AuthDigest: claim.Binding.AuthDigest, AuthMode: claim.Binding.AuthMode, Repository: claim.Repository, Worktree: claim.Worktree, WorktreeIdentity: claim.WorktreeIdentity, BaseSHA: claim.BaseSHA, RequestDigest: claim.RequestDigest}
}

// Run owns a foreground daemon lifetime. It is deliberately separate from the
// CLI package so cmd/sf can inject it without creating a cli<->daemon cycle.
func Run(ctx context.Context, configuration Config) error {
	daemon, err := Start(ctx, configuration)
	if err != nil {
		return err
	}
	return runForeground(ctx, daemon)
}

type foregroundDaemon interface {
	Serve(context.Context) error
	Close() error
}

// runForeground preserves both the serving result and every normal shutdown
// error. A defer would silently discard Close failures after a clean context
// cancellation, leaving the caller unable to distinguish complete teardown.
func runForeground(ctx context.Context, daemon foregroundDaemon) error {
	return errors.Join(daemon.Serve(ctx), daemon.Close())
}

func (daemon *Daemon) Close() error {
	daemon.mu.Lock()
	if daemon.closed {
		done := daemon.closeDone
		daemon.mu.Unlock()
		if done != nil {
			<-done
		}
		daemon.mu.Lock()
		defer daemon.mu.Unlock()
		return daemon.closeErr
	}
	daemon.closed = true
	daemon.closeDone = make(chan struct{})
	server := daemon.server
	daemon.mu.Unlock()

	// Server.Close waits for transport workers. Never hold daemon.mu or
	// runtimeMu here: a worker may be finishing qualification/control and needs
	// those locks to return before Close can safely tear down Store.
	var result error
	if server != nil {
		result = joinCloseError(result, "close daemon server", server.Close)
	}
	// Handle also covers direct in-process callers, which are not counted by
	// transport.Server. Handler admission is sealed before this wait.
	daemon.handlers.Wait()
	// Every caller joins the one shared runtime shutdown result. Serve may have
	// initiated it first, but Close still owns authority teardown and therefore
	// must report the same runtime failure to its caller and cache it for later
	// Close calls.
	if runtimeErr, _ := daemon.shutdownRuntime(); runtimeErr != nil {
		result = errors.Join(result, fmt.Errorf("close workflow runtime: %w", runtimeErr))
	}
	// shutdownRuntime has joined the runtime's Close before this detaches the
	// exact paired coordinator. Store teardown remains strictly after both.
	daemon.runtimeMu.Lock()
	coordinator := daemon.providerCoordinator
	daemon.providerCoordinator = nil
	daemon.runtimeMu.Unlock()
	if coordinator != nil {
		result = joinCloseError(result, "close provider coordinator", func() error { return daemon.closeCoordinator(coordinator) })
	}
	result = joinCloseError(result, "close store", daemon.engine.Close)
	result = joinCloseError(result, "close leader lease", daemon.lease.Close)
	daemon.mu.Lock()
	daemon.closeErr = result
	close(daemon.closeDone)
	daemon.mu.Unlock()
	return result
}

func joinCloseError(cause error, resource string, closeFn func() error) error {
	if err := closeFn(); err != nil {
		return errors.Join(cause, fmt.Errorf("%s: %w", resource, err))
	}
	return cause
}

// closeRuntime serializes runtime joining with Daemon.Close. It is called
// when Serve returns as well as during explicit shutdown, so a caller that
// cancels Serve cannot race a runtime tick against Store closure.
func (daemon *Daemon) closeRuntime() error {
	err, _ := daemon.shutdownRuntime()
	return err
}

// shutdownRuntime detaches a published runtime once and joins that same Close
// operation for every caller. It never holds runtimeMu while calling Close.
func (daemon *Daemon) shutdownRuntime() (error, bool) {
	daemon.runtimeMu.Lock()
	daemon.runtimeStopped = true
	if done := daemon.runtimeCloseDone; done != nil {
		daemon.runtimeMu.Unlock()
		<-done
		daemon.runtimeMu.Lock()
		err := daemon.runtimeCloseErr
		daemon.runtimeMu.Unlock()
		return err, false
	}
	runtime := daemon.runtime
	daemon.runtime = nil
	daemon.control = idleRuntimeController{}
	if runtime == nil {
		daemon.runtimeMu.Unlock()
		return nil, false
	}
	done := make(chan struct{})
	daemon.runtimeCloseDone = done
	daemon.runtimeMu.Unlock()
	err := runtime.Close()
	daemon.runtimeMu.Lock()
	daemon.runtimeCloseErr = err
	close(done)
	daemon.runtimeMu.Unlock()
	return err, true
}

func (daemon *Daemon) Handle(ctx context.Context, peer transport.Peer, request api.Request) api.Response {
	if !daemon.beginHandler() {
		return daemon.failure(request, "daemon_stopping", "the local daemon is stopping", true)
	}
	defer daemon.handlers.Done()
	if err := request.Validate(); err != nil {
		return daemon.failure(request, "invalid_request", "request envelope is invalid", false)
	}
	identity, err := daemon.auth.Authenticate(peer.UID, request.OperatorLabel)
	if err != nil {
		return daemon.failure(request, "operator_identity_required", "the socket peer is not authenticated for this operator label", false)
	}
	if daemon.eventProjectionPending() {
		// A prior projection failure cannot affect authority. Retry it before
		// answering the next authenticated request so a read can repair the
		// disposable view without replaying any workflow effect.
		_ = daemon.projectEvents(ctx)
	}
	var response api.Response
	switch request.Method {
	case "ticket.submit":
		response = daemon.submit(ctx, request, identity)
	case "ticket.status":
		response = daemon.statusTickets(ctx, request, identity)
	case "ticket.show":
		response = daemon.show(ctx, request, identity)
	case "ticket.logs":
		response = daemon.logs(ctx, request, identity)
	case "ticket.start":
		response = daemon.startTicket(ctx, request, identity)
	case "ticket.pause":
		response = daemon.controlTicket(ctx, request, identity, "pause")
	case "ticket.take":
		response = daemon.controlTicket(ctx, request, identity, "take")
	case "ticket.resume":
		response = daemon.resumeTicket(ctx, request, identity)
	case "ticket.retry":
		response = daemon.retryTicket(ctx, request, identity)
	case "ticket.recover":
		response = daemon.recoverTicket(ctx, request, identity)
	case "ticket.cancel":
		response = daemon.controlTicket(ctx, request, identity, "cancel")
	case "ticket.approve":
		response = daemon.operatorDecision(ctx, request, identity, "approved")
	case "ticket.reject":
		response = daemon.operatorDecision(ctx, request, identity, "rejected")
	case "daemon.status":
		response = daemon.status(request, identity)
	case "provider.qualify":
		response = daemon.qualifyProvider(ctx, request)
	default:
		response = daemon.failure(request, "not_ready", "this lifecycle operation is not enabled by the local daemon yet", false)
	}
	if response.Mutation.Attempted {
		if err := daemon.projectEvents(ctx); err != nil {
			return daemon.projectionFailure(request, response)
		}
	}
	return response
}

func (daemon *Daemon) beginHandler() bool {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	if daemon.closed {
		return false
	}
	daemon.handlers.Add(1)
	return true
}

func (daemon *Daemon) isClosed() bool {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	return daemon.closed
}

func (daemon *Daemon) qualifyProvider(ctx context.Context, request api.Request) api.Response {
	if daemon.providerQualifier == nil {
		return daemon.failure(request, "provider_unavailable", "provider qualification is not configured for this daemon", false)
	}
	var parameters struct {
		Builder  string `json:"builder"`
		Reviewer string `json:"reviewer"`
	}
	if err := json.Unmarshal(request.Parameters, &parameters); err != nil || parameters.Builder != "codex" || parameters.Reviewer != "codex" {
		return daemon.failure(request, "invalid_argument", "builder and reviewer must name the local Codex provider", false)
	}
	// Qualification changes the durable binding that a newly composed runtime
	// would use. Serialize it with Close and controller replacement so neither
	// a Store close nor an idle-controller drain can race activation.
	daemon.runtimeMu.Lock()
	defer daemon.runtimeMu.Unlock()
	if daemon.isClosed() || daemon.runtimeStopped {
		return daemon.failure(request, "daemon_stopping", "provider qualification is unavailable while the daemon is stopping", true)
	}
	// A later qualification can change the selected durable pair beneath a
	// live coordinator. There is no atomic runtime handoff yet, so reject it
	// before the qualifier receives Store authority.
	if daemon.runtime != nil {
		return daemon.failure(request, "runtime_already_active", "the qualified local workflow runtime is already active; inspect daemon status, then stop the foreground daemon before requalifying", false)
	}
	value, err := daemon.providerQualifier(ctx, daemon.store, daemon.channel, parameters.Builder, parameters.Reviewer)
	if err != nil {
		response := daemon.failure(request, "unqualified_provider", "local Codex qualification failed without invoking a model: "+safeQualificationError(err), false)
		if encoded, encodeErr := json.Marshal(value); encodeErr == nil {
			response.Data = encoded
		}
		return response
	}
	if err := daemon.activateQualifiedRuntimeLocked(); err != nil {
		response := daemon.failure(request, "runtime_activation_failed", "provider qualification was recorded, but the local workflow runtime could not be activated; retry qualification or restart the daemon", true)
		response.Mutation = api.Mutation{Attempted: true, Kind: "provider.qualify", Identity: string(daemon.channel), Observed: true}
		if encoded, encodeErr := json.Marshal(value); encodeErr == nil {
			response.Data = encoded
		}
		return response
	}
	return daemon.success(request, api.Mutation{Attempted: true, Kind: "provider.qualify", Identity: string(daemon.channel), Observed: true}, value)
}

// activateQualifiedRuntimeLocked installs the first executable local runtime
// after a durable qualification. daemon.runtimeMu must be held. Requalification of
// an already running daemon deliberately leaves its exact coordinator/runtime
// pair in place: replacing it would require proving a handoff with no work in
// flight, which this lifecycle does not yet provide.
func (daemon *Daemon) activateQualifiedRuntimeLocked() error {
	if daemon.runtime != nil {
		return nil
	}
	if daemon.runtimeStopped || daemon.isClosed() {
		return errors.New("daemon runtime is stopping")
	}
	if daemon.providerCoordinatorFactory == nil || daemon.runtimeFactory == nil {
		return errors.New("local workflow runtime activation is not configured")
	}
	coordinator, err := daemon.providerCoordinatorFactory(daemon.store, daemon.providerSupervisor)
	if err != nil {
		if coordinator != nil {
			if closeErr := daemon.closeCoordinator(coordinator); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
		return fmt.Errorf("compose qualified provider coordinator: %w", err)
	}
	if coordinator == nil {
		return errors.New("compose qualified provider coordinator: factory returned nil")
	}
	components, err := daemon.runtimeFactory(RuntimeDependencies{Store: daemon.store, Engine: daemon.engine, ProviderCoordinator: coordinator})
	if err != nil {
		// A factory may have handed its partial Runtime the exact coordinator
		// before reporting an error. Its cleanup therefore owns that dependency
		// until Runtime.Close has joined all of the runtime's goroutines.
		err = closePartialRuntimeComposition(err, components.Runtime, func() error { return daemon.closeCoordinator(coordinator) })
		return fmt.Errorf("compose qualified workflow runtime: %w", err)
	}
	if err := validateRuntimeComponents(components); err != nil {
		err = closePartialRuntimeComposition(err, components.Runtime, func() error { return daemon.closeCoordinator(coordinator) })
		return err
	}
	if components.Runtime == nil {
		return closePartialRuntimeComposition(errors.New("qualified provider has no executable workflow runtime"), nil, func() error { return daemon.closeCoordinator(coordinator) })
	}
	// The startup coordinator was intentionally idle. Retire it before the new
	// runtime can start so a cleanup failure cannot make us report an activation
	// failure after a real runtime is already live.
	if daemon.providerCoordinator != nil && daemon.providerCoordinator != coordinator {
		if err := daemon.closeCoordinator(daemon.providerCoordinator); err != nil {
			return errors.Join(err, components.Runtime.Close(), daemon.closeCoordinator(coordinator))
		}
	}
	if err := components.Runtime.Start(daemon.runtimeContext, domain.Fence{LeaderEpoch: daemon.epoch}); err != nil {
		err = closePartialRuntimeComposition(err, components.Runtime, func() error { return daemon.closeCoordinator(coordinator) })
		return fmt.Errorf("start qualified workflow runtime: %w", err)
	}
	daemon.runtime, daemon.control, daemon.providerCoordinator = components.Runtime, components.Controller, coordinator
	return nil
}

func (daemon *Daemon) closeCoordinator(coordinator *providercoord.Coordinator) error {
	if coordinator == nil {
		return nil
	}
	if daemon.closeProviderCoordinator != nil {
		return daemon.closeProviderCoordinator(coordinator)
	}
	return coordinator.Close()
}

// closePartialRuntimeComposition keeps the partial-composition ownership
// order explicit: a Runtime receives the coordinator through
// RuntimeDependencies, so it must close before that coordinator. The caller
// supplies the coordinator closer rather than exposing a concrete coordinator
// type to this generic lifecycle helper. errors.Join preserves the original
// composition failure and every cleanup failure for operator diagnosis.
func closePartialRuntimeComposition(cause error, runtime WorkflowRuntime, closeCoordinator func() error) error {
	if runtime != nil {
		cause = joinCloseError(cause, "close workflow runtime", runtime.Close)
	}
	if closeCoordinator != nil {
		cause = joinCloseError(cause, "close provider coordinator", closeCoordinator)
	}
	return cause
}

func validateRuntimeComponents(components WorkflowRuntimeComponents) error {
	if (components.Runtime == nil) != (components.Controller == nil) {
		return errors.New("factory returned an incomplete runtime/control bundle")
	}
	if components.Runtime == nil {
		return nil
	}
	if _, ok := components.Controller.(RuntimeRearmController); !ok {
		return errors.New("controller does not support runtime rearm")
	}
	if _, ok := components.Controller.(RuntimeRetirementController); !ok {
		return errors.New("controller does not support runtime retirement")
	}
	return nil
}

func safeQualificationError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "bounded probe did not complete"
	}
	for _, value := range []string{"unsafe", "unavailable", "capability", "authentication", "attestation", "qualified", "leader", "supervisor", "qualification"} {
		if strings.Contains(strings.ToLower(err.Error()), value) {
			return err.Error()
		}
	}
	return "guarded probe refused"
}

func (daemon *Daemon) projectEvents(ctx context.Context) error {
	daemon.projectionMu.Lock()
	defer daemon.projectionMu.Unlock()
	err := daemon.projector.Rebuild(ctx, events.StoreSource{Store: daemon.store, Channel: daemon.channel}, daemon.projectionPath)
	daemon.projectionPending = err != nil
	return err
}

func (daemon *Daemon) eventProjectionPending() bool {
	daemon.projectionMu.Lock()
	defer daemon.projectionMu.Unlock()
	return daemon.projectionPending
}

func (daemon *Daemon) projectionFailure(request api.Request, committed api.Response) api.Response {
	ticketID := request.Ticket
	if ticketID == "" && committed.Mutation.Kind == "ticket_submit" {
		ticketID = committed.Mutation.Identity
	}
	argv := []string{daemon.executable(), "daemon", "status"}
	if ticketID != "" {
		argv = []string{daemon.executable(), "status", ticketID}
	}
	return api.Response{
		Version: api.Version, RequestID: request.RequestID, OK: false, Mutation: committed.Mutation, Data: committed.Data,
		Error:      &api.Error{Code: "projection_unavailable", Message: "the authority mutation committed, but the redacted event projection could not be refreshed", Retryable: true},
		NextAction: &domain.NextAction{Code: "projection_unavailable", Argv: argv},
	}
}

type submitParameters struct {
	Project string         `json:"project"`
	Source  string         `json:"source"`
	New     bool           `json:"new"`
	Channel domain.Channel `json:"channel"`
}

func (daemon *Daemon) submit(ctx context.Context, request api.Request, _ domain.OperatorIdentity) api.Response {
	var parameters submitParameters
	if err := decodeParameters(request.Parameters, &parameters); err != nil || parameters.Project == "" || parameters.Source == "" || parameters.Channel != daemon.channel {
		return daemon.failure(request, "invalid_submit", "submit requires project and source", false)
	}
	parsed, err := ticket.Parse(strings.NewReader(parameters.Source))
	if err != nil {
		return daemon.failure(request, "invalid_ticket", "ticket source does not meet the local ticket format", false)
	}
	project, err := daemon.store.Project(ctx, daemon.channel, domain.ProjectID(parameters.Project))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return daemon.failure(request, "unknown_project", "ticket project is not registered", false)
		}
		return daemon.failure(request, submitErrorCode(err), "ticket project registration could not be read", errors.Is(err, store.ErrBusy))
	}
	if parsed.MergeModeExplicit && parsed.MergeMode == domain.MergeAutonomous {
		return daemon.failure(request, "autonomous_unavailable", "autonomous workflows are unavailable in v1; choose guarded or manual merge mode and resubmit the ticket", false)
	}
	effective, err := resolveSubmissionPolicy(project, parsed)
	if err != nil {
		return daemon.failure(request, "ticket_policy_refused", "ticket overrides exceed the registered project policy", false)
	}
	if effective.MergeMode == domain.MergeAutonomous {
		return daemon.failure(request, "autonomous_unavailable", "autonomous workflows are unavailable in v1; choose guarded or manual merge mode and resubmit the ticket", false)
	}
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	id, err := daemon.ids.NewTicketID(daemon.channel)
	if err != nil {
		return daemon.failure(request, "ticket_id_unavailable", "could not allocate a ticket identity", true)
	}
	record := store.Ticket{Ref: domain.TicketRef{Channel: daemon.channel, Project: domain.ProjectID(parameters.Project), Ticket: id},
		SourceDigest: parsed.Digest, Type: parsed.Type, MergeMode: effective.MergeMode, Title: parsed.Title, Problem: parsed.Problem,
		Acceptance: parsed.Acceptance, Source: parsed.Source, Priority: parsed.Priority, CreatedAt: daemon.clock.Now().UTC(),
		MaxDuration: effective.TicketTimeout, MaxCostMicroUSD: effective.MaxTicketCostMicroUSD}
	stored, created, err := daemon.store.SubmitTicketFenced(ctx, record, parameters.New, daemon.epoch)
	if err != nil {
		return daemon.failure(request, submitErrorCode(err), "ticket submission was not accepted", errors.Is(err, store.ErrBusy))
	}
	return daemon.success(request, api.Mutation{Attempted: true, Kind: "ticket_submit", Identity: string(stored.Ref.Ticket), Observed: !created}, ticketView(stored))
}

func resolveSubmissionPolicy(project store.Project, parsed ticket.Parsed) (config.Effective, error) {
	frozen, err := config.DecodeSnapshot(project.ConfigSnapshot, project.ConfigDigest)
	if err != nil {
		return config.Effective{}, fmt.Errorf("decode registered configuration: %w", err)
	}
	if frozen.Name != string(project.ID) || frozen.Repository != project.Path || frozen.BaseBranch != project.BaseRef {
		return config.Effective{}, errors.New("registered configuration does not match project identity")
	}
	override := config.TicketOverride{TicketTimeout: parsed.MaxDuration, MaxCostMicroUSD: parsed.MaxCostMicroUSD}
	if parsed.MergeModeExplicit {
		override.MergeMode = parsed.MergeMode
	}
	return config.Resolve(frozen.Machine, frozen.Project, override)
}

type ticketParameters struct {
	Project string         `json:"project"`
	Ticket  string         `json:"ticket"`
	Channel domain.Channel `json:"channel"`
}

func (daemon *Daemon) show(ctx context.Context, request api.Request, identity domain.OperatorIdentity) api.Response {
	ref, response := daemon.ticketRef(ctx, request)
	if response != nil {
		return *response
	}
	stored, err := daemon.store.Ticket(ctx, ref)
	if err != nil {
		return daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
	}
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	view := ticketDetail(stored)
	if action, ok := daemon.ticketBlockedNextAction(stored); ok {
		view["next_action"] = action
	}
	evidence, err := daemon.evidenceView(ctx, stored.Ref)
	if err != nil {
		return daemon.failure(request, evidenceErrorCode(err), "durable workflow evidence could not be authenticated", errors.Is(err, store.ErrBusy))
	}
	view["evidence"] = evidence
	view["operator"] = operatorView(identity)
	return daemon.success(request, api.Mutation{}, view)
}

const (
	logPageSize = 4096
	maxLogItems = 1000
	maxLogPages = 25
)

type logParameters struct {
	Channel domain.Channel `json:"channel"`
	Phase   string         `json:"phase"`
	Follow  bool           `json:"follow"`
	After   uint64         `json:"after"`
}

// logs reads the SQLite event authority and returns a bounded, redacted view.
// Provider transcripts and command output are deliberately not event-log
// material and can never cross this API boundary.
func (daemon *Daemon) logs(ctx context.Context, request api.Request, identity domain.OperatorIdentity) api.Response {
	var parameters logParameters
	if err := decodeParameters(request.Parameters, &parameters); err != nil || parameters.Channel != daemon.channel || !validLogPhase(parameters.Phase) {
		return daemon.failure(request, "invalid_logs", "logs requires the daemon channel and an optional valid phase", false)
	}
	ref, response := daemon.ticketRefByID(ctx, request)
	if response != nil {
		return *response
	}
	records := make([]events.Record, 0)
	after := parameters.After
	for page := 0; page < maxLogPages; page++ {
		batch, err := daemon.store.Events(ctx, daemon.channel, after, logPageSize)
		if err != nil {
			return daemon.failure(request, "logs_unavailable", "durable ticket events could not be read", errors.Is(err, store.ErrBusy))
		}
		for _, item := range batch {
			after = item.ID
			if item.Ref != ref || !eventMatchesPhase(item, parameters.Phase) {
				continue
			}
			payload := daemon.projector.Policy.JSON(json.RawMessage(item.Payload))
			records = append(records, events.Record{
				Schema: events.Schema, ID: item.ID, Channel: item.Ref.Channel, Project: item.Ref.Project,
				Ticket: item.Ref.Ticket, TicketVersion: item.TicketVersion, Trigger: item.Trigger,
				From: item.From, To: item.To, Payload: payload, CreatedAt: item.CreatedAt,
			})
			if len(records) == maxLogItems {
				break
			}
		}
		if len(records) == maxLogItems || len(batch) < logPageSize || page == maxLogPages-1 {
			break
		}
	}
	return daemon.success(request, api.Mutation{}, map[string]any{
		"channel": daemon.channel, "ticket": ref.Ticket, "phase": parameters.Phase,
		"follow": parameters.Follow, "after": parameters.After, "next_after": after,
		"operator": operatorView(identity), "events": records,
	})
}

func validLogPhase(value string) bool {
	switch domain.Phase(value) {
	case "", domain.PhasePlanning, domain.PhaseVerification, domain.PhaseBuild, domain.PhasePublish, domain.PhaseReview, domain.PhaseMerge, domain.PhaseReconcile:
		return true
	default:
		return false
	}
}

func eventMatchesPhase(event store.Event, phase string) bool {
	if phase == "" {
		return true
	}
	var payload struct {
		Phase string `json:"phase"`
	}
	if json.Unmarshal([]byte(event.Payload), &payload) == nil && payload.Phase == phase {
		return true
	}
	statePhase := func(state domain.State) domain.Phase {
		switch state {
		case domain.StatePlanning:
			return domain.PhasePlanning
		case domain.StateVerifying:
			return domain.PhaseVerification
		case domain.StateBuilding:
			return domain.PhaseBuild
		case domain.StatePublishing, domain.StateWaitingCI:
			return domain.PhasePublish
		case domain.StateReviewing, domain.StateWaitingApproval, domain.StateWaitingManualMerge:
			return domain.PhaseReview
		case domain.StateMerging:
			return domain.PhaseMerge
		case domain.StateReconciling, domain.StateDone, domain.StateExternalMerged:
			return domain.PhaseReconcile
		default:
			return ""
		}
	}
	want := domain.Phase(phase)
	return statePhase(event.From) == want || statePhase(event.To) == want
}

func (daemon *Daemon) startTicket(ctx context.Context, request api.Request, _ domain.OperatorIdentity) api.Response {
	ref, response := daemon.ticketRef(ctx, request)
	if response != nil {
		return *response
	}
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	stored, err := daemon.store.Ticket(ctx, ref)
	if err != nil {
		return daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
	}
	// These are intentionally local admission guards. Provider/Git/remote
	// checks are not part of Phase 1 and cannot make a ticket autonomous. A
	// planning ticket is a replay observation and does not select a second
	// transition.
	if stored.State == domain.StateQueued {
		project, capacityAvailable, err := daemon.store.StartPreflight(ctx, ref)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return daemon.failure(request, "unknown_project", "ticket project is not registered", false)
			}
			return daemon.failure(request, "invalid_configuration", "the durable project configuration is invalid", false)
		}
		doctorGreen := true
		if daemon.doctor != nil {
			doctorGreen = daemon.doctor(ctx, project) == nil
		}
		if _, err := daemon.spec.Select(string(stored.State), "operator_start", map[string]bool{"doctor_preflight_green": doctorGreen, "capacity_available": capacityAvailable}); err != nil {
			if !doctorGreen {
				return daemon.failure(request, "doctor_required", "local doctor preflight is not green", false)
			}
			return daemon.failure(request, "capacity_unavailable", "local capacity is already reserved", true)
		}
	}
	workflowID := fmt.Sprintf("%s/%s/%s/planning", daemon.channel, ref.Project, ref.Ticket)
	started, observed, err := daemon.store.StartWithProjectOwnership(ctx, ref, stored.Version, domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch}, workflowID, daemon.clock.Now().UTC())
	if err != nil {
		code := "start_refused"
		if errors.Is(err, store.ErrLeaseCapacity) {
			code = "capacity_unavailable"
		}
		return daemon.failure(request, code, "ticket could not enter local planning state", errors.Is(err, store.ErrBusy))
	}
	return daemon.success(request, api.Mutation{Attempted: true, Kind: "ticket_start", Identity: workflowID, Observed: observed}, ticketView(started))
}

type controlParameters struct {
	Operator string         `json:"operator"`
	Channel  domain.Channel `json:"channel"`
}

type operatorDecisionParameters struct {
	Operator string         `json:"operator"`
	Reason   string         `json:"reason"`
	Channel  domain.Channel `json:"channel"`
}

// operatorDecision keeps approval and rejection on the owner-only socket
// path. The displayed operator label is only a comparison value: the UID used
// by Store comes from the peer-authenticated identity above.
func (daemon *Daemon) operatorDecision(ctx context.Context, request api.Request, identity domain.OperatorIdentity, decision string) api.Response {
	var parameters operatorDecisionParameters
	if err := decodeParameters(request.Parameters, &parameters); err != nil || parameters.Channel != daemon.channel || (parameters.Operator != "" && parameters.Operator != identity.Label) || (decision != "approved" && decision != "rejected") {
		return daemon.failure(request, "invalid_decision", "approval or rejection requires the authenticated operator and daemon channel", false)
	}
	if decision == "approved" && parameters.Reason != "" {
		return daemon.failure(request, "invalid_decision", "approval does not accept a reason", false)
	}
	if decision == "rejected" && (!boundedOperatorReason(parameters.Reason)) {
		return daemon.failure(request, "invalid_decision", "rejection requires a bounded non-empty reason", false)
	}
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	ref, response := daemon.ticketRefByID(ctx, request)
	if response != nil {
		return *response
	}
	stored, err := daemon.store.Ticket(ctx, ref)
	if err != nil {
		return daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
	}
	candidate, err := daemon.store.RecoverableCandidate(ctx, ref)
	if err != nil {
		return daemon.failure(request, "approval_evidence_unavailable", "the exact reviewed candidate is unavailable", errors.Is(err, store.ErrBusy))
	}
	reasonDigest := ""
	if parameters.Reason != "" {
		sum := sha256.Sum256([]byte(parameters.Reason))
		reasonDigest = fmt.Sprintf("%x", sum[:])
	}
	result, err := daemon.store.ApplyOperatorDecision(ctx, store.OperatorDecisionRequest{OperatorDecision: store.OperatorDecision{
		Ref: ref, ExpectedVersion: stored.Version, Fence: domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch}, ReviewedHead: candidate.Snapshot.HeadSHA, OperatorUID: identity.UID, Decision: decision,
	}, ReasonDigest: reasonDigest})
	if err != nil {
		code, message := "decision_refused", "the decision is not valid for the current reviewed head"
		if errors.Is(err, store.ErrStaleFence) {
			code, message = "approval_head_changed", "the reviewed candidate changed; refresh checks and review before deciding"
		}
		if errors.Is(err, store.ErrBusy) {
			code, message = "decision_unavailable", "the decision could not be durably recorded yet"
		}
		return daemon.failure(request, code, message, errors.Is(err, store.ErrBusy))
	}
	updated, err := daemon.store.Ticket(ctx, ref)
	if err != nil || updated.Version != result.Version {
		return daemon.failure(request, "decision_state_unavailable", "the durable decision state could not be confirmed", true)
	}
	return daemon.success(request, api.Mutation{Attempted: true, Kind: "ticket_" + decision, Identity: string(ref.Ticket)}, ticketView(updated))
}

func boundedOperatorReason(value string) bool {
	return len(value) > 0 && len(value) <= 4096 && strings.TrimSpace(value) == value
}

func (daemon *Daemon) controlTicket(ctx context.Context, request api.Request, identity domain.OperatorIdentity, intent string) api.Response {
	// Keep one controller for the whole durable stop/drain transition. This
	// also serializes a first qualified-runtime installation with a control
	// request that began while the daemon still had only the idle controller.
	daemon.runtimeMu.Lock()
	defer daemon.runtimeMu.Unlock()
	if daemon.isClosed() || daemon.runtimeStopped {
		return daemon.failure(request, "daemon_stopping", "ticket control is unavailable while the daemon is stopping", true)
	}
	var parameters controlParameters
	if err := decodeParameters(request.Parameters, &parameters); err != nil || parameters.Channel != daemon.channel || (parameters.Operator != "" && parameters.Operator != identity.Label) {
		return daemon.failure(request, "invalid_control", "control requires the authenticated operator and daemon channel", false)
	}
	ref, response := daemon.ticketRefByID(ctx, request)
	if response != nil {
		return *response
	}
	stored, err := daemon.store.Ticket(ctx, ref)
	if err != nil {
		return daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
	}
	if intent == "cancel" && stored.State == domain.StateCancelled {
		if err := daemon.retireRuntimeControl(ctx, stored.Ref); err != nil {
			return daemon.controlFailure(request, stored, intent, "runtime_retirement_failed", "terminal runtime control cleanup failed; retry cancellation without resuming the ticket", true, true)
		}
		return daemon.controlSuccess(request, stored, intent, true)
	}
	if intent != "cancel" && stored.State == domain.StatePaused {
		if intent == "take" {
			return daemon.takeoverSuccess(ctx, request, stored, true)
		}
		return daemon.controlSuccess(request, stored, intent, true)
	}
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	eventPayload, err := json.Marshal(map[string]any{"intent": intent, "operator": identity.Label, "operator_uid": identity.UID})
	if err != nil {
		return daemon.failure(request, "internal_encoding", "operator control metadata could not be encoded", false)
	}

	if intent == "cancel" && stored.State != domain.StateCancelling {
		merged, err := daemon.control.MergeObserved(ctx, ref)
		if err != nil {
			return daemon.controlFailure(request, stored, intent, "external_state_unavailable", "merge state could not be observed before cancellation", true, false)
		}
		if merged {
			return daemon.controlFailure(request, stored, intent, "external_merge_observed", "the ticket has an external merge that must be reconciled", false, false)
		}
		result, err := daemon.engine.Signal(ctx, contracts.SignalRequest{
			Ticket: ref, TicketVersion: stored.Version, From: stored.State, Trigger: "operator_cancel",
			Fence:        domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch},
			Attributes:   map[string]string{"operator_identity_authenticated": "true", "merge_not_observed": "true"},
			EventPayload: string(eventPayload),
		})
		if err != nil {
			return daemon.controlFailure(request, stored, intent, "invalid_transition", "ticket cannot be cancelled from its current state", false, false)
		}
		confirmed, readErr := daemon.store.Ticket(ctx, ref)
		if readErr != nil || confirmed.Version != result.TicketVersion || confirmed.State != domain.StateCancelling {
			return daemon.controlFailure(request, stored, intent, "control_state_unavailable", "cancellation state could not be confirmed", true, true)
		}
		stored = confirmed
	} else if intent != "cancel" && stored.State != domain.StateStopping {
		result, err := daemon.engine.Signal(ctx, contracts.SignalRequest{
			Ticket: ref, TicketVersion: stored.Version, From: stored.State, Trigger: "operator_pause_or_take",
			Fence:        domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch},
			Attributes:   map[string]string{"operator_identity_authenticated": "true"},
			EventPayload: string(eventPayload),
		})
		if err != nil {
			return daemon.controlFailure(request, stored, intent, "invalid_transition", "ticket cannot be paused from its current state", false, false)
		}
		confirmed, readErr := daemon.store.Ticket(ctx, ref)
		if readErr != nil || confirmed.Version != result.TicketVersion || confirmed.State != domain.StateStopping {
			return daemon.controlFailure(request, stored, intent, "control_state_unavailable", "stopping state could not be confirmed", true, true)
		}
		stored = confirmed
	}
	return daemon.finishControl(ctx, request, stored, intent)
}

func (daemon *Daemon) finishControl(ctx context.Context, request api.Request, stored store.Ticket, intent string) api.Response {
	drained, err := daemon.control.Drain(ctx, stored.Ref)
	if err != nil {
		return daemon.controlFailure(request, stored, intent, "control_drain_failed", "the ticket runtime could not be drained", true, true)
	}
	if !drained {
		return daemon.controlFailure(request, stored, intent, "blocked_process", "a ticket writer is still live or quarantined", false, true)
	}
	if intent == "cancel" {
		merged, err := daemon.control.MergeObserved(ctx, stored.Ref)
		if err != nil {
			return daemon.controlFailure(request, stored, intent, "external_state_unavailable", "merge state could not be re-observed after draining", true, true)
		}
		if merged {
			return daemon.controlFailure(request, stored, intent, "external_merge_observed", "an external merge appeared while cancellation drained", false, true)
		}
	}
	target := domain.StatePaused
	if intent == "cancel" {
		target = domain.StateCancelled
	}
	eventPayload := `{"drained":true,"intent":"` + intent + `"}`
	if intent == "take" || intent == "pause" {
		inspection, inspectErr := daemon.inspectTakeover(ctx, stored.Ref)
		if inspectErr != nil {
			return daemon.controlFailure(request, stored, intent, "takeover_inspection_failed", "the runtime is drained but the local and remote worktree identity could not be captured for handoff", true, true)
		}
		baseline := store.TakeoverRemoteBaseline{
			Registered: inspection.Registered, WorktreePath: inspection.Path, WorktreeBranch: inspection.Branch,
			CandidatePresent: inspection.RemoteCandidatePresent, CandidateOID: inspection.RemoteCandidateSHA, BaseOID: inspection.RemoteBaseSHA,
		}
		if inspection.Registered {
			registered, readErr := daemon.store.Worktree(ctx, stored.Ref)
			if readErr != nil {
				return daemon.controlFailure(request, stored, intent, "takeover_inspection_failed", "the registered worktree changed while the remote handoff baseline was captured", true, true)
			}
			digest := sha256.Sum256(registered.IdentityJSON)
			baseline.WorktreeIdentity = hex.EncodeToString(digest[:])
		}
		encoded, encodeErr := json.Marshal(map[string]any{"drained": true, "intent": intent, "remote": baseline})
		if encodeErr != nil {
			return daemon.failure(request, "internal_encoding", "takeover remote evidence could not be encoded", false)
		}
		eventPayload = string(encoded)
	}
	result, err := daemon.engine.Signal(ctx, contracts.SignalRequest{
		Ticket: stored.Ref, TicketVersion: stored.Version, From: stored.State, Trigger: "process_and_effects_drained",
		Fence:        domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch},
		Attributes:   map[string]string{"no_live_writer": "true", "no_unreconciled_mutation": "true"},
		EventPayload: eventPayload,
	})
	if err != nil {
		code, message := "control_completion_failed", "drained control state could not be committed"
		if errors.Is(err, store.ErrControlNotDrained) {
			code, message = "uncertain_effect", "an external effect must be reconciled before control can complete"
		}
		return daemon.controlFailure(request, stored, intent, code, message, errors.Is(err, store.ErrBusy), true)
	}
	finished, err := daemon.store.Ticket(ctx, stored.Ref)
	if err != nil || finished.State != target || finished.Version != result.TicketVersion {
		return daemon.controlFailure(request, stored, intent, "control_state_unavailable", "completed control state could not be confirmed", true, true)
	}
	if intent == "cancel" {
		if err := daemon.retireRuntimeControl(ctx, finished.Ref); err != nil {
			// The durable terminal transition is already committed. Retrying the
			// same cancellation can only finish terminal cleanup; it never
			// reopens Store admission or the runtime stop latch.
			return daemon.controlFailure(request, finished, intent, "runtime_retirement_failed", "terminal runtime control cleanup failed; retry cancellation without resuming the ticket", true, true)
		}
	}
	if intent == "take" {
		return daemon.takeoverSuccess(ctx, request, finished, false)
	}
	return daemon.controlSuccess(request, finished, intent, false)
}

// inspectTakeover is intentionally a read-only daemon boundary. A ticket
// without a registered worktree has no editable checkout yet, so its resume
// may take the unchanged path. Once a worktree exists, a configured runtime
// inspector must reauthenticate it; local Store fields alone are not proof.
func (daemon *Daemon) inspectTakeover(ctx context.Context, ref domain.TicketRef) (contracts.TakeoverInspection, error) {
	registered, err := daemon.store.Worktree(ctx, ref)
	if errors.Is(err, store.ErrNotFound) {
		return contracts.TakeoverInspection{Clean: true, ChangeKind: "no_worktree", RemoteIdentityExact: true}, nil
	}
	if err != nil {
		return contracts.TakeoverInspection{}, err
	}
	inspector, ok := daemon.control.(RuntimeTakeoverInspector)
	if !ok {
		return contracts.TakeoverInspection{}, errors.New("authenticated takeover inspection is unavailable")
	}
	inspection, err := inspector.InspectTakeover(ctx, ref)
	if err != nil {
		return contracts.TakeoverInspection{}, err
	}
	if !inspection.Registered || inspection.Path != registered.Path || inspection.Branch != registered.Branch || inspection.BaseSHA != registered.BaseSHA || inspection.Repository == "" {
		return contracts.TakeoverInspection{}, errors.New("takeover inspection does not match the registered worktree")
	}
	return inspection, nil
}

func (daemon *Daemon) takeoverSuccess(ctx context.Context, request api.Request, stored store.Ticket, observed bool) api.Response {
	inspection, err := daemon.inspectTakeover(ctx, stored.Ref)
	if err != nil {
		return daemon.controlFailure(request, stored, "take", "takeover_inspection_failed", "ticket is paused but its worktree identity could not be authenticated for handoff", false, true)
	}
	view := ticketView(stored)
	view["control"] = "take"
	view["takeover"] = map[string]any{
		"registered": inspection.Registered, "path": inspection.Path, "branch": inspection.Branch,
		"repository": inspection.Repository, "origin": inspection.Origin, "push_origin": inspection.PushOrigin, "base_sha": inspection.BaseSHA, "head_sha": inspection.HeadSHA,
		"clean": inspection.Clean, "change_kind": inspection.ChangeKind, "changed_files": inspection.ChangedFiles,
		"source_resumable": inspection.SourceResumable, "source_commit": inspection.SourceCommit,
		"remote_candidate_present": inspection.RemoteCandidatePresent, "remote_candidate_sha": inspection.RemoteCandidateSHA,
		"remote_base_sha": inspection.RemoteBaseSHA, "remote_identity_exact": inspection.RemoteIdentityExact,
		"retained_proof_digest": inspection.RetainedProofDigest, "retained_policy_digest": inspection.RetainedPolicyDigest,
		"retained_version": inspection.RetainedVersion, "retained_leader_epoch": inspection.RetainedLeaderEpoch, "retained_runner_epoch": inspection.RetainedRunnerEpoch,
	}
	next := domain.NextAction{Code: "takeover_resume", Argv: []string{daemon.executable(), "resume", string(stored.Ref.Ticket)}}
	if disposition, dispositionErr := daemon.store.ProviderRetryDisposition(ctx, stored); dispositionErr == nil {
		switch disposition {
		case store.ProviderRetryEligible:
			next = domain.NextAction{Code: "provider_retry", Argv: []string{daemon.executable(), "retry", string(stored.Ref.Ticket)}}
		case store.ProviderRetryExhausted:
			next = domain.NextAction{Code: "provider_retry_exhausted", Argv: []string{daemon.executable(), "cancel", string(stored.Ref.Ticket)}}
		case store.ProviderRetryResubmissionRequired:
			next = domain.NextAction{Code: "provider_retry_resubmit_required", Argv: []string{daemon.executable(), "cancel", string(stored.Ref.Ticket)}}
		default:
			if retryable, retryableErr := daemon.store.RetryablePause(ctx, stored); retryableErr == nil && retryable {
				next = domain.NextAction{Code: "ticket_retry", Argv: []string{daemon.executable(), "retry", string(stored.Ref.Ticket)}}
			}
		}
	}
	view["next_action"] = next
	return daemon.success(request, api.Mutation{Attempted: true, Kind: "ticket_take", Identity: string(stored.Ref.Ticket), Observed: observed}, view)
}

func (daemon *Daemon) retireRuntimeControl(ctx context.Context, ref domain.TicketRef) error {
	retirer, ok := daemon.control.(RuntimeRetirementController)
	if !ok {
		// A no-factory composition has no in-memory workflow admission state.
		// Retaining support for its injected legacy controller is coherent as it
		// cannot expose a real runtime behind an idle control boundary.
		return nil
	}
	return retirer.Retire(ctx, ref)
}

func (daemon *Daemon) resumeTicket(ctx context.Context, request api.Request, identity domain.OperatorIdentity) api.Response {
	// See controlTicket: a rearm must use one exact runtime/control bundle and
	// cannot interleave with its first installation after qualification.
	daemon.runtimeMu.Lock()
	defer daemon.runtimeMu.Unlock()
	if daemon.isClosed() || daemon.runtimeStopped {
		return daemon.failure(request, "daemon_stopping", "ticket resume is unavailable while the daemon is stopping", true)
	}
	var parameters controlParameters
	if err := decodeParameters(request.Parameters, &parameters); err != nil || parameters.Channel != daemon.channel || (parameters.Operator != "" && parameters.Operator != identity.Label) {
		return daemon.failure(request, "invalid_resume", "resume requires the authenticated operator and daemon channel", false)
	}
	ref, response := daemon.ticketRefByID(ctx, request)
	if response != nil {
		return *response
	}
	controller, ok := daemon.control.(RuntimeRearmController)
	if !ok {
		return daemon.failure(request, "runtime_rearm_unavailable", "ticket resume is unavailable until the runtime control boundary is configured", true)
	}
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	stored, err := daemon.store.Ticket(ctx, ref)
	if err != nil {
		return daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
	}
	transitioned := false
	if stored.State == domain.StatePaused {
		retryable, retryableErr := daemon.store.RetryablePause(ctx, stored)
		if retryableErr != nil {
			return daemon.failure(request, "retry_state_unavailable", "the pause lineage could not be authenticated", true)
		}
		if retryable {
			return daemon.failure(request, "retry_required", "this pause is a bounded retry/correction stop; use ticket retry to preserve its normative lineage", false)
		}
		inspection, inspectErr := daemon.inspectTakeover(ctx, ref)
		if inspectErr != nil {
			return daemon.failure(request, "takeover_inspection_failed", "the retained worktree cannot be authenticated; resume is blocked until its repository identity is repaired", false)
		}
		attributes := map[string]string{
			"operator_identity_authenticated": "true", "prerequisites_green": "true",
		}
		sourceResume := false
		switch {
		case !inspection.RemoteIdentityExact:
			return daemon.failure(request, "takeover_remote_drift", "the candidate branch or protected base changed after take; sf retained the paused worktree and started no provider", false)
		case inspection.SourceResumable && inspection.Clean && inspection.ChangeKind == "source_commit":
			attributes["takeover_source_commit_valid"] = "true"
			attributes["verification_files_unchanged"] = "true"
			attributes["branch_remote_identity_exact"] = "true"
			sourceResume = true
		case inspection.Clean:
			attributes["takeover_diff_none"] = "true"
			attributes["branch_remote_identity_exact"] = "true"
		default:
			code, message := "takeover_changes_unadopted", "operator changes are retained but cannot yet enter an authenticated Builder cycle"
			if inspection.ChangeKind == "source_commit_required" {
				code, message = "source_commit_required", "operator source edits are retained but cannot execute directly; create one clean commit on this branch, then resume so sf can authenticate it and run a fresh Reviewer"
			} else if inspection.ChangeKind == "verification_changes" {
				code, message = "takeover_verification_changes_unadopted", "verification-owned files changed; restore verification-owned files to the authenticated baseline, then resume the ticket"
			} else if inspection.ChangeKind == "source_out_of_scope" {
				code, message = "takeover_source_out_of_scope", "operator changes are outside the approved Planner paths; retain them and amend the plan before resuming"
			}
			return daemon.failure(request, code, message, false)
		}
		fence := domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch}
		var result contracts.TransitionResult
		if sourceResume {
			baseline, baselineErr := daemon.store.OperatorTakeRemoteBaseline(ctx, ref, stored.Version)
			if baselineErr != nil {
				return daemon.failure(request, "takeover_remote_evidence_unavailable", "the take-time remote baseline is missing or malformed; sf retained the paused worktree and started no provider", false)
			}
			result, err = daemon.engine.SignalOperatorSourceResume(ctx, store.OperatorSourceResume{Ref: ref, ExpectedVersion: stored.Version, Fence: fence, Operator: identity.Label, SourceCommit: inspection.SourceCommit, Remote: baseline})
		} else {
			payload, encodeErr := json.Marshal(map[string]any{"intent": "resume", "operator": identity.Label, "change_kind": inspection.ChangeKind, "changed_files": inspection.ChangedFiles})
			if encodeErr != nil {
				return daemon.failure(request, "internal_encoding", "resume control metadata could not be encoded", false)
			}
			result, err = daemon.engine.Signal(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: stored.Version, From: stored.State, Trigger: "operator_resume", Fence: fence, Attributes: attributes, EventPayload: string(payload)})
		}
		if err != nil {
			return daemon.failure(request, "resume_transition_refused", "ticket cannot resume from its current durable state", false)
		}
		stored, err = daemon.store.Ticket(ctx, ref)
		if err != nil || stored.Version != result.TicketVersion {
			return daemon.failure(request, "resume_state_unavailable", "resume transition could not be confirmed", true)
		}
		transitioned = true
	}
	shouldRearm := transitioned
	if !shouldRearm {
		if state, ok := daemon.control.(RuntimeRearmStateController); ok {
			needed, stateErr := state.RuntimeRearmNeeded(ctx, ref)
			if stateErr != nil {
				return daemon.failure(request, "runtime_rearm_failed", "resume state could not determine whether runtime admission is sealed", true)
			}
			shouldRearm = needed
		}
	}
	if shouldRearm {
		if err := controller.Rearm(ctx, ref); err != nil {
			return daemon.failure(request, "runtime_rearm_failed", "resume is durably sealed until runtime admission is installed; retry resume after the local runtime is available", true)
		}
	}
	return daemon.success(request, api.Mutation{Attempted: transitioned, Kind: "ticket_resume", Identity: string(ref.Ticket), Observed: !transitioned}, ticketView(stored))
}

func (daemon *Daemon) retryTicket(ctx context.Context, request api.Request, identity domain.OperatorIdentity) api.Response {
	return daemon.resumeWithTrigger(ctx, request, identity, "operator_retry", "retry")
}

type recoverParameters struct {
	Operator string         `json:"operator"`
	Channel  domain.Channel `json:"channel"`
	Mode     string         `json:"mode"`
}

func (daemon *Daemon) recoverTicket(ctx context.Context, request api.Request, identity domain.OperatorIdentity) api.Response {
	// Recovery drains and may rearm a runtime admission, so it must not race
	// initial runtime composition or another control request.
	daemon.runtimeMu.Lock()
	defer daemon.runtimeMu.Unlock()
	if daemon.isClosed() || daemon.runtimeStopped {
		return daemon.failure(request, "daemon_stopping", "ticket recovery is unavailable while the daemon is stopping", true)
	}
	var parameters recoverParameters
	if err := decodeParameters(request.Parameters, &parameters); err != nil || parameters.Channel != daemon.channel || (parameters.Operator != "" && parameters.Operator != identity.Label) || (parameters.Mode != "" && parameters.Mode != "guarded") {
		return daemon.failure(request, "invalid_recover", "recover requires the authenticated operator, daemon channel, and optional guarded mode", false)
	}
	ref, response := daemon.ticketRefByID(ctx, request)
	if response != nil {
		return *response
	}
	stored, err := daemon.store.Ticket(ctx, ref)
	if err != nil {
		return daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
	}
	if stored.State != domain.StateBlocked {
		return daemon.failure(request, "invalid_transition", "only a typed blocked ticket can be recovered", false)
	}
	if stored.BlockedCode == "legacy_provider_phase_entry_unverifiable" {
		return daemon.failure(request, "legacy_provider_entry_unverifiable", "this pre-v51 provider ticket cannot safely resume; cancel it and submit a fresh ticket", false)
	}
	if nonRecoverableTicketBlocker(stored.BlockedCode) {
		return daemon.failure(request, stored.BlockedCode, "this ticket's safety boundary cannot be recovered; cancel it, then submit a fresh ticket", false)
	}
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	// A recovery is never just a counter transition: seal/drain before
	// reopening so an old writer or uncertain effect cannot survive it.
	if drained, drainErr := daemon.control.Drain(ctx, ref); drainErr != nil || !drained {
		return daemon.controlFailure(request, stored, "recover", "blocked_process", "blocked recovery requires a completed local drain", drainErr != nil, true)
	}
	trigger := "operator_recover"
	attributes := map[string]string{"operator_identity_authenticated": "true", "typed_prerequisites_satisfied": "true", "no_live_writer": "true", "runner_epoch_current": "true"}
	if parameters.Mode == "guarded" {
		if stored.BlockedCode != "autonomy_ineligible" {
			return daemon.failure(request, "recover_mode_refused", "guarded recovery is only valid for an autonomy-ineligible blocker", false)
		}
		merged, mergeErr := daemon.control.MergeObserved(ctx, ref)
		if mergeErr != nil || merged {
			return daemon.failure(request, "external_state_unavailable", "merge state must be observed before guarded recovery", mergeErr != nil)
		}
		trigger = "operator_recover_as_guarded"
		attributes = map[string]string{"operator_identity_authenticated": "true", "block_reason_autonomy_ineligible": "true", "project_allows_guarded": "true", "no_live_writer": "true", "merge_not_observed": "true"}
	}
	payload, err := json.Marshal(map[string]string{"intent": "recover", "operator": identity.Label, "mode": parameters.Mode, "blocked_code": stored.BlockedCode})
	if err != nil {
		return daemon.failure(request, "internal_encoding", "recovery metadata could not be encoded", false)
	}
	result, err := daemon.engine.Signal(ctx, contracts.SignalRequest{Ticket: ref, TicketVersion: stored.Version, From: stored.State, Trigger: trigger, Fence: domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch}, Attributes: attributes, EventPayload: string(payload)})
	if err != nil {
		return daemon.failure(request, "recover_transition_refused", "the typed blocker cannot be recovered with current prerequisites", false)
	}
	current, err := daemon.store.Ticket(ctx, ref)
	if err != nil || current.Version != result.TicketVersion {
		return daemon.failure(request, "resume_state_unavailable", "recovery transition could not be confirmed", true)
	}
	if controller, ok := daemon.control.(RuntimeRearmController); ok && current.State != domain.StatePublishing && current.State != domain.StateWaitingCI {
		if err := controller.Rearm(ctx, ref); err != nil {
			return daemon.failure(request, "runtime_rearm_failed", "recovery is durably sealed until runtime admission is installed", true)
		}
	}
	return daemon.success(request, api.Mutation{Attempted: true, Kind: "ticket_recover", Identity: string(ref.Ticket)}, ticketView(current))
}

func (daemon *Daemon) resumeWithTrigger(ctx context.Context, request api.Request, identity domain.OperatorIdentity, trigger, kind string) api.Response {
	// operator_retry is intentionally narrower than resume: it only follows a
	// direct retry/correction exhaustion pause. A normal take/pause must use
	// the inspected operator_resume path.
	//
	// It still changes the live runtime fence, so serialize it with take,
	// resume, and recover. In particular, a retry must not race a runtime
	// shutdown or a concurrent resume that could otherwise arm two workers.
	daemon.runtimeMu.Lock()
	defer daemon.runtimeMu.Unlock()
	if daemon.isClosed() || daemon.runtimeStopped {
		return daemon.failure(request, "daemon_stopping", "ticket retry is unavailable while the daemon is stopping", true)
	}
	var parameters controlParameters
	if err := decodeParameters(request.Parameters, &parameters); err != nil || parameters.Channel != daemon.channel || (parameters.Operator != "" && parameters.Operator != identity.Label) {
		return daemon.failure(request, "invalid_retry", "retry requires the authenticated operator and daemon channel", false)
	}
	ref, response := daemon.ticketRefByID(ctx, request)
	if response != nil {
		return *response
	}
	stored, err := daemon.store.Ticket(ctx, ref)
	if err != nil {
		return daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
	}
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	// A response can be lost after the authenticated retry transition commits
	// but before runtime admission is installed.  In that state the durable
	// control remains sealed and the ticket is already active, so replay must
	// finish the handoff rather than attempting a second lifecycle transition.
	if stored.State != domain.StatePaused {
		providerReplay, replayErr := daemon.store.ProviderRetryRuntimeReplay(ctx, stored)
		if errors.Is(replayErr, store.ErrProviderRetryRequiresResubmission) {
			return daemon.providerRetryResubmitFailure(request, ref, false, true)
		}
		if replayErr != nil {
			return daemon.failure(request, "retry_state_unavailable", "provider retry state could not be authenticated", true)
		}
		switch providerReplay {
		case store.ProviderRetryNeedsRearm:
			controller, ok := daemon.control.(RuntimeProviderRetryRearmController)
			if !ok {
				return daemon.providerRetryRearmFailure(request, ref, false, true, "the provider retry is durably committed, but this runtime cannot authenticate and rearm its retained worktree")
			}
			fence := domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch}
			ready, rearmErr := controller.RearmProviderRetry(ctx, ref, stored.Version, fence)
			if errors.Is(rearmErr, store.ErrProviderRetryRequiresResubmission) {
				return daemon.providerRetryResubmitFailure(request, ref, false, true)
			}
			if rearmErr != nil || !ready {
				return daemon.providerRetryRearmFailure(request, ref, false, true, "the provider retry is durably committed, but its active worktree could not be reauthenticated; runtime admission remains sealed")
			}
			current, currentErr := daemon.store.Ticket(ctx, ref)
			if currentErr != nil {
				return daemon.failure(request, "resume_state_unavailable", "provider retry state could not be confirmed after runtime admission", true)
			}
			return daemon.success(request, api.Mutation{Attempted: false, Kind: "ticket_" + kind, Identity: string(ref.Ticket), Observed: true}, ticketView(current))
		case store.ProviderRetryAlreadyRearmed:
			preflight, ok := daemon.control.(RuntimeProviderRetryWorktreePreflight)
			if !ok {
				return daemon.failure(request, "provider_retry_worktree_unavailable", "the committed provider retry cannot reauthenticate its retained worktree in this runtime", true)
			}
			ready, proofErr := preflight.AuthenticateProviderRetryWorktree(ctx, ref, stored.Version, domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch})
			if errors.Is(proofErr, store.ErrProviderRetryRequiresResubmission) {
				return daemon.providerRetryResubmitFailure(request, ref, false, true)
			}
			if proofErr != nil {
				return daemon.failure(request, "provider_retry_worktree_unavailable", "the committed provider retry could not reauthenticate its retained worktree", true)
			}
			if !ready {
				return daemon.failure(request, "provider_retry_worktree_unready", "the committed provider retry worktree is no longer pristine and authenticated; inspect it with take before continuing", false)
			}
			current, currentErr := daemon.store.Ticket(ctx, ref)
			if currentErr != nil {
				return daemon.failure(request, "resume_state_unavailable", "provider retry state could not be confirmed after runtime admission", true)
			}
			return daemon.success(request, api.Mutation{Attempted: false, Kind: "ticket_" + kind, Identity: string(ref.Ticket), Observed: true}, ticketView(current))
		case store.ProviderRetryLegacyUnsealed:
			return daemon.providerRetryResubmitFailure(request, ref, false, true)
		}
		if replay, ok := daemon.control.(RuntimeMergeRetryReplayController); ok {
			state, stateErr := replay.GuardedMergeRetryReplay(ctx, ref)
			if stateErr != nil && !errors.Is(stateErr, store.ErrStaleFence) {
				return daemon.failure(request, "runtime_rearm_failed", "retry state could not authenticate the committed merge retry", true)
			}
			if stateErr == nil && state == store.GuardedMergeRetryNeedsRearm {
				controller, ok := daemon.control.(RuntimeRearmController)
				if !ok {
					return daemon.failure(request, "runtime_rearm_unavailable", "retry is durably sealed until runtime admission is configured", true)
				}
				if err := controller.Rearm(ctx, ref); err != nil {
					return daemon.failure(request, "runtime_rearm_failed", "retry is durably sealed until runtime admission is installed; retry after the local runtime is available", true)
				}
				state = store.GuardedMergeRetryAlreadyRearmed
			}
			if stateErr == nil && state == store.GuardedMergeRetryAlreadyRearmed {
				current, currentErr := daemon.store.Ticket(ctx, ref)
				if currentErr != nil {
					return daemon.failure(request, "resume_state_unavailable", "retry state could not be confirmed after runtime admission", true)
				}
				return daemon.success(request, api.Mutation{Attempted: false, Kind: "ticket_" + kind, Identity: string(ref.Ticket), Observed: true}, ticketView(current))
			}
		}
		return daemon.failure(request, "retry_not_available", "retry is available only after a durable retry or correction exhaustion pause", false)
	}
	retryable, retryableErr := daemon.store.RetryablePause(ctx, stored)
	if retryableErr != nil {
		return daemon.failure(request, "retry_state_unavailable", "the pause lineage could not be authenticated", true)
	}
	if !retryable {
		return daemon.failure(request, "retry_not_available", "retry is available only after a durable retry or correction exhaustion pause", false)
	}
	providerRetry, providerRetryErr := daemon.store.ProviderRetryDisposition(ctx, stored)
	if providerRetryErr != nil {
		return daemon.failure(request, "retry_state_unavailable", "provider retry pause could not be authenticated", true)
	}
	if providerRetry == store.ProviderRetryExhausted {
		return daemon.failure(request, "provider_retry_exhausted", "the one permitted provider retry window has already been exhausted; cancel and resubmit the ticket", false)
	}
	if providerRetry == store.ProviderRetryResubmissionRequired {
		return daemon.providerRetryResubmitFailure(request, ref, false, false)
	}
	if providerRetry == store.ProviderRetryEligible {
		preflight, ok := daemon.control.(RuntimeProviderRetryWorktreePreflight)
		if !ok {
			return daemon.failure(request, "provider_retry_worktree_unavailable", "the retained retry worktree cannot be authenticated by this runtime; sf did not consume the retry", true)
		}
		fence := domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch}
		ready, readinessErr := preflight.AuthenticateProviderRetryWorktree(ctx, ref, stored.Version, fence)
		if errors.Is(readinessErr, store.ErrProviderRetryRequiresResubmission) {
			return daemon.providerRetryResubmitFailure(request, ref, false, false)
		}
		if readinessErr != nil {
			return daemon.failure(request, "provider_retry_worktree_unavailable", "the retained retry worktree could not be authenticated; sf did not consume the retry", true)
		}
		if !ready {
			return daemon.failure(request, "provider_retry_worktree_unready", "the retained retry worktree is not pristine and authenticated; sf did not consume the retry—inspect it with take, restore it to a clean reviewed state, then retry, or cancel and resubmit", false)
		}
		if _, ok := daemon.control.(RuntimeProviderRetryRearmController); !ok {
			return daemon.failure(request, "runtime_rearm_unavailable", "provider retry requires the sealed local runtime rearm authority; sf did not consume the retry", true)
		}
		drained, drainErr := daemon.control.Drain(ctx, ref)
		if drainErr != nil || !drained {
			return daemon.controlFailure(request, stored, kind, "blocked_process", "provider retry requires a completed local drain; sf did not consume the retry", drainErr != nil, true)
		}
		if err := daemon.lease.Validate(); err != nil {
			return daemon.providerRetryRearmFailure(request, ref, true, false, "daemon leadership changed after the provider retry runtime was durably sealed; sf did not consume the retry")
		}
		drainedTicket, ticketErr := daemon.store.Ticket(ctx, ref)
		if ticketErr != nil || drainedTicket.State != domain.StatePaused || drainedTicket.ResumeState != stored.ResumeState || drainedTicket.Version != stored.Version || drainedTicket.RunnerEpoch != stored.RunnerEpoch {
			return daemon.providerRetryRearmFailure(request, ref, true, false, "the provider retry pause changed after its runtime was durably sealed; sf did not consume the retry")
		}
		ready, readinessErr = preflight.AuthenticateProviderRetryWorktree(ctx, ref, drainedTicket.Version, domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: drainedTicket.RunnerEpoch})
		if errors.Is(readinessErr, store.ErrProviderRetryRequiresResubmission) {
			return daemon.providerRetryResubmitFailure(request, ref, true, false)
		}
		if readinessErr != nil {
			return daemon.providerRetryRearmFailure(request, ref, true, false, "the retained retry worktree could not be reauthenticated after the runtime was durably sealed; sf did not consume the retry")
		}
		if !ready {
			return daemon.providerRetryRearmFailure(request, ref, true, false, "the retained retry worktree changed while the runtime was draining; sf did not consume the retry")
		}
		stored = drainedTicket
	}
	// Merge/reconciliation retry is a post-publication mutation boundary. Stop the
	// in-memory admission before Store seals and advances the ticket so the
	// subsequent opaque rearm token has an exact stopped runtime to authorize.
	// Other semantic retries retain their existing no-control fast path.
	requiresGuardedMergeRearm := stored.ResumeState == domain.StateMerging || stored.ResumeState == domain.StateReconciling
	var mergeRearm RuntimeRearmController
	if requiresGuardedMergeRearm {
		var ok bool
		mergeRearm, ok = daemon.control.(RuntimeRearmController)
		if !ok {
			return daemon.failure(request, "runtime_rearm_unavailable", "post-publication retry requires the local runtime rearm authority", true)
		}
		drained, drainErr := daemon.control.Drain(ctx, ref)
		if drainErr != nil || !drained {
			return daemon.controlFailure(request, stored, kind, "blocked_process", "post-publication retry requires a completed local drain", drainErr != nil, true)
		}
	}
	payload, err := json.Marshal(map[string]string{"intent": kind, "operator": identity.Label})
	if err != nil {
		return daemon.failure(request, "internal_encoding", "retry metadata could not be encoded", false)
	}
	requestSignal := contracts.SignalRequest{Ticket: ref, TicketVersion: stored.Version, From: stored.State, Trigger: trigger, Fence: domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch}, Attributes: map[string]string{"operator_identity_authenticated": "true", "pause_reason_retryable": "true", "typed_prerequisites_satisfied": "true", "runner_epoch_current": "true"}, EventPayload: string(payload)}
	var result contracts.TransitionResult
	if providerRetry == store.ProviderRetryEligible {
		result, err = daemon.engine.SignalProviderRetry(ctx, requestSignal)
	} else {
		result, err = daemon.engine.Signal(ctx, requestSignal)
	}
	if err != nil {
		return daemon.failure(request, "retry_transition_refused", "the paused ticket could not start its bounded retry", false)
	}
	current, err := daemon.store.Ticket(ctx, ref)
	if err != nil || current.Version != result.TicketVersion {
		return daemon.failure(request, "resume_state_unavailable", "retry transition could not be confirmed", true)
	}
	if providerRetry == store.ProviderRetryEligible {
		if err := daemon.lease.Validate(); err != nil {
			return daemon.providerRetryRearmFailure(request, ref, true, false, "the provider retry committed, but daemon leadership changed before runtime admission; the runtime remains sealed")
		}
		controller := daemon.control.(RuntimeProviderRetryRearmController)
		ready, rearmErr := controller.RearmProviderRetry(ctx, ref, current.Version, domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: current.RunnerEpoch})
		if errors.Is(rearmErr, store.ErrProviderRetryRequiresResubmission) {
			return daemon.providerRetryResubmitFailure(request, ref, true, false)
		}
		if rearmErr != nil || !ready {
			return daemon.providerRetryRearmFailure(request, ref, true, false, "the provider retry committed, but its active worktree could not be reauthenticated; the runtime remains sealed")
		}
		return daemon.success(request, api.Mutation{Attempted: true, Kind: "ticket_" + kind, Identity: string(ref.Ticket)}, ticketView(current))
	}
	if requiresGuardedMergeRearm {
		if err := mergeRearm.Rearm(ctx, ref); err != nil {
			return daemon.failure(request, "runtime_rearm_failed", "retry is durably sealed until runtime admission is installed; retry after the local runtime is available", true)
		}
		return daemon.success(request, api.Mutation{Attempted: true, Kind: "ticket_" + kind, Identity: string(ref.Ticket)}, ticketView(current))
	}
	// Retry-exhaustion pauses normally have no sealed admission. The narrow
	// crash window after a prior control operation is different: if Store still
	// proves the exact control row sealed, rearm it while holding runtimeMu so
	// one successful retry cannot admit two runtimes.
	if state, ok := daemon.control.(RuntimeRearmStateController); ok {
		needed, stateErr := state.RuntimeRearmNeeded(ctx, ref)
		if stateErr != nil {
			return daemon.failure(request, "runtime_rearm_failed", "retry state could not determine whether runtime admission is sealed", true)
		}
		if needed {
			controller, ok := daemon.control.(RuntimeRearmController)
			if !ok {
				return daemon.failure(request, "runtime_rearm_unavailable", "retry is durably sealed until runtime admission is configured", true)
			}
			if err := controller.Rearm(ctx, ref); err != nil {
				return daemon.failure(request, "runtime_rearm_failed", "retry is durably sealed until runtime admission is installed; retry after the local runtime is available", true)
			}
		}
	}
	return daemon.success(request, api.Mutation{Attempted: true, Kind: "ticket_" + kind, Identity: string(ref.Ticket)}, ticketView(current))
}

func (daemon *Daemon) providerRetryRearmFailure(request api.Request, ref domain.TicketRef, attempted, observed bool, message string) api.Response {
	response := daemon.failure(request, "provider_retry_rearm_blocked", message+"; inspect the ticket and retained worktree, restore the exact authenticated head if needed, then run retry again", true)
	response.Mutation = api.Mutation{Attempted: attempted, Observed: observed, Kind: "ticket_retry", Identity: string(ref.Ticket)}
	return response
}

func (daemon *Daemon) providerRetryResubmitFailure(request api.Request, ref domain.TicketRef, attempted, observed bool) api.Response {
	response := daemon.failure(request, "provider_retry_resubmit_required", "this retry crosses a source-resume or verification-amendment boundary whose retained worktree cannot be safely reused; cancel and resubmit the ticket", false)
	response.Mutation = api.Mutation{Attempted: attempted, Observed: observed, Kind: "ticket_retry", Identity: string(ref.Ticket)}
	return response
}

func (daemon *Daemon) ticketRefByID(ctx context.Context, request api.Request) (domain.TicketRef, *api.Response) {
	if request.Ticket == "" {
		response := daemon.failure(request, "invalid_ticket_reference", "ticket is required", false)
		return domain.TicketRef{}, &response
	}
	stored, err := daemon.store.TicketByID(ctx, daemon.channel, domain.TicketID(request.Ticket))
	if err != nil {
		response := daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
		return domain.TicketRef{}, &response
	}
	return stored.Ref, nil
}

func (daemon *Daemon) controlSuccess(request api.Request, stored store.Ticket, intent string, observed bool) api.Response {
	view := ticketView(stored)
	view["control"] = intent
	return daemon.success(request, api.Mutation{Attempted: true, Kind: "ticket_" + intent, Identity: string(stored.Ref.Ticket), Observed: observed}, view)
}

func (daemon *Daemon) controlFailure(request api.Request, stored store.Ticket, intent, code, message string, retryable, attempted bool) api.Response {
	response := daemon.failure(request, code, message, retryable)
	response.Mutation = api.Mutation{Attempted: attempted, Kind: "ticket_" + intent, Identity: string(stored.Ref.Ticket)}
	if stored.Ref.Ticket != "" {
		response.Data, _ = json.Marshal(ticketView(stored))
	}
	return response
}

func leaseCapacities(project store.Project) (int, int, error) {
	if project.ConfigGeneration == 0 && len(project.ConfigSnapshot) == 0 && project.ConfigDigest == "" {
		defaults := config.DefaultMachineLimits()
		return defaults.MaxConcurrentTickets, defaults.MaxConcurrentTickets, nil
	}
	effective, err := config.DecodeSnapshot(project.ConfigSnapshot, project.ConfigDigest)
	if err != nil {
		return 0, 0, err
	}
	if domain.ProjectID(effective.Name) != project.ID || effective.Repository != project.Path || effective.BaseBranch != project.BaseRef {
		return 0, 0, errors.New("configuration snapshot does not match durable project identity")
	}
	return effective.Machine.MaxConcurrentTickets, effective.MaxConcurrentTickets, nil
}

func (daemon *Daemon) ticketRef(ctx context.Context, request api.Request) (domain.TicketRef, *api.Response) {
	var parameters ticketParameters
	if err := decodeParameters(request.Parameters, &parameters); err != nil {
		response := daemon.failure(request, "invalid_ticket_reference", "ticket reference parameters are invalid", false)
		return domain.TicketRef{}, &response
	}
	if parameters.Channel != daemon.channel {
		response := daemon.failure(request, "wrong_channel", "the request channel does not match this daemon", false)
		return domain.TicketRef{}, &response
	}
	if parameters.Ticket == "" {
		parameters.Ticket = request.Ticket
	}
	if parameters.Ticket == "" {
		response := daemon.failure(request, "invalid_ticket_reference", "ticket is required", false)
		return domain.TicketRef{}, &response
	}
	if parameters.Project == "" {
		stored, err := daemon.store.TicketByID(ctx, daemon.channel, domain.TicketID(parameters.Ticket))
		if err != nil {
			response := daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
			return domain.TicketRef{}, &response
		}
		return stored.Ref, nil
	}
	ref := domain.TicketRef{Channel: daemon.channel, Project: domain.ProjectID(parameters.Project), Ticket: domain.TicketID(parameters.Ticket)}
	if err := ref.Validate(); err != nil {
		response := daemon.failure(request, "invalid_ticket_reference", "project and ticket are required", false)
		return domain.TicketRef{}, &response
	}
	return ref, nil
}

func (daemon *Daemon) statusTickets(ctx context.Context, request api.Request, identity domain.OperatorIdentity) api.Response {
	var parameters struct {
		Project string         `json:"project"`
		Watch   bool           `json:"watch"`
		Channel domain.Channel `json:"channel"`
	}
	if err := decodeParameters(request.Parameters, &parameters); err != nil || parameters.Channel != daemon.channel {
		return daemon.failure(request, "wrong_channel", "status requires the daemon channel", false)
	}
	if request.Ticket != "" {
		var ref domain.TicketRef
		if parameters.Project == "" {
			stored, err := daemon.store.TicketByID(ctx, daemon.channel, domain.TicketID(request.Ticket))
			if err != nil {
				return daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
			}
			ref = stored.Ref
		} else {
			ref = domain.TicketRef{Channel: daemon.channel, Project: domain.ProjectID(parameters.Project), Ticket: domain.TicketID(request.Ticket)}
			if err := ref.Validate(); err != nil {
				return daemon.failure(request, "invalid_ticket_reference", "project and ticket are required", false)
			}
		}
		stored, err := daemon.store.Ticket(ctx, ref)
		if err != nil {
			return daemon.failure(request, "ticket_not_found", "ticket is not present in this channel", false)
		}
		evidence, err := daemon.evidenceView(ctx, stored.Ref)
		if err != nil {
			return daemon.failure(request, evidenceErrorCode(err), "durable workflow evidence could not be authenticated", errors.Is(err, store.ErrBusy))
		}
		view := map[string]any{"channel": daemon.channel, "watch": parameters.Watch, "current_version": stored.Version, "operator": operatorView(identity), "ticket": ticketView(stored), "evidence": evidence}
		if action, ok := daemon.ticketBlockedNextAction(stored); ok {
			view["next_action"] = action
		}
		return daemon.success(request, api.Mutation{}, view)
	}
	items, err := daemon.store.Tickets(ctx, daemon.channel, domain.ProjectID(parameters.Project), 1000)
	if err != nil {
		return daemon.failure(request, "status_unavailable", "ticket status could not be read", errors.Is(err, store.ErrBusy))
	}
	views := make([]map[string]any, 0, len(items))
	for _, item := range items {
		view := ticketView(item)
		if action, ok := daemon.ticketBlockedNextAction(item); ok {
			view["next_action"] = action
		}
		views = append(views, view)
	}
	return daemon.success(request, api.Mutation{}, map[string]any{"channel": daemon.channel, "watch": parameters.Watch, "leader_epoch": daemon.epoch, "operator": operatorView(identity), "tickets": views})
}

func (daemon *Daemon) status(request api.Request, identity domain.OperatorIdentity) api.Response {
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	return daemon.success(request, api.Mutation{}, map[string]any{"channel": daemon.channel, "leader_epoch": daemon.epoch, "operator": operatorView(identity), "socket_ready": true, "event_projection_ready": !daemon.eventProjectionPending()})
}

func (daemon *Daemon) success(request api.Request, mutation api.Mutation, value any) api.Response {
	data, err := json.Marshal(value)
	if err != nil {
		return daemon.failure(request, "internal_encoding", "daemon could not encode the response", false)
	}
	return api.Response{Version: api.Version, RequestID: request.RequestID, OK: true, Mutation: mutation, Data: data}
}

func (daemon *Daemon) failure(request api.Request, code, message string, retryable bool) api.Response {
	binary := daemon.executable()
	argv := []string{binary, "doctor"}
	verb := strings.TrimPrefix(request.Method, "ticket.")
	operatorVerb := func(fallback string) string {
		switch verb {
		case "take", "resume", "retry", "recover", "status":
			return verb
		default:
			return fallback
		}
	}
	if code == "autonomous_unavailable" {
		argv = []string{binary, "submit", "--help"}
	}
	if code == "unqualified_provider" {
		argv = []string{binary, "providers", "qualify", "--builder", "codex", "--reviewer", "codex"}
	}
	if code == "runtime_activation_failed" {
		argv = []string{binary, "providers", "qualify", "--builder", "codex", "--reviewer", "codex"}
	}
	if code == "runtime_already_active" {
		argv = []string{binary, "daemon", "status"}
	}
	if code == "daemon_stopping" {
		argv = []string{binary, "daemon", "status"}
	}
	if code == "terminal_replay_requires_new" {
		argv = []string{binary, "submit", "--help"}
	}
	if code == "doctor_required" || code == "operator_identity_required" {
		argv = []string{binary, "doctor"}
	}
	if code == "unknown_project" {
		argv = []string{binary, "init", "--help"}
	}
	if code == "invalid_submit" {
		argv = []string{binary, "submit", "--help"}
	}
	if code == "ticket_policy_refused" {
		argv = []string{binary, "submit", "--help"}
	}
	if code == "invalid_logs" {
		argv = []string{binary, "logs", "--help"}
	}
	// Operator control failures have a command-specific next action even when
	// the malformed request omitted its ticket. Do not collapse them into
	// doctor: the daemon already knows which control surface can recover.
	switch code {
	case "legacy_provider_entry_unverifiable":
		argv = []string{binary, "cancel", "--help"}
	case "ticket_budget_exhausted", "provider_result_indeterminate", "provider_repair_unavailable", "provider_retry_exhausted", "provider_retry_resubmit_required":
		argv = []string{binary, "cancel", "--help"}
	case "takeover_inspection_failed", "takeover_changes_unadopted", "takeover_source_out_of_scope", "takeover_remote_drift", "takeover_remote_evidence_unavailable":
		argv = []string{binary, "take", "--help"}
	case "source_commit_required":
		argv = []string{binary, operatorVerb("resume"), "--help"}
	case "takeover_verification_changes_unadopted":
		argv = []string{binary, operatorVerb("resume"), "--help"}
	case "invalid_resume":
		argv = []string{binary, "resume", "--help"}
	case "invalid_retry":
		argv = []string{binary, "retry", "--help"}
	case "invalid_recover":
		argv = []string{binary, "recover", "--help"}
	case "runtime_rearm_unavailable", "runtime_rearm_failed", "resume_state_unavailable", "resume_transition_refused":
		argv = []string{binary, operatorVerb("resume"), "--help"}
	case "retry_state_unavailable", "retry_not_available", "retry_transition_refused", "retry_required":
		argv = []string{binary, "retry", "--help"}
	case "provider_retry_worktree_unavailable":
		argv = []string{binary, "retry", "--help"}
	case "provider_retry_worktree_unready":
		argv = []string{binary, "take", "--help"}
	case "provider_retry_rearm_blocked":
		argv = []string{binary, "show", "--help"}
	case "recover_mode_refused", "recover_transition_refused":
		argv = []string{binary, "recover", "--help"}
	}
	if code == "invalid_control" || code == "invalid_ticket_reference" || code == "invalid_decision" || code == "decision_refused" || code == "approval_head_changed" {
		if verb != "" && verb != request.Method {
			argv = []string{binary, verb, "--help"}
		}
	}
	if request.Ticket != "" {
		switch code {
		case "legacy_provider_entry_unverifiable":
			argv = []string{binary, "cancel", request.Ticket}
		case "ticket_budget_exhausted", "provider_result_indeterminate", "provider_repair_unavailable", "provider_retry_exhausted", "provider_retry_resubmit_required":
			argv = []string{binary, "cancel", request.Ticket}
		case "takeover_changes_unadopted", "takeover_source_out_of_scope", "takeover_remote_drift", "takeover_remote_evidence_unavailable":
			// `take` is intentionally idempotent and prints the authenticated
			// retained path again. It is the only safe next action for edits
			// that have not crossed the Builder/proof authority.
			argv = []string{binary, "take", request.Ticket}
		case "source_commit_required":
			argv = []string{binary, operatorVerb("resume"), request.Ticket}
		case "takeover_verification_changes_unadopted":
			// The prerequisite is deliberately manual: the daemon must not adopt
			// verification-owned edits or invent an amendment. Once the operator
			// restores/commits the approved files, this executable action retries
			// the authenticated takeover inspection and resume boundary.
			argv = []string{binary, operatorVerb("resume"), request.Ticket}
		case "retry_required":
			argv = []string{binary, "retry", request.Ticket}
		case "takeover_inspection_failed":
			argv = []string{binary, "take", request.Ticket}
		case "runtime_rearm_unavailable", "runtime_rearm_failed", "resume_state_unavailable", "resume_transition_refused":
			argv = []string{binary, operatorVerb("resume"), request.Ticket}
		case "retry_state_unavailable", "retry_not_available", "retry_transition_refused":
			argv = []string{binary, "retry", request.Ticket}
		case "provider_retry_worktree_unavailable":
			argv = []string{binary, "retry", request.Ticket}
		case "provider_retry_worktree_unready":
			argv = []string{binary, "take", request.Ticket}
		case "provider_retry_rearm_blocked":
			argv = []string{binary, "show", request.Ticket}
		case "recover_mode_refused", "recover_transition_refused":
			argv = []string{binary, "recover", request.Ticket}
		}
	}
	if request.Ticket != "" {
		switch code {
		case "ticket_not_found", "invalid_transition", "external_state_unavailable", "external_merge_observed", "control_state_unavailable", "control_drain_failed", "blocked_process", "uncertain_effect", "control_completion_failed":
			argv = []string{binary, "status", request.Ticket}
		}
	}
	if code == "not_ready" {
		argv = []string{binary, "--help"}
	}
	return api.Response{Version: api.Version, RequestID: request.RequestID, OK: false, Mutation: api.Mutation{}, Error: &api.Error{Code: code, Message: message, Retryable: retryable}, NextAction: &domain.NextAction{Code: code, Argv: argv}}
}

// executable is derived from the daemon's durable channel, never the caller's
// process name. A client must be able to run the returned action against the
// same isolated socket and state root that produced the response.
func (daemon *Daemon) executable() string {
	if daemon.channel == domain.ChannelDev {
		return "sf-dev"
	}
	return "sf"
}

func nonRecoverableTicketBlocker(code string) bool {
	switch code {
	case "ticket_budget_exhausted", "provider_result_indeterminate", "provider_repair_unavailable":
		return true
	default:
		return false
	}
}

func (daemon *Daemon) ticketBlockedNextAction(value store.Ticket) (domain.NextAction, bool) {
	if !nonRecoverableTicketBlocker(value.BlockedCode) || value.Ref.Ticket == "" {
		return domain.NextAction{}, false
	}
	return domain.NextAction{Code: value.BlockedCode, Argv: []string{daemon.executable(), "cancel", string(value.Ref.Ticket)}}, true
}

func ticketView(value store.Ticket) map[string]any {
	return map[string]any{"channel": value.Ref.Channel, "project": value.Ref.Project, "ticket": value.Ref.Ticket, "state": value.State, "resume_state": value.ResumeState, "version": value.Version, "runner_epoch": value.RunnerEpoch, "merge_mode": value.MergeMode, "blocked_code": value.BlockedCode, "created_at": value.CreatedAt.UTC().Format(time.RFC3339Nano)}
}

func operatorView(identity domain.OperatorIdentity) map[string]any {
	return map[string]any{"uid": identity.UID, "username": identity.Username, "label": identity.Label}
}

func ticketDetail(value store.Ticket) map[string]any {
	view := ticketView(value)
	view["title"] = value.Title
	view["problem"] = value.Problem
	view["acceptance"] = value.Acceptance
	view["source_digest"] = value.SourceDigest
	view["source"] = string(value.Source)
	view["type"] = value.Type
	view["priority"] = value.Priority
	view["max_duration_ns"] = int64(value.MaxDuration)
	view["max_cost_micro_usd"] = value.MaxCostMicroUSD
	view["workflow_id"] = value.WorkflowID
	view["blocked_code"] = value.BlockedCode
	return view
}

func decodeParameters(raw json.RawMessage, destination any) error {
	if len(raw) == 0 {
		return errors.New("parameters are required")
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("parameters contain trailing data")
	}
	return nil
}

func submitErrorCode(err error) string {
	switch {
	case errors.Is(err, store.ErrTerminalReplay):
		return "terminal_replay_requires_new"
	case errors.Is(err, store.ErrNotFound):
		return "unknown_project"
	case errors.Is(err, store.ErrBusy):
		return "store_busy"
	default:
		return "submit_refused"
	}
}

func validateConfig(configuration Config) error {
	if !configuration.Channel.Valid() || configuration.Paths.Root == "" || configuration.Paths.Database == "" || configuration.Paths.Socket == "" || configuration.DaemonIdentity == "" {
		return errors.New("channel, paths, and daemon identity are required")
	}
	if configuration.WorkflowRuntimeFactory != nil && configuration.Controller != nil {
		return errors.New("workflow runtime factory and controller must be composed as one runtime/control bundle")
	}
	paths := []string{configuration.Paths.Root, configuration.Paths.Database, configuration.Paths.Socket, configuration.Paths.Logs, configuration.Paths.Events, configuration.Paths.Worktrees, configuration.Paths.Backups}
	if configuration.StateMachinePath != "" {
		paths = append(paths, configuration.StateMachinePath)
	}
	for _, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return fmt.Errorf("daemon paths must be absolute")
		}
	}
	for _, path := range []string{configuration.Paths.Database, configuration.Paths.Socket, configuration.Paths.Logs, configuration.Paths.Events, configuration.Paths.Worktrees, configuration.Paths.Backups} {
		if !pathWithin(configuration.Paths.Root, path) {
			return errors.New("channel paths must remain below their channel root")
		}
	}
	return nil
}

func secureChannelPaths(paths config.ChannelPaths) error {
	for _, path := range []string{paths.Root, filepath.Join(paths.Root, "run"), filepath.Dir(paths.Socket), filepath.Dir(paths.Database), filepath.Dir(paths.Database + "-wal"), filepath.Dir(paths.Database + "-shm"), paths.Logs, paths.Events, paths.Worktrees, paths.Backups} {
		if err := secureDirectory(path); err != nil {
			return err
		}
	}
	return nil
}

func loadSpecification(path string) (statemachine.Spec, error) {
	if path == "" {
		return statemachine.LoadEmbeddedApproved()
	}
	if err := validateNoSymlinkComponents(path); err != nil {
		return statemachine.Spec{}, fmt.Errorf("validate normative state machine path: %w", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return statemachine.Spec{}, fmt.Errorf("open normative state machine: %w", err)
	}
	specification, loadErr := statemachine.LoadApproved(file)
	closeErr := file.Close()
	if loadErr != nil {
		return statemachine.Spec{}, fmt.Errorf("load normative state machine: %w", loadErr)
	}
	if closeErr != nil {
		return statemachine.Spec{}, fmt.Errorf("close normative state machine: %w", closeErr)
	}
	return specification, nil
}

func boundedContext(parent context.Context, configured time.Duration) (context.Context, context.CancelFunc) {
	if configured <= 0 {
		configured = 5 * time.Second
	}
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= configured {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, configured)
}

func secureDirectory(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("directory path must be absolute")
	}
	clean := filepath.Clean(path)
	volume := filepath.VolumeName(clean)
	root := volume + string(filepath.Separator)
	parts := strings.Split(strings.TrimPrefix(strings.TrimPrefix(clean, root), volume), string(filepath.Separator))
	current := root
	for _, part := range parts {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
				return fmt.Errorf("create channel directory: %w", err)
			}
			info, err = os.Lstat(current)
		}
		if err != nil {
			return fmt.Errorf("inspect channel directory: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			// macOS exposes /var as the conventional alias for /private/var;
			// it is a system path component, not an application-controlled
			// channel alias. All components below the configured root remain
			// strictly no-symlink.
			if allowedSystemAlias(current, info) {
				continue
			}
			return errors.New("channel path is not a real directory")
		}
	}
	info, err := os.Lstat(clean)
	if err != nil {
		return fmt.Errorf("inspect channel directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("channel path is not a real directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) {
		return fmt.Errorf("channel directory is not owned by the current user")
	}
	if err := os.Chmod(clean, 0o700); err != nil {
		return fmt.Errorf("secure channel directory: %w", err)
	}
	return nil
}

// validateNoSymlinkComponents checks every existing component without
// following links. It is used for immutable inputs where creation is not
// appropriate (the state-machine artifact).
func validateNoSymlinkComponents(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("path must be absolute")
	}
	clean := filepath.Clean(path)
	root := filepath.VolumeName(clean) + string(filepath.Separator)
	current := root
	for _, part := range strings.Split(strings.TrimPrefix(clean, root), string(filepath.Separator)) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 && !allowedSystemAlias(current, info) {
			return fmt.Errorf("path component is a symlink")
		}
	}
	return nil
}

func allowedSystemAlias(path string, info os.FileInfo) bool {
	return runtime.GOOS == "darwin" && (path == "/var" || path == "/tmp") && info.Mode()&os.ModeSymlink != 0
}

func validateDatabasePath(path string) error {
	if err := validateNoSymlinkComponents(filepath.Dir(path)); err != nil {
		return fmt.Errorf("validate database parent: %w", err)
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect database path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("database path is not a regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) {
		return errors.New("database is not owned by the current user")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure database: %w", err)
	}
	return nil
}

func secureDatabaseFiles(path string) error {
	for _, candidate := range []string{path, path + "-wal", path + "-shm"} {
		if err := validateDatabasePath(candidate); err != nil {
			return err
		}
	}
	return nil
}

func pathWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	if err != nil || relative == "." {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}
