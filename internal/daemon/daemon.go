// Package daemon owns the foreground, single-owner local daemon boundary. It
// coordinates durable recovery and admits provider or repository capabilities
// only through explicitly supplied, fail-closed runtime components.
package daemon

import (
	"context"
	"crypto/rand"
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
	// ProviderCoordinatorFactory composes configured, qualified adapters after
	// the daemon opens the authoritative Store. A nil factory leaves provider
	// execution unavailable rather than inventing an adapter.
	ProviderCoordinatorFactory func(*store.Store, contracts.ProcessSupervisor) (*providercoord.Coordinator, error)
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
	gitMutationDrainer  contracts.GitMutationDrainer
	providerCoordinator *providercoord.Coordinator
	mu                  sync.Mutex
	closed              bool

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
	fail := func(cause error) (*Daemon, error) {
		_ = lease.Close()
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
		_ = database.Close()
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
			claim := store.ProviderAttemptClaim{ID: request.ClaimID, Ref: request.Ref, Phase: request.Phase, Role: request.Role, Attempt: request.Attempt, Binding: contracts.RuntimeBinding{Identity: request.Identity, BinaryDigest: request.BinaryDigest, PolicyDigest: request.PolicyDigest, AuthDigest: request.AuthDigest}, LeaseKey: request.LeaseKey, BindingDigest: request.BindingDigest, LeaderEpoch: request.LeaderEpoch, RunnerEpoch: request.RunnerEpoch, ExpectedVersion: request.ExpectedVersion, Repository: request.Repository, Worktree: request.Worktree, WorktreeIdentity: request.WorktreeIdentity, BaseSHA: request.BaseSHA}
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

	var coordinator *providercoord.Coordinator
	if configuration.ProviderCoordinatorFactory != nil {
		coordinator, err = configuration.ProviderCoordinatorFactory(database, configuration.ProviderSupervisor)
		if err != nil {
			return failStore(fmt.Errorf("compose provider coordinator: %w", err))
		}
	}
	instance := &Daemon{channel: configuration.Channel, paths: configuration.Paths, lease: lease, store: database,
		engine: engine.New(database, specification), spec: specification, doctor: configuration.Doctor, epoch: epoch, clock: configuration.Clock, ids: configuration.TicketIDs, auth: configuration.Operator, control: configuration.Controller, recoverProvider: configuration.RecoverProvider, recoveryDrainer: configuration.RecoveryDrainer, gitMutationDrainer: configuration.GitMutationDrainer, providerCoordinator: coordinator}
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
	server, err := transport.ListenWithExecutable(configuration.Paths.Socket, uint32(os.Getuid()), instance, instance.executable())
	if err != nil {
		return failStore(err)
	}
	instance.server = server
	return instance, nil
}

func (daemon *Daemon) Serve(ctx context.Context) error {
	err := daemon.server.Serve(ctx)
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
	if _, err := daemon.store.ReconcileEffects(ctx, daemon.channel, daemon.epoch); err != nil {
		return fmt.Errorf("reconcile stranded effects: %w", err)
	}
	if _, err := daemon.store.FenceRecoveredRunners(ctx, daemon.channel, daemon.epoch); err != nil {
		return fmt.Errorf("invalidate recovered runners: %w", err)
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
			req := contracts.DrainRequest{ClaimID: claim.ID, Identity: claim.Binding.Identity, Ref: claim.Ref, Phase: claim.Phase, Role: claim.Role, Attempt: claim.Attempt, LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ExpectedVersion: claim.ExpectedVersion, LeaseKey: claim.LeaseKey, BindingDigest: claim.BindingDigest, BinaryDigest: claim.Binding.BinaryDigest, PolicyDigest: claim.Binding.PolicyDigest, AuthDigest: claim.Binding.AuthDigest, Repository: claim.Repository, Worktree: claim.Worktree, WorktreeIdentity: claim.WorktreeIdentity, BaseSHA: claim.BaseSHA}
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
	return daemon.engine.RecoverChannel(ctx, daemon.channel, daemon.epoch)
}

// Run owns a foreground daemon lifetime. It is deliberately separate from the
// CLI package so cmd/sf can inject it without creating a cli<->daemon cycle.
func Run(ctx context.Context, configuration Config) error {
	daemon, err := Start(ctx, configuration)
	if err != nil {
		return err
	}
	defer daemon.Close()
	return daemon.Serve(ctx)
}

func (daemon *Daemon) Close() error {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	if daemon.closed {
		return nil
	}
	daemon.closed = true
	var result error
	if daemon.server != nil {
		result = daemon.server.Close()
	}
	if err := daemon.engine.Close(); err != nil && result == nil {
		result = err
	}
	if err := daemon.lease.Close(); err != nil && result == nil {
		result = err
	}
	return result
}

func (daemon *Daemon) Handle(ctx context.Context, peer transport.Peer, request api.Request) api.Response {
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
	case "ticket.start":
		response = daemon.startTicket(ctx, request, identity)
	case "ticket.pause":
		response = daemon.controlTicket(ctx, request, identity, "pause")
	case "ticket.cancel":
		response = daemon.controlTicket(ctx, request, identity, "cancel")
	case "daemon.status":
		response = daemon.status(request, identity)
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
	if _, err := daemon.store.Project(ctx, daemon.channel, domain.ProjectID(parameters.Project)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return daemon.failure(request, "unknown_project", "ticket project is not registered", false)
		}
		return daemon.failure(request, submitErrorCode(err), "ticket project registration could not be read", errors.Is(err, store.ErrBusy))
	}
	if parsed.MergeMode == domain.MergeAutonomous {
		return daemon.failure(request, "autonomous_unavailable", "autonomous submission is disabled until an OS-enforced profile is qualified", false)
	}
	if err := daemon.lease.Validate(); err != nil {
		return daemon.failure(request, "leader_lost", "daemon leadership is no longer valid", true)
	}
	id, err := daemon.ids.NewTicketID(daemon.channel)
	if err != nil {
		return daemon.failure(request, "ticket_id_unavailable", "could not allocate a ticket identity", true)
	}
	record := store.Ticket{Ref: domain.TicketRef{Channel: daemon.channel, Project: domain.ProjectID(parameters.Project), Ticket: id},
		SourceDigest: parsed.Digest, Type: parsed.Type, MergeMode: parsed.MergeMode, Title: parsed.Title, Problem: parsed.Problem,
		Acceptance: parsed.Acceptance, Source: parsed.Source, Priority: parsed.Priority, CreatedAt: daemon.clock.Now().UTC(),
		MaxDuration: parsed.MaxDuration, MaxCostMicroUSD: parsed.MaxCostMicroUSD}
	stored, created, err := daemon.store.SubmitTicketFenced(ctx, record, parameters.New, daemon.epoch)
	if err != nil {
		return daemon.failure(request, submitErrorCode(err), "ticket submission was not accepted", errors.Is(err, store.ErrBusy))
	}
	return daemon.success(request, api.Mutation{Attempted: true, Kind: "ticket_submit", Identity: string(stored.Ref.Ticket), Observed: !created}, ticketView(stored))
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
	view := ticketDetail(stored)
	evidence, err := daemon.evidenceView(ctx, stored.Ref)
	if err != nil {
		return daemon.failure(request, evidenceErrorCode(err), "durable workflow evidence could not be authenticated", errors.Is(err, store.ErrBusy))
	}
	view["evidence"] = evidence
	view["operator"] = operatorView(identity)
	return daemon.success(request, api.Mutation{}, view)
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
		project, err := daemon.store.Project(ctx, daemon.channel, ref.Project)
		if err != nil {
			return daemon.failure(request, "unknown_project", "ticket project is not registered", false)
		}
		globalCapacity, projectCapacity, err := leaseCapacities(project)
		if err != nil {
			return daemon.failure(request, "invalid_configuration", "the durable project configuration is invalid", false)
		}
		doctorGreen := true
		if daemon.doctor != nil {
			doctorGreen = daemon.doctor(ctx, project) == nil
		}
		globalUsed, projectUsed := 0, 0
		leases, err := daemon.store.Leases(ctx, daemon.channel)
		if err != nil {
			return daemon.failure(request, "capacity_unavailable", "capacity could not be checked", errors.Is(err, store.ErrBusy))
		}
		for _, lease := range leases {
			if lease.Scope == "global" {
				globalUsed++
			}
			if lease.Scope == "project" && lease.Ref.Project == ref.Project {
				projectUsed++
			}
		}
		capacityAvailable := globalUsed < globalCapacity && projectUsed < projectCapacity
		if _, err := daemon.spec.Select(string(stored.State), "operator_start", map[string]bool{"doctor_preflight_green": doctorGreen, "capacity_available": capacityAvailable}); err != nil {
			if !doctorGreen {
				return daemon.failure(request, "doctor_required", "local doctor preflight is not green", false)
			}
			return daemon.failure(request, "capacity_unavailable", "local capacity is already reserved", true)
		}
	}
	project, err := daemon.store.Project(ctx, daemon.channel, ref.Project)
	if err != nil {
		return daemon.failure(request, "unknown_project", "ticket project is not registered", false)
	}
	globalCapacity, projectCapacity, err := leaseCapacities(project)
	if err != nil {
		return daemon.failure(request, "invalid_configuration", "the durable project configuration is invalid", false)
	}
	workflowID := fmt.Sprintf("%s/%s/%s/planning", daemon.channel, ref.Project, ref.Ticket)
	started, observed, err := daemon.store.StartWithOwnership(ctx, ref, stored.Version, domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch}, workflowID, []store.LeaseRequest{{Scope: "global", Resource: "machine", Capacity: globalCapacity}, {Scope: "project", Resource: string(ref.Project), Capacity: projectCapacity}}, daemon.clock.Now().UTC())
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

func (daemon *Daemon) controlTicket(ctx context.Context, request api.Request, identity domain.OperatorIdentity, intent string) api.Response {
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
		return daemon.controlSuccess(request, stored, intent, true)
	}
	if intent != "cancel" && stored.State == domain.StatePaused {
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
	result, err := daemon.engine.Signal(ctx, contracts.SignalRequest{
		Ticket: stored.Ref, TicketVersion: stored.Version, From: stored.State, Trigger: "process_and_effects_drained",
		Fence:        domain.Fence{LeaderEpoch: daemon.epoch, RunnerEpoch: stored.RunnerEpoch},
		Attributes:   map[string]string{"no_live_writer": "true", "no_unreconciled_mutation": "true"},
		EventPayload: `{"drained":true,"intent":"` + intent + `"}`,
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
	return daemon.controlSuccess(request, finished, intent, false)
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
		return daemon.success(request, api.Mutation{}, map[string]any{"channel": daemon.channel, "watch": parameters.Watch, "current_version": stored.Version, "operator": operatorView(identity), "ticket": ticketView(stored), "evidence": evidence})
	}
	items, err := daemon.store.Tickets(ctx, daemon.channel, domain.ProjectID(parameters.Project), 1000)
	if err != nil {
		return daemon.failure(request, "status_unavailable", "ticket status could not be read", errors.Is(err, store.ErrBusy))
	}
	views := make([]map[string]any, 0, len(items))
	for _, item := range items {
		views = append(views, ticketView(item))
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
	if code == "autonomous_unavailable" {
		argv = []string{binary, "providers", "qualify", "--help"}
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
	if code == "invalid_control" || code == "invalid_ticket_reference" {
		if verb != "" && verb != request.Method {
			argv = []string{binary, verb, "--help"}
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
