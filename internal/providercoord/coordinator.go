// Package providercoord is a durable, exec-free provider admission layer.
package providercoord

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/redact"
	"github.com/nysa-company/sf/internal/store"
)

type Role string

const (
	RolePlanner  Role = "planner"
	RoleBuilder  Role = "builder"
	RoleReviewer Role = "reviewer"
)

type Route struct {
	Primary, Fallback string
	Capacity          int
}
type Outcome string

// ErrPersistenceFatal means durable provider state could not be finalized.
// Once observed, this coordinator refuses all later launches because the
// active claim and process ownership are no longer safely knowable.
var (
	ErrPersistenceFatal      = errors.New("provider coordinator persistence failure is fatal")
	ErrPrePublishingNotReady = errors.New("pre-publishing provider routes are not ready")
)

const (
	Completed           Outcome = "completed"
	Failed              Outcome = "failed"
	Canceled            Outcome = "canceled"
	NeedsOperator       Outcome = "needs_operator"
	BudgetExhausted     Outcome = "budget_exhausted"
	AttemptExhausted    Outcome = "attempt_exhausted"
	RepairUnavailable   Outcome = "repair_unavailable"
	ResultIndeterminate Outcome = "result_indeterminate"
)

type Request struct {
	Role Role
	// ExpectedProvider comes from the immutable ticket role configuration.
	// An explicit mismatch refuses before launch; it never selects a fallback.
	ExpectedProvider string
	Input            contracts.PhaseInput
	Validation       phaseartifact.Validation
	ExpectedVersion  uint64
	Fence            domain.Fence
	ConfigDigest     string
}
type Receipt struct {
	AttemptID                        int64
	Attempt                          int
	Provider                         domain.ProviderIdentity
	ArtifactDigest, TranscriptDigest string
	ArtifactFailureReason            contracts.ArtifactFailureReason
	UsageUnits                       int64
	// AccountingMode keeps an unverified estimate distinct from charges.
	AccountingMode               string
	ReportedCostEstimateMicroUSD *int64
	TokenUsage                   int64
	ErrorCode                    string
}
type Result struct {
	Code       Outcome
	Parsed     *phaseartifact.Parsed
	Diagnostic AdmissionDiagnostic
	// ProviderResult is populated only after Store has durably committed the
	// completed attempt. A zero key means that no immutable result was
	// persisted; callers must never derive one from receipts.
	ProviderResult store.ProviderAttemptResultKey
	Attempts       []Receipt
	NeedsOperator  bool
	CostUsed       int64
	// PersistenceFailure distinguishes an operator stop caused by uncertain
	// durable state from an ordinary provider/admission failure.
	PersistenceFailure bool
}
type Clock interface{ Now() time.Time }

// ConfigureRejectionCheckpoint is a pre-start runtime-composition operation.
// Supervisors without the optional capability cannot produce rejection
// receipts and retain ordinary conservative drain behavior.
func (c *Coordinator) ConfigureRejectionCheckpoint(inspector contracts.RejectionCheckpointInspector) error {
	if c == nil || inspector == nil {
		return errors.New("provider checkpoint inspector required")
	}
	if configurable, ok := c.supervisor.(interface {
		ConfigureRejectionCheckpoint(contracts.RejectionCheckpointInspector) error
	}); ok {
		if err := configurable.ConfigureRejectionCheckpoint(inspector); err != nil {
			return err
		}
		c.checkpointMu.Lock()
		c.checkpoint = inspector
		c.checkpointMu.Unlock()
	}
	return nil
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now().UTC() }

type Registry struct {
	mu        sync.RWMutex
	providers map[string]contracts.Provider
}

func NewRegistry() *Registry { return &Registry{providers: map[string]contracts.Provider{}} }
func (r *Registry) Register(ctx context.Context, p contracts.Provider) error {
	if r == nil || p == nil || p.Name() == "" {
		return errors.New("provider required")
	}
	id, e := p.Probe(ctx)
	// Name is a local route key; identity.Provider remains the durable,
	// operator-visible executable/provider name. This permits two configured
	// model-family profiles of one executable without claiming two binaries.
	if e != nil || id.Provider == "" || id.Model == "" || id.Family == "" || id.Version == "" {
		return errors.New("provider identity probe failed")
	}
	binding, e := p.Binding(ctx)
	if e != nil || binding.Identity != id || !validBinding(binding) {
		if e != nil {
			return e
		}
		return errors.New("provider runtime binding probe failed")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.providers[p.Name()]; ok {
		return errors.New("duplicate provider")
	}
	r.providers[p.Name()] = p
	return nil
}

func validBinding(binding contracts.RuntimeBinding) bool {
	for _, digest := range []string{binding.BinaryDigest, binding.PolicyDigest, binding.FixtureDigest, binding.AuthDigest} {
		if len(digest) != 64 || strings.ToLower(digest) != digest || strings.Trim(digest, "0123456789abcdef") != "" {
			return false
		}
	}
	switch binding.Identity.Provider {
	case "codex":
		if binding.AuthMode != "chatgpt_subscription" {
			return false
		}
	case "claude":
		if binding.AuthMode != "claude_subscription" {
			return false
		}
	case "cursor":
		if binding.AuthMode != "cursor_browser" && binding.AuthMode != "cursor_api" {
			return false
		}
	}
	return binding.Identity.Provider != "" && binding.Identity.Model != "" && binding.Identity.Family != "" && binding.Identity.Version != ""
}
func (r *Registry) get(name string) (contracts.Provider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.providers[name]
	return p, ok
}

type Coordinator struct {
	registry     *Registry
	routes       map[Role]Route
	store        *store.Store
	clock        Clock
	supervisor   contracts.ProcessSupervisor
	fatal        atomic.Bool
	fatalMu      sync.Mutex
	fatalErr     error
	checkpointMu sync.RWMutex
	checkpoint   contracts.RejectionCheckpointInspector
}

// Close is the lifecycle hook used by the foreground daemon. Provider
// processes are supervised and joined by the runtime/recovery boundaries; the
// coordinator itself currently owns no independent goroutine or descriptor.
// Keeping this hook explicit makes that ownership auditable for later runtime
// compositions without making daemon shutdown depend on a concrete type.
func (c *Coordinator) Close() error { return nil }

// ReadyForPrePublishing proves that the local Planner/Builder/Reviewer
// walking skeleton has a complete, currently usable route set.  Composition
// calls this before it gives a workflow runtime any execution authority.  An
// empty coordinator remains useful for doctor/qualification flows, but it is
// never equivalent to an executable runtime.
func (c *Coordinator) ReadyForPrePublishing() error {
	if c == nil || c.registry == nil || c.store == nil || c.supervisor == nil {
		return ErrPrePublishingNotReady
	}
	if err := c.persistenceFailure(); err != nil {
		return err
	}
	for _, role := range []Role{RolePlanner, RoleBuilder, RoleReviewer} {
		route, ok := c.routes[role]
		if !ok || route.Primary == "" {
			return ErrPrePublishingNotReady
		}
		if _, ok := c.registry.get(route.Primary); !ok {
			return ErrPrePublishingNotReady
		}
		if route.Fallback != "" {
			if _, ok := c.registry.get(route.Fallback); !ok {
				return ErrPrePublishingNotReady
			}
		}
	}
	return nil
}

func New(reg *Registry, routes map[Role]Route, database *store.Store, clock Clock, supervisor contracts.ProcessSupervisor) (*Coordinator, error) {
	if reg == nil || database == nil || supervisor == nil || len(supervisor.PublicKey()) != 32 {
		return nil, errors.New("registry, store, and process supervisor required")
	}
	copy := map[Role]Route{}
	for role, route := range routes {
		if !role.valid() || route.Primary == "" || route.Primary == route.Fallback || route.Capacity < 0 || route.Capacity > 16 {
			return nil, errors.New("invalid provider route")
		}
		if route.Capacity == 0 {
			route.Capacity = 1
		}
		if _, ok := reg.get(route.Primary); !ok {
			return nil, errors.New("unregistered primary")
		}
		if route.Fallback != "" {
			if _, ok := reg.get(route.Fallback); !ok {
				return nil, errors.New("unregistered fallback")
			}
		}
		copy[role] = route
	}
	if clock == nil {
		clock = wallClock{}
	}
	setter, ok := supervisor.(contracts.LaunchRecorderSetter)
	if !ok {
		return nil, errors.New("process supervisor must support durable launch recording")
	}
	setter.SetLaunchRecorder(func(ctx context.Context, request contracts.DrainRequest, launch contracts.ProviderLaunch) error {
		claim := store.ProviderAttemptClaim{ID: request.ClaimID, Ref: request.Ref, Phase: request.Phase, Role: request.Role, Attempt: request.Attempt, Binding: contracts.RuntimeBinding{Identity: request.Identity, BinaryDigest: request.BinaryDigest, PolicyDigest: request.PolicyDigest, AuthDigest: request.AuthDigest, AuthMode: request.AuthMode}, LeaseKey: request.LeaseKey, BindingDigest: request.BindingDigest, LeaderEpoch: request.LeaderEpoch, RunnerEpoch: request.RunnerEpoch, ExpectedVersion: request.ExpectedVersion, Repository: request.Repository, Worktree: request.Worktree, WorktreeIdentity: request.WorktreeIdentity, BaseSHA: request.BaseSHA, RequestDigest: request.RequestDigest}
		return database.RecordProviderLaunch(ctx, claim, launch)
	})
	return &Coordinator{registry: reg, routes: copy, store: database, clock: clock, supervisor: supervisor}, nil
}
func (role Role) valid() bool {
	return role == RolePlanner || role == RoleBuilder || role == RoleReviewer
}

func (c *Coordinator) Run(ctx context.Context, r Request) (result Result) {
	// Preserve the last refused admission across existing fallback routing.
	// A successful claim clears it, so later execution failures are not
	// incorrectly described as pre-attempt failures.
	var diagnostic AdmissionDiagnostic
	defer func() {
		if result.Code != Completed {
			if diagnostic != "" && result.Code == Canceled {
				diagnostic = DiagnosticCanceled
			}
			result.Diagnostic = diagnostic
		}
	}()
	if c.persistenceFailure() != nil {
		diagnostic = DiagnosticPersistenceUnavailable
		return Result{Code: NeedsOperator, NeedsOperator: true, PersistenceFailure: true}
	}
	if err := validate(r); err != nil {
		diagnostic = DiagnosticRequestInvalid
		return Result{Code: NeedsOperator, NeedsOperator: true}
	}
	ticket, err := c.store.Ticket(ctx, r.Input.Ticket)
	if err != nil || ticket.Version != r.ExpectedVersion || ticket.RunnerEpoch != r.Fence.RunnerEpoch || ticket.ConfigDigest == "" || ticket.ConfigDigest != r.ConfigDigest {
		diagnostic = DiagnosticTicketMismatch
		if err != nil {
			diagnostic = admissionErrorDiagnostic(err)
		}
		return Result{Code: NeedsOperator, NeedsOperator: true}
	}
	if r.Input.Phase == domain.PhaseReview {
		if err := c.store.ValidateFinalReviewEvidence(ctx, r.Input.Ticket, r.ExpectedVersion, r.Fence, r.Validation.ExpectedReviewedHead, r.Validation.ExpectedProofDigest); err != nil {
			diagnostic = admissionErrorDiagnostic(err)
			return Result{Code: NeedsOperator, NeedsOperator: true}
		}
	}
	// An indeterminate provider result is terminal for this phase entry. The
	// Store check is deliberately before any provider binding probe or launch,
	// so replay after restart cannot turn an uncertain observation into a
	// fallback or a second paid attempt.
	if _, indeterminate, indeterminateErr := c.store.PendingProviderResultIndeterminate(ctx, r.Input.Ticket, r.Input.Phase, string(r.Role), r.ExpectedVersion, r.Fence); indeterminateErr != nil {
		diagnostic = admissionErrorDiagnostic(indeterminateErr)
		return Result{Code: NeedsOperator, NeedsOperator: true}
	} else if indeterminate {
		diagnostic = DiagnosticResultIndeterminate
		return Result{Code: ResultIndeterminate, NeedsOperator: true}
	}
	if key, reusable, reuseErr := c.store.ReuseCurrentCompletedProviderAttempt(ctx, r.Input.Ticket, r.Input.Phase, string(r.Role), r.ExpectedVersion, r.Fence); reuseErr != nil {
		diagnostic = admissionErrorDiagnostic(reuseErr)
		return Result{Code: NeedsOperator, NeedsOperator: true}
	} else if reusable {
		diagnostic = DiagnosticEvidenceUnavailable
		if result, ok := c.reusedResult(ctx, r, key); ok {
			return result
		}
		return Result{Code: NeedsOperator, NeedsOperator: true}
	}
	pendingRepair, repairPending, repairErr := c.store.PendingProviderRepair(ctx, r.Input.Ticket, r.Input.Phase, string(r.Role), r.ExpectedVersion, r.Fence)
	if repairErr != nil {
		diagnostic = admissionErrorDiagnostic(repairErr)
		return Result{Code: NeedsOperator, NeedsOperator: true}
	}
	pendingRejection, rejectionPending, rejectionErr := c.store.PendingProviderServerRejection(ctx, r.Input.Ticket, r.Input.Phase, string(r.Role), r.ExpectedVersion, r.Fence)
	if rejectionErr != nil {
		diagnostic = admissionErrorDiagnostic(rejectionErr)
		return Result{Code: NeedsOperator, NeedsOperator: true}
	}
	if _, repairUnavailable, repairUnavailableErr := c.store.PendingProviderRepairUnavailable(ctx, r.Input.Ticket, r.Input.Phase, string(r.Role), r.ExpectedVersion, r.Fence); repairUnavailableErr != nil {
		diagnostic = admissionErrorDiagnostic(repairUnavailableErr)
		return Result{Code: NeedsOperator, NeedsOperator: true}
	} else if repairUnavailable {
		diagnostic = DiagnosticRepairUnavailable
		return Result{Code: RepairUnavailable, NeedsOperator: true}
	}
	route, ok := c.routes[r.Role]
	if !ok {
		diagnostic = DiagnosticRouteUnavailable
		return Result{Code: NeedsOperator, NeedsOperator: true}
	}
	names := []string{route.Primary}
	if route.Fallback != "" {
		names = append(names, route.Fallback)
	}
	var receipts []Receipt
	var spent int64
	backoffWaits := 0
	repairRouteIndex := -1
	for routeIndex := 0; routeIndex < len(names); routeIndex++ {
		name := names[routeIndex]
		repairRequired := routeIndex == repairRouteIndex
		if ctx.Err() != nil {
			diagnostic = DiagnosticCanceled
			return Result{Code: Canceled, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
		}
		p, ok := c.registry.get(name)
		if !ok {
			diagnostic = DiagnosticRouteUnavailable
			if rejectionPending {
				return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			if repairRequired {
				return Result{Code: RepairUnavailable, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			continue
		}
		binding, err := p.Binding(ctx)
		// Route names may distinguish separately pinned model/family profiles
		// of one underlying provider executable. The durable identity remains
		// in binding.Identity and is validated by Store; do not compare it to
		// the local registry route alias.
		if err != nil || p.Name() != name {
			diagnostic = DiagnosticBindingUnavailable
			if rejectionPending {
				return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			if repairRequired {
				return Result{Code: RepairUnavailable, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			continue
		}
		if rejectionPending && binding != pendingRejection.Binding {
			diagnostic = DiagnosticBindingUnavailable
			return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
		}
		if repairPending {
			if binding != pendingRepair.Binding {
				diagnostic = DiagnosticBindingUnavailable
				continue
			}
			repairRouteIndex = routeIndex
			repairRequired = true
		}
		remaining := ticket.CreatedAt.Add(ticket.MaxDuration).Sub(c.clock.Now())
		if remaining <= 0 {
			diagnostic = DiagnosticBudgetExhausted
			return Result{Code: BudgetExhausted, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
		}
		if r.ExpectedProvider != "" && binding.Identity.Provider != r.ExpectedProvider {
			diagnostic = DiagnosticProviderMismatch
			return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
		}
		timeout := r.Input.Timeout
		if timeout > remaining {
			timeout = remaining
		}
		attemptCtx, cancel := context.WithTimeout(ctx, timeout)
		claimInput := r.Input
		claimInput.Timeout = timeout
		claimInput.Provider, claimInput.AuthMode = binding.Identity, binding.AuthMode
		claimInput.LeaderEpoch, claimInput.RunnerEpoch, claimInput.ExpectedVersion = r.Fence.LeaderEpoch, r.Fence.RunnerEpoch, r.ExpectedVersion
		claim, err := c.store.BeginProviderAttempt(attemptCtx, store.ProviderAttemptRequest{Ref: r.Input.Ticket, ExpectedVersion: r.ExpectedVersion, Fence: r.Fence, Phase: r.Input.Phase, Role: string(r.Role), Binding: binding, ConfigDigest: r.ConfigDigest, Capacity: route.Capacity, At: c.clock.Now(), ExpectedHead: r.Validation.ExpectedReviewedHead, ExpectedProof: r.Validation.ExpectedProofDigest, Repository: r.Input.Repository, Worktree: r.Input.Worktree, WorktreeIdentity: r.Input.WorktreeIdentity, BaseSHA: r.Input.BaseSHA, SupervisorKey: c.supervisor.PublicKey(), Input: claimInput})
		if err != nil {
			cancel()
			diagnostic = admissionErrorDiagnostic(err)
			var backoff *store.ProviderRetryBackoffError
			if errors.As(err, &backoff) {
				// This deadline is read from authenticated durable evidence.
				// Bound repeats even if an injected/frozen clock does not advance.
				if backoffWaits >= 2 || waitServerRejectionBackoff(ctx, c.clock, backoff.NotBefore) != nil {
					if ctx.Err() != nil {
						return Result{Code: Canceled, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
					}
					return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
				}
				backoffWaits++
				routeIndex--
				continue
			}
			if errors.Is(err, store.ErrProviderServerRejection) {
				return Result{Code: ResultIndeterminate, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			if errors.Is(err, store.ErrProviderResultIndeterminate) {
				return Result{Code: ResultIndeterminate, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			if errors.Is(err, store.ErrBudgetExhausted) {
				return Result{Code: BudgetExhausted, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			if errors.Is(err, store.ErrProviderAttemptReusable) {
				key, reusable, reuseErr := c.store.ReuseCurrentCompletedProviderAttempt(ctx, r.Input.Ticket, r.Input.Phase, string(r.Role), r.ExpectedVersion, r.Fence)
				if reuseErr == nil && reusable {
					if result, ok := c.reusedResult(ctx, r, key); ok {
						result.Attempts, result.CostUsed = receipts, spent
						return result
					}
				}
				return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			if errors.Is(err, store.ErrProviderRepairUnavailable) {
				return Result{Code: RepairUnavailable, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			// The global phase-entry attempt window wins over the planned
			// same-binding repair. This can happen when a definite pre-launch
			// primary failure consumed attempt one and an availability fallback
			// produced an invalid artifact on attempt two. Report the durable
			// exhaustion consistently both before and after restart; the runtime
			// binding is still available, so RepairUnavailable would be false.
			if errors.Is(err, store.ErrProviderAttemptLimit) {
				return Result{Code: AttemptExhausted, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			if repairRequired {
				return Result{Code: RepairUnavailable, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			if errors.Is(err, store.ErrProviderCapacity) {
				return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			continue
		}
		diagnostic = ""
		input := r.Input
		input.Timeout = timeout
		if !bindClaimToInput(&input, claim, r, binding.Identity) {
			// The Store claim is the sole authority for launch identity. A
			// caller-provided PhaseInput must never be allowed to drift from it.
			cancel()
			finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
			quarantineErr := c.store.QuarantineProviderAttempt(finishCtx, claim, r.ExpectedVersion, r.Fence, c.clock.Now())
			finishCancel()
			if quarantineErr != nil {
				c.markPersistenceFailure(quarantineErr)
			}
			return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent, PersistenceFailure: quarantineErr != nil}
		}
		input = claim.Input
		invocation, invokeErr := p.Invocation(attemptCtx, input)
		checkpointRefused := false
		if invokeErr == nil {
			invokeErr = c.inspectRejectionRetryBeforeLaunch(attemptCtx, claim)
			checkpointRefused = invokeErr != nil
		}
		if invokeErr != nil {
			// Invocation is adapter-only and occurs before the supervisor owns a
			// child. This is the sole definite no-process failure path; every
			// supervisor.Run error remains quarantined because pre-exec may have
			// already created a provider process group.
			cancel()
			finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
			finishErr := c.store.FailProviderAttemptBeforeLaunch(finishCtx, claim, r.ExpectedVersion, r.Fence, c.clock.Now())
			retiredForControl := false
			if errors.Is(finishErr, store.ErrStaleFence) {
				// Operator control commits its ticket/runner revocation before it
				// cancels the runtime.  If that revocation wins this race, the
				// ordinary old-fence terminal path must fail.  Retire only the
				// exact still-unlaunched claim under Store's sealed-control proof;
				// never reinterpret the adapter error as current-fence evidence.
				finishErr = c.store.RetireUnlaunchedProviderAttemptAfterControlInvalidation(finishCtx, claim, c.clock.Now())
				retiredForControl = finishErr == nil
			}
			finishCancel()
			receipts = append(receipts, Receipt{AttemptID: claim.ID, Attempt: claim.Attempt, Provider: binding.Identity, ErrorCode: "provider_invocation_failed"})
			if finishErr != nil {
				c.markPersistenceFailure(finishErr)
				return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent, PersistenceFailure: true}
			}
			if retiredForControl || ctx.Err() != nil {
				return Result{Code: Canceled, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			if checkpointRefused {
				return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			if repairRequired {
				return Result{Code: RepairUnavailable, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			continue
		}
		var raw contracts.PhaseResult
		var runErr error
		commandResult, commandErr := c.supervisor.Run(attemptCtx, drainRequest(claim), invocation, input)
		if commandErr != nil {
			runErr = commandErr
		} else {
			raw, runErr = p.Parse(attemptCtx, input, commandResult)
		}
		if runErr != nil {
			reportProviderRunFailure(commandResult, commandErr != nil, runErr)
		}
		cancel()
		cancelled := ctx.Err() != nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded)
		// Returning from Run, including a provider error, is not proof that its
		// process group drained. Every terminal path must obtain an explicit
		// supervisor proof before releasing the durable claim and lease.
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		drain, serverReceipt, drainErr := drainWithServerRejection(drainCtx, c.supervisor, claim, commandErr != nil && !cancelled)
		drainCancel()
		if drainErr != nil {
			quarantineCtx, quarantineCancel := context.WithTimeout(context.Background(), 5*time.Second)
			quarantineErr := c.store.QuarantineProviderAttempt(quarantineCtx, claim, r.ExpectedVersion, r.Fence, c.clock.Now())
			quarantineCancel()
			if quarantineErr != nil {
				c.markPersistenceFailure(quarantineErr)
			}
			return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent, PersistenceFailure: quarantineErr != nil}
		}
		if contracts.UsesMultiCLIRequestLimit(binding.Identity.Provider) {
			// Persist optional estimates without assigning monetary authority.
			// A failure/binding mismatch has unknown cost, never verified zero.
			estimate := raw.ReportedCostEstimateMicroUSD
			if commandErr != nil || raw.Provider != binding.Identity {
				estimate = nil
			}
			accountingCtx, accountingCancel := context.WithTimeout(context.Background(), 5*time.Second)
			accountingErr := c.store.RecordProviderCostEstimate(accountingCtx, claim, drain, estimate)
			accountingCancel()
			// Revoked attempts must still reach the existing signed-drain
			// retirement path below; they cannot append a current observation.
			if accountingErr != nil && !errors.Is(accountingErr, store.ErrStaleFence) {
				c.markPersistenceFailure(accountingErr)
				return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent, PersistenceFailure: true}
			}
		}
		state, outcome := "failed", "failed"
		if cancelled {
			state, outcome = "cancelled", "cancelled"
		}
		accountingAccepted := c.store.ProviderResultAccountingAccepted(ctx, claim, raw)
		valid := !cancelled && runErr == nil && raw.Outcome == contracts.PhaseResultCompleted && raw.Provider == binding.Identity && accountingAccepted
		trustedUsage := int64(0)
		if raw.UsageTrusted && raw.UsageUnits >= 0 {
			trustedUsage = raw.UsageUnits
		}
		var parsed phaseartifact.Parsed
		var artifactFailureReason contracts.ArtifactFailureReason
		// Repair requires accepted accounting: verified usage or explicit
		// estimate policy plus a durable observation. Command ambiguity still
		// cannot enter repair, regardless of accounting mode.
		indeterminateResult := !cancelled && (commandErr != nil || runErr != nil && raw.Outcome != contracts.PhaseResultInvalidArtifact || !accountingAccepted || raw.Provider != binding.Identity || (raw.Outcome != contracts.PhaseResultCompleted && raw.Outcome != contracts.PhaseResultInvalidArtifact))
		if serverReceipt != nil && !cancelled {
			outcome = "server_rejected"
		} else if indeterminateResult {
			outcome = "result_indeterminate"
		} else if !cancelled && raw.Outcome == contracts.PhaseResultInvalidArtifact {
			outcome = contracts.PhaseResultInvalidArtifact
			artifactFailureReason = raw.ArtifactFailureReason
			if !contracts.ValidArtifactFailureReason(artifactFailureReason) {
				artifactFailureReason = contracts.ArtifactFailureAdapterDeclared
			}
		} else if valid {
			parsed, err = phaseartifact.Parse(input.Phase, raw, r.Validation)
			if err == nil {
				if err = phaseartifact.ValidateMutationPaths(parsed, raw.ChangedFiles, input.AllowedPaths); err == nil {
					state, outcome = "completed", contracts.PhaseResultCompleted
				} else {
					outcome = contracts.PhaseResultInvalidArtifact
					artifactFailureReason = contracts.ArtifactFailureMutationPath
				}
			} else {
				outcome = contracts.PhaseResultInvalidArtifact
				artifactFailureReason = contracts.ArtifactFailureSchema
				if input.Phase == domain.PhaseBuild {
					reportBuilderValidationFailure(err)
				}
				if input.Phase == domain.PhasePlanning {
					reportPlannerValidationFailure(err)
				}
			}
		}
		receipt := Receipt{AttemptID: claim.ID, Attempt: claim.Attempt, Provider: binding.Identity, ArtifactDigest: safeDigest(raw.Artifact), TranscriptDigest: safeDigest([]byte(raw.Transcript)), ArtifactFailureReason: artifactFailureReason, UsageUnits: trustedUsage}
		if raw.UsageTrusted {
			receipt.AccountingMode = "verified_charge"
		} else if accountingAccepted {
			receipt.AccountingMode = "reported_estimate_v1"
			receipt.ReportedCostEstimateMicroUSD = raw.ReportedCostEstimateMicroUSD
		} else {
			receipt.AccountingMode = "unknown"
		}
		if raw.TokenUsageTrusted {
			receipt.TokenUsage = max(raw.TokenUsage, 0)
		}
		if outcome == "server_rejected" {
			receipt.ErrorCode = "server_rejected"
			receipt.TranscriptDigest = serverReceipt.Evidence.StreamDigest
		} else if outcome == "result_indeterminate" {
			receipt.ErrorCode = "result_indeterminate"
		} else if outcome == contracts.PhaseResultInvalidArtifact {
			receipt.ErrorCode = "invalid_artifact"
		} else if runErr != nil {
			receipt.ErrorCode = "provider_error"
		} else if !valid {
			receipt.ErrorCode = "invalid_result"
		}
		receipts = append(receipts, receipt)
		finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
		finishedAt := c.clock.Now()
		var finishErr error
		var durableResult store.ProviderAttemptResult
		if outcome == "server_rejected" {
			finishErr = c.store.FinishProviderAttemptWithServerRejection(finishCtx, claim, drain, *serverReceipt, finishedAt)
		} else if state == "completed" {
			durableResult, finishErr = c.store.CompleteProviderAttemptSuccess(finishCtx, claim, drain, r.ExpectedVersion, r.Fence, raw, r.Validation, finishedAt)
		} else if outcome == contracts.PhaseResultInvalidArtifact {
			finishErr = c.store.FinishProviderAttemptWithArtifactFailure(finishCtx, claim, drain, r.ExpectedVersion, r.Fence, artifactFailureReason, trustedUsage, finishedAt)
		} else if outcome == "result_indeterminate" {
			reason := raw.FailureReason
			if commandErr != nil {
				reason = contracts.ProviderFailureCommand
			} else if !accountingAccepted || raw.Provider != binding.Identity {
				reason = contracts.ProviderFailureBinding
			} else if !contracts.ValidProviderFailureReason(reason) {
				reason = contracts.ProviderFailureAdapter
			}
			finishErr = c.store.FinishProviderAttemptWithIndeterminateFailure(finishCtx, claim, drain, r.ExpectedVersion, r.Fence, reason, trustedUsage, finishedAt)
		} else {
			finishErr = c.store.FinishProviderAttempt(finishCtx, claim, drain, r.ExpectedVersion, r.Fence, state, outcome, trustedUsage, finishedAt)
		}
		finishCancel()
		if finishErr != nil {
			if errors.Is(finishErr, store.ErrStaleFence) {
				// A successful/error provider response that arrives after the
				// operator revoked this runner is not completion evidence.  The
				// signed drain proof authorizes only cancellation of the exact old
				// attempt and release of its exact provider lease.  This also
				// covers the narrow race where the provider returns before the
				// cancellation becomes visible through ctx.Err().
				retireCtx, retireCancel := context.WithTimeout(context.Background(), 5*time.Second)
				retireErr := c.store.RetireProviderAttemptAfterControlInvalidation(retireCtx, claim, drain, c.clock.Now())
				retireCancel()
				if retireErr == nil {
					receipts[len(receipts)-1].ErrorCode = "provider_control_revoked"
					receipts[len(receipts)-1].UsageUnits = 0
					receipts[len(receipts)-1].TokenUsage = 0
					return Result{Code: Canceled, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
				}
				finishErr = retireErr
			}
			if errors.Is(finishErr, store.ErrBudgetExhausted) {
				quarantineCtx, quarantineCancel := context.WithTimeout(context.Background(), 5*time.Second)
				budgetErr := c.store.FailProviderAttemptBudget(quarantineCtx, claim, drain, r.ExpectedVersion, r.Fence, trustedUsage, finishedAt)
				quarantineCancel()
				if budgetErr != nil {
					c.markPersistenceFailure(budgetErr)
					return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent, PersistenceFailure: true}
				}
				return Result{Code: BudgetExhausted, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			c.markPersistenceFailure(finishErr)
			return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent, PersistenceFailure: true}
		}
		spent += trustedUsage
		if cancelled {
			// A Store-issued repair that reaches a durable cancelled endpoint
			// under its own attempt deadline is no longer safely retryable. Keep
			// outer/control cancellation distinct: the caller's cancellation (or
			// stale-fence retirement above) owns that disposition and must remain
			// Canceled.
			if repairRequired && ctx.Err() == nil {
				return Result{Code: RepairUnavailable, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			return Result{Code: Canceled, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
		}
		if outcome == "result_indeterminate" {
			return Result{Code: ResultIndeterminate, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
		}
		if outcome == "server_rejected" {
			// Repeat this exact route only. Store owns the remaining shared
			// attempt budget and deadline, including after daemon restart.
			pendingRejection, rejectionPending = claim, true
			routeIndex--
			continue
		}
		if state == "completed" {
			return Result{Code: Completed, Parsed: &parsed, ProviderResult: store.ProviderAttemptResultKey{AttemptID: durableResult.AttemptID, Ref: durableResult.Claim.Ref, Phase: durableResult.Claim.Phase, Attempt: durableResult.Claim.Attempt}, Attempts: receipts, CostUsed: spent}
		}
		if spent >= ticket.MaxCostMicroUSD {
			return Result{Code: BudgetExhausted, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
		}
		if outcome == contracts.PhaseResultInvalidArtifact {
			// A schema-invalid but otherwise trusted provider result is the one
			// bounded repairable failure in the v1 provider contract. Admit the
			// exact same route once more immediately, before any configured
			// fallback: fallbacks handle provider availability, while invalid
			// output is repaired by the binding that produced it.
			if repairRouteIndex < 0 {
				names = append(names, "")
				copy(names[routeIndex+2:], names[routeIndex+1:])
				names[routeIndex+1] = name
				repairRouteIndex = routeIndex + 1
				continue
			}
			// The configured fallback or the one same-primary repair also
			// returned an invalid artifact. Surface exhaustion immediately so
			// the worker durably pauses before a later clean-worktree admission
			// can strand on the intentionally retained partial changes.
			if routeIndex == repairRouteIndex {
				return Result{Code: AttemptExhausted, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
		}
	}
	if repairPending {
		return Result{Code: RepairUnavailable, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
	}
	return Result{Code: Failed, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
}

func (c *Coordinator) markPersistenceFailure(err error) {
	if err == nil {
		return
	}
	c.fatalMu.Lock()
	if c.fatal.CompareAndSwap(false, true) {
		c.fatalErr = errors.Join(ErrPersistenceFatal, err)
	}
	c.fatalMu.Unlock()
}

func (c *Coordinator) persistenceFailure() error {
	if !c.fatal.Load() {
		return nil
	}
	c.fatalMu.Lock()
	defer c.fatalMu.Unlock()
	return c.fatalErr
}

// Recover drains a provider before releasing an old fenced claim. It does not
// guess from PID or time alone.
func (c *Coordinator) Recover(ctx context.Context, ref domain.TicketRef, staleRunner, leader uint64, name string) error {
	claims, err := c.store.ActiveProviderAttempts(ctx, ref.Channel)
	if err != nil {
		return err
	}
	var match *store.ProviderAttempt
	for index := range claims {
		claim := &claims[index]
		if claim.Ref == ref && claim.RunnerEpoch == staleRunner && claim.Binding.Identity.Provider == name {
			if match != nil {
				return errors.New("multiple provider recovery claims match")
			}
			match = claim
		}
	}
	if match == nil {
		return store.ErrNotFound
	}
	return c.RecoverClaim(ctx, *match, leader)
}

func (c *Coordinator) recoverClaim(ctx context.Context, claim store.ProviderAttempt, leader uint64) error {
	drain, err := c.supervisor.Drain(ctx, drainRequest(claim.ProviderAttemptClaim))
	if err != nil {
		return err
	}
	return c.store.RecoverProviderAttemptClaimWithProof(ctx, claim, leader, drain, c.clock.Now())
}

// RecoverClaim is the daemon integration boundary. It uses the provider and
// stale runner identity persisted with the claim, so callers cannot recover a
// different provider by guessing a name.
func (c *Coordinator) RecoverClaim(ctx context.Context, claim store.ProviderAttempt, leader uint64) error {
	return c.recoverClaim(ctx, claim, leader)
}

func drainRequest(claim store.ProviderAttemptClaim) contracts.DrainRequest {
	return contracts.DrainRequest{ClaimID: claim.ID, Identity: claim.Binding.Identity, Ref: claim.Ref, Phase: claim.Phase, Role: claim.Role, Attempt: claim.Attempt, LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ExpectedVersion: claim.ExpectedVersion, LeaseKey: claim.LeaseKey, BindingDigest: claim.BindingDigest, BinaryDigest: claim.Binding.BinaryDigest, PolicyDigest: claim.Binding.PolicyDigest, AuthDigest: claim.Binding.AuthDigest, AuthMode: claim.Binding.AuthMode, Repository: claim.Repository, Worktree: claim.Worktree, WorktreeIdentity: claim.WorktreeIdentity, BaseSHA: claim.BaseSHA, RequestDigest: claim.RequestDigest}
}

func (c *Coordinator) reusedResult(ctx context.Context, request Request, key store.ProviderAttemptResultKey) (Result, bool) {
	result, parsed, err := c.store.LoadCurrentProviderAttemptResult(ctx, key, request.ExpectedVersion, request.Fence)
	if err != nil || result.Claim.Role != string(request.Role) || !reusedInputMatches(request, result.Claim) {
		return Result{}, false
	}
	canonical, _, err := phaseartifact.CanonicalValidation(request.Validation)
	if err != nil || !bytes.Equal(canonical, result.Validation) {
		return Result{}, false
	}
	return Result{Code: Completed, ProviderResult: key, Parsed: &parsed}, true
}

func reusedInputMatches(request Request, claim store.ProviderAttemptClaim) bool {
	if request.ExpectedProvider != "" && claim.Binding.Identity.Provider != request.ExpectedProvider {
		return false
	}
	input := request.Input
	input.Provider, input.AuthMode, input.Attempt = claim.Binding.Identity, claim.Binding.AuthMode, claim.Attempt
	input.LeaderEpoch, input.RunnerEpoch, input.ExpectedVersion = claim.LeaderEpoch, claim.RunnerEpoch, claim.ExpectedVersion
	// Repair is Store-owned just like provider identity and attempt number. A
	// caller retries the same logical phase request without manufacturing this
	// marker; reuse must rehydrate the exact durable launch input.
	input.Repair = claim.Input.Repair
	if claim.Input.Timeout <= 0 || claim.Input.Timeout > input.Timeout {
		return false
	}
	input.Timeout = claim.Input.Timeout
	return contracts.PhaseInputMatchesAuthenticatedClaim(input, claim.Input, claim.RequestDigest)
}

// bindClaimToInput is the last coordinator-side authentication point before
// an adapter invocation. BeginProviderAttempt returns the durable claim, so
// all execution identity fields are copied from that claim rather than being
// trusted from a caller's PhaseInput. Non-zero claim fields supplied by a
// caller are checked first to catch a split-brain request instead of silently
// overwriting it.
func bindClaimToInput(input *contracts.PhaseInput, claim store.ProviderAttemptClaim, request Request, identity domain.ProviderIdentity) bool {
	if input == nil || input.Repair != nil || claim.Role != string(request.Role) || claim.LeaderEpoch != request.Fence.LeaderEpoch || claim.RunnerEpoch != request.Fence.RunnerEpoch || claim.ExpectedVersion != request.ExpectedVersion || claim.Binding.Identity != identity || claim.RequestDigest == "" || !contracts.PhaseInputDigestMatches(claim.Input, claim.RequestDigest) {
		return false
	}
	expected := *input
	if expected.Provider == (domain.ProviderIdentity{}) {
		expected.Provider = identity
	}
	if expected.AuthMode == "" {
		expected.AuthMode = claim.Binding.AuthMode
	}
	if expected.Attempt == 0 {
		expected.Attempt = claim.Attempt
	}
	if expected.LeaderEpoch == 0 {
		expected.LeaderEpoch = claim.LeaderEpoch
	}
	if expected.RunnerEpoch == 0 {
		expected.RunnerEpoch = claim.RunnerEpoch
	}
	if expected.ExpectedVersion == 0 {
		expected.ExpectedVersion = claim.ExpectedVersion
	}
	expected.Repair = claim.Input.Repair
	if !contracts.PhaseInputMatchesAuthenticatedClaim(expected, claim.Input, claim.RequestDigest) {
		return false
	}
	*input = claim.Input
	return true
}
func validate(r Request) error {
	if !r.Role.valid() || r.Input.Ticket.Validate() != nil || r.ExpectedVersion == 0 || r.Fence.LeaderEpoch == 0 || r.Fence.RunnerEpoch == 0 || r.ConfigDigest == "" || len(r.ConfigDigest) != 64 || r.Input.Profile != contracts.ProfileGuarded || r.Input.Timeout <= 0 || r.Input.Timeout > 10*time.Minute || strings.TrimSpace(r.Input.Prompt) == "" || len(r.Input.Prompt) > 64<<10 || !cleanAbs(r.Input.Repository) || !cleanAbs(r.Input.Worktree) || r.Input.WorktreeIdentity == "" || len(r.Input.BaseSHA) != 40 || len(r.Input.Schema) == 0 || len(r.Input.Schema) > 1<<20 {
		return errors.New("invalid request")
	}
	if r.Input.Provider != (domain.ProviderIdentity{}) || r.Input.AuthMode != "" || r.Input.RequestDigest != "" || r.Input.Repair != nil {
		return errors.New("provider registry owns identity")
	}
	if !allowedPathPrefixes(r.Input.AllowedPaths) {
		return errors.New("invalid allowed paths")
	}
	if r.Input.Phase != phase(r.Role) && !(r.Role == RoleReviewer && (r.Input.Phase == domain.PhaseVerification || r.Input.Phase == domain.PhaseReview)) {
		return errors.New("role phase mismatch")
	}
	return nil
}
func phase(r Role) domain.Phase {
	if r == RolePlanner {
		return domain.PhasePlanning
	}
	if r == RoleBuilder {
		return domain.PhaseBuild
	}
	return domain.PhaseReview
}
func cleanAbs(v string) bool { return filepath.IsAbs(v) && filepath.Clean(v) == v && v != "/" }

// Codex's sandbox has a worktree-root permission, not a per-path allowlist.
// The worktree is therefore the trusted repository scope; this check is the
// fail-closed authority before a provider result can be accepted.
func allowedPathPrefixes(paths []string) bool {
	if len(paths) == 0 {
		return false
	}
	seen := map[string]bool{}
	for _, path := range paths {
		if path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path || (path != "." && (path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator)))) || seen[path] {
			return false
		}
		seen[path] = true
	}
	return true
}

func changedFilesAllowed(changed, allowed []string) bool {
	if !allowedPathPrefixes(allowed) {
		return false
	}
	for _, file := range changed {
		if file == "" || filepath.IsAbs(file) || filepath.Clean(file) != file || file == "." || file == ".." || strings.HasPrefix(file, ".."+string(filepath.Separator)) {
			return false
		}
		ok := false
		for _, prefix := range allowed {
			if prefix == "." || file == prefix || strings.HasPrefix(file, prefix+string(filepath.Separator)) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}
func max(v int64, x int64) int64 {
	if v < x {
		return x
	}
	return v
}
func safeDigest(v []byte) string {
	if len(v) == 0 {
		return ""
	}
	if len(v) > 64<<10 {
		v = v[:64<<10]
	}
	sum := sha256.Sum256([]byte(redact.String(string(v))))
	return "sha256:" + fmtHex(sum[:])
}
func fmtHex(v []byte) string {
	const h = "0123456789abcdef"
	b := make([]byte, len(v)*2)
	for i, x := range v {
		b[2*i] = h[x>>4]
		b[2*i+1] = h[x&15]
	}
	return string(b)
}
