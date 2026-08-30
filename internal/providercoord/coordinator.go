// Package providercoord is a durable, exec-free provider admission layer.
package providercoord

import (
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
var ErrPersistenceFatal = errors.New("provider coordinator persistence failure is fatal")

const (
	Completed       Outcome = "completed"
	Failed          Outcome = "failed"
	Canceled        Outcome = "canceled"
	NeedsOperator   Outcome = "needs_operator"
	BudgetExhausted Outcome = "budget_exhausted"
)

type Request struct {
	Role            Role
	Input           contracts.PhaseInput
	Validation      phaseartifact.Validation
	ExpectedVersion uint64
	Fence           domain.Fence
	ConfigDigest    string
}
type Receipt struct {
	Attempt                          int
	Provider                         domain.ProviderIdentity
	ArtifactDigest, TranscriptDigest string
	UsageUnits                       int64
	TokenUsage                       int64
	ErrorCode                        string
}
type Result struct {
	Code          Outcome
	Parsed        *phaseartifact.Parsed
	Attempts      []Receipt
	NeedsOperator bool
	CostUsed      int64
	// PersistenceFailure distinguishes an operator stop caused by uncertain
	// durable state from an ordinary provider/admission failure.
	PersistenceFailure bool
}
type Clock interface{ Now() time.Time }
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
	if binding.Identity.Provider == "codex" && binding.AuthMode != "chatgpt_subscription" {
		return false
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
	registry   *Registry
	routes     map[Role]Route
	store      *store.Store
	clock      Clock
	supervisor contracts.ProcessSupervisor
	fatal      atomic.Bool
	fatalMu    sync.Mutex
	fatalErr   error
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

func (c *Coordinator) Run(ctx context.Context, r Request) Result {
	if c.persistenceFailure() != nil {
		return Result{Code: NeedsOperator, NeedsOperator: true, PersistenceFailure: true}
	}
	if err := validate(r); err != nil {
		return Result{Code: NeedsOperator, NeedsOperator: true}
	}
	ticket, err := c.store.Ticket(ctx, r.Input.Ticket)
	if err != nil || ticket.Version != r.ExpectedVersion || ticket.RunnerEpoch != r.Fence.RunnerEpoch || ticket.ConfigDigest == "" || ticket.ConfigDigest != r.ConfigDigest {
		return Result{Code: NeedsOperator, NeedsOperator: true}
	}
	if r.Input.Phase == domain.PhaseReview {
		if err := c.store.ValidateFinalReviewEvidence(ctx, r.Input.Ticket, r.ExpectedVersion, r.Fence, r.Validation.ExpectedReviewedHead, r.Validation.ExpectedProofDigest); err != nil {
			return Result{Code: NeedsOperator, NeedsOperator: true}
		}
	}
	route, ok := c.routes[r.Role]
	if !ok {
		return Result{Code: NeedsOperator, NeedsOperator: true}
	}
	names := []string{route.Primary}
	if route.Fallback != "" {
		names = append(names, route.Fallback)
	}
	var receipts []Receipt
	var spent int64
	for _, name := range names {
		if ctx.Err() != nil {
			return Result{Code: Canceled, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
		}
		p, ok := c.registry.get(name)
		if !ok {
			continue
		}
		binding, err := p.Binding(ctx)
		// Route names may distinguish separately pinned model/family profiles
		// of one underlying provider executable. The durable identity remains
		// in binding.Identity and is validated by Store; do not compare it to
		// the local registry route alias.
		if err != nil || p.Name() != name {
			continue
		}
		remaining := ticket.CreatedAt.Add(ticket.MaxDuration).Sub(c.clock.Now())
		if remaining <= 0 {
			return Result{Code: BudgetExhausted, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
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
			if errors.Is(err, store.ErrProviderCapacity) {
				return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
			}
			continue
		}
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
		var raw contracts.PhaseResult
		var runErr error
		if invokeErr != nil {
			runErr = invokeErr
		} else {
			commandResult, commandErr := c.supervisor.Run(attemptCtx, drainRequest(claim), invocation, input)
			if commandErr != nil {
				runErr = commandErr
			} else {
				raw, runErr = p.Parse(attemptCtx, input, commandResult)
			}
		}
		cancel()
		cancelled := ctx.Err() != nil || errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded)
		// Returning from Run, including a provider error, is not proof that its
		// process group drained. Every terminal path must obtain an explicit
		// supervisor proof before releasing the durable claim and lease.
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		drain, drainErr := c.supervisor.Drain(drainCtx, drainRequest(claim))
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
		state, outcome := "failed", "failed"
		if cancelled {
			state, outcome = "cancelled", "cancelled"
		}
		valid := !cancelled && runErr == nil && raw.Provider == binding.Identity && raw.UsageTrusted && raw.UsageUnits >= 0 && changedFilesAllowed(raw.ChangedFiles, input.AllowedPaths)
		var parsed phaseartifact.Parsed
		if valid {
			parsed, err = phaseartifact.Parse(input.Phase, raw, r.Validation)
			if err == nil {
				state, outcome = "completed", "completed"
			} else {
				outcome = "invalid_artifact"
			}
		}
		receipt := Receipt{Attempt: claim.Attempt, Provider: binding.Identity, ArtifactDigest: safeDigest(raw.Artifact), TranscriptDigest: safeDigest([]byte(raw.Transcript)), UsageUnits: max(raw.UsageUnits, 0)}
		if raw.TokenUsageTrusted {
			receipt.TokenUsage = max(raw.TokenUsage, 0)
		}
		if runErr != nil {
			receipt.ErrorCode = "provider_error"
		} else if !valid {
			receipt.ErrorCode = "invalid_result"
		} else if outcome == "invalid_artifact" {
			receipt.ErrorCode = "invalid_artifact"
		}
		receipts = append(receipts, receipt)
		finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
		finishErr := c.store.FinishProviderAttempt(finishCtx, claim, drain, r.ExpectedVersion, r.Fence, state, outcome, max(raw.UsageUnits, 0), c.clock.Now())
		finishCancel()
		if finishErr != nil {
			if errors.Is(finishErr, store.ErrBudgetExhausted) {
				quarantineCtx, quarantineCancel := context.WithTimeout(context.Background(), 5*time.Second)
				budgetErr := c.store.FailProviderAttemptBudget(quarantineCtx, claim, drain, r.ExpectedVersion, r.Fence, c.clock.Now())
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
		spent += max(raw.UsageUnits, 0)
		if !raw.UsageTrusted {
			return Result{Code: NeedsOperator, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
		}
		if cancelled {
			return Result{Code: Canceled, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
		}
		if spent >= ticket.MaxCostMicroUSD {
			return Result{Code: BudgetExhausted, Attempts: receipts, NeedsOperator: true, CostUsed: spent}
		}
		if state == "completed" {
			return Result{Code: Completed, Parsed: &parsed, Attempts: receipts, CostUsed: spent}
		}
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

// bindClaimToInput is the last coordinator-side authentication point before
// an adapter invocation. BeginProviderAttempt returns the durable claim, so
// all execution identity fields are copied from that claim rather than being
// trusted from a caller's PhaseInput. Non-zero claim fields supplied by a
// caller are checked first to catch a split-brain request instead of silently
// overwriting it.
func bindClaimToInput(input *contracts.PhaseInput, claim store.ProviderAttemptClaim, request Request, identity domain.ProviderIdentity) bool {
	if input == nil || claim.Role != string(request.Role) || claim.LeaderEpoch != request.Fence.LeaderEpoch || claim.RunnerEpoch != request.Fence.RunnerEpoch || claim.ExpectedVersion != request.ExpectedVersion || claim.Binding.Identity != identity || claim.RequestDigest == "" || !contracts.PhaseInputDigestMatches(claim.Input, claim.RequestDigest) {
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
	_, expectedDigest, err := contracts.CanonicalPhaseInput(expected)
	if err != nil || expectedDigest != claim.RequestDigest {
		return false
	}
	*input = claim.Input
	return true
}
func validate(r Request) error {
	if !r.Role.valid() || r.Input.Ticket.Validate() != nil || r.ExpectedVersion == 0 || r.Fence.LeaderEpoch == 0 || r.Fence.RunnerEpoch == 0 || r.ConfigDigest == "" || len(r.ConfigDigest) != 64 || r.Input.Profile != contracts.ProfileGuarded || r.Input.Timeout <= 0 || r.Input.Timeout > 10*time.Minute || strings.TrimSpace(r.Input.Prompt) == "" || len(r.Input.Prompt) > 64<<10 || !cleanAbs(r.Input.Repository) || !cleanAbs(r.Input.Worktree) || r.Input.WorktreeIdentity == "" || len(r.Input.BaseSHA) != 40 || len(r.Input.Schema) == 0 || len(r.Input.Schema) > 1<<20 {
		return errors.New("invalid request")
	}
	if r.Input.Provider != (domain.ProviderIdentity{}) || r.Input.AuthMode != "" || r.Input.RequestDigest != "" {
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
