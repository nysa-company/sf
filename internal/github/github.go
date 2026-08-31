// Package github is the narrow, noninteractive gh CLI boundary. It contains no
// token handling. Mutations require caller-fenced durable effects; NewStoreClient
// is the local composition helper that supplies those SQLite-backed authorities.
package github

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

var (
	ErrNoMatchingPR      = errors.New("no factory-owned pull request matches identity")
	ErrAmbiguousPR       = errors.New("multiple factory-owned pull requests match identity")
	ErrPolicyRefusal     = errors.New("github policy refused operation")
	ErrExternalMerged    = errors.New("pull request was merged outside the exact-head factory flow")
	ErrChecksPending     = errors.New("required checks remain pending")
	ErrChecksFailed      = errors.New("required checks failed or changed identity")
	ErrApprovalInvalid   = errors.New("approval is not bound to current reviewed head")
	ErrResponseTooLarge  = errors.New("github response exceeded bound")
	ErrMalformedResponse = errors.New("github response is malformed")
	ErrCreateUncertain   = contracts.ErrDraftCreateUncertain
	ErrUpdateUncertain   = contracts.ErrPullRequestUpdateUncertain
	// These are reserved for adapters that can prove no mutation command was
	// handed off. They are deliberately distinct from generic command errors.
	ErrCreateBeforeStart       = contracts.ErrDraftCreateBeforeStart
	ErrUpdateBeforeStart       = contracts.ErrPullRequestUpdateBeforeStart
	ErrGuardedMergeUnavailable = errors.New("sf-managed guarded merge is unavailable without server-enforced strict protected-base checks; observe a manual merge instead")
	ErrProcessCleanup          = contracts.ErrExternalCleanupUncertain
	ErrCleanupQuarantineFatal  = contracts.ErrExternalCleanupQuarantineFatal
	// ErrRunnerBusy means this invocation did not acquire the runner's launch
	// ownership. Client must not call Cleanup for another invocation's run.
	ErrRunnerBusy = errors.New("gh runner is already in use")
)

const maxResponse = 1 << 20

var maxGHDeadline = 2 * time.Minute

type Client struct {
	binaryPath      string
	home            string
	configDir       string                  // explicit existing gh auth/config authority, never a temp substitute
	env             []string                // only SF_FAKE_GH_STATE is permitted for fake-gh tests.
	runner          SupervisedCommandRunner // required by NewClient; supervisor-owned in production
	validateClaimFn func(context.Context, domain.ExternalEffectClaim) error
	mutationGuard   contracts.ExternalMutationGuard
	// VerifyProtectedBranch is supplied by the Git boundary.  It is the
	// authority that freshly fetches the protected base ref and proves that the
	// reported merge commit is contained by that exact ref.
	verifyProtectedBranch contracts.MergeBranchVerifier
	mergeIntents          contracts.MergeIntentRecorder
	quarantiner           contracts.ExternalMutationQuarantineAuthority
	cleanupLatched        *atomic.Bool
	runMu                 *sync.Mutex // serializes one Run+Cleanup ownership pair
}

type Principal struct{ Login string }
type PRMatch struct {
	Identity             contracts.PullRequestIdentity
	Draft, Merged, Ready bool
	Title, Body          string
	MergeCommit          string
	BaseHeadOID          string
	State                string
	MergeState           string
	AutoMerge            bool
}

// SupervisedCommandRunner is the only process capability accepted by the
// GitHub boundary. Its Cleanup method is part of the authority contract: the
// caller must not regain the mutation gate until descendants and output
// writers are gone or cleanup has returned an uncertainty error.
type SupervisedCommandRunner interface {
	Run(context.Context, string, []string, []string) ([]byte, error)
	// Cleanup must return a drain or quarantine proof. An absent proof blocks
	// the effect and keeps the mutation gate quarantined.
	Cleanup(context.Context) (CleanupProof, error)
}
type CleanupProof struct {
	Drained     bool
	Quarantined bool
}

// Cleanup success is exactly a drain without quarantine. A simultaneous
// drained/quarantined report is contradictory evidence, not an optimistic
// drain: an ExternalMutationGate will remain latched on the resulting error.
func (p CleanupProof) valid() bool { return p.Drained && !p.Quarantined }

type commandRunnerFunc func(context.Context, string, []string, []string) ([]byte, error)

func (f commandRunnerFunc) Run(ctx context.Context, binary string, args, env []string) ([]byte, error) {
	return f(ctx, binary, args, env)
}
func (f commandRunnerFunc) Cleanup(context.Context) (CleanupProof, error) {
	return CleanupProof{Drained: true}, nil
}

type CheckIdentity struct{ Name, ExternalID string }
type MergeOutcome string

const (
	MergeApplied  MergeOutcome = "merged"
	MergeExternal MergeOutcome = "external_merged"
)

var _ contracts.GitHub = (*Client)(nil)

// NewClient composes a caller-supplied protected-branch verifier with the
// durable mutation and quarantine authorities. Daemon wiring remains outside
// this package until the hardened Git integration exists.
func NewClient(binary, home, configDir string, runner SupervisedCommandRunner, validate func(context.Context, domain.ExternalEffectClaim) error, guard contracts.ExternalMutationGuard, verifier contracts.MergeBranchVerifier, intents contracts.MergeIntentRecorder, quarantiner contracts.ExternalMutationQuarantineAuthority) (*Client, error) {
	if binary == "" || !filepath.IsAbs(binary) || home == "" || !filepath.IsAbs(home) || configDir == "" || !filepath.IsAbs(configDir) || runner == nil || validate == nil || guard == nil || verifier == nil || intents == nil || quarantiner == nil {
		return nil, ErrPolicyRefusal
	}
	return &Client{binaryPath: binary, home: home, configDir: configDir, runner: runner, validateClaimFn: validate, mutationGuard: guard, verifyProtectedBranch: verifier, mergeIntents: intents, quarantiner: quarantiner, cleanupLatched: &atomic.Bool{}, runMu: &sync.Mutex{}}, nil
}

// NewStoreClient supplies SQLite's durable-effect, guard, and quarantine
// authority. The caller injects the protected-branch verifier used for
// reconciliation; it is never a substitute for a base CAS at launch.
func NewStoreClient(binary, home, configDir string, runner SupervisedCommandRunner, database *store.Store, verifier contracts.MergeBranchVerifier) (*Client, error) {
	if database == nil {
		return nil, ErrPolicyRefusal
	}
	return NewClient(binary, home, configDir, runner, database.ValidateExternalEffectClaim, database.ExternalMutationGuard(), verifier, database, database)
}

type authHost struct {
	Login       string `json:"login"`
	Active      bool   `json:"active"`
	State       string `json:"state"`
	Host        string `json:"host"`
	Scopes      string `json:"scopes"`
	GitProtocol string `json:"gitProtocol"`
	TokenSource string `json:"tokenSource"`
	// Token is emitted only when --show-token is passed. The client does not
	// pass that flag, but accepting the documented field keeps the decoded
	// hosts shape stable without surfacing credentials.
	Token string `json:"token"`
}

func (c Client) AuthStatus(ctx context.Context) error {
	var auth struct {
		Hosts map[string][]authHost `json:"hosts"`
	}
	if err := c.json(ctx, &auth, "auth", "status", "--json", "hosts"); err != nil {
		return err
	}
	if _, err := activeLogin(auth.Hosts); err != nil {
		return ErrMalformedResponse
	}
	return nil
}
func (c Client) Repository(ctx context.Context, identity contracts.RepositoryIdentity) (contracts.RepositoryIdentity, error) {
	if _, err := c.Preflight(ctx, identity); err != nil {
		return contracts.RepositoryIdentity{}, err
	}
	return identity, nil
}
func (c Client) FindPullRequest(ctx context.Context, identity contracts.PullRequestIdentity) (contracts.PullRequestIdentity, bool, error) {
	match, err := c.Observe(ctx, identity)
	if errors.Is(err, ErrNoMatchingPR) {
		return contracts.PullRequestIdentity{}, false, nil
	}
	if err != nil {
		return contracts.PullRequestIdentity{}, false, err
	}
	return match.Identity, true, nil
}

// ObserveDraftPullRequest is the typed recovery view used by the publication
// coordinator.  It never adopts a merely familiar branch: ObservePublicationCandidate
// rejects foreign and ambiguous source/base matches before returning a fact.
func (c Client) ObserveDraftPullRequest(ctx context.Context, identity contracts.PullRequestIdentity) (contracts.PullRequestIdentity, string, bool, bool, error) {
	match, found, err := c.ObservePublicationCandidate(ctx, identity)
	if err != nil || !found {
		return contracts.PullRequestIdentity{}, "", false, false, err
	}
	return match.Identity, match.State, match.Draft, true, nil
}

// ObserveFactoryPullRequestOutput is the publication-transition witness. The
// exact current marker proves ownership while title/body prove that the effect
// request that was confirmed is still the live PR output.
func (c Client) ObserveFactoryPullRequestOutput(ctx context.Context, expected contracts.PullRequestIdentity, title, body string) (contracts.PullRequestIdentity, string, bool, bool, error) {
	match, found, err := c.ObservePublicationCandidate(ctx, expected)
	if err != nil || !found {
		return contracts.PullRequestIdentity{}, "", false, false, err
	}
	applied := match.State == "OPEN" && match.Draft && match.Title == title && match.Body == body+"\n\n"+ownershipMarker(match.Identity)
	return match.Identity, match.State, match.Draft, applied, nil
}

// RefreshFactoryPullRequestIdentity re-observes one already-persisted factory
// pull request after a correction advances its source head. It is deliberately
// a continuity check, not a branch-name lookup: both the prior identity and
// the expected replacement identity name the same numbered PR and source.
//
// A correction can leave the old ownership marker in the PR body, or a lost
// response from a preceding idempotent update can already have installed the
// new marker. Either is sufficient, but a missing or substituted marker never
// authorizes adoption.
func (c Client) RefreshFactoryPullRequestIdentity(ctx context.Context, prior, expected contracts.PullRequestIdentity) (contracts.PullRequestIdentity, error) {
	match, err := c.refreshFactoryPullRequest(ctx, prior, expected)
	if err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	return match.Identity, nil
}

// ObserveFactoryPullRequestUpdate is the correction output witness.  Unlike
// the create observer, it does not treat a known PR with stale title/body as
// an applied effect. The prior marker remains a continuity proof only; the
// requested replacement body must carry the new identity marker.
func (c Client) ObserveFactoryPullRequestUpdate(ctx context.Context, prior, expected contracts.PullRequestIdentity, title, body string) (contracts.PullRequestIdentity, string, bool, bool, error) {
	match, err := c.refreshFactoryPullRequest(ctx, prior, expected)
	if err != nil {
		return contracts.PullRequestIdentity{}, "", false, false, err
	}
	marked := body + "\n\n" + ownershipMarker(match.Identity)
	applied := match.State == "OPEN" && match.Draft && match.Title == title && match.Body == marked
	return match.Identity, match.State, match.Draft, applied, nil
}

// UpdateFactoryPullRequest is intentionally separate from UpdatePullRequest:
// a correction may still carry the old marker before the edit, which must be
// authenticated against the durable prior identity rather than accepted by the
// ordinary current-marker observer.
func (c Client) UpdateFactoryPullRequest(ctx context.Context, durable domain.ExternalEffectClaim, prior, expected contracts.PullRequestIdentity, title, body string) error {
	current, err := c.refreshFactoryPullRequest(ctx, prior, expected)
	if err != nil {
		return err
	}
	if !sameExact(current.Identity, expected) || current.State != "OPEN" || current.Merged || !current.Draft {
		return ErrPolicyRefusal
	}
	marked := body + "\n\n" + ownershipMarker(current.Identity)
	if err := c.validateClaim(ctx, durable, current.Identity, "pr_edit", requestDigest("pr_edit", current.Identity, title, body)); err != nil {
		return err
	}
	if current.Title == title && current.Body == marked {
		return nil
	}
	_, runErr := c.mutateFactoryCorrectionExact(ctx, durable, prior, expected, current.Identity, "pr", "edit", fmt.Sprint(current.Identity.Number), "--repo", repoArg(current.Identity.Repository), "--title", title, "--body", marked)
	if errors.Is(runErr, ErrProcessCleanup) || errors.Is(runErr, ErrCleanupQuarantineFatal) {
		return runErr
	}
	_, _, _, applied, observeErr := c.ObserveFactoryPullRequestUpdate(ctx, prior, expected, title, body)
	if errors.Is(observeErr, ErrProcessCleanup) || errors.Is(observeErr, ErrCleanupQuarantineFatal) {
		return observeErr
	}
	if observeErr == nil && applied {
		return nil
	}
	// Once mutateFactoryCorrectionExact has entered its command handoff, neither
	// a stale output nor a failed post-write observation proves that the edit did
	// not happen.  Keep the effect uncertain so a later exact output witness can
	// settle it; do not let the worker reinterpret this as semantic absence.
	if errors.Is(runErr, ErrPolicyRefusal) || errors.Is(runErr, ErrRunnerBusy) || errors.Is(runErr, ErrUpdateBeforeStart) {
		return runErr
	}
	if observeErr != nil {
		return fmt.Errorf("%w: output observation unavailable: %v", ErrUpdateUncertain, observeErr)
	}
	if runErr != nil {
		return fmt.Errorf("%w: command result unavailable", ErrUpdateUncertain)
	}
	return fmt.Errorf("%w: requested output was not observed", ErrUpdateUncertain)
}

func (c Client) refreshFactoryPullRequest(ctx context.Context, prior, expected contracts.PullRequestIdentity) (PRMatch, error) {
	if !validPublicationPRIdentity(prior) || !validPublicationPRIdentity(expected) || !sameRefreshContinuity(prior, expected) || prior.HeadOID == expected.HeadOID {
		return PRMatch{}, ErrPolicyRefusal
	}
	var values []prWire
	if err := c.json(ctx, &values, "pr", "list", "--repo", repoArg(prior.Repository), "--state", "all", "--limit", "100", "--json", prFields); err != nil {
		return PRMatch{}, err
	}
	if len(values) == 100 {
		return PRMatch{}, ErrAmbiguousPR
	}
	var match *PRMatch
	for _, value := range values {
		candidate, err := value.identityUnmarked(prior.Repository)
		if err != nil {
			return PRMatch{}, err
		}
		// The persisted number is an exact factory identity. A row for that
		// number that no longer proves the old source/base is a substitution,
		// not an opportunity to adopt a PR with a familiar branch name.
		if candidate.Number == prior.Number {
			// The correction may advance the head, but it must still be for the
			// exact live protected-base witness selected for this candidate.
			if !sameRefreshContinuity(prior, candidate) || !validOID(candidate.BaseOID) || candidate.BaseOID != expected.BaseOID || candidate.HeadOID != expected.HeadOID || !refreshMarkerPresent(value.Body, prior, expected) || value.State != "OPEN" || value.MergedAt != nil {
				return PRMatch{}, ErrNoMatchingPR
			}
			if match != nil {
				return PRMatch{}, ErrAmbiguousPR
			}
			observed := value.match(candidate)
			match = &observed
			continue
		}
		// One factory source branch has one durable PR. A second open row is
		// ambiguity even if it targets a different base branch.
		if samePRSource(candidate, prior) && value.State == "OPEN" && value.MergedAt == nil {
			return PRMatch{}, ErrAmbiguousPR
		}
	}
	if match == nil {
		return PRMatch{}, ErrNoMatchingPR
	}
	return *match, nil
}
func (c Client) CreateDraftPullRequest(ctx context.Context, durable domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, title, body string) (contracts.PullRequestIdentity, error) {
	if !validIdentity(identity) || !validTitle(title) || !validBody(body) {
		return contracts.PullRequestIdentity{}, ErrPolicyRefusal
	}
	// Public effect calls always require a durable claim, including the
	// idempotent adoption path.  The claim is checked again below immediately
	// before a create mutation after the absence observation.
	if err := c.validateClaim(ctx, durable, identity, "draft_pr", requestDigest("draft_pr", identity, title, body)); err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	if match, found, err := c.ObservePublicationCandidate(ctx, identity); err == nil && found {
		if !adoptableDraft(match) {
			return contracts.PullRequestIdentity{}, ErrPolicyRefusal
		}
		return match.Identity, nil
	} else if err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	// Validate after the exact absence observation and immediately before the
	// only mutation, so a stale effect cannot create a replacement PR.
	if err := c.validateClaim(ctx, durable, identity, "draft_pr", requestDigest("draft_pr", identity, title, body)); err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	markedBody := body + "\n\n" + ownershipMarker(identity)
	// Creation has no GitHub-side base/head CAS. The durable launch handoff
	// therefore re-observes the exact base, source, and PR absence while the
	// mutation gate is held; a pre-handoff list result is never sufficient.
	// These are observations, not a remote lock: refs may still move before
	// GitHub processes the command, so a non-exact or unavailable post-handoff
	// result remains uncertain and is never blindly retried.
	_, runErr := c.mutateCreateExact(ctx, durable, identity, "pr", "create", "--repo", repoArg(identity.Repository), "--head", identity.HeadOwner+":"+identity.HeadRef, "--base", identity.BaseRef, "--draft", "--title", title, "--body", markedBody)
	if errors.Is(runErr, ErrProcessCleanup) || errors.Is(runErr, ErrCleanupQuarantineFatal) {
		return contracts.PullRequestIdentity{}, runErr
	}
	// Both a delivered response and a lost response are reconciled by the same
	// exact ownership observation; command output is never object evidence.
	match, found, observeErr := c.ObservePublicationCandidate(ctx, identity)
	if errors.Is(observeErr, ErrProcessCleanup) || errors.Is(observeErr, ErrCleanupQuarantineFatal) {
		return contracts.PullRequestIdentity{}, observeErr
	}
	if observeErr == nil && found && adoptableDraft(match) {
		return match.Identity, nil
	}
	if observeErr == nil {
		// A clean absence after the mutation handoff is not proof that no PR
		// was created. This includes a command error: gh may have applied the
		// mutation before losing its response. Reconcile instead of retrying
		// (or attempting a number-only close) on every such path. A closed
		// exact PR is the one deterministic conflict we can safely report.
		if found {
			return contracts.PullRequestIdentity{}, ErrPolicyRefusal
		}
		// The mutation boundary proved that no command was handed off. A clean
		// exact absence therefore remains a retryable, pre-start failure rather
		// than being conservatively upgraded to uncertainty.
		if errors.Is(runErr, ErrCreateBeforeStart) || errors.Is(runErr, ErrRunnerBusy) {
			return contracts.PullRequestIdentity{}, runErr
		}
		if runErr != nil {
			conflict, conflictErr := c.observeClosedPublicationConflict(ctx, identity)
			if conflictErr != nil {
				return contracts.PullRequestIdentity{}, fmt.Errorf("%w: %v", ErrCreateUncertain, conflictErr)
			}
			if conflict {
				return contracts.PullRequestIdentity{}, ErrPolicyRefusal
			}
		}
		return contracts.PullRequestIdentity{}, fmt.Errorf("%w: command result unavailable", ErrCreateUncertain)
	}
	if runErr != nil {
		return contracts.PullRequestIdentity{}, fmt.Errorf("%w: command result unavailable", ErrCreateUncertain)
	}
	// A create response that cannot be reconciled is not safe to compensate:
	// gh exposes no API-side expected-identity precondition for closing a PR by
	// number. Leave the remote object untouched and require reconciliation.
	return contracts.PullRequestIdentity{}, fmt.Errorf("%w: %v", ErrCreateUncertain, observeErr)
}
func (c Client) UpdatePullRequest(ctx context.Context, durable domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, title, body string) error {
	if !validIdentity(identity) || !validTitle(title) || !validBody(body) {
		return ErrPolicyRefusal
	}
	if err := c.validateClaim(ctx, durable, identity, "pr_edit", requestDigest("pr_edit", identity, title, body)); err != nil {
		return err
	}
	observed, err := c.Observe(ctx, identity)
	if err != nil {
		return err
	}
	if observed.State != "OPEN" || observed.Merged {
		return ErrPolicyRefusal
	}
	marked := body + "\n\n" + ownershipMarker(observed.Identity)
	if sameExact(observed.Identity, identity) && observed.Title == title && observed.Body == marked {
		return nil
	}
	identity = observed.Identity
	if err := c.validateClaim(ctx, durable, identity, "pr_edit", requestDigest("pr_edit", identity, title, body)); err != nil {
		return err
	}
	_, runErr := c.mutateExact(ctx, durable, identity, "pr", "edit", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository), "--title", title, "--body", marked)
	if errors.Is(runErr, ErrProcessCleanup) {
		return runErr
	}
	post, observeErr := c.Observe(ctx, identity)
	if errors.Is(observeErr, ErrProcessCleanup) {
		return observeErr
	}
	if observeErr == nil && sameExact(post.Identity, identity) && post.State == "OPEN" && !post.Merged && post.Title == title && post.Body == marked {
		return nil
	}
	if observeErr != nil {
		return ErrPolicyRefusal
	}
	if runErr != nil {
		return runErr
	}
	return ErrPolicyRefusal
}
func (c Client) RequiredChecks(ctx context.Context, identity contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
	return c.checks(ctx, identity)
}
func (c Client) MarkReady(ctx context.Context, durable domain.ExternalEffectClaim, identity contracts.PullRequestIdentity) error {
	observed, err := c.Observe(ctx, identity)
	if err != nil {
		if errors.Is(err, ErrNoMatchingPR) {
			return ErrPolicyRefusal
		}
		return err
	}
	if observed.State != "OPEN" || observed.Merged {
		return ErrPolicyRefusal
	}
	identity = observed.Identity
	if err := c.validateClaim(ctx, durable, identity, "pr_ready", requestDigest("pr_ready", identity)); err != nil {
		return err
	}
	_, err = c.mutateReadyExact(ctx, durable, identity, "pr", "ready", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository))
	if errors.Is(err, ErrProcessCleanup) {
		return err
	}
	observed, observeErr := c.Observe(ctx, identity)
	if errors.Is(observeErr, ErrProcessCleanup) {
		return observeErr
	}
	if observeErr == nil && sameExact(observed.Identity, identity) && observed.State == "OPEN" && !observed.Merged && observed.Ready && !observed.Draft {
		return nil
	}
	// GitHub exposes no expected-head CAS for ready. If the post-state is not
	// the exact expected state, leave the original effect uncertain/blocked for
	// reconciliation. A compensating --undo would be a second mutation without
	// its own durable effect claim and could race a legitimate operator action.
	return ErrPolicyRefusal
}

func (c Client) MergeExactHead(ctx context.Context, durable domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, headOID, method string, authorization domain.MergeAuthorization) error {
	observed, err := c.Observe(ctx, identity)
	if err != nil {
		return err
	}
	queued, queueErr := c.mergeQueued(ctx, observed.Identity)
	if queueErr != nil || c.verifyProtectedBranch == nil || observed.Draft || observed.Merged || observed.AutoMerge || queued || queueState(observed.MergeState) || observed.State != "OPEN" {
		return ErrPolicyRefusal
	}
	identity = observed.Identity
	baseOID, baseOK := exactBaseBinding(authorization)
	if !authorization.Approved || !authorization.GatesGreen || authorization.ReviewedHead != headOID || authorization.CurrentHead != headOID || !baseOK || observed.Identity.BaseOID != baseOID {
		return ErrApprovalInvalid
	}
	if err := c.validateClaim(ctx, durable, identity, "merge", requestDigest("merge", identity, headOID, method, authorization.ReviewedBaseSHA, authorization.CurrentBaseSHA, authorization.ReviewedBaseHeadOID, authorization.CurrentBaseHeadOID)); err != nil {
		return err
	}
	if method != "merge" && method != "squash" && method != "rebase" || c.mergeIntents == nil {
		return ErrPolicyRefusal
	}
	protection, err := c.strictProtection(ctx, identity.Repository, identity.BaseRef, method)
	if err != nil {
		return err
	}
	if err := c.mergeIntents.RecordMergeIntent(ctx, domain.MergeIntent{Ref: durable.Ref, SemanticKey: durable.SemanticKey, RequestDigest: durable.RequestDigest, TicketVersion: durable.TicketVersion, LeaderEpoch: durable.LeaderEpoch, RunnerEpoch: durable.RunnerEpoch, ClaimEpoch: durable.ClaimEpoch, RepositoryHost: identity.Repository.Host, RepositoryOwner: identity.Repository.Owner, RepositoryName: identity.Repository.Name, PullRequestNumber: identity.Number, HeadOID: headOID, BaseRef: identity.BaseRef, OriginalBaseOID: baseOID, ProtectionRuleID: protection.ID, ProtectionKind: protection.Kind, ProtectionChecksDigest: protection.ChecksDigest, StrictStatusChecks: true, AdminEnforced: protection.AdminEnforced, ActiveRulesetCount: uint32(protection.ActiveRulesetCount), Method: method}); err != nil {
		return err
	}
	args := []string{"pr", "merge", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository), "--match-head-commit", headOID, "--" + method}
	_, runErr := c.mutationGuard.RunExternalMutation(ctx, durable, func(runCtx context.Context) ([]byte, error) {
		latest, err := c.view(runCtx, identity)
		if err != nil || !sameExact(latest.Identity, identity) || latest.State != "OPEN" || latest.Draft || latest.Merged || latest.AutoMerge || queueState(latest.MergeState) {
			return nil, ErrPolicyRefusal
		}
		queued, err := c.mergeQueued(runCtx, latest.Identity)
		if err != nil || queued {
			return nil, ErrPolicyRefusal
		}
		freshProtection, err := c.strictProtection(runCtx, identity.Repository, identity.BaseRef, method)
		if err != nil || !sameProtectionWitness(freshProtection, protection) {
			return nil, ErrGuardedMergeUnavailable
		}
		return c.run(runCtx, args...)
	})
	if errors.Is(runErr, ErrProcessCleanup) || errors.Is(runErr, ErrPolicyRefusal) || errors.Is(runErr, ErrGuardedMergeUnavailable) {
		return runErr
	}
	// A gh response can be lost after the server applies the expected-head
	// mutation. Reconcile both delivered and unavailable command responses.
	return c.reconcileStrictMerge(ctx, identity, headOID, baseOID)
}

func (c Client) validateClaim(ctx context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, requiredKind, digest string) error {
	if c.validateClaimFn == nil || c.mutationGuard == nil || claim.SemanticKey == "" || claim.Kind != requiredKind || claim.RequestDigest != digest || !validIdentity(identity) {
		return ErrPolicyRefusal
	}
	return c.validateClaimFn(ctx, claim)
}

func (c Client) mutateCreateExact(ctx context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, args ...string) ([]byte, error) {
	if c.mutationGuard == nil {
		return nil, ErrPolicyRefusal
	}
	handedOff := false
	output, err := c.mutationGuard.RunExternalMutation(ctx, claim, func(runCtx context.Context) ([]byte, error) {
		// GitHub has no create-side base/head CAS. Re-read the protected base,
		// source, and PR absence inside the durable launch gate. This proves the
		// last pre-handoff observations matched the durable identity; it cannot
		// prevent remote movement before GitHub processes the command.
		if err := c.observeBaseExact(runCtx, identity, identity.BaseOID); err != nil {
			if errors.Is(err, ErrProcessCleanup) || errors.Is(err, ErrCleanupQuarantineFatal) {
				return nil, err
			}
			return nil, ErrPolicyRefusal
		}
		if err := c.observeSourceExact(runCtx, identity); err != nil {
			if errors.Is(err, ErrProcessCleanup) || errors.Is(err, ErrCleanupQuarantineFatal) {
				return nil, err
			}
			return nil, ErrPolicyRefusal
		}
		if _, found, err := c.ObservePublicationCandidate(runCtx, identity); err != nil || found {
			if errors.Is(err, ErrProcessCleanup) || errors.Is(err, ErrCleanupQuarantineFatal) {
				return nil, err
			}
			return nil, ErrPolicyRefusal
		}
		return c.runWithHandoff(runCtx, &handedOff, args...)
	})
	if err != nil && (!handedOff || errors.Is(err, ErrRunnerBusy)) && !errors.Is(err, ErrProcessCleanup) && !errors.Is(err, ErrCleanupQuarantineFatal) {
		return nil, fmt.Errorf("%w: %v", ErrCreateBeforeStart, err)
	}
	return output, err
}

// observeSourceExact binds a create to the exact branch tip selected by the
// durable effect.  PR absence alone cannot prove that the branch did not move
// between the preflight observation and launch.
func (c Client) observeSourceExact(ctx context.Context, identity contracts.PullRequestIdentity) error {
	// The ref lives in the source repository, which may be a fork; querying the
	// base repository would silently validate the wrong branch tip.
	path := "repos/" + identity.HeadOwner + "/" + identity.HeadRepository + "/git/ref/heads/" + identity.HeadRef
	output, err := c.run(ctx, "api", path)
	if err != nil {
		if errors.Is(err, ErrProcessCleanup) || errors.Is(err, ErrCleanupQuarantineFatal) {
			return err
		}
		return ErrPolicyRefusal
	}
	var ref struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if json.Unmarshal(output, &ref) != nil || ref.Object.SHA != identity.HeadOID {
		return ErrPolicyRefusal
	}
	return nil
}

// observeBaseExact reads the protected ref from the base repository, never
// from a fork that happens to host the source branch.
func (c Client) observeBaseExact(ctx context.Context, identity contracts.PullRequestIdentity, baseOID string) error {
	if !validOID(baseOID) {
		return ErrPolicyRefusal
	}
	path := "repos/" + identity.Repository.Owner + "/" + identity.Repository.Name + "/git/ref/heads/" + identity.BaseRef
	output, err := c.run(ctx, "api", path)
	if err != nil {
		if errors.Is(err, ErrProcessCleanup) || errors.Is(err, ErrCleanupQuarantineFatal) {
			return err
		}
		return ErrPolicyRefusal
	}
	var ref struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if json.Unmarshal(output, &ref) != nil || ref.Object.SHA != baseOID {
		return ErrPolicyRefusal
	}
	return nil
}

// mutateExact re-observes the exact factory identity inside the durable launch
// handoff. A PR number alone is never sufficient authorization to mutate.
func (c Client) mutateExact(ctx context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, args ...string) ([]byte, error) {
	if c.mutationGuard == nil {
		return nil, ErrPolicyRefusal
	}
	return c.mutationGuard.RunExternalMutation(ctx, claim, func(runCtx context.Context) ([]byte, error) {
		observed, err := c.view(runCtx, identity)
		if errors.Is(err, ErrProcessCleanup) || errors.Is(err, ErrCleanupQuarantineFatal) {
			return nil, err
		}
		if err != nil || !sameExact(observed.Identity, identity) || observed.State != "OPEN" || observed.Merged {
			return nil, ErrPolicyRefusal
		}
		return c.run(runCtx, args...)
	})
}

func (c Client) mutateFactoryCorrectionExact(ctx context.Context, claim domain.ExternalEffectClaim, prior, expected, identity contracts.PullRequestIdentity, args ...string) ([]byte, error) {
	if c.mutationGuard == nil {
		return nil, ErrPolicyRefusal
	}
	handedOff := false
	output, err := c.mutationGuard.RunExternalMutation(ctx, claim, func(runCtx context.Context) ([]byte, error) {
		observed, err := c.refreshFactoryPullRequest(runCtx, prior, expected)
		if errors.Is(err, ErrProcessCleanup) || errors.Is(err, ErrCleanupQuarantineFatal) {
			return nil, err
		}
		if err != nil || !sameExact(observed.Identity, identity) || observed.State != "OPEN" || observed.Merged || !observed.Draft {
			return nil, ErrPolicyRefusal
		}
		return c.runWithHandoff(runCtx, &handedOff, args...)
	})
	if err != nil && (!handedOff || errors.Is(err, ErrRunnerBusy)) && !errors.Is(err, ErrProcessCleanup) && !errors.Is(err, ErrCleanupQuarantineFatal) {
		return nil, fmt.Errorf("%w: %v", ErrUpdateBeforeStart, err)
	}
	return output, err
}

func readyLaunchSafe(observed PRMatch, identity contracts.PullRequestIdentity) bool {
	return sameExact(observed.Identity, identity) && observed.State == "OPEN" && !observed.Merged && !observed.AutoMerge && !queueState(observed.MergeState)
}

func (c Client) mutateReadyExact(ctx context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, args ...string) ([]byte, error) {
	if c.mutationGuard == nil {
		return nil, ErrPolicyRefusal
	}
	return c.mutationGuard.RunExternalMutation(ctx, claim, func(runCtx context.Context) ([]byte, error) {
		observed, err := c.view(runCtx, identity)
		if errors.Is(err, ErrProcessCleanup) {
			return nil, ErrProcessCleanup
		}
		if err != nil || !readyLaunchSafe(observed, identity) {
			return nil, ErrPolicyRefusal
		}
		return c.run(runCtx, args...)
	})
}

func requestDigest(operation string, identity contracts.PullRequestIdentity, values ...string) string {
	input := operation + "\x00" + repoArg(identity.Repository) + "\x00" + identity.HeadOwner + "\x00" + identity.HeadRepository + "\x00" + identity.HeadRef + "\x00" + identity.HeadOID + "\x00" + identity.BaseRef + "\x00" + identity.BaseOID
	for _, value := range values {
		input += "\x00" + value
	}
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

// CanonicalDraftPullRequestRequestDigest is the durable request binding used
// by CreateDraftPullRequest. Publication coordinators must use this rather
// than duplicate the adapter's private serialization.
func CanonicalDraftPullRequestRequestDigest(identity contracts.PullRequestIdentity, title, body string) string {
	return requestDigest("draft_pr", identity, title, body)
}

// CanonicalPullRequestUpdateRequestDigest is the durable request binding used
// by UpdatePullRequest.
func CanonicalPullRequestUpdateRequestDigest(identity contracts.PullRequestIdentity, title, body string) string {
	return requestDigest("pr_edit", identity, title, body)
}

func (c Client) Preflight(ctx context.Context, repository contracts.RepositoryIdentity) (Principal, error) {
	if err := validRepository(repository); err != nil {
		return Principal{}, err
	}
	var auth struct {
		Hosts map[string][]authHost `json:"hosts"`
	}
	if err := c.json(ctx, &auth, "auth", "status", "--json", "hosts"); err != nil {
		return Principal{}, err
	}
	login, err := activeLogin(auth.Hosts)
	if err != nil {
		return Principal{}, ErrMalformedResponse
	}
	var repo struct {
		NameWithOwner string `json:"nameWithOwner"`
		URL           string `json:"url"`
	}
	if err := c.json(ctx, &repo, "repo", "view", "--repo", repoArg(repository), "--json", "nameWithOwner,url"); err != nil {
		return Principal{}, err
	}
	if repo.NameWithOwner != repository.Owner+"/"+repository.Name || repo.URL == "" {
		return Principal{}, fmt.Errorf("%w: repository preflight mismatch", ErrPolicyRefusal)
	}
	return Principal{Login: login}, nil
}

func activeLogin(hosts map[string][]authHost) (string, error) {
	active := ""
	for _, host := range hosts["github.com"] {
		if host.Active && host.Login != "" && (host.Host == "" || host.Host == "github.com") && (host.State == "success" || host.State == "active") {
			if active != "" {
				return "", ErrMalformedResponse
			}
			active = host.Login
		}
	}
	if active == "" {
		return "", ErrMalformedResponse
	}
	return active, nil
}

// Observe returns exactly one marker-bearing factory PR, or a typed zero/many
// result. Head owner/repository/ref/OID/base are all compared; a branch name
// alone can never be adopted.
func (c Client) Observe(ctx context.Context, want contracts.PullRequestIdentity) (PRMatch, error) {
	return c.observeFactoryPullRequest(ctx, want)
}

// observeFactoryPullRequest retains the generic marker-bearing lookup
// contract. In particular, callers that only need descriptive lookup may not
// have a protected-base OID. Publication creation/adoption must instead call
// ObservePublicationCandidate below, which is deliberately stricter.
func (c Client) observeFactoryPullRequest(ctx context.Context, want contracts.PullRequestIdentity) (PRMatch, error) {
	if !validIdentity(want) {
		return PRMatch{}, ErrPolicyRefusal
	}
	var values []prWire
	if err := c.json(ctx, &values, "pr", "list", "--repo", repoArg(want.Repository), "--state", "all", "--limit", "100", "--json", prFields); err != nil {
		return PRMatch{}, err
	}
	if len(values) == 100 {
		return PRMatch{}, ErrAmbiguousPR
	}
	var matches []PRMatch
	for _, value := range values {
		candidate, err := value.identity(want.Repository)
		if errors.Is(err, ErrNoMatchingPR) {
			continue
		}
		if err != nil {
			return PRMatch{}, err
		}
		if sameExact(candidate, want) {
			matches = append(matches, value.match(candidate))
		}
	}
	switch len(matches) {
	case 0:
		return PRMatch{}, ErrNoMatchingPR
	case 1:
		return matches[0], nil
	default:
		return PRMatch{}, ErrAmbiguousPR
	}
}

// ObservePublicationCandidate inventories the bounded pull-request list for
// the exact source and base selected for publication. It deliberately sees
// unmarked PRs as well: an open PR from this exact source branch is never
// semantic absence, even when it targets a different base branch.
// It returns found=false only when no open matching PR exists.
func (c Client) ObservePublicationCandidate(ctx context.Context, want contracts.PullRequestIdentity) (PRMatch, bool, error) {
	if !validPublicationIdentity(want) {
		return PRMatch{}, false, ErrPolicyRefusal
	}
	var values []prWire
	if err := c.json(ctx, &values, "pr", "list", "--repo", repoArg(want.Repository), "--state", "all", "--limit", "100", "--json", prFields); err != nil {
		return PRMatch{}, false, err
	}
	if len(values) == 100 {
		return PRMatch{}, false, ErrAmbiguousPR
	}
	var matches []PRMatch
	for _, value := range values {
		candidate, err := value.identityUnmarked(want.Repository)
		if err != nil {
			return PRMatch{}, false, err
		}
		// Closed/merged rows are historical. For each open exact source branch,
		// classify BaseRef before HeadOID so branch reuse against a sibling base
		// cannot be reinterpreted as absence.
		if !samePRSource(candidate, want) || value.State != "OPEN" || value.MergedAt != nil {
			continue
		}
		if candidate.BaseRef != want.BaseRef {
			return PRMatch{}, false, ErrPolicyRefusal
		}
		if !validOID(candidate.BaseOID) || candidate.BaseOID != want.BaseOID || candidate.HeadOID != want.HeadOID {
			return PRMatch{}, false, ErrPolicyRefusal
		}
		if !strings.Contains(value.Body, ownershipMarker(candidate)) || !sameExact(candidate, want) {
			return PRMatch{}, false, ErrPolicyRefusal
		}
		matches = append(matches, value.match(candidate))
	}
	switch len(matches) {
	case 0:
		return PRMatch{}, false, nil
	case 1:
		return matches[0], true, nil
	default:
		return PRMatch{}, false, ErrAmbiguousPR
	}
}

// ObservePublishedPullRequest is the all-state observation boundary used
// after publication. It is intentionally separate from
// ObservePublicationCandidate, whose open-draft semantics skip merged and
// closed rows. The durable PR number and source identity are exact, while
// BaseOID is deliberately not compared because GitHub may advance the base
// ref as part of merging the PR.
func (c Client) ObservePublishedPullRequest(ctx context.Context, want contracts.PullRequestIdentity) (PRMatch, error) {
	if !validPersistedPRIdentity(want) {
		return PRMatch{}, ErrPolicyRefusal
	}
	var values []prWire
	if err := c.json(ctx, &values, "pr", "list", "--repo", repoArg(want.Repository), "--state", "all", "--limit", "100", "--json", prFields); err != nil {
		return PRMatch{}, err
	}
	if len(values) == 100 {
		return PRMatch{}, ErrAmbiguousPR
	}
	var found *PRMatch
	for _, value := range values {
		candidate, err := value.identityUnmarked(want.Repository)
		if err != nil {
			return PRMatch{}, err
		}
		if !samePublishedSource(candidate, want) {
			continue
		}
		// A second PR for the exact durable source is not safe to classify by
		// branch familiarity. Refuse it even when only one row has the durable
		// number.
		if candidate.Number != want.Number {
			return PRMatch{}, ErrAmbiguousPR
		}
		if !strings.Contains(value.Body, ownershipMarker(candidate)) {
			return PRMatch{}, ErrNoMatchingPR
		}
		if value.State != "OPEN" && value.State != "CLOSED" && value.State != "MERGED" {
			return PRMatch{}, ErrMalformedResponse
		}
		if !validOID(candidate.BaseOID) {
			return PRMatch{}, ErrMalformedResponse
		}
		mergedByTimestamp := value.MergedAt != nil
		mergedByState := value.State == "MERGED"
		if mergedByTimestamp != mergedByState {
			// GitHub's all-state witness must agree on both independent merge
			// signals. Treating a contradictory row as merged could allow a
			// closed, unmerged PR to bypass the durable BaseOID binding.
			return PRMatch{}, ErrMalformedResponse
		}
		if value.MergedAt != nil {
			if _, err := time.Parse(time.RFC3339, *value.MergedAt); err != nil {
				return PRMatch{}, ErrMalformedResponse
			}
		}
		if mergedByState && (value.Draft || value.MergeCommit == nil || !validOID(value.MergeCommit.OID)) {
			// A merged PR cannot remain a draft, and merge completion must carry
			// a concrete commit witness. Do not downgrade an impossible row to an
			// unmerged result during cancellation.
			return PRMatch{}, ErrMalformedResponse
		}
		if !mergedByState && (!validOID(candidate.BaseOID) || candidate.BaseOID != want.BaseOID) {
			// An open or closed-unmerged PR is still bound to the exact base
			// witness that was published. Base movement is permitted only once
			// GitHub has independently marked this exact PR merged.
			return PRMatch{}, ErrNoMatchingPR
		}
		observed := value.match(candidate)
		observed.Merged = mergedByState
		if found != nil {
			return PRMatch{}, ErrAmbiguousPR
		}
		found = &observed
	}
	if found == nil {
		return PRMatch{}, ErrNoMatchingPR
	}
	return *found, nil
}

// observeClosedPublicationConflict is intentionally separate from
// ObservePublicationCandidate: closed rows are historical for inventory, but
// a closed PR with the exact source/head is a deterministic create conflict
// when the create command itself has failed. It lets CreateDraft preserve the
// operation-facing refusal without treating an arbitrary gh error as safe to
// retry.
func (c Client) observeClosedPublicationConflict(ctx context.Context, want contracts.PullRequestIdentity) (bool, error) {
	var values []prWire
	if err := c.json(ctx, &values, "pr", "list", "--repo", repoArg(want.Repository), "--state", "all", "--limit", "100", "--json", prFields); err != nil {
		return false, err
	}
	if len(values) == 100 {
		return false, ErrAmbiguousPR
	}
	for _, value := range values {
		candidate, err := value.identityUnmarked(want.Repository)
		if err != nil {
			return false, err
		}
		if candidate.HeadOwner == want.HeadOwner && candidate.HeadRepository == want.HeadRepository && candidate.HeadRef == want.HeadRef && candidate.HeadOID == want.HeadOID && candidate.BaseRef == want.BaseRef && (value.State != "OPEN" || value.MergedAt != nil) {
			return true, nil
		}
	}
	return false, nil
}

func (c Client) WaitChecks(ctx context.Context, identity contracts.PullRequestIdentity, required []CheckIdentity, initial, maximum time.Duration) ([]contracts.RequiredCheck, error) {
	ctx, cancel := boundedGHContext(ctx)
	defer cancel()
	if initial <= 0 {
		initial = 50 * time.Millisecond
	}
	if maximum < initial {
		maximum = initial
	}
	delay := initial
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrChecksPending, err)
		}
		checks, err := c.checks(ctx, identity)
		if err != nil && !errors.Is(err, ErrChecksPending) {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("%w: %v", ErrChecksPending, ctx.Err())
			}
			return nil, err
		}
		status := err
		if status == nil {
			status = evaluateChecks(checks, required)
		}
		if status == nil {
			return checks, nil
		}
		if errors.Is(status, ErrChecksFailed) {
			return nil, status
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("%w: %v", ErrChecksPending, ctx.Err())
		case <-timer.C:
		}
		if delay < maximum {
			delay *= 2
			if delay > maximum {
				delay = maximum
			}
		}
	}
}

func (c Client) checks(ctx context.Context, identity contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
	if !validIdentity(identity) {
		return nil, ErrPolicyRefusal
	}
	before, err := c.Observe(ctx, identity)
	if err != nil {
		return nil, ErrChecksFailed
	}
	if !sameExact(before.Identity, identity) || before.State != "OPEN" || before.Merged {
		return nil, ErrChecksFailed
	}
	// Use the observed server-side number for the checks request.  A caller
	// may intentionally omit Number while binding the exact source identity.
	identity = before.Identity
	var wire []checkWire
	// Ask GitHub for the server-defined required set. Without --required, an
	// optional successful check could make a caller-provided subset appear
	// authoritative while a required workflow has not even created a run yet.
	if err := c.json(ctx, &wire, "pr", "checks", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository), "--required", "--json", "name,state,workflow,link,bucket"); err != nil {
		return nil, err
	}
	after, err := c.Observe(ctx, identity)
	if err != nil || !sameExact(after.Identity, identity) || after.State != "OPEN" || after.Merged {
		return nil, ErrChecksFailed
	}
	if len(wire) == 0 {
		return nil, ErrChecksFailed
	}
	checks := make([]contracts.RequiredCheck, 0, len(wire))
	for _, check := range wire {
		if !validCheck(check.Name, check.Link, check.Workflow, check.Bucket) {
			return nil, ErrMalformedResponse
		}
		identity := check.Link
		if check.Workflow != "" || check.Bucket != "" {
			identity = check.Workflow + "\x00" + check.Link + "\x00" + check.Bucket
		}
		checks = append(checks, contracts.RequiredCheck{Name: check.Name, State: check.State, ExternalID: identity})
	}
	return checks, nil
}

type checkWire struct {
	Name     string `json:"name"`
	State    string `json:"state"`
	Workflow string `json:"workflow"`
	Link     string `json:"link"`
	Bucket   string `json:"bucket"`
}

func evaluateChecks(actual []contracts.RequiredCheck, required []CheckIdentity) error {
	seen := map[string]bool{}
	pending := false
	for _, check := range actual {
		key := check.Name + "\x00" + check.ExternalID
		if seen[key] {
			return ErrChecksFailed
		}
		seen[key] = true
		if check.State != "SUCCESS" && check.State != "PENDING" && check.State != "QUEUED" && check.State != "IN_PROGRESS" {
			return ErrChecksFailed
		}
		if check.State == "PENDING" || check.State == "QUEUED" || check.State == "IN_PROGRESS" {
			pending = true
		}
	}
	for _, check := range required {
		if !seen[check.Name+"\x00"+check.ExternalID] {
			return ErrChecksFailed
		}
	}
	if pending {
		return ErrChecksPending
	}
	return nil
}

// exactBaseBinding rejects the subtle but dangerous case where local Git and
// GitHub each agree with themselves while naming different protected bases.
func exactBaseBinding(authorization domain.MergeAuthorization) (string, bool) {
	base := authorization.ReviewedBaseSHA
	return base, validOID(base) && authorization.CurrentBaseSHA == base && authorization.ReviewedBaseHeadOID == base && authorization.CurrentBaseHeadOID == base
}

type strictProtectionWitness struct {
	Kind               string
	ID                 string
	AdminEnforced      bool
	ActiveRulesetCount int
	ChecksDigest       string
	Checks             []string
}

type appliedBranchRule struct {
	Type              string         `json:"type"`
	RulesetSourceType string         `json:"ruleset_source_type"`
	RulesetSource     string         `json:"ruleset_source"`
	RulesetID         int64          `json:"ruleset_id"`
	Parameters        map[string]any `json:"parameters"`
}

func sameProtectionWitness(left, right strictProtectionWitness) bool {
	leftChecks, rightChecks := append([]string(nil), left.Checks...), append([]string(nil), right.Checks...)
	sort.Strings(leftChecks)
	sort.Strings(rightChecks)
	if left.Kind != right.Kind || left.ID != right.ID || left.AdminEnforced != right.AdminEnforced || left.ActiveRulesetCount != right.ActiveRulesetCount || left.ChecksDigest != right.ChecksDigest || len(leftChecks) != len(rightChecks) {
		return false
	}
	for index := range leftChecks {
		if leftChecks[index] != rightChecks[index] {
			return false
		}
	}
	return true
}

// strictProtection proves the server-side invariant relied on at merge time:
// GitHub must reject a PR that is not up to date with its protected base. The
// CLI carries only expected-head CAS, so absent this exact strict rule (or if
// any bypass allowance exists) sf refuses to mutate rather than treating a
// client-side base GET as atomic.
func (c Client) strictProtection(ctx context.Context, repository contracts.RepositoryIdentity, baseRef string, requestedMethod ...string) (strictProtectionWitness, error) {
	var response struct {
		Data *struct {
			Repository *struct {
				Ref *struct {
					BranchProtectionRule *struct {
						ID                          string `json:"id"`
						Pattern                     string `json:"pattern"`
						RequiresStrictStatusChecks  bool   `json:"requiresStrictStatusChecks"`
						IsAdminEnforced             bool   `json:"isAdminEnforced"`
						BypassPullRequestAllowances struct {
							TotalCount int `json:"totalCount"`
						} `json:"bypassPullRequestAllowances"`
						BypassForcePushAllowances struct {
							TotalCount int `json:"totalCount"`
						} `json:"bypassForcePushAllowances"`
					} `json:"branchProtectionRule"`
				} `json:"ref"`
			} `json:"repository"`
		} `json:"data"`
	}
	query := "query($owner:String!,$name:String!,$qualifiedRef:String!){repository(owner:$owner,name:$name){ref(qualifiedName:$qualifiedRef){branchProtectionRule{id pattern requiresStrictStatusChecks isAdminEnforced bypassPullRequestAllowances(first:1){totalCount} bypassForcePushAllowances(first:1){totalCount}}}}}"
	if err := c.json(ctx, &response, "api", "--hostname", "github.com", "graphql", "-f", "query="+query, "-F", "owner="+repository.Owner, "-F", "name="+repository.Name, "-F", "qualifiedRef=refs/heads/"+baseRef); err != nil {
		return strictProtectionWitness{}, err
	}
	var activeRules []appliedBranchRule
	endpoint := "repos/" + repoArg(repository) + "/rules/branches/" + url.PathEscape(baseRef) + "?per_page=100&page=1"
	if err := c.json(ctx, &activeRules, "api", "--hostname", "github.com", "--method", "GET", endpoint); err != nil || len(activeRules) >= 100 {
		return strictProtectionWitness{}, ErrGuardedMergeUnavailable
	}
	inventory, err := c.auditEvaluateRulesets(ctx, repository, baseRef)
	if err != nil {
		return strictProtectionWitness{}, err
	}
	// GitHub's exact-branch rules endpoint returns every active rule that
	// applies to this ref, including inherited organization/enterprise rules.
	// It avoids treating a lossy ruleset-list summary or wildcard matching as
	// authority. A direct rule mixed with a ruleset is ambiguous by design.
	rulesets := map[int64]appliedRulesetRef{}
	for _, rule := range activeRules {
		if rule.RulesetID == 0 {
			continue
		}
		if rule.Type == "" || !bounded(rule.RulesetSourceType, 64) || !bounded(rule.RulesetSource, 256) {
			return strictProtectionWitness{}, ErrGuardedMergeUnavailable
		}
		candidate := appliedRulesetRef{ID: rule.RulesetID, SourceType: rule.RulesetSourceType, Source: rule.RulesetSource}
		if existing, found := rulesets[rule.RulesetID]; found && existing != candidate {
			return strictProtectionWitness{}, ErrGuardedMergeUnavailable
		}
		rulesets[rule.RulesetID] = candidate
	}
	if len(rulesets) == 0 {
		if len(activeRules) == 0 && response.Data != nil && response.Data.Repository != nil && response.Data.Repository.Ref != nil && response.Data.Repository.Ref.BranchProtectionRule != nil {
			rule := response.Data.Repository.Ref.BranchProtectionRule
			if rule.Pattern == baseRef && rule.ID != "" && rule.RequiresStrictStatusChecks && rule.IsAdminEnforced && rule.BypassPullRequestAllowances.TotalCount == 0 && rule.BypassForcePushAllowances.TotalCount == 0 {
				return strictProtectionWitness{Kind: "classic", ID: rule.ID, AdminEnforced: true}, nil
			}
		}
		return strictProtectionWitness{}, ErrGuardedMergeUnavailable
	}
	hasClassicRule := response.Data != nil && response.Data.Repository != nil && response.Data.Repository.Ref != nil && response.Data.Repository.Ref.BranchProtectionRule != nil
	if hasClassicRule || len(rulesets) != 1 || len(activeRules) != lenRulesetRules(activeRules) {
		return strictProtectionWitness{}, ErrGuardedMergeUnavailable
	}
	var selected appliedRulesetRef
	for _, selected = range rulesets {
	}
	method := ""
	if len(requestedMethod) == 1 {
		method = requestedMethod[0]
	}
	summary, found := inventory.Summaries[selected.ID]
	if !found || summary.SourceType != selected.SourceType || summary.Source != selected.Source || (summary.Enforcement != "active" && summary.Enforcement != "enabled") {
		return strictProtectionWitness{}, ErrGuardedMergeUnavailable
	}
	detail, hasListed := inventory.Details[selected.ID]
	if summary.Enforcement == "enabled" && !hasListed {
		// An enabled list item is ambiguous until its detail proves active.
		return strictProtectionWitness{}, ErrGuardedMergeUnavailable
	}
	return c.rulesetProtection(ctx, repository, baseRef, method, selected, detail, hasListed)
}

func lenRulesetRules(rules []appliedBranchRule) int {
	count := 0
	for _, rule := range rules {
		if rule.RulesetID != 0 {
			count++
		}
	}
	return count
}

type rulesetRefCondition struct {
	Include []string `json:"include"`
	Exclude []string `json:"exclude"`
}

type rulesetNameCondition struct {
	Include   []string `json:"include"`
	Exclude   []string `json:"exclude"`
	Protected *bool    `json:"protected"`
}

type rulesetConditions struct {
	RefName          *rulesetRefCondition  `json:"ref_name"`
	RepositoryName   *rulesetNameCondition `json:"repository_name"`
	OrganizationName *rulesetNameCondition `json:"organization_name"`
}

type rulesetRule struct {
	Type       string         `json:"type"`
	Parameters map[string]any `json:"parameters"`
}

type rulesetLink struct {
	Href string `json:"href"`
}

type rulesetLinks struct {
	Self *rulesetLink `json:"self"`
	HTML *rulesetLink `json:"html"`
}

type rulesetWire struct {
	ID           int64              `json:"id"`
	Name         string             `json:"name"`
	Target       string             `json:"target"`
	Source       string             `json:"source"`
	SourceType   string             `json:"source_type"`
	Enforcement  string             `json:"enforcement"`
	Conditions   *rulesetConditions `json:"conditions"`
	Rules        []rulesetRule      `json:"rules"`
	BypassActors []json.RawMessage  `json:"bypass_actors"`
	NodeID       string             `json:"node_id"`
	Links        *rulesetLinks      `json:"_links"`
	CreatedAt    string             `json:"created_at"`
	UpdatedAt    string             `json:"updated_at"`
	// This is a separate, direct signal about the authenticated merge actor.
	// It must never be inferred from (or substituted for) bypass_actors.
	CurrentUserCanBypass string `json:"current_user_can_bypass"`
}

// rulesetSummaryWire is the documented list payload. Conditions, bypass
// actors, and rules exist only on the detail endpoint, so they must never be
// inferred from this summary.
type rulesetSummaryWire struct {
	ID          int64         `json:"id"`
	Name        string        `json:"name"`
	Target      string        `json:"target"`
	Source      string        `json:"source"`
	SourceType  string        `json:"source_type"`
	Enforcement string        `json:"enforcement"`
	NodeID      string        `json:"node_id"`
	Links       *rulesetLinks `json:"_links"`
	CreatedAt   string        `json:"created_at"`
	UpdatedAt   string        `json:"updated_at"`
}

type rulesetInventory struct {
	Summaries map[int64]rulesetSummaryWire
	Details   map[int64]rulesetWire
}

func (c Client) auditEvaluateRulesets(ctx context.Context, repository contracts.RepositoryIdentity, baseRef string) (rulesetInventory, error) {
	var listed []rulesetSummaryWire
	endpoint := "repos/" + repoArg(repository) + "/rulesets?includes_parents=true&targets=branch&per_page=100&page=1"
	if err := c.json(ctx, &listed, "api", "--hostname", "github.com", "--method", "GET", endpoint); err != nil || len(listed) >= 100 {
		return rulesetInventory{}, ErrGuardedMergeUnavailable
	}
	seen := make(map[int64]bool, len(listed))
	inventory := rulesetInventory{Summaries: make(map[int64]rulesetSummaryWire, len(listed)), Details: make(map[int64]rulesetWire)}
	for _, summary := range listed {
		if !validRulesetSummary(summary) || seen[summary.ID] {
			return rulesetInventory{}, ErrGuardedMergeUnavailable
		}
		seen[summary.ID] = true
		inventory.Summaries[summary.ID] = summary
		switch summary.Enforcement {
		case "disabled":
			continue
		case "active":
			// The exact-branch endpoint is the active authority; avoid a
			// potentially unbounded detail fan-out for unrelated active rules.
			continue
		case "enabled", "evaluate":
			var detail rulesetWire
			if err := c.json(ctx, &detail, "api", "--hostname", "github.com", "--method", "GET", "repos/"+repoArg(repository)+"/rulesets/"+fmt.Sprint(summary.ID)+"?includes_parents=true"); err != nil || !validRulesetMetadata(detail) || !sameRulesetSummary(summary, detail) {
				return rulesetInventory{}, ErrGuardedMergeUnavailable
			}
			// The list API spells active rules "enabled" in its documented
			// response. Detail is the authority for whether this summary is an
			// active policy (already covered by the exact-branch endpoint) or
			// an evaluate policy that needs conservative applicability auditing.
			switch detail.Enforcement {
			case "active":
				if summary.Enforcement == "evaluate" {
					return rulesetInventory{}, ErrGuardedMergeUnavailable
				}
				inventory.Details[summary.ID] = detail
			case "evaluate":
				if evaluateRulesetMayApply(detail, baseRef) {
					return rulesetInventory{}, ErrGuardedMergeUnavailable
				}
			default:
				return rulesetInventory{}, ErrGuardedMergeUnavailable
			}
		default:
			return rulesetInventory{}, ErrGuardedMergeUnavailable
		}
	}
	return inventory, nil
}

func validRulesetSummary(value rulesetSummaryWire) bool {
	if value.ID <= 0 || !bounded(value.Name, 256) || value.Target != "branch" || !bounded(value.Source, 256) || !validRulesetSourceType(value.SourceType) || !bounded(value.NodeID, 512) || value.Links == nil || value.Links.Self == nil || value.Links.HTML == nil || !bounded(value.Links.Self.Href, 4096) || !bounded(value.Links.HTML.Href, 4096) {
		return false
	}
	_, createdErr := time.Parse(time.RFC3339, value.CreatedAt)
	_, updatedErr := time.Parse(time.RFC3339, value.UpdatedAt)
	return createdErr == nil && updatedErr == nil
}

func validRulesetSourceType(value string) bool {
	return value == "Repository" || value == "Organization" || value == "Enterprise"
}

func sameRulesetSummary(summary rulesetSummaryWire, detail rulesetWire) bool {
	return summary.ID == detail.ID && summary.Name == detail.Name && summary.Target == detail.Target && summary.Source == detail.Source && summary.SourceType == detail.SourceType && summary.NodeID == detail.NodeID && summary.CreatedAt == detail.CreatedAt && summary.UpdatedAt == detail.UpdatedAt
}

// The list endpoint intentionally omits conditions. An evaluate policy is
// ignored only when its detail names exact different refs; every wildcard,
// special token, missing condition, or unknown pattern remains potentially
// applicable and therefore blocks a guarded merge.
func evaluateRulesetMayApply(detail rulesetWire, baseRef string) bool {
	if detail.Target != "branch" || detail.Conditions == nil || detail.Conditions.RefName == nil || len(detail.Conditions.RefName.Include) == 0 {
		return true
	}
	for _, exclude := range detail.Conditions.RefName.Exclude {
		if !validRulesetRefPattern(exclude) {
			return true
		}
	}
	wanted := "refs/heads/" + baseRef
	for _, include := range detail.Conditions.RefName.Include {
		if !validRulesetRefPattern(include) || include == wanted || strings.HasPrefix(include, "~") || strings.ContainsAny(include, "*?[") {
			return true
		}
	}
	return false
}

func validRulesetRefPattern(value string) bool {
	if value == "~ALL" || value == "~DEFAULT_BRANCH" {
		return true
	}
	if !strings.HasPrefix(value, "refs/heads/") {
		return false
	}
	ref := strings.TrimPrefix(value, "refs/heads/")
	if validRef(ref) {
		return true
	}
	// A GitHub ref glob is inherently potentially applicable. We only need to
	// distinguish a syntactically plausible glob from malformed wire data.
	return bounded(ref, 255) && strings.ContainsAny(ref, "*?[") && !strings.HasPrefix(ref, "/") && !strings.HasSuffix(ref, "/") && !strings.Contains(ref, "..") && !strings.ContainsAny(ref, " ~^:\\\r\n")
}

type appliedRulesetRef struct {
	ID                 int64
	SourceType, Source string
}

func (c Client) rulesetProtection(ctx context.Context, repository contracts.RepositoryIdentity, baseRef, method string, expected appliedRulesetRef, listed rulesetWire, hasListed bool) (strictProtectionWitness, error) {
	detail := listed
	if !hasListed {
		if err := c.json(ctx, &detail, "api", "--hostname", "github.com", "--method", "GET", "repos/"+repoArg(repository)+"/rulesets/"+fmt.Sprint(expected.ID)); err != nil {
			return strictProtectionWitness{}, ErrGuardedMergeUnavailable
		}
	}
	if detail.ID != expected.ID || !validRulesetMetadata(detail) {
		return strictProtectionWitness{}, ErrGuardedMergeUnavailable
	}
	if detail.Target != "branch" || detail.SourceType != expected.SourceType || detail.Source != expected.Source || detail.SourceType != "Repository" || detail.Source != repository.Owner+"/"+repository.Name || detail.Enforcement != "active" || detail.Conditions == nil || detail.Conditions.RefName == nil || len(detail.Conditions.RefName.Include) != 1 || detail.Conditions.RefName.Include[0] != "refs/heads/"+baseRef || len(detail.Conditions.RefName.Exclude) != 0 || detail.BypassActors == nil || len(detail.BypassActors) != 0 || detail.CurrentUserCanBypass != "never" {
		return strictProtectionWitness{}, ErrGuardedMergeUnavailable
	}
	var pullRules, statusRules, nonFastForwardRules, deletionRules int
	var checks []string
	for _, rule := range detail.Rules {
		switch rule.Type {
		case "pull_request":
			pullRules++
			if !validAllowedMergeMethods(rule.Parameters["allowed_merge_methods"], method) {
				return strictProtectionWitness{}, ErrGuardedMergeUnavailable
			}
		case "required_status_checks":
			statusRules++
			strict, ok := rule.Parameters["strict_required_status_checks_policy"].(bool)
			if !ok || !strict {
				return strictProtectionWitness{}, ErrGuardedMergeUnavailable
			}
			values, ok := rule.Parameters["required_status_checks"].([]any)
			if !ok || len(values) == 0 {
				return strictProtectionWitness{}, ErrGuardedMergeUnavailable
			}
			for _, value := range values {
				identity, ok := canonicalRulesetCheck(value)
				if !ok || containsCheck(checks, identity) {
					return strictProtectionWitness{}, ErrGuardedMergeUnavailable
				}
				checks = append(checks, identity)
			}
		case "non_fast_forward":
			nonFastForwardRules++
			if len(rule.Parameters) != 0 {
				return strictProtectionWitness{}, ErrGuardedMergeUnavailable
			}
		case "deletion":
			deletionRules++
			if len(rule.Parameters) != 0 {
				return strictProtectionWitness{}, ErrGuardedMergeUnavailable
			}
		default:
			// The accepted Nysa witness is intentionally only the four policy
			// types whose merge semantics are explicitly established here.
			return strictProtectionWitness{}, ErrGuardedMergeUnavailable
		}
	}
	if pullRules != 1 || statusRules != 1 || nonFastForwardRules != 1 || deletionRules != 1 || len(checks) == 0 {
		return strictProtectionWitness{}, ErrGuardedMergeUnavailable
	}
	sort.Strings(checks)
	digest := sha256.Sum256([]byte(strings.Join(checks, "\x00")))
	return strictProtectionWitness{Kind: "ruleset", ID: fmt.Sprint(detail.ID), AdminEnforced: true, ActiveRulesetCount: 1, Checks: checks, ChecksDigest: hex.EncodeToString(digest[:])}, nil
}

func validRulesetMetadata(value rulesetWire) bool {
	if value.ID <= 0 || !bounded(value.Name, 256) || !bounded(value.Source, 256) || !validRulesetSourceType(value.SourceType) || !bounded(value.NodeID, 512) || value.Links == nil || value.Links.Self == nil || value.Links.HTML == nil || !bounded(value.Links.Self.Href, 4096) || !bounded(value.Links.HTML.Href, 4096) {
		return false
	}
	_, createdErr := time.Parse(time.RFC3339, value.CreatedAt)
	_, updatedErr := time.Parse(time.RFC3339, value.UpdatedAt)
	return createdErr == nil && updatedErr == nil
}

func validAllowedMergeMethods(value any, wanted string) bool {
	if wanted != "merge" && wanted != "squash" && wanted != "rebase" {
		return false
	}
	values, ok := value.([]any)
	if !ok || len(values) == 0 {
		return false
	}
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		method, ok := value.(string)
		if !ok || (method != "merge" && method != "squash" && method != "rebase") || seen[method] {
			return false
		}
		seen[method] = true
	}
	return seen[wanted]
}

func canonicalRulesetCheck(value any) (string, bool) {
	entry, ok := value.(map[string]any)
	if !ok {
		return "", false
	}
	contextName, ok := entry["context"].(string)
	if !ok || !bounded(contextName, 1024) {
		return "", false
	}
	integration, found := entry["integration_id"]
	if !found {
		return contextName + "\x00-", true
	}
	id, ok := integration.(float64)
	if !ok || id <= 0 || id != float64(int64(id)) || id > float64(1<<53-1) {
		return "", false
	}
	return contextName + "\x00" + fmt.Sprint(int64(id)), true
}

func containsCheck(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (c Client) reconcileStrictMerge(ctx context.Context, identity contracts.PullRequestIdentity, headOID, originalBaseOID string) error {
	observed, err := c.viewNumber(ctx, identity.Repository, identity.Number)
	if err != nil {
		return err
	}
	if !observed.Merged || !sameMergeIdentity(observed.Identity, identity) || observed.Identity.HeadOID != headOID || observed.State != "MERGED" || observed.MergeCommit == "" || c.verifyProtectedBranch == nil {
		return ErrPolicyRefusal
	}
	proof, err := c.verifyProtectedBranch.VerifyProtectedBranch(ctx, identity.Repository, identity.BaseRef, observed.MergeCommit, originalBaseOID)
	if err != nil {
		return err
	}
	if proof.Repository != identity.Repository || proof.BaseRef != identity.BaseRef || proof.MergeCommit != observed.MergeCommit || proof.OriginalBaseOID != originalBaseOID || !validOID(proof.BaseHeadOID) || !proof.Contains {
		return ErrPolicyRefusal
	}
	return nil
}

// ObserveMergeIntent is the restart-safe recovery adapter. It reconstructs no
// authority from a digest: all repository, fence, head, base and protection
// facts came from the durable MergeIntent and are checked before confirmation.
func (c Client) ObserveMergeIntent(ctx context.Context, intent domain.MergeIntent) (string, error) {
	if intent.RepositoryHost != "github.com" || !intent.StrictStatusChecks || !intent.AdminEnforced || intent.ValidateProtectionWitness() != nil {
		return "", ErrPolicyRefusal
	}
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: intent.RepositoryHost, Owner: intent.RepositoryOwner, Name: intent.RepositoryName}, Number: intent.PullRequestNumber, HeadOID: intent.HeadOID, BaseRef: intent.BaseRef, FactoryOwned: true}
	observed, err := c.viewNumber(ctx, identity.Repository, identity.Number)
	if err != nil || !observed.Merged || observed.Identity.Repository != identity.Repository || observed.Identity.Number != identity.Number || observed.Identity.HeadOID != intent.HeadOID || observed.Identity.BaseRef != intent.BaseRef || !observed.Identity.FactoryOwned || observed.MergeCommit == "" {
		return "", ErrExternalMerged
	}
	if err := c.reconcileStrictMerge(ctx, observed.Identity, intent.HeadOID, intent.OriginalBaseOID); err != nil {
		return "", err
	}
	return identity.Repository.Owner + "/" + identity.Repository.Name + "@" + observed.MergeCommit, nil
}

// sameMergeIdentity intentionally excludes BaseOID. GitHub may report the
// current protected-ref tip for a merged PR, and that tip must advance on a
// successful merge. Original-base ancestry is instead proven by the Git
// verifier with the structured persisted witness.
func sameMergeIdentity(left, right contracts.PullRequestIdentity) bool {
	return left.Repository == right.Repository && left.Number == right.Number && left.HeadOwner == right.HeadOwner && left.HeadRepository == right.HeadRepository && left.HeadRef == right.HeadRef && left.HeadOID == right.HeadOID && left.BaseRef == right.BaseRef && left.FactoryOwned && right.FactoryOwned
}

type ApprovalBinding struct {
	ReviewedHead, CurrentHead string
	Invalidated               bool
}

func (a ApprovalBinding) Validate() error {
	if a.Invalidated || a.ReviewedHead == "" || a.ReviewedHead != a.CurrentHead {
		return ErrApprovalInvalid
	}
	return nil
}

// mergeQueued uses GraphQL because gh pr view/list --json do not expose the
// merge queue entry on supported gh releases. Any non-null entry fails closed.
func (c Client) mergeQueued(ctx context.Context, identity contracts.PullRequestIdentity) (bool, error) {
	if !validIdentity(identity) || identity.Number <= 0 {
		return false, ErrPolicyRefusal
	}
	var response struct {
		Data *struct {
			Repository *struct {
				PullRequest *struct {
					MergeQueueEntry json.RawMessage `json:"mergeQueueEntry"`
				} `json:"pullRequest"`
			} `json:"repository"`
		} `json:"data"`
	}
	query := "query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){mergeQueueEntry{position}}}}"
	if err := c.json(ctx, &response, "api", "--hostname", "github.com", "graphql", "-f", "query="+query, "-F", "owner="+identity.Repository.Owner, "-F", "name="+identity.Repository.Name, "-F", fmt.Sprintf("number=%d", identity.Number)); err != nil {
		return false, err
	}
	if response.Data == nil || response.Data.Repository == nil || response.Data.Repository.PullRequest == nil || len(response.Data.Repository.PullRequest.MergeQueueEntry) == 0 {
		return false, ErrMalformedResponse
	}
	entry := response.Data.Repository.PullRequest.MergeQueueEntry
	if string(entry) != "null" && !json.Valid(entry) {
		return false, ErrMalformedResponse
	}
	return presentJSON(entry), nil
}

func (c Client) view(ctx context.Context, identity contracts.PullRequestIdentity) (PRMatch, error) {
	var value prWire
	if err := c.json(ctx, &value, "pr", "view", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository), "--json", prFields); err != nil {
		return PRMatch{}, err
	}
	parsed, err := value.identity(identity.Repository)
	if err != nil {
		return PRMatch{}, err
	}
	return value.match(parsed), nil
}

func (c Client) viewNumber(ctx context.Context, repository contracts.RepositoryIdentity, number int) (PRMatch, error) {
	var value prWire
	if err := c.json(ctx, &value, "pr", "view", fmt.Sprint(number), "--repo", repoArg(repository), "--json", prFields); err != nil {
		return PRMatch{}, err
	}
	parsed, err := value.identity(repository)
	if err != nil {
		return PRMatch{}, err
	}
	return value.match(parsed), nil
}

const prFields = "number,title,body,headRepositoryOwner,headRepository,headRefName,headRefOid,baseRefName,baseRefOid,isDraft,mergedAt,mergeCommit,state,mergeStateStatus,autoMergeRequest"

type prWire struct {
	Number         int `json:"number"`
	HeadRepository struct {
		NameWithOwner string `json:"nameWithOwner"`
	} `json:"headRepository"`
	HeadRepositoryOwner struct {
		Login string `json:"login"`
	} `json:"headRepositoryOwner"`
	HeadRef     string  `json:"headRefName"`
	HeadOID     string  `json:"headRefOid"`
	BaseRef     string  `json:"baseRefName"`
	BaseOID     string  `json:"baseRefOid"`
	Draft       bool    `json:"isDraft"`
	Title       string  `json:"title"`
	Body        string  `json:"body"`
	MergedAt    *string `json:"mergedAt"`
	MergeCommit *struct {
		OID string `json:"oid"`
	} `json:"mergeCommit"`
	State            string          `json:"state"`
	MergeState       string          `json:"mergeStateStatus"`
	AutoMergeRequest json.RawMessage `json:"autoMergeRequest"`
}

func (p prWire) match(identity contracts.PullRequestIdentity) PRMatch {
	mergeCommit := ""
	if p.MergeCommit != nil && validOID(p.MergeCommit.OID) {
		mergeCommit = p.MergeCommit.OID
	}
	return PRMatch{Identity: identity, Draft: p.Draft, Merged: p.MergedAt != nil, Ready: !p.Draft, Title: p.Title, Body: p.Body, MergeCommit: mergeCommit, BaseHeadOID: p.BaseOID, State: p.State, MergeState: p.MergeState, AutoMerge: presentJSON(p.AutoMergeRequest)}
}

func presentJSON(value json.RawMessage) bool { return len(value) > 0 && string(value) != "null" }
func queueState(value string) bool           { return value == "QUEUED" || value == "ENQUEUED" }

func (p prWire) identity(repository contracts.RepositoryIdentity) (contracts.PullRequestIdentity, error) {
	identity, err := p.identityUnmarked(repository)
	if err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	if !strings.Contains(p.Body, ownershipMarker(identity)) {
		return contracts.PullRequestIdentity{}, ErrNoMatchingPR
	}
	return identity, nil
}

// identityUnmarked retains the strict wire validation used by Observe while
// letting RefreshFactoryPullRequestIdentity accept either adjacent durable
// marker during a head-only correction.
func (p prWire) identityUnmarked(repository contracts.RepositoryIdentity) (contracts.PullRequestIdentity, error) {
	owner, name, ok := strings.Cut(p.HeadRepository.NameWithOwner, "/")
	if !ok || !validRepositoryPart(owner) || !validRepositoryPart(name) || p.HeadRepositoryOwner.Login != owner || p.Number <= 0 || p.HeadRef == "" || !validOID(p.HeadOID) || !validRef(p.HeadRef) || !validRef(p.BaseRef) {
		return contracts.PullRequestIdentity{}, ErrMalformedResponse
	}
	return contracts.PullRequestIdentity{Repository: repository, Number: p.Number, HeadOwner: owner, HeadRepository: name, HeadRef: p.HeadRef, HeadOID: p.HeadOID, BaseRef: p.BaseRef, BaseOID: p.BaseOID, FactoryOwned: true}, nil
}
func sameExact(left, right contracts.PullRequestIdentity) bool {
	return left.Repository == right.Repository && left.HeadOwner == right.HeadOwner && left.HeadRepository == right.HeadRepository && left.HeadRef == right.HeadRef && left.HeadOID == right.HeadOID && left.BaseRef == right.BaseRef && (left.BaseOID == "" || right.BaseOID == "" || left.BaseOID == right.BaseOID) && left.FactoryOwned && right.FactoryOwned && (right.Number == 0 || left.Number == right.Number)
}
func adoptableDraft(match PRMatch) bool { return match.Draft && !match.Merged && match.State == "OPEN" }
func validRepository(value contracts.RepositoryIdentity) error {
	if value.Host != "github.com" || strings.Count(value.Owner, "/") != 0 || strings.Count(value.Name, "/") != 0 || !validRepositoryPart(value.Owner) || !validRepositoryPart(value.Name) {
		return ErrPolicyRefusal
	}
	return nil
}
func validCheck(name, link, workflow, bucket string) bool {
	return bounded(name, 256) && bounded(link, 2048) && (workflow == "" || bounded(workflow, 512)) && (bucket == "" || bounded(bucket, 256))
}
func validIdentity(value contracts.PullRequestIdentity) bool {
	return validRepository(value.Repository) == nil && validRepositoryPart(value.HeadOwner) && validRepositoryPart(value.HeadRepository) && validRef(value.HeadRef) && validOID(value.HeadOID) && validRef(value.BaseRef) && value.FactoryOwned
}
func validPersistedPRIdentity(value contracts.PullRequestIdentity) bool {
	return validIdentity(value) && value.Number > 0
}

// validPublicationIdentity is intentionally narrower than validIdentity:
// generic lookup callers may legitimately lack a base OID, but publication
// adoption must bind the PR to the exact protected-base witness.
func validPublicationIdentity(value contracts.PullRequestIdentity) bool {
	return validIdentity(value) && validOID(value.BaseOID)
}
func validPublicationPRIdentity(value contracts.PullRequestIdentity) bool {
	return validPublicationIdentity(value) && value.Number > 0
}
func sameRefreshSourceAndBase(left, right contracts.PullRequestIdentity) bool {
	return samePRSource(left, right) && left.BaseRef == right.BaseRef
}
func samePRSource(left, right contracts.PullRequestIdentity) bool {
	return left.Repository == right.Repository && left.HeadOwner == right.HeadOwner && left.HeadRepository == right.HeadRepository && left.HeadRef == right.HeadRef && left.FactoryOwned && right.FactoryOwned
}
func samePublishedSource(left, right contracts.PullRequestIdentity) bool {
	return samePRSource(left, right) && left.HeadOID == right.HeadOID && left.BaseRef == right.BaseRef
}
func sameRefreshContinuity(left, right contracts.PullRequestIdentity) bool {
	return left.Number == right.Number && sameRefreshSourceAndBase(left, right)
}
func refreshMarkerPresent(body string, prior, expected contracts.PullRequestIdentity) bool {
	return strings.Contains(body, ownershipMarker(prior)) || strings.Contains(body, ownershipMarker(expected))
}
func repoArg(value contracts.RepositoryIdentity) string { return value.Owner + "/" + value.Name }
func ownershipMarker(value contracts.PullRequestIdentity) string {
	return "<!-- sf:v1 repository=" + repoArg(value.Repository) + " head=" + value.HeadOwner + "/" + value.HeadRepository + ":" + value.HeadRef + " oid=" + value.HeadOID + " base=" + value.BaseRef + " -->"
}
func validOID(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, r := range value {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}
func validRef(value string) bool {
	return bounded(value, 255) && !strings.HasPrefix(value, "/") && !strings.HasSuffix(value, "/") && !strings.Contains(value, "..") && !strings.ContainsAny(value, " ~^:?*[\\\r\n")
}
func bounded(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value && !strings.ContainsRune(value, '\x00')
}
func validRepositoryPart(value string) bool {
	if !bounded(value, 100) {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}
func validTitle(value string) bool { return bounded(value, 256) }
func validBody(value string) bool {
	return len(value) <= 64<<10 && !strings.ContainsRune(value, '\x00')
}

func (c Client) json(ctx context.Context, destination any, args ...string) error {
	output, err := c.run(ctx, args...)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(io.LimitReader(strings.NewReader(string(output)), maxResponse+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("%w: %v", ErrMalformedResponse, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrMalformedResponse
	}
	return nil
}
func (c Client) run(ctx context.Context, args ...string) ([]byte, error) {
	return c.runWithHandoff(ctx, nil, args...)
}

// runWithHandoff flips handedOff immediately before runner invocation. This
// keeps environment/configuration/cleanup failures provably pre-mutation while
// treating a runner error as an uncertain post-handoff outcome.
func (c Client) runWithHandoff(ctx context.Context, handedOff *bool, args ...string) ([]byte, error) {
	if c.runMu != nil {
		c.runMu.Lock()
		defer c.runMu.Unlock()
	}
	if c.cleanupLatched != nil && c.cleanupLatched.Load() {
		return nil, ErrCleanupQuarantineFatal
	}
	if c.quarantiner == nil {
		return nil, ErrPolicyRefusal
	}
	quarantined, err := c.quarantiner.ExternalMutationsQuarantined(ctx)
	if err != nil || quarantined {
		return nil, ErrProcessCleanup
	}
	ctx, cancel := boundedGHContext(ctx)
	defer cancel()
	env, err := c.environment()
	if err != nil {
		return nil, err
	}
	if c.runner != nil {
		if handedOff != nil {
			*handedOff = true
		}
		output, runErr := c.runner.Run(ctx, c.binary(), args, env)
		if errors.Is(runErr, ErrRunnerBusy) {
			return nil, runErr
		}
		proof, cleanupErr := c.runner.Cleanup(ctx)
		if cleanupErr != nil || !proof.valid() || errors.Is(runErr, ErrProcessCleanup) {
			return nil, c.quarantineCleanup()
		}
		if len(output) > maxResponse {
			return nil, ErrResponseTooLarge
		}
		if runErr != nil {
			if isChecksPending(args, runErr) {
				return nil, ErrChecksPending
			}
			return nil, fmt.Errorf("gh command failed")
		}
		return output, nil
	}
	return nil, ErrPolicyRefusal
}

func (c Client) quarantineCleanup() error {
	if c.cleanupLatched != nil {
		c.cleanupLatched.Store(true)
	}
	if c.quarantiner == nil {
		return ErrCleanupQuarantineFatal
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.quarantiner.QuarantineExternalMutations(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrCleanupQuarantineFatal, err)
	}
	return ErrProcessCleanup
}

func isChecksPending(args []string, err error) bool {
	if len(args) < 2 || args[0] != "pr" || args[1] != "checks" {
		return false
	}
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == 8
}
func (c Client) binary() string {
	if c.binaryPath != "" {
		return c.binaryPath
	}
	return ""
}
func (c Client) environment() ([]string, error) {
	if c.home == "" || !filepath.IsAbs(c.home) || c.configDir == "" || !filepath.IsAbs(c.configDir) || c.binaryPath == "" || !filepath.IsAbs(c.binaryPath) {
		return nil, ErrPolicyRefusal
	}
	env := []string{"HOME=" + c.home, "GH_CONFIG_DIR=" + c.configDir, "GH_PROMPT_DISABLED=1", "GIT_TERMINAL_PROMPT=0", "NO_COLOR=1", "PATH=/usr/bin:/bin:/usr/sbin:/sbin"}
	for _, entry := range c.env {
		if !strings.HasPrefix(entry, "SF_FAKE_GH_STATE=") {
			return nil, ErrPolicyRefusal
		}
		env = append(env, entry)
	}
	return env, nil
}

func boundedGHContext(parent context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= maxGHDeadline {
		return parent, func() {}
	}
	return context.WithTimeout(parent, maxGHDeadline)
}

func runBounded(ctx context.Context, binary string, args, env []string) ([]byte, error) {
	// Setpgid lets us terminate the direct process group, but is not a
	// containment primitive: a child can call setsid and double-fork away.
	// Callers must inject a supervisor that proves drain or quarantine before
	// this result can cross a mutation boundary.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	command := exec.Command(binary, args...)
	command.Env = env
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdoutRead, stdoutWrite, pipeErr := os.Pipe()
	if pipeErr != nil {
		return nil, pipeErr
	}
	stderrRead, stderrWrite, pipeErr := os.Pipe()
	if pipeErr != nil {
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		return nil, pipeErr
	}
	command.Stdout = stdoutWrite
	command.Stderr = stderrWrite
	buffer := &boundedBuffer{limit: maxResponse}
	if err := ctx.Err(); err != nil {
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		_ = stderrRead.Close()
		_ = stderrWrite.Close()
		return nil, err
	}
	if err := command.Start(); err != nil {
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		_ = stderrRead.Close()
		_ = stderrWrite.Close()
		return nil, err
	}
	// The child owns the write ends. Keeping a parent write descriptor open
	// would mask escaped descendants and make the retained-pipe case harder to
	// distinguish during cleanup.
	_ = stdoutWrite.Close()
	_ = stderrWrite.Close()
	var streams sync.WaitGroup
	streams.Add(2)
	go func() { defer streams.Done(); _, _ = io.Copy(buffer, stdoutRead) }()
	go func() { defer streams.Done(); _, _ = io.Copy(buffer, stderrRead) }()
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	var err error
	streamsUncertain := false
	select {
	case err = <-done:
	case <-ctx.Done():
		// Kill the complete process group so a hung gh helper cannot outlive the
		// bounded handoff or keep lifecycle draining blocked.
		if command.Process != nil {
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		}
		select {
		case err = <-done:
		case <-time.After(250 * time.Millisecond):
			// A detached descendant may retain the pipes after the owner dies.
			// Close our read ends so Wait cannot hold the lifecycle gate forever.
			_ = stdoutRead.Close()
			_ = stderrRead.Close()
			select {
			case err = <-done:
			case <-time.After(250 * time.Millisecond):
				return buffer.snapshot(), ErrProcessCleanup
			}
		}
		if err == nil {
			err = ctx.Err()
		}
	}
	streamsDone := make(chan struct{})
	go func() { streams.Wait(); close(streamsDone) }()
	select {
	case <-streamsDone:
	case <-time.After(250 * time.Millisecond):
		streamsUncertain = true
		_ = stdoutRead.Close()
		_ = stderrRead.Close()
		select {
		case <-streamsDone:
		case <-time.After(250 * time.Millisecond):
			return buffer.snapshot(), ErrProcessCleanup
		}
	}
	if streamsUncertain {
		return buffer.snapshot(), ErrProcessCleanup
	}
	if buffer.exceeded {
		return buffer.snapshot(), ErrResponseTooLarge
	}
	return buffer.snapshot(), err
}

type boundedBuffer struct {
	mu       sync.Mutex
	data     []byte
	limit    int
	exceeded bool
}

func (b *boundedBuffer) Write(value []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.data)+len(value) > b.limit {
		remaining := b.limit - len(b.data)
		if remaining > 0 {
			b.data = append(b.data, value[:remaining]...)
		}
		b.exceeded = true
		return len(value), nil
	}
	b.data = append(b.data, value...)
	return len(value), nil
}

func (b *boundedBuffer) snapshot() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.data...)
}
