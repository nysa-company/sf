// Package github is the narrow, noninteractive gh CLI boundary. It contains
// no token handling and deliberately has no SQLite dependency: callers fence
// plan/claim/observe with the application effects table before invoking it.
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
	ErrNoMatchingPR            = errors.New("no factory-owned pull request matches identity")
	ErrAmbiguousPR             = errors.New("multiple factory-owned pull requests match identity")
	ErrPolicyRefusal           = errors.New("github policy refused operation")
	ErrExternalMerged          = errors.New("pull request was merged outside the exact-head factory flow")
	ErrChecksPending           = errors.New("required checks remain pending")
	ErrChecksFailed            = errors.New("required checks failed or changed identity")
	ErrApprovalInvalid         = errors.New("approval is not bound to current reviewed head")
	ErrResponseTooLarge        = errors.New("github response exceeded bound")
	ErrMalformedResponse       = errors.New("github response is malformed")
	ErrCreateUncertain         = errors.New("github pull request creation is uncertain; reconcile before retrying")
	ErrGuardedMergeUnavailable = errors.New("sf-managed guarded merge is unavailable without server-enforced strict protected-base checks; observe a manual merge instead")
	ErrProcessCleanup          = contracts.ErrExternalCleanupUncertain
	ErrCleanupQuarantineFatal  = contracts.ErrExternalCleanupQuarantineFatal
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
	verifyProtectedBranch contracts.ProtectedBranchVerifier
	mergeIntents          contracts.MergeIntentRecorder
	quarantiner           contracts.ExternalMutationQuarantineAuthority
	cleanupLatched        *atomic.Bool
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
func NewClient(binary, home, configDir string, runner SupervisedCommandRunner, validate func(context.Context, domain.ExternalEffectClaim) error, guard contracts.ExternalMutationGuard, verifier contracts.ProtectedBranchVerifier, intents contracts.MergeIntentRecorder, quarantiner contracts.ExternalMutationQuarantineAuthority) (*Client, error) {
	if binary == "" || !filepath.IsAbs(binary) || home == "" || !filepath.IsAbs(home) || configDir == "" || !filepath.IsAbs(configDir) || runner == nil || validate == nil || guard == nil || verifier == nil || intents == nil || quarantiner == nil {
		return nil, ErrPolicyRefusal
	}
	return &Client{binaryPath: binary, home: home, configDir: configDir, runner: runner, validateClaimFn: validate, mutationGuard: guard, verifyProtectedBranch: verifier, mergeIntents: intents, quarantiner: quarantiner, cleanupLatched: &atomic.Bool{}}, nil
}

// NewStoreClient supplies SQLite's durable-effect, guard, and quarantine
// authority. The caller injects the protected-branch verifier used for
// reconciliation; it is never a substitute for a base CAS at launch.
func NewStoreClient(binary, home, configDir string, runner SupervisedCommandRunner, database *store.Store, verifier contracts.ProtectedBranchVerifier) (*Client, error) {
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
	if match, err := c.Observe(ctx, identity); err == nil {
		if !adoptableDraft(match) {
			return contracts.PullRequestIdentity{}, ErrPolicyRefusal
		}
		return match.Identity, nil
	} else if !errors.Is(err, ErrNoMatchingPR) {
		return contracts.PullRequestIdentity{}, err
	}
	// Validate after the exact absence observation and immediately before the
	// only mutation, so a stale effect cannot create a replacement PR.
	if err := c.validateClaim(ctx, durable, identity, "draft_pr", requestDigest("draft_pr", identity, title, body)); err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	markedBody := body + "\n\n" + ownershipMarker(identity)
	// Creation has no GitHub-side CAS.  The durable launch handoff therefore
	// performs one final exact absence/source observation while the mutation
	// gate is held; a pre-handoff list result is never sufficient.
	_, runErr := c.mutateCreateExact(ctx, durable, identity, "pr", "create", "--repo", repoArg(identity.Repository), "--head", identity.HeadOwner+":"+identity.HeadRef, "--base", identity.BaseRef, "--draft", "--title", title, "--body", markedBody)
	if errors.Is(runErr, ErrProcessCleanup) {
		return contracts.PullRequestIdentity{}, runErr
	}
	// Both a delivered response and a lost response are reconciled by the same
	// exact ownership observation; command output is never object evidence.
	match, observeErr := c.Observe(ctx, identity)
	if errors.Is(observeErr, ErrProcessCleanup) {
		return contracts.PullRequestIdentity{}, observeErr
	}
	if observeErr == nil && adoptableDraft(match) {
		return match.Identity, nil
	}
	if observeErr == nil {
		return contracts.PullRequestIdentity{}, ErrPolicyRefusal
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
	protection, err := c.strictProtection(ctx, identity.Repository, identity.BaseRef)
	if err != nil {
		return err
	}
	if err := c.mergeIntents.RecordMergeIntent(ctx, domain.MergeIntent{Ref: durable.Ref, SemanticKey: durable.SemanticKey, RequestDigest: durable.RequestDigest, TicketVersion: durable.TicketVersion, LeaderEpoch: durable.LeaderEpoch, RunnerEpoch: durable.RunnerEpoch, ClaimEpoch: durable.ClaimEpoch, RepositoryHost: identity.Repository.Host, RepositoryOwner: identity.Repository.Owner, RepositoryName: identity.Repository.Name, PullRequestNumber: identity.Number, HeadOID: headOID, BaseRef: identity.BaseRef, OriginalBaseOID: baseOID, ProtectionRuleID: protection.ID, StrictStatusChecks: true, AdminEnforced: protection.AdminEnforced, ActiveRulesetCount: uint32(protection.ActiveRulesetCount), Method: method}); err != nil {
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
		freshProtection, err := c.strictProtection(runCtx, identity.Repository, identity.BaseRef)
		if err != nil || freshProtection != protection {
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
	return c.mutationGuard.RunExternalMutation(ctx, claim, func(runCtx context.Context) ([]byte, error) {
		if err := c.observeSourceExact(runCtx, identity); err != nil {
			if errors.Is(err, ErrProcessCleanup) {
				return nil, ErrProcessCleanup
			}
			return nil, ErrPolicyRefusal
		}
		if _, err := c.Observe(runCtx, identity); !errors.Is(err, ErrNoMatchingPR) {
			if errors.Is(err, ErrProcessCleanup) {
				return nil, ErrProcessCleanup
			}
			return nil, ErrPolicyRefusal
		}
		return c.run(runCtx, args...)
	})
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
		if errors.Is(err, ErrProcessCleanup) {
			return ErrProcessCleanup
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
		if errors.Is(err, ErrProcessCleanup) {
			return ErrProcessCleanup
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
		if errors.Is(err, ErrProcessCleanup) {
			return nil, ErrProcessCleanup
		}
		if err != nil || !sameExact(observed.Identity, identity) || observed.State != "OPEN" || observed.Merged {
			return nil, ErrPolicyRefusal
		}
		return c.run(runCtx, args...)
	})
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
		if err != nil {
			if errors.Is(err, ErrNoMatchingPR) {
				continue
			}
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
	if err := c.json(ctx, &wire, "pr", "checks", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository), "--json", "name,state,workflow,link,bucket"); err != nil {
		return nil, err
	}
	after, err := c.Observe(ctx, identity)
	if err != nil || !sameExact(after.Identity, identity) || after.State != "OPEN" || after.Merged {
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
	ID                 string
	AdminEnforced      bool
	ActiveRulesetCount int
}

// strictProtection proves the server-side invariant relied on at merge time:
// GitHub must reject a PR that is not up to date with its protected base. The
// CLI carries only expected-head CAS, so absent this exact strict rule (or if
// any bypass allowance exists) sf refuses to mutate rather than treating a
// client-side base GET as atomic.
func (c Client) strictProtection(ctx context.Context, repository contracts.RepositoryIdentity, baseRef string) (strictProtectionWitness, error) {
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
	if response.Data == nil || response.Data.Repository == nil || response.Data.Repository.Ref == nil || response.Data.Repository.Ref.BranchProtectionRule == nil {
		return strictProtectionWitness{}, ErrGuardedMergeUnavailable
	}
	var activeRules []json.RawMessage
	endpoint := "repos/" + repoArg(repository) + "/rules/branches/" + url.PathEscape(baseRef) + "?per_page=1&page=1"
	if err := c.json(ctx, &activeRules, "api", "--hostname", "github.com", "--method", "GET", endpoint); err != nil || len(activeRules) != 0 {
		return strictProtectionWitness{}, ErrGuardedMergeUnavailable
	}
	rule := response.Data.Repository.Ref.BranchProtectionRule
	if rule.Pattern == baseRef && rule.ID != "" && rule.RequiresStrictStatusChecks && rule.IsAdminEnforced && rule.BypassPullRequestAllowances.TotalCount == 0 && rule.BypassForcePushAllowances.TotalCount == 0 {
		return strictProtectionWitness{ID: rule.ID, AdminEnforced: true, ActiveRulesetCount: 0}, nil
	}
	return strictProtectionWitness{}, ErrGuardedMergeUnavailable
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
	if intent.RepositoryHost != "github.com" || !intent.StrictStatusChecks || !intent.AdminEnforced || intent.ActiveRulesetCount != 0 {
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
	owner, name, ok := strings.Cut(p.HeadRepository.NameWithOwner, "/")
	if !ok || !validRepositoryPart(owner) || !validRepositoryPart(name) || p.HeadRepositoryOwner.Login != owner || p.Number <= 0 || p.HeadRef == "" || !validOID(p.HeadOID) || !validRef(p.HeadRef) || !validRef(p.BaseRef) {
		return contracts.PullRequestIdentity{}, ErrMalformedResponse
	}
	identity := contracts.PullRequestIdentity{Repository: repository, Number: p.Number, HeadOwner: owner, HeadRepository: name, HeadRef: p.HeadRef, HeadOID: p.HeadOID, BaseRef: p.BaseRef, BaseOID: p.BaseOID, FactoryOwned: true}
	if !strings.Contains(p.Body, ownershipMarker(identity)) {
		return contracts.PullRequestIdentity{}, ErrNoMatchingPR
	}
	return identity, nil
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
		output, runErr := c.runner.Run(ctx, c.binary(), args, env)
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
