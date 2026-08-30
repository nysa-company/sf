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
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
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
)

const maxResponse = 1 << 20

var maxGHDeadline = 2 * time.Minute

type Client struct {
	Binary        string
	Home          string
	ConfigDir     string   // explicit existing gh auth/config authority, never a temp substitute
	Env           []string // only SF_FAKE_GH_STATE is permitted for fake-gh tests.
	Run           func(context.Context, string, []string, []string) ([]byte, error)
	ValidateClaim func(context.Context, domain.ExternalEffectClaim) error
	MutationGuard contracts.ExternalMutationGuard
	// VerifyProtectedBranch is supplied by the Git boundary.  It is the
	// authority that freshly fetches the protected base ref and proves that the
	// reported merge commit is contained by that exact ref.
	VerifyProtectedBranch contracts.ProtectedBranchVerifier
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
	MergeQueued          bool
}
type EffectPlan struct {
	SemanticKey string
	Identity    contracts.PullRequestIdentity
}
type EffectClaim struct {
	Plan    EffectPlan
	Claimed bool
}
type CheckIdentity struct{ Name, ExternalID string }
type MergeOutcome string

const (
	MergeApplied  MergeOutcome = "merged"
	MergeExternal MergeOutcome = "external_merged"
)

var _ contracts.GitHub = (*Client)(nil)

// NewClient is the production wiring point. The orchestrator supplies the
// Store mutation handoff and the Git-boundary protected-branch verifier; a
// guarded merge client is never constructed without both authorities.
func NewClient(binary, home, configDir string, validate func(context.Context, domain.ExternalEffectClaim) error, guard contracts.ExternalMutationGuard, verifier contracts.ProtectedBranchVerifier) (*Client, error) {
	if binary == "" || !filepath.IsAbs(binary) || home == "" || !filepath.IsAbs(home) || configDir == "" || !filepath.IsAbs(configDir) || validate == nil || guard == nil || verifier == nil {
		return nil, ErrPolicyRefusal
	}
	return &Client{Binary: binary, Home: home, ConfigDir: configDir, ValidateClaim: validate, MutationGuard: guard, VerifyProtectedBranch: verifier}, nil
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
	_, runErr := c.mutate(ctx, durable, "pr", "create", "--repo", repoArg(identity.Repository), "--head", identity.HeadOwner+":"+identity.HeadRef, "--base", identity.BaseRef, "--draft", "--title", title, "--body", markedBody)
	// Both a delivered response and a lost response are reconciled by the same
	// exact ownership observation; command output is never object evidence.
	match, observeErr := c.Observe(ctx, identity)
	if observeErr == nil {
		return match.Identity, nil
	}
	if runErr != nil {
		return contracts.PullRequestIdentity{}, runErr
	}
	return contracts.PullRequestIdentity{}, observeErr
}
func (c Client) UpdatePullRequest(ctx context.Context, durable domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, title, body string) error {
	observed, err := c.Observe(ctx, identity)
	if err != nil {
		return err
	}
	identity = observed.Identity
	if err := c.validateClaim(ctx, durable, identity, "pr_update", requestDigest("pr_update", identity, title, body)); err != nil {
		return err
	}
	return c.updateWithClaim(ctx, durable, EffectClaim{Plan: EffectPlan{SemanticKey: durable.SemanticKey, Identity: identity}, Claimed: true}, observed, title, body)
}
func (c Client) RequiredChecks(ctx context.Context, identity contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
	return c.checks(ctx, identity)
}
func (c Client) MarkReady(ctx context.Context, durable domain.ExternalEffectClaim, identity contracts.PullRequestIdentity) error {
	observed, err := c.Observe(ctx, identity)
	if err != nil {
		return err
	}
	identity = observed.Identity
	if err := c.validateClaim(ctx, durable, identity, "pr_ready", requestDigest("pr_ready", identity)); err != nil {
		return err
	}
	_, err = c.mutateExact(ctx, durable, identity, "pr", "ready", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository))
	observed, observeErr := c.Observe(ctx, identity)
	if observeErr == nil && sameExact(observed.Identity, identity) && observed.Ready && !observed.Draft {
		return nil
	}
	if observeErr != nil {
		return ErrPolicyRefusal
	}
	if err == nil {
		return ErrPolicyRefusal
	}
	return err
}

func (c Client) MergeExactHead(ctx context.Context, durable domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, headOID, method string, authorization domain.MergeAuthorization) error {
	observed, err := c.Observe(ctx, identity)
	if err != nil {
		return err
	}
	if c.VerifyProtectedBranch == nil || observed.Draft || observed.Merged || observed.AutoMerge || observed.MergeQueued || queueState(observed.MergeState) || observed.State != "OPEN" {
		return ErrPolicyRefusal
	}
	identity = observed.Identity
	if err := c.validateClaim(ctx, durable, identity, "merge", requestDigest("merge", identity, headOID, method)); err != nil {
		return err
	}
	if !authorization.Approved || !authorization.GatesGreen || authorization.ReviewedHead != headOID || authorization.CurrentHead != headOID {
		return ErrApprovalInvalid
	}
	_, err = c.mergeWithClaim(ctx, durable, EffectClaim{Plan: EffectPlan{SemanticKey: durable.SemanticKey, Identity: identity}, Claimed: true}, observed, headOID, domain.MergeGuarded, method)
	return err
}

func (c Client) validateClaim(ctx context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, requiredKind, digest string) error {
	if c.ValidateClaim == nil || c.MutationGuard == nil || claim.SemanticKey == "" || claim.Kind != requiredKind || claim.RequestDigest != digest || !validIdentity(identity) {
		return ErrPolicyRefusal
	}
	return c.ValidateClaim(ctx, claim)
}

func (c Client) mutate(ctx context.Context, claim domain.ExternalEffectClaim, args ...string) ([]byte, error) {
	if c.MutationGuard == nil {
		return nil, ErrPolicyRefusal
	}
	return c.MutationGuard.RunExternalMutation(ctx, claim, func(runCtx context.Context) ([]byte, error) {
		return c.run(runCtx, args...)
	})
}

// mutateExact re-observes the exact factory identity inside the durable launch
// handoff. A PR number alone is never sufficient authorization to mutate.
func (c Client) mutateExact(ctx context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, args ...string) ([]byte, error) {
	if c.MutationGuard == nil {
		return nil, ErrPolicyRefusal
	}
	return c.MutationGuard.RunExternalMutation(ctx, claim, func(runCtx context.Context) ([]byte, error) {
		observed, err := c.view(runCtx, identity)
		if err != nil || !sameExact(observed.Identity, identity) {
			return nil, ErrPolicyRefusal
		}
		return c.run(runCtx, args...)
	})
}
func requestDigest(operation string, identity contracts.PullRequestIdentity, values ...string) string {
	input := operation + "\x00" + repoArg(identity.Repository) + "\x00" + identity.HeadOwner + "\x00" + identity.HeadRepository + "\x00" + identity.HeadRef + "\x00" + identity.HeadOID + "\x00" + identity.BaseRef
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

func (c Client) Plan(identity contracts.PullRequestIdentity, semanticKey string) (EffectPlan, error) {
	if semanticKey == "" || !validIdentity(identity) {
		return EffectPlan{}, fmt.Errorf("semantic key and complete factory PR identity are required")
	}
	return EffectPlan{SemanticKey: semanticKey, Identity: identity}, nil
}
func (c Client) Claim(plan EffectPlan) (EffectClaim, error) {
	if plan.SemanticKey == "" || !validIdentity(plan.Identity) {
		return EffectClaim{}, ErrPolicyRefusal
	}
	return EffectClaim{Plan: plan, Claimed: true}, nil
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

func (c Client) createOrAdopt(ctx context.Context, claim EffectClaim, title, body string) (PRMatch, error) {
	if !claim.Claimed || !validTitle(title) || !validBody(body) {
		return PRMatch{}, ErrPolicyRefusal
	}
	if match, err := c.Observe(ctx, claim.Plan.Identity); err == nil {
		if !adoptableDraft(match) {
			return PRMatch{}, ErrPolicyRefusal
		}
		return match, nil
	} else if !errors.Is(err, ErrNoMatchingPR) {
		return PRMatch{}, err
	}
	markedBody := body + "\n\n" + ownershipMarker(claim.Plan.Identity)
	_, err := c.run(ctx, "pr", "create", "--repo", repoArg(claim.Plan.Identity.Repository), "--head", claim.Plan.Identity.HeadOwner+":"+claim.Plan.Identity.HeadRef, "--base", claim.Plan.Identity.BaseRef, "--draft", "--title", title, "--body", markedBody)
	if err != nil {
		// A dropped response is indistinguishable from a network error. Read
		// first and adopt only the one exact, factory-marked remote object.
		return c.Observe(ctx, claim.Plan.Identity)
	}
	return c.Observe(ctx, claim.Plan.Identity)
}

func (c Client) updateOrObserve(ctx context.Context, claim EffectClaim, current PRMatch, title, body string) error {
	if !claim.Claimed || !sameExact(current.Identity, claim.Plan.Identity) || !validTitle(title) || !validBody(body) {
		return ErrPolicyRefusal
	}
	_, err := c.run(ctx, "pr", "edit", fmt.Sprint(current.Identity.Number), "--repo", repoArg(current.Identity.Repository), "--title", title, "--body", body+"\n\n"+ownershipMarker(current.Identity))
	if err == nil {
		return nil
	}
	markedBody := body + "\n\n" + ownershipMarker(current.Identity)
	observed, observeErr := c.Observe(ctx, current.Identity)
	if observeErr == nil && observed.Identity.Number == current.Identity.Number && observed.Title == title && observed.Body == markedBody {
		return nil
	}
	return err
}

func (c Client) updateWithClaim(ctx context.Context, durable domain.ExternalEffectClaim, claim EffectClaim, current PRMatch, title, body string) error {
	if !claim.Claimed || !sameExact(current.Identity, claim.Plan.Identity) || !validTitle(title) || !validBody(body) {
		return ErrPolicyRefusal
	}
	_, err := c.mutateExact(ctx, durable, current.Identity, "pr", "edit", fmt.Sprint(current.Identity.Number), "--repo", repoArg(current.Identity.Repository), "--title", title, "--body", body+"\n\n"+ownershipMarker(current.Identity))
	markedBody := body + "\n\n" + ownershipMarker(current.Identity)
	observed, observeErr := c.Observe(ctx, current.Identity)
	if observeErr == nil && sameExact(observed.Identity, current.Identity) && observed.Title == title && observed.Body == markedBody {
		return nil
	}
	if observeErr != nil {
		return ErrPolicyRefusal
	}
	if err == nil {
		return ErrPolicyRefusal
	}
	return err
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
	var wire []checkWire
	if err := c.json(ctx, &wire, "pr", "checks", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository), "--json", "name,state,workflow,link,bucket"); err != nil {
		return nil, err
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

func (c Client) merge(ctx context.Context, claim EffectClaim, pr PRMatch, reviewedHead string, mode domain.MergeMode, method string) (MergeOutcome, error) {
	return c.mergeRun(ctx, claim, pr, reviewedHead, mode, method, func(args ...string) ([]byte, error) { return c.run(ctx, args...) })
}

func (c Client) mergeWithClaim(ctx context.Context, durable domain.ExternalEffectClaim, claim EffectClaim, pr PRMatch, reviewedHead string, mode domain.MergeMode, method string) (MergeOutcome, error) {
	return c.mergeRun(ctx, claim, pr, reviewedHead, mode, method, func(args ...string) ([]byte, error) { return c.mutate(ctx, durable, args...) })
}

func (c Client) mergeRun(ctx context.Context, claim EffectClaim, pr PRMatch, reviewedHead string, mode domain.MergeMode, method string, mutate func(...string) ([]byte, error)) (MergeOutcome, error) {
	if mode == domain.MergeManual {
		return "", fmt.Errorf("%w: manual mode never mutates readiness or merge", ErrPolicyRefusal)
	}
	if mode == domain.MergeAutonomous {
		return "", fmt.Errorf("%w: autonomous merge is unavailable without a passing native profile", ErrPolicyRefusal)
	}
	if mode != domain.MergeGuarded || !claim.Claimed || pr.Draft || pr.Merged || pr.AutoMerge || pr.MergeQueued || queueState(pr.MergeState) || pr.State != "" && pr.State != "OPEN" || reviewedHead == "" || reviewedHead != pr.Identity.HeadOID || method != "merge" && method != "squash" && method != "rebase" {
		return "", ErrPolicyRefusal
	}
	args := []string{"pr", "merge", fmt.Sprint(pr.Identity.Number), "--repo", repoArg(pr.Identity.Repository), "--match-head-commit", reviewedHead, "--" + method}
	// A CLI exit status is never merge evidence.  In particular a dropped
	// response, a successful queue enrollment, and a successful auto-merge
	// request must all be distinguished by a fresh exact PR observation.
	_, _ = mutate(args...)
	observed, err := c.viewNumber(ctx, pr.Identity.Repository, pr.Identity.Number)
	if err != nil {
		return "", err
	}
	if observed.Merged {
		if !sameExact(observed.Identity, pr.Identity) || observed.Identity.HeadOID != reviewedHead {
			return MergeExternal, ErrExternalMerged
		}
		if observed.State != "MERGED" || observed.MergeCommit == "" || observed.Identity.BaseRef != pr.Identity.BaseRef || c.VerifyProtectedBranch == nil {
			return "", ErrPolicyRefusal
		}
		proof, verifyErr := c.VerifyProtectedBranch.VerifyProtectedBranch(ctx, pr.Identity.Repository, pr.Identity.BaseRef, observed.MergeCommit)
		if verifyErr != nil {
			return "", verifyErr
		}
		if proof.Repository != pr.Identity.Repository || proof.BaseRef != pr.Identity.BaseRef || proof.MergeCommit != observed.MergeCommit || !validOID(observed.BaseHeadOID) || proof.BaseHeadOID != observed.BaseHeadOID || !proof.Contains {
			return "", ErrPolicyRefusal
		}
		return MergeApplied, nil
	}
	return "", ErrPolicyRefusal
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

const prFields = "number,title,body,headRepositoryOwner,headRepository,headRefName,headRefOid,baseRefName,baseRefOid,isDraft,mergedAt,mergeCommit,state,mergeStateStatus,autoMergeRequest,mergeQueueEntry"

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
	MergeQueueEntry  json.RawMessage `json:"mergeQueueEntry"`
}

func (p prWire) match(identity contracts.PullRequestIdentity) PRMatch {
	mergeCommit := ""
	if p.MergeCommit != nil && validOID(p.MergeCommit.OID) {
		mergeCommit = p.MergeCommit.OID
	}
	return PRMatch{Identity: identity, Draft: p.Draft, Merged: p.MergedAt != nil, Ready: !p.Draft, Title: p.Title, Body: p.Body, MergeCommit: mergeCommit, BaseHeadOID: p.BaseOID, State: p.State, MergeState: p.MergeState, AutoMerge: presentJSON(p.AutoMergeRequest), MergeQueued: presentJSON(p.MergeQueueEntry)}
}

func presentJSON(value json.RawMessage) bool { return len(value) > 0 && string(value) != "null" }
func queueState(value string) bool           { return value == "QUEUED" || value == "ENQUEUED" }

func (p prWire) identity(repository contracts.RepositoryIdentity) (contracts.PullRequestIdentity, error) {
	owner, name, ok := strings.Cut(p.HeadRepository.NameWithOwner, "/")
	if !ok || !validRepositoryPart(owner) || !validRepositoryPart(name) || p.HeadRepositoryOwner.Login != owner || p.Number <= 0 || p.HeadRef == "" || !validOID(p.HeadOID) || !validRef(p.HeadRef) || !validRef(p.BaseRef) {
		return contracts.PullRequestIdentity{}, ErrMalformedResponse
	}
	identity := contracts.PullRequestIdentity{Repository: repository, Number: p.Number, HeadOwner: owner, HeadRepository: name, HeadRef: p.HeadRef, HeadOID: p.HeadOID, BaseRef: p.BaseRef, FactoryOwned: true}
	if !strings.Contains(p.Body, ownershipMarker(identity)) {
		return contracts.PullRequestIdentity{}, ErrNoMatchingPR
	}
	return identity, nil
}
func sameExact(left, right contracts.PullRequestIdentity) bool {
	return left.Repository == right.Repository && left.HeadOwner == right.HeadOwner && left.HeadRepository == right.HeadRepository && left.HeadRef == right.HeadRef && left.HeadOID == right.HeadOID && left.BaseRef == right.BaseRef && left.FactoryOwned && right.FactoryOwned && (right.Number == 0 || left.Number == right.Number)
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
	ctx, cancel := boundedGHContext(ctx)
	defer cancel()
	env, err := c.environment()
	if err != nil {
		return nil, err
	}
	if c.Run != nil {
		output, runErr := c.Run(ctx, c.binary(), args, env)
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
	output, runErr := runBounded(ctx, c.binary(), args, env)
	if errors.Is(runErr, ErrResponseTooLarge) {
		return nil, runErr
	}
	if runErr != nil {
		if isChecksPending(args, runErr) {
			return nil, ErrChecksPending
		}
		return nil, fmt.Errorf("gh command failed")
	}
	return output, nil
}

func isChecksPending(args []string, err error) bool {
	if len(args) < 2 || args[0] != "pr" || args[1] != "checks" {
		return false
	}
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == 8
}
func (c Client) binary() string {
	if c.Binary != "" {
		return c.Binary
	}
	return ""
}
func (c Client) environment() ([]string, error) {
	if c.Home == "" || !filepath.IsAbs(c.Home) || c.ConfigDir == "" || !filepath.IsAbs(c.ConfigDir) || c.Binary == "" || !filepath.IsAbs(c.Binary) {
		return nil, ErrPolicyRefusal
	}
	env := []string{"HOME=" + c.Home, "GH_CONFIG_DIR=" + c.ConfigDir, "GH_PROMPT_DISABLED=1", "GIT_TERMINAL_PROMPT=0", "NO_COLOR=1", "PATH=/usr/bin:/bin:/usr/sbin:/sbin"}
	for _, entry := range c.Env {
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
	command := exec.Command(binary, args...)
	command.Env = env
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	buffer := &boundedBuffer{limit: maxResponse}
	command.Stdout, command.Stderr = buffer, buffer
	if err := command.Start(); err != nil {
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		// Kill the complete process group so a hung gh helper cannot outlive the
		// bounded handoff or keep lifecycle draining blocked.
		if command.Process != nil {
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		}
		err = <-done
		if err == nil {
			err = ctx.Err()
		}
	}
	if buffer.exceeded {
		return buffer.data, ErrResponseTooLarge
	}
	return buffer.data, err
}

type boundedBuffer struct {
	data     []byte
	limit    int
	exceeded bool
}

func (b *boundedBuffer) Write(value []byte) (int, error) {
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
