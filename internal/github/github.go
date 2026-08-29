// Package github is the narrow, noninteractive gh CLI boundary. It contains
// no token handling and deliberately has no SQLite dependency: callers fence
// plan/claim/observe with the application effects table before invoking it.
package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

type Client struct {
	Binary string
	Home   string
	Env    []string // only SF_FAKE_GH_STATE is permitted for fake-gh tests.
	Run    func(context.Context, string, []string, []string) ([]byte, error)
}

type Principal struct{ Login string }
type PRMatch struct {
	Identity             contracts.PullRequestIdentity
	Draft, Merged, Ready bool
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

func (c Client) AuthStatus(ctx context.Context) error {
	var auth struct {
		Login string `json:"login"`
	}
	if err := c.json(ctx, &auth, "auth", "status", "--json", "login"); err != nil {
		return err
	}
	if auth.Login == "" {
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
func (c Client) CreateDraftPullRequest(ctx context.Context, identity contracts.PullRequestIdentity, title, body, semanticKey string) (contracts.PullRequestIdentity, error) {
	if semanticKey == "" {
		semanticKey = "github-pr:" + identity.HeadOID
	}
	plan, err := c.Plan(identity, semanticKey)
	if err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	claim, err := c.Claim(plan)
	if err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	match, err := c.CreateOrAdopt(ctx, claim, title, body)
	return match.Identity, err
}
func (c Client) UpdatePullRequest(ctx context.Context, identity contracts.PullRequestIdentity, title, body string) error {
	plan, err := c.Plan(identity, "github-pr-update:"+identity.HeadOID)
	if err != nil {
		return err
	}
	claim, err := c.Claim(plan)
	if err != nil {
		return err
	}
	return c.UpdateOrObserve(ctx, claim, PRMatch{Identity: identity}, title, body)
}
func (c Client) RequiredChecks(ctx context.Context, identity contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
	return c.checks(ctx, identity)
}
func (c Client) MarkReady(ctx context.Context, identity contracts.PullRequestIdentity, _ domain.Fence) error {
	_, err := c.run(ctx, "pr", "ready", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository), "--head", identity.HeadOwner+":"+identity.HeadRef, "--base", identity.BaseRef, "--head-oid", identity.HeadOID, "--sf-owned", "true")
	if err == nil {
		return nil
	}
	_, observeErr := c.Observe(ctx, identity)
	if observeErr == nil {
		return nil
	}
	return err
}
func (c Client) MergeExactHead(ctx context.Context, identity contracts.PullRequestIdentity, headOID, method string, _ domain.Fence) error {
	plan, err := c.Plan(identity, "github-merge:"+headOID)
	if err != nil {
		return err
	}
	claim, err := c.Claim(plan)
	if err != nil {
		return err
	}
	_, err = c.Merge(ctx, claim, PRMatch{Identity: identity}, headOID, domain.MergeGuarded, method)
	return err
}

func (c Client) Preflight(ctx context.Context, repository contracts.RepositoryIdentity) (Principal, error) {
	if err := validRepository(repository); err != nil {
		return Principal{}, err
	}
	var auth struct {
		Login string `json:"login"`
	}
	if err := c.json(ctx, &auth, "auth", "status", "--json", "login"); err != nil {
		return Principal{}, err
	}
	if auth.Login == "" {
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
	return Principal{Login: auth.Login}, nil
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
	if err := c.json(ctx, &values, "pr", "list", "--repo", repoArg(want.Repository), "--state", "all", "--json", prFields); err != nil {
		return PRMatch{}, err
	}
	var matches []PRMatch
	for _, value := range values {
		candidate, err := value.identity(want.Repository)
		if err != nil {
			return PRMatch{}, err
		}
		if sameExact(candidate, want) {
			matches = append(matches, PRMatch{Identity: candidate, Draft: value.Draft, Merged: value.Merged, Ready: value.Ready})
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

func (c Client) CreateOrAdopt(ctx context.Context, claim EffectClaim, title, body string) (PRMatch, error) {
	if !claim.Claimed {
		return PRMatch{}, ErrPolicyRefusal
	}
	if match, err := c.Observe(ctx, claim.Plan.Identity); err == nil {
		return match, nil
	} else if !errors.Is(err, ErrNoMatchingPR) {
		return PRMatch{}, err
	}
	var created prWire
	err := c.json(ctx, &created, "pr", "create", "--repo", repoArg(claim.Plan.Identity.Repository), "--head", claim.Plan.Identity.HeadOwner+":"+claim.Plan.Identity.HeadRef, "--base", claim.Plan.Identity.BaseRef, "--head-oid", claim.Plan.Identity.HeadOID, "--sf-owned", "true", "--draft", "--title", title, "--body", body, "--json", prFields)
	if err != nil {
		// A dropped response is indistinguishable from a network error. Read
		// first and adopt only the one exact, factory-marked remote object.
		return c.Observe(ctx, claim.Plan.Identity)
	}
	identity, err := created.identity(claim.Plan.Identity.Repository)
	if err != nil || !sameExact(identity, claim.Plan.Identity) {
		return PRMatch{}, fmt.Errorf("%w: create identity mismatch", ErrMalformedResponse)
	}
	return PRMatch{Identity: identity, Draft: created.Draft, Merged: created.Merged, Ready: created.Ready}, nil
}

func (c Client) UpdateOrObserve(ctx context.Context, claim EffectClaim, current PRMatch, title, body string) error {
	if !claim.Claimed || !sameExact(current.Identity, claim.Plan.Identity) {
		return ErrPolicyRefusal
	}
	_, err := c.run(ctx, "pr", "edit", fmt.Sprint(current.Identity.Number), "--repo", repoArg(current.Identity.Repository), "--title", title, "--body", body)
	if err == nil {
		return nil
	}
	_, observeErr := c.Observe(ctx, current.Identity)
	if observeErr == nil {
		return nil
	}
	return err
}

func (c Client) WaitChecks(ctx context.Context, identity contracts.PullRequestIdentity, required []CheckIdentity, initial, maximum time.Duration) ([]contracts.RequiredCheck, error) {
	if initial <= 0 {
		initial = 50 * time.Millisecond
	}
	if maximum < initial {
		maximum = initial
	}
	delay := initial
	for {
		checks, err := c.checks(ctx, identity)
		if err != nil {
			return nil, err
		}
		status := evaluateChecks(checks, required)
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
	var checks []contracts.RequiredCheck
	if err := c.json(ctx, &checks, "pr", "checks", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository), "--head", identity.HeadOwner+":"+identity.HeadRef, "--base", identity.BaseRef, "--head-oid", identity.HeadOID, "--sf-owned", "true", "--json", "name,externalId,state"); err != nil {
		return nil, err
	}
	return checks, nil
}

func evaluateChecks(actual []contracts.RequiredCheck, required []CheckIdentity) error {
	if len(actual) != len(required) {
		return ErrChecksFailed
	}
	seen := map[string]bool{}
	for _, check := range actual {
		key := check.Name + "\x00" + check.ExternalID
		if seen[key] {
			return ErrChecksFailed
		}
		seen[key] = true
		if check.State == "PENDING" || check.State == "QUEUED" || check.State == "IN_PROGRESS" {
			return ErrChecksPending
		}
		if check.State != "SUCCESS" {
			return ErrChecksFailed
		}
	}
	for _, check := range required {
		if !seen[check.Name+"\x00"+check.ExternalID] {
			return ErrChecksFailed
		}
	}
	return nil
}

func (c Client) Merge(ctx context.Context, claim EffectClaim, pr PRMatch, reviewedHead string, mode domain.MergeMode, method string) (MergeOutcome, error) {
	if mode == domain.MergeManual {
		return "", fmt.Errorf("%w: manual mode never mutates readiness or merge", ErrPolicyRefusal)
	}
	if mode == domain.MergeAutonomous {
		return "", fmt.Errorf("%w: autonomous merge is unavailable without a passing native profile", ErrPolicyRefusal)
	}
	if mode != domain.MergeGuarded || !claim.Claimed || reviewedHead == "" || reviewedHead != pr.Identity.HeadOID || method != "merge" && method != "squash" && method != "rebase" {
		return "", ErrPolicyRefusal
	}
	args := []string{"pr", "merge", fmt.Sprint(pr.Identity.Number), "--repo", repoArg(pr.Identity.Repository), "--match-head-commit", reviewedHead, "--" + method}
	if _, err := c.run(ctx, args...); err == nil {
		return MergeApplied, nil
	}
	observed, err := c.viewNumber(ctx, pr.Identity.Repository, pr.Identity.Number)
	if err != nil {
		return "", err
	}
	if observed.Merged && sameExact(observed.Identity, pr.Identity) && observed.Identity.HeadOID == reviewedHead {
		return MergeApplied, nil
	}
	if observed.Merged {
		return MergeExternal, ErrExternalMerged
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
	if err := c.json(ctx, &value, "pr", "view", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository), "--head", identity.HeadOwner+":"+identity.HeadRef, "--base", identity.BaseRef, "--head-oid", identity.HeadOID, "--sf-owned", "true", "--json", prFields); err != nil {
		return PRMatch{}, err
	}
	parsed, err := value.identity(identity.Repository)
	if err != nil {
		return PRMatch{}, err
	}
	return PRMatch{Identity: parsed, Draft: value.Draft, Merged: value.Merged, Ready: value.Ready}, nil
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
	return PRMatch{Identity: parsed, Draft: value.Draft, Merged: value.Merged, Ready: value.Ready}, nil
}

const prFields = "number,repository,headRepository,headRefName,headRefOid,baseRefName,isDraft,sfOwned,merged,ready"

type prWire struct {
	Number     int `json:"number"`
	Repository struct {
		NameWithOwner string `json:"nameWithOwner"`
	} `json:"repository"`
	HeadRepository struct {
		NameWithOwner string `json:"nameWithOwner"`
	} `json:"headRepository"`
	HeadRef      string `json:"headRefName"`
	HeadOID      string `json:"headRefOid"`
	BaseRef      string `json:"baseRefName"`
	Draft        bool   `json:"isDraft"`
	FactoryOwned bool   `json:"sfOwned"`
	Merged       bool   `json:"merged"`
	Ready        bool   `json:"ready"`
}

func (p prWire) identity(repository contracts.RepositoryIdentity) (contracts.PullRequestIdentity, error) {
	owner, name, ok := strings.Cut(p.HeadRepository.NameWithOwner, "/")
	if !ok || owner == "" || name == "" || !p.FactoryOwned || p.Number <= 0 || p.HeadRef == "" || p.HeadOID == "" || p.BaseRef == "" {
		return contracts.PullRequestIdentity{}, ErrMalformedResponse
	}
	return contracts.PullRequestIdentity{Repository: repository, Number: p.Number, HeadOwner: owner, HeadRepository: name, HeadRef: p.HeadRef, HeadOID: p.HeadOID, BaseRef: p.BaseRef, FactoryOwned: true}, nil
}
func sameExact(left, right contracts.PullRequestIdentity) bool {
	return left.Repository == right.Repository && left.HeadOwner == right.HeadOwner && left.HeadRepository == right.HeadRepository && left.HeadRef == right.HeadRef && left.HeadOID == right.HeadOID && left.BaseRef == right.BaseRef && left.FactoryOwned && right.FactoryOwned && (right.Number == 0 || left.Number == right.Number)
}
func validRepository(value contracts.RepositoryIdentity) error {
	if value.Host != "github.com" || value.Owner == "" || value.Name == "" {
		return ErrPolicyRefusal
	}
	return nil
}
func validIdentity(value contracts.PullRequestIdentity) bool {
	return validRepository(value.Repository) == nil && value.HeadOwner != "" && value.HeadRepository != "" && value.HeadRef != "" && value.HeadOID != "" && value.BaseRef != "" && value.FactoryOwned
}
func repoArg(value contracts.RepositoryIdentity) string { return value.Owner + "/" + value.Name }

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
			return nil, fmt.Errorf("gh command failed")
		}
		return output, nil
	}
	command := exec.CommandContext(ctx, c.binary(), args...)
	command.Env = env
	output, runErr := command.CombinedOutput()
	if len(output) > maxResponse {
		return nil, ErrResponseTooLarge
	}
	if runErr != nil {
		return nil, fmt.Errorf("gh command failed")
	}
	return output, nil
}
func (c Client) binary() string {
	if c.Binary != "" {
		return c.Binary
	}
	return "gh"
}
func (c Client) environment() ([]string, error) {
	home := c.Home
	if home == "" {
		home = filepath.Join(os.TempDir(), "sf-gh-home")
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return nil, err
	}
	env := []string{"HOME=" + home, "GH_CONFIG_DIR=" + filepath.Join(home, "gh"), "GH_PROMPT_DISABLED=1", "GIT_TERMINAL_PROMPT=0", "NO_COLOR=1"}
	for _, entry := range c.Env {
		if !strings.HasPrefix(entry, "SF_FAKE_GH_STATE=") {
			return nil, ErrPolicyRefusal
		}
		env = append(env, entry)
	}
	return env, nil
}
