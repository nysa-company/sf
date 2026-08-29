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
	Binary        string
	Home          string
	ConfigDir     string   // explicit existing gh auth/config authority, never a temp substitute
	Env           []string // only SF_FAKE_GH_STATE is permitted for fake-gh tests.
	Run           func(context.Context, string, []string, []string) ([]byte, error)
	ValidateClaim func(context.Context, domain.ExternalEffectClaim) error
}

type Principal struct{ Login string }
type PRMatch struct {
	Identity             contracts.PullRequestIdentity
	Draft, Merged, Ready bool
	Title, Body          string
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
		Hosts map[string][]struct {
			Login    string `json:"login"`
			Active   bool   `json:"active"`
			State    string `json:"state"`
			Scopes   string `json:"scopes"`
			Protocol string `json:"protocol"`
		} `json:"hosts"`
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
	if err := c.validateClaim(ctx, durable, identity, "draft_pr", requestDigest("draft_pr", identity, title, body)); err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	plan := EffectPlan{SemanticKey: durable.SemanticKey, Identity: identity}
	match, err := c.createOrAdopt(ctx, EffectClaim{Plan: plan, Claimed: true}, title, body)
	return match.Identity, err
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
	return c.updateOrObserve(ctx, EffectClaim{Plan: EffectPlan{SemanticKey: durable.SemanticKey, Identity: identity}, Claimed: true}, observed, title, body)
}
func (c Client) RequiredChecks(ctx context.Context, identity contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
	return c.checks(ctx, identity)
}
func (c Client) MarkReady(ctx context.Context, durable domain.ExternalEffectClaim, identity contracts.PullRequestIdentity) error {
	if _, err := c.Observe(ctx, identity); err != nil {
		return err
	}
	if err := c.validateClaim(ctx, durable, identity, "pr_ready", requestDigest("pr_ready", identity)); err != nil {
		return err
	}
	_, err := c.run(ctx, "pr", "ready", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository))
	if err == nil {
		return nil
	}
	observed, observeErr := c.Observe(ctx, identity)
	if observeErr == nil && observed.Ready && !observed.Draft {
		return nil
	}
	return err
}

func (c Client) MergeExactHead(ctx context.Context, durable domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, headOID, method string, authorization domain.MergeAuthorization) error {
	observed, err := c.Observe(ctx, identity)
	if err != nil {
		return err
	}
	identity = observed.Identity
	if err := c.validateClaim(ctx, durable, identity, "merge", requestDigest("merge", identity, headOID, method)); err != nil {
		return err
	}
	if !authorization.Approved || !authorization.GatesGreen || authorization.ReviewedHead != headOID || authorization.CurrentHead != headOID {
		return ErrApprovalInvalid
	}
	_, err = c.merge(ctx, EffectClaim{Plan: EffectPlan{SemanticKey: durable.SemanticKey, Identity: identity}, Claimed: true}, PRMatch{Identity: identity}, headOID, domain.MergeGuarded, method)
	return err
}

func (c Client) validateClaim(ctx context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, requiredKind, digest string) error {
	if c.ValidateClaim == nil || claim.SemanticKey == "" || claim.Kind != requiredKind || claim.RequestDigest != digest || !validIdentity(identity) {
		return ErrPolicyRefusal
	}
	return c.ValidateClaim(ctx, claim)
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
		Hosts map[string][]struct {
			Login    string `json:"login"`
			Active   bool   `json:"active"`
			State    string `json:"state"`
			Scopes   string `json:"scopes"`
			Protocol string `json:"protocol"`
		} `json:"hosts"`
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

func activeLogin(hosts map[string][]struct {
	Login    string `json:"login"`
	Active   bool   `json:"active"`
	State    string `json:"state"`
	Scopes   string `json:"scopes"`
	Protocol string `json:"protocol"`
}) (string, error) {
	active := ""
	for _, host := range hosts["github.com"] {
		if host.Active && host.Login != "" && (host.State == "" || host.State == "active") {
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
			matches = append(matches, PRMatch{Identity: candidate, Draft: value.Draft, Merged: value.MergedAt != nil, Ready: !value.Draft, Title: value.Title, Body: value.Body})
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
	var wire []checkWire
	if err := c.json(ctx, &wire, "pr", "checks", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository), "--json", "name,state,workflow,link,bucket"); err != nil {
		return nil, err
	}
	checks := make([]contracts.RequiredCheck, 0, len(wire))
	for _, check := range wire {
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
	if err := c.json(ctx, &value, "pr", "view", fmt.Sprint(identity.Number), "--repo", repoArg(identity.Repository), "--json", prFields); err != nil {
		return PRMatch{}, err
	}
	parsed, err := value.identity(identity.Repository)
	if err != nil {
		return PRMatch{}, err
	}
	return PRMatch{Identity: parsed, Draft: value.Draft, Merged: value.MergedAt != nil, Ready: !value.Draft, Title: value.Title, Body: value.Body}, nil
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
	return PRMatch{Identity: parsed, Draft: value.Draft, Merged: value.MergedAt != nil, Ready: !value.Draft, Title: value.Title, Body: value.Body}, nil
}

const prFields = "number,title,body,headRepositoryOwner,headRepository,headRefName,headRefOid,baseRefName,isDraft,mergedAt,state"

type prWire struct {
	Number         int `json:"number"`
	HeadRepository struct {
		NameWithOwner string `json:"nameWithOwner"`
	} `json:"headRepository"`
	HeadRepositoryOwner struct {
		Login string `json:"login"`
	} `json:"headRepositoryOwner"`
	HeadRef  string  `json:"headRefName"`
	HeadOID  string  `json:"headRefOid"`
	BaseRef  string  `json:"baseRefName"`
	Draft    bool    `json:"isDraft"`
	Title    string  `json:"title"`
	Body     string  `json:"body"`
	MergedAt *string `json:"mergedAt"`
	State    string  `json:"state"`
}

func (p prWire) identity(repository contracts.RepositoryIdentity) (contracts.PullRequestIdentity, error) {
	owner, name, ok := strings.Cut(p.HeadRepository.NameWithOwner, "/")
	if !ok || owner == "" || name == "" || p.HeadRepositoryOwner.Login != owner || p.Number <= 0 || p.HeadRef == "" || !validOID(p.HeadOID) || !validRef(p.HeadRef) || !validRef(p.BaseRef) {
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
func validRepository(value contracts.RepositoryIdentity) error {
	if value.Host != "github.com" || !validRepositoryPart(value.Owner) || !validRepositoryPart(value.Name) {
		return ErrPolicyRefusal
	}
	return nil
}
func validIdentity(value contracts.PullRequestIdentity) bool {
	return validRepository(value.Repository) == nil && bounded(value.HeadOwner, 100) && bounded(value.HeadRepository, 100) && validRef(value.HeadRef) && validOID(value.HeadOID) && validRef(value.BaseRef) && value.FactoryOwned
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
			return nil, fmt.Errorf("gh command failed")
		}
		return output, nil
	}
	output, runErr := runBounded(ctx, c.binary(), args, env)
	if errors.Is(runErr, ErrResponseTooLarge) {
		return nil, runErr
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
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= 2*time.Minute {
		return parent, func() {}
	}
	return context.WithTimeout(parent, 2*time.Minute)
}

func runBounded(ctx context.Context, binary string, args, env []string) ([]byte, error) {
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = env
	buffer := &boundedBuffer{limit: maxResponse}
	command.Stdout, command.Stderr = buffer, buffer
	err := command.Run()
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
