package testkit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type ResponseMode string

const (
	ResponseDeliver       ResponseMode = "deliver"
	ResponseDropAfterCall ResponseMode = "drop_after_mutation"
	ResponseErrorBefore   ResponseMode = "error_before_mutation"
	ResponseErrorAfter    ResponseMode = "error_after_mutation"
)

// fakeCreatePolicyConflict marks a deterministic refusal found before the
// fake hands a create mutation to its response/mutation simulator. Keeping the
// original error as the message preserves the contract-test diagnostic while
// allowing CreateDraftPullRequest to distinguish it from a lost response.
type fakeCreatePolicyConflict struct{ err error }

func (e *fakeCreatePolicyConflict) Error() string { return e.err.Error() }
func (e *fakeCreatePolicyConflict) Unwrap() error { return e.err }
func (e *fakeCreatePolicyConflict) Is(target error) bool {
	return target == contracts.ErrDraftCreateBeforeStart
}

// PullRequest is the durable fake-remote record. Identity intentionally
// includes head repository/ref/OID and the sf ownership bit; headRefName alone
// is never enough to adopt or merge a PR.
type PullRequest struct {
	Identity    contracts.PullRequestIdentity `json:"identity"`
	Title       string                        `json:"title"`
	Body        string                        `json:"body"`
	Draft       bool                          `json:"draft"`
	Merged      bool                          `json:"merged"`
	Ready       bool                          `json:"ready"`
	MergeCommit string                        `json:"merge_commit,omitempty"`
}

type Mutation struct {
	Operation string `json:"operation"`
	Number    int    `json:"number,omitempty"`
	HeadOID   string `json:"head_oid,omitempty"`
}

// FakeRuleset mirrors the repository-ruleset payload consumed by the bounded
// GitHub adapter. Rule parameters remain generic so malformed wire shapes can
// be tested without making the fake more permissive than GitHub.
type FakeRuleset struct {
	ID                   int64                  `json:"id"`
	Name                 string                 `json:"name"`
	Target               string                 `json:"target"`
	Source               string                 `json:"source"`
	SourceType           string                 `json:"source_type"`
	Enforcement          string                 `json:"enforcement"`
	Conditions           *FakeRulesetConditions `json:"conditions"`
	Rules                []FakeRulesetRule      `json:"rules"`
	BypassActors         []any                  `json:"bypass_actors"`
	NodeID               string                 `json:"node_id"`
	Links                FakeRulesetLinks       `json:"_links"`
	CreatedAt            string                 `json:"created_at"`
	UpdatedAt            string                 `json:"updated_at"`
	CurrentUserCanBypass string                 `json:"current_user_can_bypass"`
}

type FakeRulesetLinks struct {
	Self FakeRulesetLink `json:"self"`
	HTML FakeRulesetLink `json:"html"`
}

type FakeRulesetLink struct {
	Href string `json:"href"`
}

type FakeRulesetConditions struct {
	RefName *FakeRulesetRefCondition `json:"ref_name"`
}

type FakeRulesetRefCondition struct {
	Include []string `json:"include"`
	Exclude []string `json:"exclude"`
}

type FakeRulesetRule struct {
	Type       string         `json:"type"`
	Parameters map[string]any `json:"parameters"`
}

// FakeGHState is JSON-serializable so a fake-gh subprocess can share it with
// an integration test. Mutations are recorded separately from response
// deliveries: a dropped response still leaves one applied remote mutation.
type FakeGHState struct {
	Schema        string                       `json:"schema"`
	Authenticated bool                         `json:"authenticated"`
	Repository    contracts.RepositoryIdentity `json:"repository"`
	// BaseHeadOID is the independently observable protected ref tip. Keeping
	// it durable lets boundary tests move main between observations without
	// pretending a PR's historical baseRefOid is the live protected ref.
	BaseHeadOID                 string                            `json:"base_head_oid"`
	BaseRef                     string                            `json:"base_ref"`
	StrictStatusChecks          bool                              `json:"strict_status_checks"`
	AdminEnforced               bool                              `json:"admin_enforced"`
	ActiveRulesetCount          int                               `json:"active_ruleset_count"`
	ClassicProtection           bool                              `json:"classic_protection"`
	RequiredStatusCheckContexts []string                          `json:"required_status_check_contexts,omitempty"`
	Rulesets                    []FakeRuleset                     `json:"rulesets,omitempty"`
	BypassPullRequestAllowances int                               `json:"bypass_pull_request_allowances"`
	BypassForcePushAllowances   int                               `json:"bypass_force_push_allowances"`
	MergeQueued                 bool                              `json:"merge_queued"`
	NextPR                      int                               `json:"next_pr"`
	PRs                         []PullRequest                     `json:"prs"`
	Checks                      map[int][]contracts.RequiredCheck `json:"checks"`
	ResponseScripts             map[string][]ResponseMode         `json:"response_scripts,omitempty"`
	Mutations                   []Mutation                        `json:"mutations"`
	Deliveries                  []string                          `json:"deliveries"`
}

const (
	fakeGHLockRetry    = 5 * time.Millisecond
	fakeGHLockDeadline = 5 * time.Second
	prJSONFields       = "number,title,body,headRepositoryOwner,headRepository,headRefName,headRefOid,baseRefName,baseRefOid,isDraft,mergedAt,mergeCommit,state,mergeStateStatus,autoMergeRequest"
)

func NewFakeGH(path string, repository contracts.RepositoryIdentity) (*FakeGH, error) {
	if path == "" {
		return nil, errors.New("testkit: fake-gh state path is required")
	}
	state := FakeGHState{
		Schema:             "sf.testkit.fake-gh/v1",
		Repository:         repository,
		BaseHeadOID:        strings.Repeat("c", 40),
		BaseRef:            "main",
		StrictStatusChecks: true,
		AdminEnforced:      true,
		ClassicProtection:  true,
		NextPR:             1,
		Checks:             make(map[int][]contracts.RequiredCheck),
		ResponseScripts:    make(map[string][]ResponseMode),
	}
	f := &FakeGH{path: path}
	if err := f.initialize(state); err != nil {
		return nil, err
	}
	return f, nil
}

// SetBaseHeadOIDForTest advances or rewinds the fake protected ref. It is a
// test-only remote mutation used to prove that guarded merge launch refuses a
// base which moved after review.
func (f *FakeGH) SetBaseHeadOIDForTest(oid string) error {
	if !fakeOID(oid) {
		return errors.New("testkit: base OID must be an exact SHA")
	}
	return f.withState(func() (bool, error) {
		f.state.BaseHeadOID = oid
		return true, nil
	})
}

func (f *FakeGH) SetBranchProtectionForTest(strict bool, bypassAllowances int) error {
	if bypassAllowances < 0 {
		return errors.New("testkit: bypass allowance count cannot be negative")
	}
	return f.withState(func() (bool, error) {
		f.state.StrictStatusChecks = strict
		f.state.BypassPullRequestAllowances = bypassAllowances
		return true, nil
	})
}

// SetRequiredStatusCheckContextsForTest configures the exact classic branch
// protection contexts emitted by the GraphQL protection witness.
func (f *FakeGH) SetRequiredStatusCheckContextsForTest(contexts ...string) error {
	for _, context := range contexts {
		if strings.TrimSpace(context) == "" {
			return errors.New("testkit: required status context is empty")
		}
	}
	return f.withState(func() (bool, error) {
		f.state.RequiredStatusCheckContexts = append([]string(nil), contexts...)
		return true, nil
	})
}

func (f *FakeGH) SetProtectionWitnessForTest(strict, admin bool, bypassAllowances, rulesets int) error {
	if bypassAllowances < 0 || rulesets < 0 {
		return errors.New("testkit: protection count cannot be negative")
	}
	return f.withState(func() (bool, error) {
		f.state.StrictStatusChecks, f.state.AdminEnforced, f.state.BypassPullRequestAllowances, f.state.ActiveRulesetCount = strict, admin, bypassAllowances, rulesets
		f.state.ClassicProtection = rulesets == 0
		return true, nil
	})
}

// SetRulesetsForTest installs a complete repository-ruleset list and disables
// the classic branch-protection witness.
func (f *FakeGH) SetRulesetsForTest(rulesets ...FakeRuleset) error {
	return f.withState(func() (bool, error) {
		for index := range rulesets {
			normalizeFakeRuleset(&rulesets[index], f.state.Repository)
		}
		f.state.Rulesets = append([]FakeRuleset(nil), rulesets...)
		f.state.ActiveRulesetCount = len(rulesets)
		f.state.ClassicProtection = false
		return true, nil
	})
}

func normalizeFakeRuleset(ruleset *FakeRuleset, repository contracts.RepositoryIdentity) {
	if ruleset.Name == "" {
		ruleset.Name = "ruleset-" + strconv.FormatInt(ruleset.ID, 10)
	}
	if ruleset.NodeID == "" {
		ruleset.NodeID = "RRS_fake_" + strconv.FormatInt(ruleset.ID, 10)
	}
	if ruleset.Links.Self.Href == "" {
		ruleset.Links.Self.Href = "https://api.github.com/repos/" + repository.Owner + "/" + repository.Name + "/rulesets/" + strconv.FormatInt(ruleset.ID, 10)
	}
	if ruleset.Links.HTML.Href == "" {
		ruleset.Links.HTML.Href = "https://github.com/" + repository.Owner + "/" + repository.Name + "/rules/" + strconv.FormatInt(ruleset.ID, 10)
	}
	if ruleset.CreatedAt == "" {
		ruleset.CreatedAt = "2023-07-15T08:43:03Z"
	}
	if ruleset.UpdatedAt == "" {
		ruleset.UpdatedAt = "2023-08-23T16:29:47Z"
	}
	if ruleset.CurrentUserCanBypass == "" {
		ruleset.CurrentUserCanBypass = "never"
	}
}

func (f *FakeGH) SetBypassForcePushAllowancesForTest(count int) error {
	if count < 0 {
		return errors.New("testkit: bypass allowance count cannot be negative")
	}
	return f.withState(func() (bool, error) {
		f.state.BypassForcePushAllowances = count
		return true, nil
	})
}

func (f *FakeGH) SetMergeQueuedForTest(queued bool) error {
	return f.withState(func() (bool, error) {
		f.state.MergeQueued = queued
		return true, nil
	})
}

func (f *FakeGH) SetPullRequestHeadOIDForTest(number int, oid string) error {
	if number <= 0 || !fakeOID(oid) {
		return errors.New("testkit: pull request number and head OID are required")
	}
	return f.withState(func() (bool, error) {
		for index := range f.state.PRs {
			if f.state.PRs[index].Identity.Number == number {
				f.state.PRs[index].Identity.HeadOID = oid
				return true, nil
			}
		}
		return false, errors.New("testkit: pull request not found")
	})
}

// SetPullRequestTextForTest simulates a post-effect remote edit. It is test
// setup only; factory mutations must still use their durable effect claims.
func (f *FakeGH) SetPullRequestTextForTest(number int, title, body string) error {
	if number <= 0 {
		return errors.New("testkit: pull request number is required")
	}
	return f.withState(func() (bool, error) {
		for index := range f.state.PRs {
			if f.state.PRs[index].Identity.Number == number {
				f.state.PRs[index].Title, f.state.PRs[index].Body = title, body
				return true, nil
			}
		}
		return false, errors.New("testkit: pull request not found")
	})
}

// OpenFakeGH loads existing durable state. It refuses an unknown schema rather
// than silently interpreting a fixture with different semantics.
func OpenFakeGH(path string) (*FakeGH, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var state FakeGHState
	if err := json.Unmarshal(b, &state); err != nil {
		return nil, fmt.Errorf("testkit: decode fake-gh state: %w", err)
	}
	if state.Schema != "sf.testkit.fake-gh/v1" {
		return nil, fmt.Errorf("testkit: unsupported fake-gh schema %q", state.Schema)
	}
	if state.Checks == nil {
		state.Checks = make(map[int][]contracts.RequiredCheck)
	}
	if state.ResponseScripts == nil {
		state.ResponseScripts = make(map[string][]ResponseMode)
	}
	if !state.ClassicProtection && state.ActiveRulesetCount == 0 && len(state.Rulesets) == 0 {
		state.ClassicProtection = true
	}
	if !fakeOID(state.BaseHeadOID) {
		state.BaseHeadOID = strings.Repeat("c", 40)
	}
	return &FakeGH{path: path}, nil
}

type FakeGH struct {
	mu    sync.Mutex
	path  string
	state FakeGHState
}

var _ contracts.GitHub = (*FakeGH)(nil)

func (f *FakeGH) Snapshot() FakeGHState {
	var snapshot FakeGHState
	_ = f.withState(func() (bool, error) {
		snapshot = cloneState(f.state)
		return false, nil
	})
	return snapshot
}

func (f *FakeGH) SetAuthenticated(value bool) error {
	return f.withState(func() (bool, error) {
		f.state.Authenticated = value
		return true, nil
	})
}

func (f *FakeGH) SetResponse(operation string, modes ...ResponseMode) error {
	if operation == "" {
		return errors.New("testkit: response operation is required")
	}
	return f.withState(func() (bool, error) {
		f.state.ResponseScripts[operation] = append([]ResponseMode(nil), modes...)
		return true, nil
	})
}

func (f *FakeGH) SetChecks(number int, checks ...contracts.RequiredCheck) error {
	return f.withState(func() (bool, error) {
		f.state.Checks[number] = append([]contracts.RequiredCheck(nil), checks...)
		return true, nil
	})
}

// InjectPullRequestForTest models remote state the factory did not create,
// including hostile or impossible-looking API responses used to prove that
// recovery never adopts a human-owned lookalike.
func (f *FakeGH) InjectPullRequestForTest(pr PullRequest) error {
	return f.withState(func() (bool, error) {
		f.state.PRs = append(f.state.PRs, pr)
		if pr.Identity.Number >= f.state.NextPR {
			f.state.NextPR = pr.Identity.Number + 1
		}
		return true, nil
	})
}

func (f *FakeGH) MutationCount(operation string) int {
	count := 0
	_ = f.withState(func() (bool, error) {
		for _, mutation := range f.state.Mutations {
			if mutation.Operation == operation {
				count++
			}
		}
		return false, nil
	})
	return count
}

func (f *FakeGH) DeliveryCount(operation string) int {
	count := 0
	_ = f.withState(func() (bool, error) {
		for _, delivery := range f.state.Deliveries {
			if delivery == operation {
				count++
			}
		}
		return false, nil
	})
	return count
}

func (f *FakeGH) Name() string { return "fake-gh" }

func (f *FakeGH) AuthStatus(context.Context) error {
	return f.withState(func() (bool, error) { return false, f.requireAuthLocked() })
}

func (f *FakeGH) Repository(_ context.Context, identity contracts.RepositoryIdentity) (contracts.RepositoryIdentity, error) {
	var result contracts.RepositoryIdentity
	err := f.withState(func() (bool, error) {
		if !sameRepository(identity, f.state.Repository) {
			return false, errors.New("fake-gh: repository identity mismatch")
		}
		result = f.state.Repository
		return false, nil
	})
	return result, err
}

func (f *FakeGH) FindPullRequest(_ context.Context, want contracts.PullRequestIdentity) (contracts.PullRequestIdentity, bool, error) {
	if !want.FactoryOwned {
		return contracts.PullRequestIdentity{}, false, errors.New("fake-gh: factory-owned lookup is required")
	}
	var match contracts.PullRequestIdentity
	found := false
	err := f.withState(func() (bool, error) {
		for _, pr := range f.state.PRs {
			if identityMatches(pr.Identity, want) {
				if found {
					match = contracts.PullRequestIdentity{}
					found = false
					return false, errors.New("fake-gh: ambiguous matching pull requests")
				}
				match = pr.Identity
				found = true
			}
		}
		return false, nil
	})
	return match, found, err
}

// ObserveDraftPullRequest mirrors the production recovery contract while
// retaining the fake's strict exact-identity matching.  A foreign matching PR
// is a refusal, never an absence that could permit a duplicate factory PR.
func (f *FakeGH) ObserveDraftPullRequest(_ context.Context, want contracts.PullRequestIdentity) (contracts.PullRequestIdentity, string, bool, bool, error) {
	if !want.FactoryOwned {
		return contracts.PullRequestIdentity{}, "", false, false, errors.New("fake-gh: factory-owned lookup is required")
	}
	var match PullRequest
	count := 0
	err := f.withState(func() (bool, error) {
		for _, pr := range f.state.PRs {
			if !samePRSourceAndBase(pr.Identity, want) {
				continue
			}
			if !pr.Identity.FactoryOwned {
				return false, errors.New("fake-gh: foreign pull request conflicts with the factory identity")
			}
			if !identityMatches(pr.Identity, want) {
				return false, errors.New("fake-gh: factory pull request source identity drifted")
			}
			count++
			match = pr
		}
		if count > 1 {
			return false, errors.New("fake-gh: ambiguous matching pull requests")
		}
		return false, nil
	})
	if err != nil || count == 0 {
		return contracts.PullRequestIdentity{}, "", false, false, err
	}
	return match.Identity, "OPEN", match.Draft, true, nil
}

func (f *FakeGH) ObserveFactoryPullRequestOutput(_ context.Context, want contracts.PullRequestIdentity, title, body string) (contracts.PullRequestIdentity, string, bool, bool, error) {
	var match PullRequest
	count := 0
	err := f.withState(func() (bool, error) {
		for _, pr := range f.state.PRs {
			if !samePRSourceAndBase(pr.Identity, want) {
				continue
			}
			if !pr.Identity.FactoryOwned || !identityMatches(pr.Identity, want) {
				return false, errors.New("fake-gh: factory pull request source identity drifted")
			}
			count++
			match = pr
		}
		if count > 1 {
			return false, errors.New("fake-gh: ambiguous matching pull requests")
		}
		return false, nil
	})
	if err != nil || count == 0 {
		return contracts.PullRequestIdentity{}, "", false, false, err
	}
	marked := body + "\n\n" + ownershipMarkerForFake(match.Identity)
	applied := match.Draft && match.Title == title && match.Body == marked
	return match.Identity, "OPEN", match.Draft, applied, nil
}

// RefreshFactoryPullRequestIdentity models the production correction lookup:
// the durable PR number and source branch stay fixed while its head advances.
func (f *FakeGH) RefreshFactoryPullRequestIdentity(_ context.Context, prior, expected contracts.PullRequestIdentity) (contracts.PullRequestIdentity, error) {
	match, err := f.refreshFactoryPullRequest(prior, expected)
	if err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	return match.Identity, nil
}

func (f *FakeGH) ObserveFactoryPullRequestUpdate(_ context.Context, prior, expected contracts.PullRequestIdentity, title, body string) (contracts.PullRequestIdentity, string, bool, bool, error) {
	match, err := f.refreshFactoryPullRequest(prior, expected)
	if err != nil {
		return contracts.PullRequestIdentity{}, "", false, false, err
	}
	marked := body + "\n\n" + ownershipMarkerForFake(match.Identity)
	applied := match.Draft && match.Title == title && match.Body == marked
	return match.Identity, "OPEN", match.Draft, applied, nil
}

func (f *FakeGH) UpdateFactoryPullRequest(_ context.Context, claim domain.ExternalEffectClaim, prior, expected contracts.PullRequestIdentity, title, body string) error {
	match, err := f.refreshFactoryPullRequest(prior, expected)
	if err != nil {
		return err
	}
	if err := validateFakeClaim(claim, "pr_edit", match.Identity, title, body); err != nil {
		return err
	}
	if match.Draft && match.Title == title && match.Body == body+"\n\n"+ownershipMarkerForFake(match.Identity) {
		return nil
	}
	err = f.updateUnchecked(match.Identity, title, body)
	if err != nil && !errors.Is(err, contracts.ErrPullRequestUpdateBeforeStart) {
		return fmt.Errorf("%w: fake-gh update response unavailable", contracts.ErrPullRequestUpdateUncertain)
	}
	return err
}

func (f *FakeGH) refreshFactoryPullRequest(prior, expected contracts.PullRequestIdentity) (PullRequest, error) {
	var match PullRequest
	count := 0
	err := f.withState(func() (bool, error) {
		if prior.Number <= 0 || !prior.FactoryOwned || !expected.FactoryOwned || !fakeOID(prior.BaseOID) || !fakeOID(expected.BaseOID) || !samePRSourceAndBase(prior, expected) || prior.HeadOID == expected.HeadOID {
			return false, errors.New("fake-gh: invalid correction identity")
		}
		for _, pr := range f.state.PRs {
			if pr.Identity.Number != prior.Number {
				if samePRSourceAndBase(pr.Identity, prior) {
					return false, errors.New("fake-gh: ambiguous correction pull request")
				}
				continue
			}
			if !samePRSourceAndBase(pr.Identity, prior) || !fakeOID(pr.Identity.BaseOID) || pr.Identity.BaseOID != expected.BaseOID || pr.Identity.HeadOID != expected.HeadOID || pr.Merged || !strings.Contains(pr.Body, ownershipMarkerForFake(prior)) && !strings.Contains(pr.Body, ownershipMarkerForFake(expected)) {
				return false, errors.New("fake-gh: correction pull request drifted")
			}
			count++
			match = pr
		}
		if count != 1 {
			return false, errors.New("fake-gh: correction pull request missing or ambiguous")
		}
		return false, nil
	})
	return match, err
}

func (f *FakeGH) CreateDraftPullRequest(_ context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, title, body string) (contracts.PullRequestIdentity, error) {
	if err := validateFakeClaim(claim, "draft_pr", identity, title, body); err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	created, err := f.createDraftUnchecked(identity, title, body)
	var policyConflict *fakeCreatePolicyConflict
	if err != nil && !errors.Is(err, contracts.ErrDraftCreateBeforeStart) && !errors.As(err, &policyConflict) {
		return contracts.PullRequestIdentity{}, fmt.Errorf("%w: fake-gh create response unavailable", contracts.ErrDraftCreateUncertain)
	}
	return created, err
}

func (f *FakeGH) createDraftUnchecked(identity contracts.PullRequestIdentity, title, body string) (contracts.PullRequestIdentity, error) {
	var result contracts.PullRequestIdentity
	var operationErr error
	err := f.withState(func() (bool, error) {
		if err := f.requireAuthLocked(); err != nil {
			return false, err
		}
		if err := f.validateIdentityLocked(identity); err != nil {
			return false, err
		}
		for _, existing := range f.state.PRs {
			if samePRSourceAndBase(existing.Identity, identity) {
				if !existing.Identity.FactoryOwned {
					return false, &fakeCreatePolicyConflict{err: errors.New("fake-gh: human-owned pull request conflicts with the factory identity")}
				}
				return false, &fakeCreatePolicyConflict{err: errors.New("fake-gh: matching pull request already exists")}
			}
		}
		if mode, err := f.beforeMutationLocked("pr_create"); err != nil {
			return false, err
		} else if mode == ResponseErrorBefore {
			f.consumeResponseLocked("pr_create")
			return true, fmt.Errorf("%w: fake-gh create failed before mutation", contracts.ErrDraftCreateBeforeStart)
		}
		if identity.Number == 0 {
			identity.Number = f.state.NextPR
			f.state.NextPR++
		}
		if identity.BaseOID == "" {
			identity.BaseOID = f.state.BaseHeadOID
		}
		identity.FactoryOwned = true
		if !strings.Contains(body, "<!-- sf:v1 ") {
			body += "\n\n" + ownershipMarkerForFake(identity)
		}
		f.state.PRs = append(f.state.PRs, PullRequest{Identity: identity, Title: title, Body: body, Draft: true})
		result, operationErr = f.finishMutationLocked("pr_create", identity)
		return true, operationErr
	})
	return result, err
}

func (f *FakeGH) UpdatePullRequest(_ context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, title, body string) error {
	if err := validateFakeClaim(claim, "pr_edit", identity, title, body); err != nil {
		return err
	}
	return f.updateUnchecked(identity, title, body)
}

func (f *FakeGH) updateUnchecked(identity contracts.PullRequestIdentity, title, body string) error {
	return f.withState(func() (bool, error) {
		index, err := f.findLocked(identity)
		if err != nil {
			return false, err
		}
		if mode, err := f.beforeMutationLocked("pr_edit"); err != nil {
			return false, err
		} else if mode == ResponseErrorBefore {
			f.consumeResponseLocked("pr_edit")
			return true, fmt.Errorf("%w: fake-gh edit failed before mutation", contracts.ErrPullRequestUpdateBeforeStart)
		}
		if !strings.Contains(body, "<!-- sf:v1 ") {
			body += "\n\n" + ownershipMarkerForFake(identity)
		}
		f.state.PRs[index].Title, f.state.PRs[index].Body = title, body
		f.recordMutationLocked("pr_edit", identity)
		return true, f.finishErrorLocked("pr_edit")
	})
}

func (f *FakeGH) RequiredChecks(_ context.Context, identity contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
	var checks []contracts.RequiredCheck
	err := f.withState(func() (bool, error) {
		index, err := f.findLocked(identity)
		if err != nil {
			return false, err
		}
		checks = append([]contracts.RequiredCheck(nil), f.state.Checks[f.state.PRs[index].Identity.Number]...)
		return false, nil
	})
	return checks, err
}

// ObserveCIRequiredCheckPolicy supplies the same authenticated, read-only
// policy/check boundary used by the production poller. The fake's durable
// state is intentionally the source of truth so restart tests do not rely on
// an in-memory test-only policy.
func (f *FakeGH) ObserveCIRequiredCheckPolicy(_ context.Context, identity contracts.PullRequestIdentity) (contracts.CIRequiredCheckPolicyObservation, error) {
	var result contracts.CIRequiredCheckPolicyObservation
	err := f.withState(func() (bool, error) {
		index, err := f.findLocked(identity)
		if err != nil {
			return false, err
		}
		pr := f.state.PRs[index].Identity
		if !identityMatches(pr, identity) || pr.BaseOID == "" {
			return false, errors.New("fake-gh: pull request identity drifted")
		}
		checks := append([]contracts.RequiredCheck(nil), f.state.Checks[pr.Number]...)
		result = contracts.CIRequiredCheckPolicyObservation{
			PullRequest: pr, ProtectedBranchRef: pr.BaseRef, ProtectedBranchOID: pr.BaseOID,
			PolicySourceDigest: strings.Repeat("a", 64), AuthenticatedPrincipal: "fake-gh",
			RequiredChecks: checks, ObservedAt: time.Now().UTC(),
		}
		return false, nil
	})
	return result, err
}

func (f *FakeGH) MarkReady(_ context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity) error {
	if err := validateFakeClaim(claim, "pr_ready", identity); err != nil {
		return err
	}
	return f.readyUnchecked(identity)
}

func (f *FakeGH) readyUnchecked(identity contracts.PullRequestIdentity) error {
	return f.withState(func() (bool, error) {
		index, err := f.findLocked(identity)
		if err != nil {
			return false, err
		}
		if mode, err := f.beforeMutationLocked("pr_ready"); err != nil {
			return false, err
		} else if mode == ResponseErrorBefore {
			f.consumeResponseLocked("pr_ready")
			return true, errors.New("fake-gh: ready failed before mutation")
		}
		f.state.PRs[index].Draft, f.state.PRs[index].Ready = false, true
		f.recordMutationLocked("pr_ready", identity)
		return true, f.finishErrorLocked("pr_ready")
	})
}

func (f *FakeGH) MergeExactHead(_ context.Context, claim domain.ExternalEffectClaim, identity contracts.PullRequestIdentity, headOID, method string, authorization domain.MergeAuthorization) error {
	if err := validateFakeMergeAuthorization(identity, headOID, authorization); err != nil {
		return err
	}
	if err := validateFakeClaim(claim, "merge", identity, headOID, method, authorization.ReviewedBaseSHA, authorization.CurrentBaseSHA, authorization.ReviewedBaseHeadOID, authorization.CurrentBaseHeadOID); err != nil {
		return err
	}
	return f.mergeUnchecked(identity, headOID, method)
}

func (f *FakeGH) mergeUnchecked(identity contracts.PullRequestIdentity, headOID, method string) error {
	return f.withState(func() (bool, error) {
		index, err := f.findLocked(identity)
		if err != nil {
			return false, err
		}
		if f.state.PRs[index].Identity.BaseOID != f.state.BaseHeadOID {
			return false, errors.New("fake-gh: protected base moved after authorization")
		}
		if headOID == "" || headOID != f.state.PRs[index].Identity.HeadOID || headOID != identity.HeadOID {
			return false, errors.New("fake-gh: exact reviewed head mismatch")
		}
		if f.state.PRs[index].Draft {
			return false, errors.New("fake-gh: draft pull request cannot merge")
		}
		validRuleset := f.state.ActiveRulesetCount == 1 && len(f.state.Rulesets) == 1 && f.state.Rulesets[0].Enforcement == "active"
		if !f.state.StrictStatusChecks || !f.state.AdminEnforced || f.state.BypassPullRequestAllowances != 0 || f.state.BypassForcePushAllowances != 0 || (f.state.ActiveRulesetCount != 0 && !validRuleset) {
			return false, errors.New("fake-gh: strict protected-base enforcement is required")
		}
		if method != "merge" && method != "squash" && method != "rebase" {
			return false, errors.New("fake-gh: unsupported merge method")
		}
		if mode, err := f.beforeMutationLocked("pr_merge"); err != nil {
			return false, err
		} else if mode == ResponseErrorBefore {
			f.consumeResponseLocked("pr_merge")
			return true, errors.New("fake-gh: merge failed before mutation")
		}
		f.state.PRs[index].Merged = true
		// A hosted merge produces a new immutable merge result.  The fake keeps
		// it distinct from the reviewed head so the boundary cannot mistake a
		// head OID for protected-branch evidence.
		f.state.PRs[index].MergeCommit = strings.Repeat("b", 40)
		// The protected ref advances to the merge result. PR baseRefOid remains
		// the original PR witness, so reconciliation must not compare it to this
		// live post-merge tip.
		f.state.BaseHeadOID = f.state.PRs[index].MergeCommit
		f.recordMutationLocked("pr_merge", identity)
		return true, f.finishErrorLocked("pr_merge")
	})
}

func (f *FakeGH) requireAuthLocked() error {
	if !f.state.Authenticated {
		return errors.New("fake-gh: authentication required")
	}
	return nil
}

func (f *FakeGH) validateIdentityLocked(identity contracts.PullRequestIdentity) error {
	if !sameRepository(identity.Repository, f.state.Repository) || identity.HeadOwner == "" || identity.HeadRepository == "" || identity.HeadRef == "" || identity.HeadOID == "" || identity.BaseRef == "" {
		return errors.New("fake-gh: incomplete PR identity")
	}
	return nil
}

func (f *FakeGH) findLocked(identity contracts.PullRequestIdentity) (int, error) {
	if err := f.requireAuthLocked(); err != nil {
		return -1, err
	}
	for index, pr := range f.state.PRs {
		if identityMatches(pr.Identity, identity) {
			return index, nil
		}
	}
	return -1, errors.New("fake-gh: pull request identity not found")
}

func identityMatches(actual, want contracts.PullRequestIdentity) bool {
	if !actual.FactoryOwned {
		return false
	}
	if want.Number != 0 && actual.Number != want.Number {
		return false
	}
	if want.Repository != (contracts.RepositoryIdentity{}) && !sameRepository(actual.Repository, want.Repository) {
		return false
	}
	for _, pair := range [][2]string{{actual.HeadOwner, want.HeadOwner}, {actual.HeadRepository, want.HeadRepository}, {actual.HeadRef, want.HeadRef}, {actual.HeadOID, want.HeadOID}, {actual.BaseRef, want.BaseRef}} {
		if pair[1] != "" && pair[0] != pair[1] {
			return false
		}
	}
	return true
}

func samePRSourceAndBase(a, b contracts.PullRequestIdentity) bool {
	return sameRepository(a.Repository, b.Repository) &&
		a.HeadOwner == b.HeadOwner &&
		a.HeadRepository == b.HeadRepository &&
		a.HeadRef == b.HeadRef &&
		a.BaseRef == b.BaseRef
}

func sameRepository(a, b contracts.RepositoryIdentity) bool {
	return a.Host == b.Host && a.Owner == b.Owner && a.Name == b.Name
}

// EffectClaimForTest creates the exact durable-claim shape expected by the
// in-process fake. It intentionally mirrors the public boundary digest so
// tests cannot model an unfenced mutation as a successful remote call.
func EffectClaimForTest(kind string, identity contracts.PullRequestIdentity, values ...string) domain.ExternalEffectClaim {
	return domain.ExternalEffectClaim{
		SemanticKey:   "fake-effect-" + kind,
		Ref:           domain.TicketRef{Channel: domain.ChannelDev, Project: "fake-project", Ticket: "SF-00000001"},
		Kind:          kind,
		RequestDigest: fakeRequestDigest(kind, identity, values...),
		TicketVersion: 1,
		LeaderEpoch:   1,
		RunnerEpoch:   1,
		ClaimEpoch:    1,
	}
}

func validateFakeClaim(claim domain.ExternalEffectClaim, kind string, identity contracts.PullRequestIdentity, values ...string) error {
	if claim.Ref.Validate() != nil || claim.SemanticKey == "" || claim.Kind != kind || claim.TicketVersion == 0 || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || claim.ClaimEpoch == 0 || claim.RequestDigest == "" || claim.RequestDigest != fakeRequestDigest(kind, identity, values...) {
		return errors.New("fake-gh: exact durable effect claim is required")
	}
	return nil
}

func validateFakeMergeAuthorization(identity contracts.PullRequestIdentity, headOID string, authorization domain.MergeAuthorization) error {
	base := authorization.ReviewedBaseSHA
	if !authorization.Approved || !authorization.GatesGreen || authorization.ReviewedHead != headOID || authorization.CurrentHead != headOID || !fakeOID(base) || authorization.CurrentBaseSHA != base || authorization.ReviewedBaseHeadOID != base || authorization.CurrentBaseHeadOID != base || identity.BaseOID != base {
		return errors.New("fake-gh: exact approved head/base authorization is required")
	}
	return nil
}

func fakeRequestDigest(operation string, identity contracts.PullRequestIdentity, values ...string) string {
	input := operation + "\x00" + identity.Repository.Owner + "/" + identity.Repository.Name + "\x00" + identity.HeadOwner + "\x00" + identity.HeadRepository + "\x00" + identity.HeadRef + "\x00" + identity.HeadOID + "\x00" + identity.BaseRef + "\x00" + identity.BaseOID
	for _, value := range values {
		input += "\x00" + value
	}
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:])
}

func (f *FakeGH) beforeMutationLocked(operation string) (ResponseMode, error) {
	script := f.state.ResponseScripts[operation]
	if len(script) == 0 {
		return ResponseDeliver, nil
	}
	return script[0], nil
}

func (f *FakeGH) consumeResponseLocked(operation string) {
	script := f.state.ResponseScripts[operation]
	if len(script) > 0 {
		f.state.ResponseScripts[operation] = script[1:]
	}
}

func (f *FakeGH) recordMutationLocked(operation string, identity contracts.PullRequestIdentity) {
	f.state.Mutations = append(f.state.Mutations, Mutation{Operation: operation, Number: identity.Number, HeadOID: identity.HeadOID})
}

func (f *FakeGH) finishMutationLocked(operation string, identity contracts.PullRequestIdentity) (contracts.PullRequestIdentity, error) {
	mode := ResponseDeliver
	if script := f.state.ResponseScripts[operation]; len(script) > 0 {
		mode = script[0]
		f.state.ResponseScripts[operation] = script[1:]
	}
	f.state.Mutations = append(f.state.Mutations, Mutation{Operation: operation, Number: identity.Number, HeadOID: identity.HeadOID})
	if mode == ResponseDeliver {
		f.state.Deliveries = append(f.state.Deliveries, operation)
	}
	switch mode {
	case ResponseDropAfterCall:
		return contracts.PullRequestIdentity{}, errors.New("fake-gh: response lost after mutation")
	case ResponseErrorAfter:
		return contracts.PullRequestIdentity{}, errors.New("fake-gh: response failed after mutation")
	default:
		return identity, nil
	}
}

func (f *FakeGH) finishErrorLocked(operation string) error {
	mode := ResponseDeliver
	if script := f.state.ResponseScripts[operation]; len(script) > 0 {
		mode = script[0]
		f.state.ResponseScripts[operation] = script[1:]
	}
	if mode == ResponseDeliver {
		f.state.Deliveries = append(f.state.Deliveries, operation)
	}
	if mode == ResponseDropAfterCall || mode == ResponseErrorAfter {
		return errors.New("fake-gh: response lost after mutation")
	}
	return nil
}

func (f *FakeGH) initialize(state FakeGHState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return err
	}
	release, err := acquireFakeGHLock(f.path)
	if err != nil {
		return err
	}
	defer func() { _ = release() }()
	if _, err := os.Lstat(f.path); err == nil {
		return errors.New("testkit: fake-gh state already exists")
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := saveFakeGHState(f.path, state); err != nil {
		return err
	}
	f.state = cloneState(state)
	return nil
}

// withState serializes every durable fake-remote operation across processes.
// A lock is never stolen: a stuck fixture yields a bounded explicit error
// instead of risking an ambiguous or duplicate external effect.
func (f *FakeGH) withState(operation func() (changed bool, err error)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	release, err := acquireFakeGHLock(f.path)
	if err != nil {
		return err
	}
	defer func() { _ = release() }()
	state, err := loadFakeGHState(f.path)
	if err != nil {
		return err
	}
	f.state = state
	changed, operationErr := operation()
	if changed {
		if err := saveFakeGHState(f.path, f.state); err != nil {
			return err
		}
	}
	return operationErr
}

func loadFakeGHState(path string) (FakeGHState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return FakeGHState{}, err
	}
	var state FakeGHState
	if err := json.Unmarshal(b, &state); err != nil {
		return FakeGHState{}, fmt.Errorf("testkit: decode fake-gh state: %w", err)
	}
	if state.Schema != "sf.testkit.fake-gh/v1" {
		return FakeGHState{}, fmt.Errorf("testkit: unsupported fake-gh schema %q", state.Schema)
	}
	if state.Checks == nil {
		state.Checks = make(map[int][]contracts.RequiredCheck)
	}
	if state.ResponseScripts == nil {
		state.ResponseScripts = make(map[string][]ResponseMode)
	}
	if !state.ClassicProtection && state.ActiveRulesetCount == 0 && len(state.Rulesets) == 0 {
		state.ClassicProtection = true
	}
	if !fakeOID(state.BaseHeadOID) {
		state.BaseHeadOID = strings.Repeat("c", 40)
	}
	return state, nil
}

func acquireFakeGHLock(path string) (func() error, error) {
	lockPath := path + ".lock"
	deadline := time.Now().Add(fakeGHLockDeadline)
	for {
		err := os.Mkdir(lockPath, 0o700)
		if err == nil {
			return func() error { return os.Remove(lockPath) }, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return nil, fmt.Errorf("testkit: acquire fake-gh lock: %w", err)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("testkit: fake-gh state lock remained held for %s; verify no fixture process owns %s", fakeGHLockDeadline, lockPath)
		}
		time.Sleep(fakeGHLockRetry)
	}
}

func saveFakeGHState(path string, state FakeGHState) error {
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func cloneState(state FakeGHState) FakeGHState {
	b, _ := json.Marshal(state)
	var copy FakeGHState
	_ = json.Unmarshal(b, &copy)
	return copy
}

// Run accepts the bounded, explicit argv shape used by the fake-gh command.
// It is useful for contract tests that want to validate a complete invocation
// without spawning a process.
func (f *FakeGH) Run(argv []string) ([]byte, error) {
	if len(argv) == 0 {
		return nil, errors.New("fake-gh: empty argv")
	}
	if argv[0] == "gh" {
		argv = argv[1:]
	}
	if err := validateOfficialArgv(argv); err != nil {
		return nil, err
	}
	if len(argv) >= 2 && argv[0] == "auth" && argv[1] == "status" {
		if option(argv, "--json") != "" {
			if err := f.AuthStatus(context.Background()); err != nil {
				return nil, err
			}
			return json.Marshal(map[string]any{
				"hosts": map[string]any{
					"github.com": []map[string]any{{"login": "sf-test", "active": true, "state": "success", "host": "github.com", "scopes": "repo", "gitProtocol": "https", "tokenSource": "fake"}},
				},
			})
		}
		return []byte("authenticated\n"), f.AuthStatus(context.Background())
	}
	if len(argv) >= 2 && argv[0] == "repo" && argv[1] == "view" {
		return f.runRepoView(argv)
	}
	if len(argv) >= 5 && argv[0] == "api" && option(argv, "--hostname") == "github.com" && option(argv, "--method") == "GET" && strings.Contains(argv[len(argv)-1], "/rules/branches/") {
		snapshot := f.Snapshot()
		if len(snapshot.Rulesets) == 0 {
			if snapshot.ActiveRulesetCount != 0 {
				return []byte(`[{"type":"pull_request"}]`), nil
			}
			return []byte(`[]`), nil
		}
		var applied []map[string]any
		for _, ruleset := range snapshot.Rulesets {
			if ruleset.Enforcement != "active" {
				continue
			}
			for _, rule := range ruleset.Rules {
				applied = append(applied, map[string]any{"type": rule.Type, "ruleset_source_type": ruleset.SourceType, "ruleset_source": ruleset.Source, "ruleset_id": ruleset.ID, "parameters": rule.Parameters})
			}
		}
		return json.Marshal(applied)
	}
	if len(argv) >= 5 && argv[0] == "api" && option(argv, "--hostname") == "github.com" && option(argv, "--method") == "GET" && strings.Contains(argv[len(argv)-1], "/rulesets") {
		snapshot := f.Snapshot()
		path := argv[len(argv)-1]
		if strings.Contains(path, "/rulesets/") {
			repositoryPath := snapshot.Repository.Owner + "/" + snapshot.Repository.Name
			id := strings.TrimPrefix(strings.Split(path, "?")[0], "repos/"+repositoryPath+"/rulesets/")
			for _, ruleset := range snapshot.Rulesets {
				if fmt.Sprint(ruleset.ID) == id {
					return json.Marshal(ruleset)
				}
			}
			return []byte(`{}`), nil
		}
		summaries := make([]map[string]any, 0, len(snapshot.Rulesets))
		for _, ruleset := range snapshot.Rulesets {
			enforcement := ruleset.Enforcement
			if enforcement == "active" {
				enforcement = "enabled"
			}
			summaries = append(summaries, map[string]any{"id": ruleset.ID, "name": ruleset.Name, "target": ruleset.Target, "source_type": ruleset.SourceType, "source": ruleset.Source, "enforcement": enforcement, "node_id": ruleset.NodeID, "_links": ruleset.Links, "created_at": ruleset.CreatedAt, "updated_at": ruleset.UpdatedAt})
		}
		return json.Marshal(summaries)
	}
	if len(argv) >= 2 && argv[0] == "api" && argv[1] == "--hostname" {
		if strings.Contains(graphqlQuery(argv), "branchProtectionRule") {
			snapshot := f.Snapshot()
			value := map[string]any{
				"data": map[string]any{"repository": map[string]any{"ref": map[string]any{"branchProtectionRule": map[string]any{
					"id": "fake-rule-main", "pattern": "main", "requiresStrictStatusChecks": snapshot.StrictStatusChecks, "isAdminEnforced": snapshot.AdminEnforced,
					"requiredStatusCheckContexts": snapshot.RequiredStatusCheckContexts,
					"bypassPullRequestAllowances": map[string]int{"totalCount": snapshot.BypassPullRequestAllowances}, "bypassForcePushAllowances": map[string]int{"totalCount": snapshot.BypassForcePushAllowances},
				}}}},
			}
			if !snapshot.ClassicProtection {
				value["data"].(map[string]any)["repository"].(map[string]any)["ref"] = nil
			}
			return json.Marshal(value)
		}
		if f.Snapshot().MergeQueued {
			return []byte(`{"data":{"repository":{"pullRequest":{"mergeQueueEntry":{"position":1}}}}}`), nil
		}
		return []byte(`{"data":{"repository":{"pullRequest":{"mergeQueueEntry":null}}}}`), nil
	}
	if len(argv) >= 2 && argv[0] == "api" && strings.HasPrefix(argv[1], "repos/") {
		return f.runSourceRef(argv)
	}
	if len(argv) >= 2 && argv[0] == "pr" {
		switch argv[1] {
		case "create":
			return f.runCreate(argv)
		case "view":
			return f.runView(argv)
		case "list":
			return f.runList(argv)
		case "edit":
			return f.runEdit(argv)
		case "ready":
			return f.runReady(argv)
		case "checks":
			return f.runChecks(argv)
		case "merge":
			return f.runMerge(argv)
		}
	}
	return nil, errors.New("fake-gh: unsupported invocation")
}

func (f *FakeGH) runSourceRef(argv []string) ([]byte, error) {
	const marker = "/git/ref/heads/"
	path := argv[1]
	index := strings.Index(path, marker)
	if index < 0 {
		return nil, errors.New("fake-gh: unsupported source ref")
	}
	sourcePath, sourceRef, ok := strings.Cut(strings.TrimPrefix(path, "repos/"), marker)
	if !ok {
		return nil, errors.New("fake-gh: malformed source ref")
	}
	sourceOwner, sourceRepo, ok := strings.Cut(sourcePath, "/")
	if !ok {
		return nil, errors.New("fake-gh: malformed source repository")
	}
	ref := sourceRef
	sha := strings.Repeat("a", 40)
	snapshot := f.Snapshot()
	baseRef := snapshot.BaseRef
	if baseRef == "" { // tolerate test state written before BaseRef was added.
		baseRef = "main"
	}
	if sourceOwner == snapshot.Repository.Owner && sourceRepo == snapshot.Repository.Name && ref == baseRef {
		return json.Marshal(map[string]any{"object": map[string]string{"sha": snapshot.BaseHeadOID}})
	}
	for _, pr := range snapshot.PRs {
		if pr.Identity.HeadOwner == sourceOwner && pr.Identity.HeadRepository == sourceRepo && pr.Identity.HeadRef == ref {
			sha = pr.Identity.HeadOID
			break
		}
	}
	return json.Marshal(map[string]any{"object": map[string]string{"sha": sha}})
}

// validateOfficialArgv deliberately accepts only the public gh CLI grammar
// used by the adapter.  This makes a fake-only flag or a quiet argv drift fail
// tests before it can look like a supported GitHub operation.
func validateOfficialArgv(argv []string) error {
	if len(argv) < 2 {
		return errors.New("fake-gh: incomplete invocation")
	}
	key := argv[0] + " " + argv[1]
	require := func(flag, value string) error {
		if option(argv, flag) != value {
			return fmt.Errorf("fake-gh: %s requires %s=%q", key, flag, value)
		}
		return nil
	}
	allowed := map[string]bool{}
	switch key {
	case "api --hostname":
		// Exact GraphQL lookup or a github.com-pinned active-rules REST lookup.
		if len(argv) >= 6 && argv[2] == "github.com" && argv[3] == "--method" && argv[4] == "GET" && strings.HasPrefix(argv[5], "repos/") && ((strings.Contains(argv[5], "/rules/branches/") && strings.HasSuffix(argv[5], "?per_page=100&page=1")) || strings.Contains(argv[5], "/rulesets/") || strings.HasSuffix(argv[5], "/rulesets?includes_parents=true&targets=branch&per_page=100&page=1")) {
			allowed["--method"] = true
			break
		}
		if len(argv) < 6 || argv[2] != "github.com" || argv[3] != "graphql" {
			return fmt.Errorf("fake-gh: incomplete %s", key)
		}
	case "api --method":
		allowed["--method"] = true
		if len(argv) != 4 || argv[2] != "GET" || !strings.HasPrefix(argv[3], "repos/") || !strings.Contains(argv[3], "/rules/branches/") || !strings.HasSuffix(argv[3], "?per_page=100&page=1") {
			return fmt.Errorf("fake-gh: incomplete %s", key)
		}
	case "auth status":
		allowed["--json"] = true
		if err := require("--json", "hosts"); err != nil {
			return err
		}
	case "repo view":
		allowed["--repo"], allowed["--json"] = true, true
		if err := require("--json", "nameWithOwner,url"); err != nil {
			return err
		}
	case "pr create":
		for _, flag := range []string{"--repo", "--head", "--base", "--draft", "--title", "--body"} {
			allowed[flag] = true
		}
		if !hasFlag(argv, "--draft") || option(argv, "--repo") == "" || option(argv, "--head") == "" || option(argv, "--base") == "" || option(argv, "--title") == "" || option(argv, "--body") == "" {
			return fmt.Errorf("fake-gh: incomplete %s", key)
		}
	case "pr list":
		for _, flag := range []string{"--repo", "--state", "--limit", "--json"} {
			allowed[flag] = true
		}
		if err := require("--state", "all"); err != nil {
			return err
		}
		if err := require("--limit", "100"); err != nil {
			return err
		}
		if err := require("--json", prJSONFields); err != nil {
			return err
		}
	case "pr view":
		allowed["--repo"], allowed["--json"] = true, true
		if prNumber(argv) <= 0 {
			return fmt.Errorf("fake-gh: %s requires a number", key)
		}
		if err := require("--json", prJSONFields); err != nil {
			return err
		}
	case "pr edit":
		for _, flag := range []string{"--repo", "--title", "--body"} {
			allowed[flag] = true
		}
		if prNumber(argv) <= 0 || option(argv, "--repo") == "" || option(argv, "--title") == "" || option(argv, "--body") == "" {
			return fmt.Errorf("fake-gh: incomplete %s", key)
		}
	case "pr ready":
		allowed["--repo"] = true
		if prNumber(argv) <= 0 || option(argv, "--repo") == "" {
			return fmt.Errorf("fake-gh: incomplete %s", key)
		}
	case "pr checks":
		allowed["--repo"], allowed["--json"], allowed["--required"] = true, true, true
		if prNumber(argv) <= 0 {
			return fmt.Errorf("fake-gh: %s requires a number", key)
		}
		if !hasFlag(argv, "--required") {
			return fmt.Errorf("fake-gh: %s must request the server-defined required set", key)
		}
		if err := require("--json", "name,state,workflow,link,bucket"); err != nil {
			return err
		}
	case "pr merge":
		for _, flag := range []string{"--repo", "--match-head-commit", "--merge", "--squash", "--rebase"} {
			allowed[flag] = true
		}
		if prNumber(argv) <= 0 || option(argv, "--repo") == "" || option(argv, "--match-head-commit") == "" || mergeMethod(argv) == "" {
			return fmt.Errorf("fake-gh: incomplete %s", key)
		}
	default:
		if argv[0] == "api" && strings.HasPrefix(argv[1], "repos/") && strings.Contains(argv[1], "/git/ref/heads/") {
			break
		}
		return errors.New("fake-gh: unsupported invocation")
	}
	for index := 2; index < len(argv); index++ {
		arg := argv[index]
		if !strings.HasPrefix(arg, "--") {
			continue
		}
		name := arg
		if before, _, ok := strings.Cut(arg, "="); ok {
			name = before
		}
		if !allowed[name] {
			return fmt.Errorf("fake-gh: unsupported flag %s", name)
		}
	}
	return nil
}

func hasFlag(argv []string, wanted string) bool {
	for _, arg := range argv {
		if arg == wanted {
			return true
		}
	}
	return false
}

func (f *FakeGH) runList(argv []string) ([]byte, error) {
	var results []map[string]any
	err := f.withState(func() (bool, error) {
		if err := f.requireAuthLocked(); err != nil {
			return false, err
		}
		for _, pr := range f.state.PRs {
			if repo := option(argv, "--repo"); repo != "" && repo != pr.Identity.Repository.Owner+"/"+pr.Identity.Repository.Name {
				continue
			}
			// baseRefOid is the PR's recorded base identity, not the current
			// protected-ref tip. Keeping the row's value visible lets strict
			// publication inventory reject a stale/mismatched PR base witness.
			results = append(results, prJSON(pr, pr.Identity.BaseOID))
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(results)
}

func option(argv []string, name string) string {
	for index, arg := range argv {
		if arg == name && index+1 < len(argv) {
			return argv[index+1]
		}
		if strings.HasPrefix(arg, name+"=") {
			return strings.TrimPrefix(arg, name+"=")
		}
	}
	return ""
}

func graphqlQuery(argv []string) string {
	for _, value := range argv {
		if strings.HasPrefix(value, "query=") {
			return strings.TrimPrefix(value, "query=")
		}
	}
	return ""
}

func prNumber(argv []string) int {
	for _, arg := range argv[2:] {
		if strings.HasPrefix(arg, "-") {
			continue
		}
		if n, err := strconv.Atoi(arg); err == nil {
			return n
		}
	}
	return 0
}

func (f *FakeGH) runRepoView(argv []string) ([]byte, error) {
	identity := contracts.RepositoryIdentity{Host: "github.com"}
	value := option(argv, "--repo")
	parts := strings.Split(value, "/")
	if len(parts) == 2 {
		identity.Owner, identity.Name = parts[0], parts[1]
	}
	result, err := f.Repository(context.Background(), identity)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]string{"nameWithOwner": result.Owner + "/" + result.Name, "url": "https://" + result.Host + "/" + result.Owner + "/" + result.Name})
}

func identityFromArgs(argv []string, number int) contracts.PullRequestIdentity {
	repo := strings.Split(option(argv, "--repo"), "/")
	head := strings.Split(option(argv, "--head"), ":")
	base := option(argv, "--base")
	identity := contracts.PullRequestIdentity{Number: number, Repository: contracts.RepositoryIdentity{Host: "github.com"}, BaseRef: base, HeadOID: option(argv, "--head-oid")}
	if len(repo) == 2 {
		identity.Repository.Owner, identity.Repository.Name = repo[0], repo[1]
	}
	if len(head) == 2 {
		identity.HeadOwner, identity.HeadRef = head[0], head[1]
		identity.HeadRepository = identity.Repository.Name
	} else {
		identity.HeadRef = option(argv, "--head")
		identity.HeadOwner = identity.Repository.Owner
		identity.HeadRepository = identity.Repository.Name
	}
	identity.FactoryOwned = option(argv, "--sf-owned") == "true"
	return identity
}

func (f *FakeGH) runCreate(argv []string) ([]byte, error) {
	identity := identityFromArgs(argv, 0)
	identityFromMarker(&identity, option(argv, "--body"))
	pr, err := f.createDraftUnchecked(identity, option(argv, "--title"), option(argv, "--body"))
	if err != nil {
		return nil, err
	}
	return []byte("https://github.com/" + pr.Repository.Owner + "/" + pr.Repository.Name + "/pull/" + strconv.Itoa(pr.Number) + "\n"), nil
}

func identityFromMarker(identity *contracts.PullRequestIdentity, body string) {
	start := strings.LastIndex(body, "<!-- sf:v1 ")
	if start < 0 {
		return
	}
	fields := strings.Fields(strings.TrimSuffix(body[start+len("<!-- sf:v1 "):], " -->"))
	for _, field := range fields {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch key {
		case "oid":
			identity.HeadOID = value
		case "base":
			identity.BaseRef = value
		case "head":
			ownerRepo, ref, ok := strings.Cut(value, ":")
			if !ok {
				continue
			}
			owner, repo, ok := strings.Cut(ownerRepo, "/")
			if ok {
				identity.HeadOwner = owner
				identity.HeadRepository = repo
				identity.HeadRef = ref
			}
		}
	}
	identity.FactoryOwned = identity.HeadOID != ""
}

func (f *FakeGH) runView(argv []string) ([]byte, error) {
	number := prNumber(argv)
	identity := identityFromArgs(argv, number)
	var pr PullRequest
	var baseHeadOID string
	err := f.withState(func() (bool, error) {
		if number > 0 && identity.HeadRef == "" {
			if err := f.requireAuthLocked(); err != nil {
				return false, err
			}
			for _, candidate := range f.state.PRs {
				if candidate.Identity.Number == number && sameRepository(candidate.Identity.Repository, identity.Repository) {
					pr = candidate
					baseHeadOID = candidate.Identity.BaseOID
					return false, nil
				}
			}
			return false, errors.New("fake-gh: pull request identity not found")
		}
		index, err := f.findLocked(identity)
		if err != nil {
			return false, err
		}
		pr = f.state.PRs[index]
		baseHeadOID = pr.Identity.BaseOID
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return json.Marshal(prJSON(pr, baseHeadOID))
}

func (f *FakeGH) runEdit(argv []string) ([]byte, error) {
	number := prNumber(argv)
	identity := identityFromArgs(argv, number)
	if err := f.updateUnchecked(identity, option(argv, "--title"), option(argv, "--body")); err != nil {
		return nil, err
	}
	return []byte("{}"), nil
}

func (f *FakeGH) runReady(argv []string) ([]byte, error) {
	identity := identityFromArgs(argv, prNumber(argv))
	if identity.HeadRef == "" {
		if err := f.withState(func() (bool, error) {
			for _, candidate := range f.state.PRs {
				if candidate.Identity.Number == identity.Number && sameRepository(candidate.Identity.Repository, identity.Repository) {
					identity = candidate.Identity
					return false, nil
				}
			}
			return false, errors.New("fake-gh: pull request identity not found")
		}); err != nil {
			return nil, err
		}
	}
	if err := f.readyUnchecked(identity); err != nil {
		return nil, err
	}
	return []byte("{}"), nil
}

func (f *FakeGH) runChecks(argv []string) ([]byte, error) {
	identity := identityFromArgs(argv, prNumber(argv))
	if identity.HeadRef == "" {
		if err := f.withState(func() (bool, error) {
			for _, candidate := range f.state.PRs {
				if candidate.Identity.Number == identity.Number && sameRepository(candidate.Identity.Repository, identity.Repository) {
					identity = candidate.Identity
					return false, nil
				}
			}
			return false, errors.New("fake-gh: pull request identity not found")
		}); err != nil {
			return nil, err
		}
	}
	checks, err := f.RequiredChecks(context.Background(), identity)
	if err != nil {
		return nil, err
	}
	values := make([]map[string]string, 0, len(checks))
	for _, check := range checks {
		values = append(values, map[string]string{"name": check.Name, "state": check.State, "workflow": "", "link": check.ExternalID, "bucket": ""})
	}
	return json.Marshal(values)
}

func (f *FakeGH) runMerge(argv []string) ([]byte, error) {
	identity := identityFromArgs(argv, prNumber(argv))
	headOID := option(argv, "--match-head-commit")
	identity.HeadOID = headOID
	if err := f.mergeUnchecked(identity, headOID, mergeMethod(argv)); err != nil {
		return nil, err
	}
	return []byte("{}"), nil
}

func mergeMethod(argv []string) string {
	for _, method := range []string{"merge", "squash", "rebase"} {
		for _, arg := range argv {
			if arg == "--"+method {
				return method
			}
		}
	}
	return ""
}

func prJSON(pr PullRequest, baseHeadOID string) map[string]any {
	identity, draft, merged := pr.Identity, pr.Draft, pr.Merged
	body := pr.Body
	if body == "" {
		body = ownershipMarkerForFake(identity)
	}
	value := map[string]any{
		"number":              identity.Number,
		"headRepository":      map[string]string{"nameWithOwner": identity.HeadOwner + "/" + identity.HeadRepository},
		"headRepositoryOwner": map[string]string{"login": identity.HeadOwner},
		"headRefName":         identity.HeadRef,
		"headRefOid":          identity.HeadOID,
		"baseRefName":         identity.BaseRef,
		"baseRefOid":          baseHeadOID,
		"isDraft":             draft,
		"title":               pr.Title,
		"body":                body,
		"state":               map[bool]string{true: "MERGED", false: "OPEN"}[merged],
	}
	if merged {
		value["mergedAt"] = "2026-01-01T00:00:00Z"
		mergeCommit := pr.MergeCommit
		if mergeCommit == "" {
			mergeCommit = strings.Repeat("b", 40)
		}
		value["mergeCommit"] = map[string]string{"oid": mergeCommit}
	} else {
		value["mergedAt"] = nil
		value["mergeCommit"] = nil
	}
	value["autoMergeRequest"] = nil
	value["mergeStateStatus"] = "CLEAN"
	return value
}

func ownershipMarkerForFake(value contracts.PullRequestIdentity) string {
	return "<!-- sf:v1 repository=" + value.Repository.Owner + "/" + value.Repository.Name + " head=" + value.HeadOwner + "/" + value.HeadRepository + ":" + value.HeadRef + " oid=" + value.HeadOID + " base=" + value.BaseRef + " -->"
}

func fakeOID(value string) bool {
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
