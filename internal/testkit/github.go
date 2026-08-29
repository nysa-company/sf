package testkit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

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

// PullRequest is the durable fake-remote record. Identity intentionally
// includes head repository/ref/OID and the sf ownership bit; headRefName alone
// is never enough to adopt or merge a PR.
type PullRequest struct {
	Identity contracts.PullRequestIdentity `json:"identity"`
	Title    string                        `json:"title"`
	Body     string                        `json:"body"`
	Draft    bool                          `json:"draft"`
	Merged   bool                          `json:"merged"`
	Ready    bool                          `json:"ready"`
}

type Mutation struct {
	Operation string `json:"operation"`
	Number    int    `json:"number,omitempty"`
	HeadOID   string `json:"head_oid,omitempty"`
}

// FakeGHState is JSON-serializable so a fake-gh subprocess can share it with
// an integration test. Mutations are recorded separately from response
// deliveries: a dropped response still leaves one applied remote mutation.
type FakeGHState struct {
	Schema          string                            `json:"schema"`
	Authenticated   bool                              `json:"authenticated"`
	Repository      contracts.RepositoryIdentity      `json:"repository"`
	NextPR          int                               `json:"next_pr"`
	PRs             []PullRequest                     `json:"prs"`
	Checks          map[int][]contracts.RequiredCheck `json:"checks"`
	ResponseScripts map[string][]ResponseMode         `json:"response_scripts,omitempty"`
	Mutations       []Mutation                        `json:"mutations"`
	Deliveries      []string                          `json:"deliveries"`
}

func NewFakeGH(path string, repository contracts.RepositoryIdentity) (*FakeGH, error) {
	if path == "" {
		return nil, errors.New("testkit: fake-gh state path is required")
	}
	state := FakeGHState{
		Schema:          "sf.testkit.fake-gh/v1",
		Repository:      repository,
		NextPR:          1,
		Checks:          make(map[int][]contracts.RequiredCheck),
		ResponseScripts: make(map[string][]ResponseMode),
	}
	f := &FakeGH{path: path, state: state}
	if err := f.save(); err != nil {
		return nil, err
	}
	return f, nil
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
	return &FakeGH{path: path, state: state}, nil
}

type FakeGH struct {
	mu    sync.Mutex
	path  string
	state FakeGHState
}

var _ contracts.GitHub = (*FakeGH)(nil)

func (f *FakeGH) Snapshot() FakeGHState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneState(f.state)
}

func (f *FakeGH) SetAuthenticated(value bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state.Authenticated = value
	return f.saveLocked()
}

func (f *FakeGH) SetResponse(operation string, modes ...ResponseMode) error {
	if operation == "" {
		return errors.New("testkit: response operation is required")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state.ResponseScripts[operation] = append([]ResponseMode(nil), modes...)
	return f.saveLocked()
}

func (f *FakeGH) SetChecks(number int, checks ...contracts.RequiredCheck) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state.Checks[number] = append([]contracts.RequiredCheck(nil), checks...)
	return f.saveLocked()
}

func (f *FakeGH) MutationCount(operation string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, mutation := range f.state.Mutations {
		if mutation.Operation == operation {
			count++
		}
	}
	return count
}

func (f *FakeGH) DeliveryCount(operation string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, delivery := range f.state.Deliveries {
		if delivery == operation {
			count++
		}
	}
	return count
}

func (f *FakeGH) Name() string { return "fake-gh" }

func (f *FakeGH) AuthStatus(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.state.Authenticated {
		return errors.New("fake-gh: authentication required")
	}
	return nil
}

func (f *FakeGH) Repository(_ context.Context, identity contracts.RepositoryIdentity) (contracts.RepositoryIdentity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !sameRepository(identity, f.state.Repository) {
		return contracts.RepositoryIdentity{}, errors.New("fake-gh: repository identity mismatch")
	}
	return f.state.Repository, nil
}

func (f *FakeGH) FindPullRequest(_ context.Context, want contracts.PullRequestIdentity) (contracts.PullRequestIdentity, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, pr := range f.state.PRs {
		if identityMatches(pr.Identity, want) {
			return pr.Identity, true, nil
		}
	}
	return contracts.PullRequestIdentity{}, false, nil
}

func (f *FakeGH) CreateDraftPullRequest(_ context.Context, identity contracts.PullRequestIdentity, title, body, _ string) (contracts.PullRequestIdentity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.requireAuthLocked(); err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	if err := f.validateIdentityLocked(identity); err != nil {
		return contracts.PullRequestIdentity{}, err
	}
	for _, existing := range f.state.PRs {
		if identityMatches(existing.Identity, identity) {
			return contracts.PullRequestIdentity{}, errors.New("fake-gh: matching pull request already exists")
		}
	}
	if mode, err := f.beforeMutationLocked("pr_create"); err != nil {
		return contracts.PullRequestIdentity{}, err
	} else if mode == ResponseErrorBefore {
		f.consumeResponseLocked("pr_create")
		return contracts.PullRequestIdentity{}, errors.New("fake-gh: create failed before mutation")
	}
	if identity.Number == 0 {
		identity.Number = f.state.NextPR
		f.state.NextPR++
	}
	identity.FactoryOwned = true
	f.state.PRs = append(f.state.PRs, PullRequest{Identity: identity, Title: title, Body: body, Draft: true})
	return f.finishMutationLocked("pr_create", identity)
}

func (f *FakeGH) UpdatePullRequest(_ context.Context, identity contracts.PullRequestIdentity, title, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	index, err := f.findLocked(identity)
	if err != nil {
		return err
	}
	if mode, err := f.beforeMutationLocked("pr_edit"); err != nil {
		return err
	} else if mode == ResponseErrorBefore {
		f.consumeResponseLocked("pr_edit")
		return errors.New("fake-gh: edit failed before mutation")
	}
	f.state.PRs[index].Title, f.state.PRs[index].Body = title, body
	f.recordMutationLocked("pr_edit", identity)
	return f.finishErrorLocked("pr_edit")
}

func (f *FakeGH) RequiredChecks(_ context.Context, identity contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	index, err := f.findLocked(identity)
	if err != nil {
		return nil, err
	}
	checks := f.state.Checks[f.state.PRs[index].Identity.Number]
	return append([]contracts.RequiredCheck(nil), checks...), nil
}

func (f *FakeGH) MarkReady(_ context.Context, identity contracts.PullRequestIdentity, _ domain.Fence) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	index, err := f.findLocked(identity)
	if err != nil {
		return err
	}
	if mode, err := f.beforeMutationLocked("pr_ready"); err != nil {
		return err
	} else if mode == ResponseErrorBefore {
		f.consumeResponseLocked("pr_ready")
		return errors.New("fake-gh: ready failed before mutation")
	}
	f.state.PRs[index].Draft, f.state.PRs[index].Ready = false, true
	f.recordMutationLocked("pr_ready", identity)
	return f.finishErrorLocked("pr_ready")
}

func (f *FakeGH) MergeExactHead(_ context.Context, identity contracts.PullRequestIdentity, headOID, method string, _ domain.Fence) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	index, err := f.findLocked(identity)
	if err != nil {
		return err
	}
	if headOID == "" || headOID != f.state.PRs[index].Identity.HeadOID || headOID != identity.HeadOID {
		return errors.New("fake-gh: exact reviewed head mismatch")
	}
	if method != "merge" && method != "squash" && method != "rebase" {
		return errors.New("fake-gh: unsupported merge method")
	}
	if mode, err := f.beforeMutationLocked("pr_merge"); err != nil {
		return err
	} else if mode == ResponseErrorBefore {
		f.consumeResponseLocked("pr_merge")
		return errors.New("fake-gh: merge failed before mutation")
	}
	f.state.PRs[index].Merged = true
	f.recordMutationLocked("pr_merge", identity)
	return f.finishErrorLocked("pr_merge")
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
	if want.FactoryOwned && !actual.FactoryOwned {
		return false
	}
	return true
}

func sameRepository(a, b contracts.RepositoryIdentity) bool {
	return a.Host == b.Host && a.Owner == b.Owner && a.Name == b.Name
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
		_ = f.saveLocked()
	}
}

func (f *FakeGH) recordMutationLocked(operation string, identity contracts.PullRequestIdentity) {
	f.state.Mutations = append(f.state.Mutations, Mutation{Operation: operation, Number: identity.Number, HeadOID: identity.HeadOID})
	_ = f.saveLocked()
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
	if err := f.saveLocked(); err != nil {
		return contracts.PullRequestIdentity{}, err
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
	if err := f.saveLocked(); err != nil {
		return err
	}
	if mode == ResponseDropAfterCall || mode == ResponseErrorAfter {
		return errors.New("fake-gh: response lost after mutation")
	}
	return nil
}

func (f *FakeGH) save() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saveLocked()
}

func (f *FakeGH) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(f.path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(f.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
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
	if len(argv) >= 2 && argv[0] == "auth" && argv[1] == "status" {
		return []byte("authenticated\n"), f.AuthStatus(context.Background())
	}
	if len(argv) >= 2 && argv[0] == "repo" && argv[1] == "view" {
		return f.runRepoView(argv)
	}
	if len(argv) >= 2 && argv[0] == "pr" {
		switch argv[1] {
		case "create":
			return f.runCreate(argv)
		case "view":
			return f.runView(argv)
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
	pr, err := f.CreateDraftPullRequest(context.Background(), identity, option(argv, "--title"), option(argv, "--body"), "")
	if err != nil {
		return nil, err
	}
	return json.Marshal(prJSON(pr, true, false, false))
}

func (f *FakeGH) runView(argv []string) ([]byte, error) {
	number := prNumber(argv)
	identity := identityFromArgs(argv, number)
	f.mu.Lock()
	index, err := f.findLocked(identity)
	if err != nil {
		f.mu.Unlock()
		return nil, err
	}
	pr := f.state.PRs[index]
	f.mu.Unlock()
	return json.Marshal(prJSON(pr.Identity, pr.Draft, pr.Merged, pr.Ready))
}

func (f *FakeGH) runEdit(argv []string) ([]byte, error) {
	number := prNumber(argv)
	identity := identityFromArgs(argv, number)
	if err := f.UpdatePullRequest(context.Background(), identity, option(argv, "--title"), option(argv, "--body")); err != nil {
		return nil, err
	}
	return []byte("{}"), nil
}

func (f *FakeGH) runReady(argv []string) ([]byte, error) {
	identity := identityFromArgs(argv, prNumber(argv))
	if err := f.MarkReady(context.Background(), identity, domain.Fence{}); err != nil {
		return nil, err
	}
	return []byte("{}"), nil
}

func (f *FakeGH) runChecks(argv []string) ([]byte, error) {
	identity := identityFromArgs(argv, prNumber(argv))
	checks, err := f.RequiredChecks(context.Background(), identity)
	if err != nil {
		return nil, err
	}
	return json.Marshal(checks)
}

func (f *FakeGH) runMerge(argv []string) ([]byte, error) {
	identity := identityFromArgs(argv, prNumber(argv))
	headOID := option(argv, "--match-head-commit")
	identity.HeadOID = headOID
	if err := f.MergeExactHead(context.Background(), identity, headOID, mergeMethod(argv), domain.Fence{}); err != nil {
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

func prJSON(identity contracts.PullRequestIdentity, draft, merged, ready bool) map[string]any {
	return map[string]any{
		"number":         identity.Number,
		"repository":     map[string]string{"nameWithOwner": identity.Repository.Owner + "/" + identity.Repository.Name},
		"headRepository": map[string]string{"nameWithOwner": identity.HeadOwner + "/" + identity.HeadRepository},
		"headRefName":    identity.HeadRef,
		"headRefOid":     identity.HeadOID,
		"baseRefName":    identity.BaseRef,
		"isDraft":        draft,
		"sfOwned":        identity.FactoryOwned,
		"merged":         merged,
		"ready":          ready,
	}
}
