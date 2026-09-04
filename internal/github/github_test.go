package github

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

type verifierFunc func(context.Context, contracts.RepositoryIdentity, string, string, string) (contracts.ProtectedBranchObservation, error)

type mutationGuardFunc func(context.Context, domain.ExternalEffectClaim, func(context.Context) ([]byte, error)) ([]byte, error)

// supervisedFakeRunner is intentionally a real command runner rather than a
// canned Client method: the integration test below exercises the same bounded
// argv/env/process seam that production composition uses. Its cleanup proof
// models a supervisor that has drained every fake-gh child before the next
// mutation handoff.
type supervisedFakeRunner struct{}

func (supervisedFakeRunner) Run(ctx context.Context, binary string, args, env []string) ([]byte, error) {
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = env
	return command.Output()
}
func (supervisedFakeRunner) Cleanup(context.Context) (CleanupProof, error) {
	return CleanupProof{Drained: true}, nil
}

type contradictoryCleanupRunner struct{}

func (contradictoryCleanupRunner) Run(context.Context, string, []string, []string) ([]byte, error) {
	return []byte(`{}`), nil
}
func (contradictoryCleanupRunner) Cleanup(context.Context) (CleanupProof, error) {
	return CleanupProof{Drained: true, Quarantined: true}, nil
}

type intentRecorderFunc func(context.Context, domain.MergeIntent) error

func (f intentRecorderFunc) RecordMergeIntent(ctx context.Context, intent domain.MergeIntent) error {
	return f(ctx, intent)
}
func (f intentRecorderFunc) RecordGuardedMergeObservation(context.Context, domain.MergeIntent, contracts.PublishedPullRequestObservation) error {
	return nil
}

type cleanupQuarantinerFunc func(context.Context) error

func (f cleanupQuarantinerFunc) QuarantineExternalMutations(ctx context.Context) error { return f(ctx) }
func (f cleanupQuarantinerFunc) ExternalMutationsQuarantined(context.Context) (bool, error) {
	return false, nil
}

func (f mutationGuardFunc) RunExternalMutation(ctx context.Context, claim domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
	return f(ctx, claim, start)
}

func (f verifierFunc) VerifyProtectedBranch(ctx context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
	return f(ctx, repository, baseRef, mergeCommit, originalBaseOID)
}

func fixture(t *testing.T) (*Client, *testkit.FakeGH, contracts.PullRequestIdentity) {
	t.Helper()
	root := t.TempDir()
	state := filepath.Join(root, "fake-gh.json")
	repository := contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}
	fake, err := testkit.NewFakeGH(state, repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "fake-gh")
	command := exec.Command("go", "build", "-o", binary, "./cmd/fake-gh")
	command.Dir = filepath.Join("..", "..")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build fake-gh: %v\n%s", err, output)
	}
	client := &Client{binaryPath: binary, home: filepath.Join(root, "home"), configDir: filepath.Join(root, "gh-config"), env: []string{"SF_FAKE_GH_STATE=" + state}, runner: commandRunnerFunc(runBounded), validateClaimFn: func(context.Context, domain.ExternalEffectClaim) error { return nil }, mutationGuard: fixtureGuard(), mergeIntents: intentRecorderFunc(func(context.Context, domain.MergeIntent) error { return nil }), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), verifyProtectedBranch: verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
		return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
	})}
	identity := contracts.PullRequestIdentity{Repository: repository, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", BaseOID: strings.Repeat("c", 40), FactoryOwned: true}
	return client, fake, identity
}

func testClaim(kind string, identity contracts.PullRequestIdentity, values ...string) domain.ExternalEffectClaim {
	if kind == "merge" && identity.BaseOID == "" {
		identity.BaseOID = strings.Repeat("c", 40)
	}
	if kind == "merge" && len(values) == 2 {
		values = append(values, strings.Repeat("c", 40), strings.Repeat("c", 40), strings.Repeat("c", 40), strings.Repeat("c", 40))
	}
	return domain.ExternalEffectClaim{
		SemanticKey:   "test-" + kind,
		Ref:           domain.TicketRef{Channel: "dev", Project: "example", Ticket: "SF-44"},
		Kind:          kind,
		RequestDigest: requestDigest(kind, identity, values...),
		TicketVersion: 1,
		LeaderEpoch:   1,
		RunnerEpoch:   1,
		ClaimEpoch:    1,
	}
}

func testAuthorization(identity contracts.PullRequestIdentity) domain.MergeAuthorization {
	base := identity.BaseOID
	if base == "" {
		base = strings.Repeat("c", 40)
	}
	return domain.MergeAuthorization{ReviewedHead: identity.HeadOID, CurrentHead: identity.HeadOID, ReviewedBaseSHA: base, CurrentBaseSHA: base, ReviewedBaseHeadOID: base, CurrentBaseHeadOID: base, Approved: true, GatesGreen: true}
}

func createDraft(t *testing.T, client *Client, identity contracts.PullRequestIdentity, title, body string) PRMatch {
	t.Helper()
	claim := testClaim("draft_pr", identity, title, body)
	created, err := client.CreateDraftPullRequest(context.Background(), claim, identity, title, body)
	if err != nil {
		t.Fatal(err)
	}
	match, err := client.Observe(context.Background(), created)
	if err != nil {
		t.Fatal(err)
	}
	return match
}

func TestRefreshFactoryPullRequestIdentityAcceptsOldMarkerAfterHeadCorrection(t *testing.T) {
	client, fake, identity := fixture(t)
	prior := createDraft(t, client, identity, "title", "body").Identity
	expected := prior
	expected.HeadOID = strings.Repeat("b", 40)
	if err := fake.SetPullRequestHeadOIDForTest(prior.Number, expected.HeadOID); err != nil {
		t.Fatal(err)
	}
	before := fake.Snapshot().Mutations
	got, err := client.RefreshFactoryPullRequestIdentity(context.Background(), prior, expected)
	if err != nil {
		t.Fatalf("refresh old marker: %v", err)
	}
	if !sameExact(got, expected) || got.Number != prior.Number {
		t.Fatalf("refresh identity=%+v expected=%+v", got, expected)
	}
	after := fake.Snapshot().Mutations
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("read-only refresh mutated fake github: before=%+v after=%+v", before, after)
	}
}

func TestUpdateFactoryPullRequestAcceptsPriorMarkerAndObservesExactOutput(t *testing.T) {
	client, fake, identity := fixture(t)
	prior := createDraft(t, client, identity, "before", "before body").Identity
	expected := prior
	expected.HeadOID = strings.Repeat("b", 40)
	if err := fake.SetPullRequestHeadOIDForTest(prior.Number, expected.HeadOID); err != nil {
		t.Fatal(err)
	}
	claim := testClaim("pr_edit", expected, "after", "after body")
	if err := client.UpdateFactoryPullRequest(context.Background(), claim, prior, expected, "after", "after body"); err != nil {
		t.Fatalf("correction update=%v", err)
	}
	got, state, draft, applied, err := client.ObserveFactoryPullRequestUpdate(context.Background(), prior, expected, "after", "after body")
	if err != nil || !applied || state != "OPEN" || !draft || !sameExact(got, expected) {
		t.Fatalf("correction output got=%+v state=%q draft=%v applied=%v err=%v", got, state, draft, applied, err)
	}
	got, state, draft, applied, err = client.ObserveFactoryPullRequestOutput(context.Background(), expected, "after", "after body")
	if err != nil || !applied || state != "OPEN" || !draft || !sameExact(got, expected) {
		t.Fatalf("published output got=%+v state=%q draft=%v applied=%v err=%v", got, state, draft, applied, err)
	}
	if err := fake.SetPullRequestTextForTest(expected.Number, "foreign title", "after body\n\n"+ownershipMarker(expected)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, applied, err = client.ObserveFactoryPullRequestOutput(context.Background(), expected, "after", "after body"); err != nil || applied {
		t.Fatalf("foreign output applied=%v err=%v", applied, err)
	}
}

func TestUpdateFactoryPullRequestReconcilesLostResponseWithoutSecondEdit(t *testing.T) {
	client, fake, identity := fixture(t)
	prior := createDraft(t, client, identity, "before", "before body").Identity
	expected := prior
	expected.HeadOID = strings.Repeat("b", 40)
	if err := fake.SetPullRequestHeadOIDForTest(prior.Number, expected.HeadOID); err != nil {
		t.Fatal(err)
	}
	if err := fake.SetResponse("pr_edit", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	claim := testClaim("pr_edit", expected, "after", "after body")
	if err := client.UpdateFactoryPullRequest(context.Background(), claim, prior, expected, "after", "after body"); err != nil {
		t.Fatalf("lost correction update=%v", err)
	}
	if err := client.UpdateFactoryPullRequest(context.Background(), claim, prior, expected, "after", "after body"); err != nil || fake.MutationCount("pr_edit") != 1 {
		t.Fatalf("replay err=%v mutations=%d", err, fake.MutationCount("pr_edit"))
	}
}

func TestUpdateFactoryPullRequestPreservesBeforeStartAfterCleanAbsence(t *testing.T) {
	client, fake, identity := fixture(t)
	prior := createDraft(t, client, identity, "before", "before body").Identity
	expected := prior
	expected.HeadOID = strings.Repeat("b", 40)
	if err := fake.SetPullRequestHeadOIDForTest(prior.Number, expected.HeadOID); err != nil {
		t.Fatal(err)
	}
	// The guard documents that it never invoked the mutation callback. The
	// post-attempt exact observation remains stale, but that is clean absence
	// of an edit rather than a reason to upgrade a proven pre-start refusal.
	client.mutationGuard = mutationGuardFunc(func(context.Context, domain.ExternalEffectClaim, func(context.Context) ([]byte, error)) ([]byte, error) {
		return nil, errors.New("worker unavailable before launch")
	})
	claim := testClaim("pr_edit", expected, "after", "after body")
	if err := client.UpdateFactoryPullRequest(context.Background(), claim, prior, expected, "after", "after body"); !errors.Is(err, ErrUpdateBeforeStart) {
		t.Fatalf("clean absence reclassified before-start update: %v", err)
	}
	if got := fake.MutationCount("pr_edit"); got != 0 {
		t.Fatalf("pre-start update mutated %d times", got)
	}
}

func TestUpdateFactoryPostHandoffHeadMoveRemainsUncertainWithoutReplay(t *testing.T) {
	client, fake, identity := fixture(t)
	prior := createDraft(t, client, identity, "before", "before body").Identity
	expected := prior
	expected.HeadOID = strings.Repeat("b", 40)
	if err := fake.SetPullRequestHeadOIDForTest(prior.Number, expected.HeadOID); err != nil {
		t.Fatal(err)
	}
	moved := false
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if len(args) >= 2 && args[0] == "pr" && args[1] == "edit" {
			moved = true
			if err := fake.SetPullRequestHeadOIDForTest(prior.Number, strings.Repeat("d", 40)); err != nil {
				return nil, err
			}
			return nil, errors.New("server result unavailable after dispatch")
		}
		return fake.Run(args)
	})
	claim := testClaim("pr_edit", expected, "after", "after body")
	if err := client.UpdateFactoryPullRequest(context.Background(), claim, prior, expected, "after", "after body"); !errors.Is(err, ErrUpdateUncertain) {
		t.Fatalf("post-handoff head move err=%v", err)
	}
	if !moved || fake.MutationCount("pr_edit") != 0 {
		t.Fatalf("post-handoff edit moved=%v mutations=%d", moved, fake.MutationCount("pr_edit"))
	}
	if err := client.UpdateFactoryPullRequest(context.Background(), claim, prior, expected, "after", "after body"); err == nil || fake.MutationCount("pr_edit") != 0 {
		t.Fatalf("unreconciled edit replay err=%v mutations=%d", err, fake.MutationCount("pr_edit"))
	}
}

func TestRefreshFactoryPullRequestIdentityRefusals(t *testing.T) {
	prior := contracts.PullRequestIdentity{Number: 7, Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", BaseOID: strings.Repeat("c", 40), FactoryOwned: true}
	expected := prior
	expected.HeadOID = strings.Repeat("b", 40)
	wire := func(identity contracts.PullRequestIdentity, body, state string, merged bool) map[string]any {
		return map[string]any{
			"number": identity.Number, "title": "title", "body": body,
			"headRepositoryOwner": map[string]string{"login": identity.HeadOwner},
			"headRepository":      map[string]string{"nameWithOwner": identity.HeadOwner + "/" + identity.HeadRepository},
			"headRefName":         identity.HeadRef, "headRefOid": identity.HeadOID, "baseRefName": identity.BaseRef, "baseRefOid": strings.Repeat("c", 40),
			"isDraft": false, "mergedAt": map[bool]any{true: "2026-01-01T00:00:00Z", false: nil}[merged], "mergeCommit": nil,
			"state": state, "mergeStateStatus": "CLEAN", "autoMergeRequest": nil,
		}
	}
	payload := func(values ...map[string]any) []byte {
		encoded, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		return encoded
	}
	newMarker := ownershipMarker(expected)
	oldMarker := ownershipMarker(prior)
	cases := []struct {
		name            string
		prior, expected contracts.PullRequestIdentity
		response        []byte
		want            error
	}{
		{"new-marker-replay", prior, expected, payload(wire(expected, newMarker, "OPEN", false)), nil},
		{"zero", prior, expected, payload(), ErrNoMatchingPR},
		{"marker-substituted", prior, expected, payload(wire(expected, ownershipMarker(func() contracts.PullRequestIdentity { x := expected; x.HeadRef = "foreign"; return x }()), "OPEN", false)), ErrNoMatchingPR},
		{"closed", prior, expected, payload(wire(expected, oldMarker, "CLOSED", false)), ErrNoMatchingPR},
		{"merged", prior, expected, payload(wire(expected, oldMarker, "MERGED", true)), ErrNoMatchingPR},
		{"multiple", prior, expected, payload(wire(expected, oldMarker, "OPEN", false), wire(expected, newMarker, "OPEN", false)), ErrAmbiguousPR},
		{"foreign-same-source", prior, expected, payload(wire(func() contracts.PullRequestIdentity { x := expected; x.Number = 8; return x }(), newMarker, "OPEN", false), wire(expected, oldMarker, "OPEN", false)), ErrAmbiguousPR},
		{"second-open-source-different-base", prior, expected, payload(wire(expected, oldMarker, "OPEN", false), wire(func() contracts.PullRequestIdentity { x := expected; x.Number = 8; x.BaseRef = "release"; return x }(), newMarker, "OPEN", false)), ErrAmbiguousPR},
		{"response-source-changed", prior, expected, payload(wire(func() contracts.PullRequestIdentity { x := expected; x.HeadRef = "other"; return x }(), oldMarker, "OPEN", false)), ErrNoMatchingPR},
		{"response-base-changed", prior, expected, payload(wire(func() contracts.PullRequestIdentity { x := expected; x.BaseRef = "release"; return x }(), oldMarker, "OPEN", false)), ErrNoMatchingPR},
		{"changed-source", prior, func() contracts.PullRequestIdentity { x := expected; x.HeadRef = "other"; return x }(), nil, ErrPolicyRefusal},
		{"changed-base", prior, func() contracts.PullRequestIdentity { x := expected; x.BaseRef = "release"; return x }(), nil, ErrPolicyRefusal},
		{"changed-number", prior, func() contracts.PullRequestIdentity { x := expected; x.Number = 8; return x }(), nil, ErrPolicyRefusal},
		{"changed-repository", prior, func() contracts.PullRequestIdentity { x := expected; x.Repository.Name = "other"; return x }(), nil, ErrPolicyRefusal},
		{"missing-oid", prior, func() contracts.PullRequestIdentity { x := expected; x.HeadOID = ""; return x }(), nil, ErrPolicyRefusal},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var calls [][]string
			client := refreshTestClient(t, test.response, &calls)
			got, err := client.RefreshFactoryPullRequestIdentity(context.Background(), test.prior, test.expected)
			if test.want == nil {
				if err != nil || !sameExact(got, expected) {
					t.Fatalf("refresh got=%+v err=%v", got, err)
				}
			} else if !errors.Is(err, test.want) {
				t.Fatalf("refresh err=%v want %v", err, test.want)
			}
			if test.response == nil {
				if len(calls) != 0 {
					t.Fatalf("invalid caller identity launched command: %#v", calls)
				}
				return
			}
			wantArgs := []string{"pr", "list", "--repo", "example/app", "--state", "all", "--limit", "100", "--json", prFields}
			if !reflect.DeepEqual(calls, [][]string{wantArgs}) {
				t.Fatalf("refresh calls=%#v want only read list %#v", calls, wantArgs)
			}
		})
	}

	t.Run("full-page", func(t *testing.T) {
		values := make([]map[string]any, 100)
		for index := range values {
			identity := expected
			identity.Number = index + 1
			values[index] = wire(identity, newMarker, "OPEN", false)
		}
		var calls [][]string
		_, err := refreshTestClient(t, payload(values...), &calls).RefreshFactoryPullRequestIdentity(context.Background(), prior, expected)
		if !errors.Is(err, ErrAmbiguousPR) || len(calls) != 1 {
			t.Fatalf("full-page err=%v calls=%#v", err, calls)
		}
	})

	t.Run("malformed-and-unknown", func(t *testing.T) {
		for _, response := range [][]byte{[]byte(`[{"number":7}]`), []byte(`[{"number":7,"unexpected":true}]`)} {
			var calls [][]string
			_, err := refreshTestClient(t, response, &calls).RefreshFactoryPullRequestIdentity(context.Background(), prior, expected)
			if !errors.Is(err, ErrMalformedResponse) || len(calls) != 1 {
				t.Fatalf("response=%s err=%v calls=%#v", response, err, calls)
			}
		}
	})
}

func TestObservePublishedPullRequestAcceptsMergedBaseMovementAndRejectsIdentityDrift(t *testing.T) {
	identity := contracts.PullRequestIdentity{Number: 7, Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", BaseOID: strings.Repeat("c", 40), FactoryOwned: true}
	wire := func(value contracts.PullRequestIdentity, marker string, state string, merged bool) []byte {
		body := marker
		var mergeCommit any
		if merged {
			mergeCommit = map[string]string{"oid": strings.Repeat("e", 40)}
		}
		payload := map[string]any{"number": value.Number, "title": "title", "body": body,
			"headRepositoryOwner": map[string]string{"login": value.HeadOwner}, "headRepository": map[string]string{"nameWithOwner": value.HeadOwner + "/" + value.HeadRepository},
			"headRefName": value.HeadRef, "headRefOid": value.HeadOID, "baseRefName": value.BaseRef, "baseRefOid": strings.Repeat("d", 40), "isDraft": false,
			"mergedAt": map[bool]any{true: "2026-01-01T00:00:00Z", false: nil}[merged], "mergeCommit": mergeCommit, "state": state, "mergeStateStatus": "CLEAN", "autoMergeRequest": nil}
		encoded, _ := json.Marshal([]map[string]any{payload})
		return encoded
	}
	mergedResponse := wire(identity, ownershipMarker(identity), "MERGED", true)
	var calls [][]string
	got, err := refreshTestClient(t, mergedResponse, &calls).ObservePublishedPullRequest(context.Background(), identity)
	if err != nil || !got.Merged || got.State != "MERGED" || got.Draft || got.Identity.BaseOID == identity.BaseOID {
		t.Fatalf("merged observation=%+v err=%v", got, err)
	}
	for _, state := range []string{"OPEN", "CLOSED"} {
		var calls [][]string
		if _, err := refreshTestClient(t, wire(identity, ownershipMarker(identity), state, false), &calls).ObservePublishedPullRequest(context.Background(), identity); !errors.Is(err, ErrNoMatchingPR) {
			t.Fatalf("unmerged %s BaseOID drift err=%v", state, err)
		}
	}
	for _, malformed := range []struct {
		name   string
		state  string
		merged bool
	}{
		{name: "merged state without timestamp", state: "MERGED"},
		{name: "timestamp without merged state", state: "CLOSED", merged: true},
		{name: "malformed merged timestamp", state: "MERGED", merged: true},
	} {
		t.Run(malformed.name, func(t *testing.T) {
			var calls [][]string
			response := wire(identity, ownershipMarker(identity), malformed.state, malformed.merged)
			if malformed.name == "malformed merged timestamp" {
				response = []byte(strings.Replace(string(response), "2026-01-01T00:00:00Z", "not-a-timestamp", 1))
			}
			if _, err := refreshTestClient(t, response, &calls).ObservePublishedPullRequest(context.Background(), identity); !errors.Is(err, ErrMalformedResponse) {
				t.Fatalf("inconsistent merge witness err=%v", err)
			}
		})
	}
	var malformedBaseCalls [][]string
	malformedBase := []byte(strings.Replace(string(mergedResponse), "\"baseRefOid\":\"dddddddddddddddddddddddddddddddddddddddd\"", "\"baseRefOid\":\"not-an-oid\"", 1))
	if _, err := refreshTestClient(t, malformedBase, &malformedBaseCalls).ObservePublishedPullRequest(context.Background(), identity); !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("malformed merged BaseOID err=%v", err)
	}
	for name, wrong := range map[string]contracts.PullRequestIdentity{
		"wrong-number": func() contracts.PullRequestIdentity { value := identity; value.Number = 8; return value }(),
		"wrong-source": func() contracts.PullRequestIdentity { value := identity; value.HeadRef = "foreign"; return value }(),
		"wrong-base":   func() contracts.PullRequestIdentity { value := identity; value.BaseRef = "release"; return value }(),
		"wrong-head": func() contracts.PullRequestIdentity {
			value := identity
			value.HeadOID = strings.Repeat("b", 40)
			return value
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			var calls [][]string
			if _, err := refreshTestClient(t, wire(wrong, ownershipMarker(wrong), "MERGED", true), &calls).ObservePublishedPullRequest(context.Background(), identity); err == nil {
				t.Fatalf("wrong identity err=%v", err)
			}
		})
	}
	var callsMarker [][]string
	if _, err := refreshTestClient(t, wire(identity, "<!-- foreign -->", "MERGED", true), &callsMarker).ObservePublishedPullRequest(context.Background(), identity); !errors.Is(err, ErrNoMatchingPR) {
		t.Fatalf("wrong marker err=%v", err)
	}
}

func TestObserveExternalMergeAcceptsPostPublicationHeadDriftOnlyWithHistoricalMarker(t *testing.T) {
	want := contracts.PullRequestIdentity{Number: 7, Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", BaseOID: strings.Repeat("c", 40), FactoryOwned: true}
	observed := want
	observed.HeadOID = strings.Repeat("b", 40)
	payload := map[string]any{"number": observed.Number, "title": "title", "body": ownershipMarker(want),
		"headRepositoryOwner": map[string]string{"login": observed.HeadOwner}, "headRepository": map[string]string{"nameWithOwner": observed.HeadOwner + "/" + observed.HeadRepository},
		"headRefName": observed.HeadRef, "headRefOid": observed.HeadOID, "baseRefName": observed.BaseRef, "baseRefOid": strings.Repeat("d", 40), "isDraft": false,
		"mergedAt": "2026-01-01T00:00:00Z", "mergeCommit": map[string]string{"oid": strings.Repeat("e", 40)}, "state": "MERGED", "mergeStateStatus": "CLEAN", "autoMergeRequest": nil}
	response, _ := json.Marshal([]map[string]any{payload})
	var calls [][]string
	got, err := refreshTestClient(t, response, &calls).ObserveExternalMerge(context.Background(), want)
	if err != nil || !got.Merged || got.Identity.HeadOID != observed.HeadOID || got.Identity.Number != want.Number {
		t.Fatalf("external merge observation=%+v err=%v", got, err)
	}
	var markerCalls [][]string
	payload["body"] = "<!-- foreign -->"
	response, _ = json.Marshal([]map[string]any{payload})
	if _, err := refreshTestClient(t, response, &markerCalls).ObserveExternalMerge(context.Background(), want); !errors.Is(err, ErrNoMatchingPR) {
		t.Fatalf("missing historical marker err=%v", err)
	}
}

func refreshTestClient(t *testing.T, response []byte, calls *[][]string) *Client {
	t.Helper()
	return &Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		*calls = append(*calls, append([]string(nil), args...))
		if !reflect.DeepEqual(args, []string{"pr", "list", "--repo", "example/app", "--state", "all", "--limit", "100", "--json", prFields}) {
			return nil, errors.New("unexpected command")
		}
		return response, nil
	})}
}

func TestStoreClaimGuardAndClientComposeExactHeadFlow(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	database, err := store.Open(ctx, filepath.Join(root, "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "example", Ticket: "SF-44"}
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "example", Path: root, BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "integration", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "github-integration")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/example/SF-44/publish", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	repository := contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}
	state := filepath.Join(root, "fake-gh.json")
	fake, err := testkit.NewFakeGH(state, repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "fake-gh")
	build := exec.Command("go", "build", "-o", binary, "./cmd/fake-gh")
	build.Dir = filepath.Join("..", "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build fake-gh: %v\n%s", err, output)
	}
	verified := false
	client, err := NewStoreClient(binary, filepath.Join(root, "home"), filepath.Join(root, "gh-config"), supervisedFakeRunner{}, database, verifierFunc(func(_ context.Context, gotRepository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
		verified = gotRepository == repository && baseRef == "main" && originalBaseOID == strings.Repeat("c", 40)
		return contracts.ProtectedBranchObservation{Repository: gotRepository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	client.env = []string{"SF_FAKE_GH_STATE=" + state}
	identity := contracts.PullRequestIdentity{Repository: repository, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", BaseOID: strings.Repeat("c", 40), FactoryOwned: true}
	claim := func(kind, digest string) domain.ExternalEffectClaim {
		fence := store.EffectFence{SemanticKey: "integration/" + kind, Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}
		if _, err := database.PlanEffect(ctx, store.EffectPlan{SemanticKey: fence.SemanticKey, Ref: ref, Kind: kind, TicketVersion: fence.TicketVersion, Fence: fence.Fence, RequestDigest: digest}); err != nil {
			t.Fatal(err)
		}
		claimed, err := database.ClaimEffect(ctx, fence)
		if err != nil || !claimed.Claimed {
			t.Fatalf("claim %s: %+v err=%v", kind, claimed, err)
		}
		return claimed.ExternalClaim()
	}
	created, err := client.CreateDraftPullRequest(ctx, claim("draft_pr", requestDigest("draft_pr", identity, "title", "body")), identity, "title", "body")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.MarkReady(ctx, claim("pr_ready", requestDigest("pr_ready", created)), created); err != nil {
		t.Fatal(err)
	}
	authorization := testAuthorization(created)
	mergeDigest := requestDigest("merge", created, created.HeadOID, "squash", authorization.ReviewedBaseSHA, authorization.CurrentBaseSHA, authorization.ReviewedBaseHeadOID, authorization.CurrentBaseHeadOID)
	mergeClaim := claim("merge", mergeDigest)
	if err := client.MergeExactHead(ctx, mergeClaim, created, created.HeadOID, "squash", authorization); err != nil {
		t.Fatalf("guarded merge=%v", err)
	}
	intent, found, err := database.MergeIntent(ctx, mergeClaim.SemanticKey)
	if err != nil || !found || intent.OriginalBaseOID != created.BaseOID || !intent.StrictStatusChecks || intent.ProtectionRuleID == "" || fake.MutationCount("pr_merge") != 1 || !verified {
		t.Fatalf("strict guarded intent=%+v found=%v err=%v verified=%v merge mutations=%d", intent, found, err, verified, fake.MutationCount("pr_merge"))
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := store.Open(ctx, filepath.Join(root, "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	persisted, found, err := restarted.MergeIntent(ctx, mergeClaim.SemanticKey)
	if err != nil || !found || persisted.OriginalBaseOID != created.BaseOID || persisted.ProtectionRuleID != intent.ProtectionRuleID || !persisted.StrictStatusChecks {
		t.Fatalf("restart merge intent=%+v found=%v err=%v", persisted, found, err)
	}
}

func TestContractMutationRequiresClaimValidator(t *testing.T) {
	client, _, identity := fixture(t)
	claim := testClaim("draft_pr", identity, "title", "body")
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); err != nil {
		t.Fatalf("validated claim=%v", err)
	}
	client.validateClaimFn = nil
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("missing validator=%v", err)
	}
}

func TestNewClientRejectsMissingAuthoritiesAndLiteralCannotRun(t *testing.T) {
	if _, err := NewClient("/bin/echo", t.TempDir(), t.TempDir(), commandRunnerFunc(func(context.Context, string, []string, []string) ([]byte, error) {
		return nil, nil
	}), func(context.Context, domain.ExternalEffectClaim) error { return nil }, mutationGuardFunc(func(context.Context, domain.ExternalEffectClaim, func(context.Context) ([]byte, error)) ([]byte, error) {
		return nil, nil
	}), verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
		return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
	}), intentRecorderFunc(func(context.Context, domain.MergeIntent) error { return nil }), cleanupQuarantinerFunc(func(context.Context) error { return nil })); err != nil {
		t.Fatalf("valid client rejected: %v", err)
	}
	client := Client{}
	if err := client.AuthStatus(context.Background()); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("literal err=%v", err)
	}
}

func TestAuthStatusAcceptsOfficialHostsStateShape(t *testing.T) {
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		want := []string{"auth", "status", "--json", "hosts"}
		if !reflect.DeepEqual(args, want) {
			return nil, errors.New("unexpected auth argv")
		}
		return []byte(`{"hosts":{"github.com":[{"state":"success","active":true,"host":"github.com","login":"sf-test","tokenSource":"keyring","scopes":"repo","gitProtocol":"https"}]}}`), nil
	}), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), mutationGuard: mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		return start(ctx)
	}), validateClaimFn: func(context.Context, domain.ExternalEffectClaim) error { return nil }}
	if err := client.AuthStatus(context.Background()); err != nil {
		t.Fatalf("official auth status shape=%v", err)
	}
}

func TestPreflightUsesPositionalRepositoryArg(t *testing.T) {
	var calls [][]string
	client := Client{
		binaryPath: "/bin/echo",
		home:       t.TempDir(),
		configDir:  t.TempDir(),
		runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
			calls = append(calls, append([]string(nil), args...))
			switch len(calls) {
			case 1:
				return []byte(`{"hosts":{"github.com":[{"state":"success","active":true,"host":"github.com","login":"sf-test"}]}}`), nil
			case 2:
				return []byte(`{"nameWithOwner":"example/app","url":"https://github.com/example/app"}`), nil
			default:
				return nil, errors.New("unexpected extra invocation")
			}
		}),
		quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }),
	}
	repository := contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}
	principal, err := client.Preflight(context.Background(), repository)
	if err != nil || principal.Login != "sf-test" {
		t.Fatalf("preflight=%+v err=%v", principal, err)
	}
	want := [][]string{
		{"auth", "status", "--json", "hosts"},
		{"repo", "view", "example/app", "--json", "nameWithOwner,url"},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("argv=%#v want %#v", calls, want)
	}
	for _, arg := range calls[1] {
		if arg == "--repo" {
			t.Fatal("repo view must not use --repo")
		}
	}
}

func TestMergeQueueGraphQLFailsClosedBeforeMerge(t *testing.T) {
	client, _, identity := fixture(t)
	identity.Number = 7
	called := false
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		called = true
		if len(args) < 4 || args[0] != "api" || args[1] != "--hostname" || args[2] != "github.com" || args[3] != "graphql" {
			return nil, errors.New("wrong queue argv")
		}
		return []byte(`{"data":{"repository":{"pullRequest":{"mergeQueueEntry":{"position":1}}}}}`), nil
	})
	queued, err := client.mergeQueued(context.Background(), identity)
	if err != nil || !queued || !called {
		t.Fatalf("queue observation queued=%v called=%v err=%v", queued, called, err)
	}
}

func TestPreflightCreateLostResponseAndExactAdoption(t *testing.T) {
	client, fake, identity := fixture(t)
	principal, err := client.Preflight(context.Background(), identity.Repository)
	if err != nil || principal.Login != "sf-test" {
		t.Fatalf("preflight=%+v err=%v", principal, err)
	}
	claim := testClaim("draft_pr", identity, "title", "<!-- sf:owned -->")
	if err := fake.SetResponse("pr_create", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	created, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "<!-- sf:owned -->")
	pr, observeErr := client.Observe(context.Background(), created)
	if err != nil || observeErr != nil || pr.Identity.Number != 1 || !pr.Draft {
		t.Fatalf("create/adopt=%+v err=%v", pr, err)
	}
	if fake.MutationCount("pr_create") != 1 {
		t.Fatalf("create mutations=%d", fake.MutationCount("pr_create"))
	}
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "<!-- sf:owned -->"); err != nil {
		t.Fatalf("idempotent adopt=%v", err)
	}
	if err := fake.InjectPullRequestForTest(testkit.PullRequest{Identity: pr.Identity, Draft: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Observe(context.Background(), identity); !errors.Is(err, ErrAmbiguousPR) {
		t.Fatalf("ambiguous=%v", err)
	}
}

func TestPublicationInventoryRejectsForeignAndAmbiguousOpenSourceBase(t *testing.T) {
	makePR := func(identity contracts.PullRequestIdentity, owned bool) testkit.PullRequest {
		body := "human"
		if owned {
			body = ownershipMarker(identity)
		}
		identity.FactoryOwned = owned
		return testkit.PullRequest{Identity: identity, Body: body, Title: "title", Draft: true}
	}
	for _, tc := range []struct {
		name string
		prs  func(contracts.PullRequestIdentity) []testkit.PullRequest
		want error
	}{
		{"foreign", func(i contracts.PullRequestIdentity) []testkit.PullRequest {
			i.Number = 7
			return []testkit.PullRequest{makePR(i, false)}
		}, ErrPolicyRefusal},
		{"duplicate owned", func(i contracts.PullRequestIdentity) []testkit.PullRequest {
			a, b := i, i
			a.Number, b.Number = 7, 8
			return []testkit.PullRequest{makePR(a, true), makePR(b, true)}
		}, ErrAmbiguousPR},
		{"owned and foreign", func(i contracts.PullRequestIdentity) []testkit.PullRequest {
			a, b := i, i
			a.Number, b.Number = 7, 8
			return []testkit.PullRequest{makePR(a, true), makePR(b, false)}
		}, ErrPolicyRefusal},
		{"stale owned head", func(i contracts.PullRequestIdentity) []testkit.PullRequest {
			i.Number, i.HeadOID = 7, strings.Repeat("b", 40)
			return []testkit.PullRequest{makePR(i, true)}
		}, ErrPolicyRefusal},
		{"mismatched base oid", func(i contracts.PullRequestIdentity) []testkit.PullRequest {
			i.Number, i.BaseOID = 7, strings.Repeat("d", 40)
			return []testkit.PullRequest{makePR(i, true)}
		}, ErrPolicyRefusal},
		{"different base ref", func(i contracts.PullRequestIdentity) []testkit.PullRequest {
			i.Number, i.BaseRef = 7, "release"
			return []testkit.PullRequest{makePR(i, true)}
		}, ErrPolicyRefusal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, fake, identity := fixture(t)
			for _, pr := range tc.prs(identity) {
				if err := fake.InjectPullRequestForTest(pr); err != nil {
					t.Fatal(err)
				}
			}
			if _, _, err := client.ObservePublicationCandidate(context.Background(), identity); !errors.Is(err, tc.want) {
				t.Fatalf("inventory err=%v", err)
			}
			claim := testClaim("draft_pr", identity, "title", "body")
			if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); !errors.Is(err, tc.want) {
				t.Fatalf("create err=%v", err)
			}
			if fake.MutationCount("pr_create") != 0 {
				t.Fatal("foreign or ambiguous PR permitted create")
			}
		})
	}
}

func TestPublicationInventoryAcceptsDocumentedRepositoryObjectFields(t *testing.T) {
	identity := contracts.PullRequestIdentity{
		Repository:     contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"},
		HeadOwner:      "example",
		HeadRepository: "app",
		HeadRef:        "sf/dev/example/SF-44-random",
		HeadOID:        strings.Repeat("a", 40),
		BaseRef:        "main",
		BaseOID:        strings.Repeat("c", 40),
		FactoryOwned:   true,
	}
	payload, err := json.Marshal([]map[string]any{{
		"number":              7,
		"title":               "title",
		"body":                ownershipMarker(identity),
		"headRepositoryOwner": map[string]any{"id": "O_example", "login": identity.HeadOwner},
		"headRepository":      map[string]any{"id": "R_example", "name": identity.HeadRepository, "nameWithOwner": identity.HeadOwner + "/" + identity.HeadRepository},
		"headRefName":         identity.HeadRef,
		"headRefOid":          identity.HeadOID,
		"baseRefName":         identity.BaseRef,
		"baseRefOid":          identity.BaseOID,
		"isDraft":             true,
		"mergedAt":            nil,
		"mergeCommit":         nil,
		"state":               "OPEN",
		"mergeStateStatus":    "CLEAN",
		"autoMergeRequest":    nil,
	}})
	if err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	match, found, err := refreshTestClient(t, payload, &calls).ObservePublicationCandidate(context.Background(), identity)
	if err != nil || !found || match.Identity.Number != 7 || !sameExact(match.Identity, identity) {
		t.Fatalf("live-shaped publication inventory match=%+v found=%v err=%v", match, found, err)
	}
}

func TestPublicationInventoryRefusesEmptyExpectedBaseWithoutMutation(t *testing.T) {
	client, fake, identity := fixture(t)
	identity.BaseOID = ""
	claim := testClaim("draft_pr", identity, "title", "body")
	if _, _, err := client.ObservePublicationCandidate(context.Background(), identity); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("empty base inventory err=%v", err)
	}
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("empty base create err=%v", err)
	}
	if got := fake.MutationCount("pr_create"); got != 0 {
		t.Fatalf("empty base launched create %d times", got)
	}
}

func TestPublicationInventoryIgnoresClosedAndNonmatchingPRs(t *testing.T) {
	client, fake, identity := fixture(t)
	closed := identity
	closed.Number, closed.FactoryOwned = 7, false
	if err := fake.InjectPullRequestForTest(testkit.PullRequest{Identity: closed, Body: "human", Draft: true, Merged: true}); err != nil {
		t.Fatal(err)
	}
	nonmatching := identity
	nonmatching.Number, nonmatching.HeadRef, nonmatching.FactoryOwned = 8, "sf/dev/other", false
	if err := fake.InjectPullRequestForTest(testkit.PullRequest{Identity: nonmatching, Body: "human", Draft: true}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := client.ObservePublicationCandidate(context.Background(), identity); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
}

func TestFakeCreateExistingIdentityConflictRemainsDeterministic(t *testing.T) {
	for _, test := range []struct {
		name, text string
		owned      bool
	}{
		{name: "human-owned", text: "human-owned", owned: false},
		{name: "factory-owned", text: "matching pull request", owned: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, fake, identity := fixture(t)
			existing := identity
			existing.Number, existing.FactoryOwned = 7, test.owned
			if err := fake.InjectPullRequestForTest(testkit.PullRequest{Identity: existing, Draft: true}); err != nil {
				t.Fatal(err)
			}
			if _, err := fake.CreateDraftPullRequest(context.Background(), testkit.EffectClaimForTest("draft_pr", identity, "title", "body"), identity, "title", "body"); err == nil || !strings.Contains(err.Error(), test.text) || !errors.Is(err, contracts.ErrDraftCreateBeforeStart) || errors.Is(err, contracts.ErrDraftCreateUncertain) {
				t.Fatalf("existing %s conflict err=%v", test.name, err)
			}
			if got := fake.MutationCount("pr_create"); got != 0 {
				t.Fatalf("existing %s launched create %d times", test.name, got)
			}
		})
	}
}

func TestCreateUncertainNeverAttemptsNumberOnlyOrphanClose(t *testing.T) {
	client, _, identity := fixture(t)
	var closed bool
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		switch {
		case len(args) >= 2 && args[0] == "pr" && args[1] == "list":
			return []byte("[]"), nil
		case len(args) >= 2 && args[0] == "api" && args[1] == "repos/example/app/git/ref/heads/main":
			return []byte(`{"object":{"sha":"cccccccccccccccccccccccccccccccccccccccc"}}`), nil
		case len(args) >= 2 && args[0] == "api":
			return []byte(`{"object":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`), nil
		case len(args) >= 2 && args[0] == "pr" && args[1] == "create":
			return []byte("https://github.com/example/app/pull/999\n"), nil
		case len(args) >= 2 && args[0] == "pr" && args[1] == "close":
			closed = true
			return nil, nil
		default:
			return nil, errors.New("unexpected command")
		}
	})
	claim := testClaim("draft_pr", identity, "title", "body")
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); !errors.Is(err, ErrCreateUncertain) {
		t.Fatalf("uncertain create err=%v", err)
	}
	if closed {
		t.Fatal("uncertain create attempted number-only orphan close")
	}
}

func TestCreatePostHandoffBaseMoveRemainsUncertainWithoutReplay(t *testing.T) {
	client, fake, identity := fixture(t)
	moved := false
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if len(args) >= 2 && args[0] == "pr" && args[1] == "create" {
			moved = true
			if err := fake.SetBaseHeadOIDForTest(strings.Repeat("d", 40)); err != nil {
				return nil, err
			}
			return nil, errors.New("server result unavailable after dispatch")
		}
		return fake.Run(args)
	})
	claim := testClaim("draft_pr", identity, "title", "body")
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); !errors.Is(err, ErrCreateUncertain) {
		t.Fatalf("post-handoff base move err=%v", err)
	}
	if !moved || fake.MutationCount("pr_create") != 0 {
		t.Fatalf("post-handoff create moved=%v mutations=%d", moved, fake.MutationCount("pr_create"))
	}
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); !errors.Is(err, ErrCreateBeforeStart) || fake.MutationCount("pr_create") != 0 {
		t.Fatalf("unreconciled create replay err=%v mutations=%d", err, fake.MutationCount("pr_create"))
	}
}

func TestCreateFinalHandoffRefusesIfPRAppearsBeforeLaunch(t *testing.T) {
	client, _, identity := fixture(t)
	created := false
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if len(args) >= 2 && args[0] == "pr" && args[1] == "list" {
			return []byte("[]"), nil
		}
		if len(args) >= 2 && args[0] == "api" && args[1] == "repos/example/app/git/ref/heads/main" {
			return []byte(`{"object":{"sha":"cccccccccccccccccccccccccccccccccccccccc"}}`), nil
		}
		if len(args) >= 2 && args[0] == "api" {
			return []byte(`{"object":{"sha":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}`), nil
		}
		if len(args) >= 2 && args[0] == "pr" && args[1] == "create" {
			created = true
		}
		return []byte("{}"), nil
	})
	claim := testClaim("draft_pr", identity, "title", "body")
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); !errors.Is(err, ErrCreateBeforeStart) {
		t.Fatalf("handoff race err=%v", err)
	}
	if created {
		t.Fatal("create launched after exact in-handoff identity appeared")
	}
}

func TestCreateFinalHandoffRefusesMovedProtectedBase(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*Client, *testkit.FakeGH) error
	}{
		{
			name: "before-handoff",
			prepare: func(_ *Client, fake *testkit.FakeGH) error {
				return fake.SetBaseHeadOIDForTest(strings.Repeat("d", 40))
			},
		},
		{
			name: "inside-handoff",
			prepare: func(client *Client, fake *testkit.FakeGH) error {
				client.mutationGuard = mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
					if err := fake.SetBaseHeadOIDForTest(strings.Repeat("d", 40)); err != nil {
						return nil, err
					}
					return start(ctx)
				})
				return nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, fake, identity := fixture(t)
			if err := test.prepare(client, fake); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CreateDraftPullRequest(context.Background(), testClaim("draft_pr", identity, "title", "body"), identity, "title", "body"); !errors.Is(err, ErrCreateBeforeStart) {
				t.Fatalf("base move err=%v", err)
			}
			if got := fake.MutationCount("pr_create"); got != 0 {
				t.Fatalf("base move launched create %d times", got)
			}
		})
	}
}

func TestCreateFinalHandoffRefusesUnavailableOrMalformedBaseObservation(t *testing.T) {
	for _, test := range []struct {
		name      string
		baseReply []byte
		baseErr   error
	}{
		{name: "unavailable", baseErr: errors.New("base unavailable")},
		{name: "malformed", baseReply: []byte(`{"object":{}}`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, fake, identity := fixture(t)
			created := false
			client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
				switch args[0] + " " + args[1] {
				case "pr list":
					return []byte("[]"), nil
				case "api repos/example/app/git/ref/heads/main":
					return test.baseReply, test.baseErr
				case "pr create":
					created = true
					return []byte("{}"), nil
				default:
					return nil, errors.New("unexpected command")
				}
			})
			if _, err := client.CreateDraftPullRequest(context.Background(), testClaim("draft_pr", identity, "title", "body"), identity, "title", "body"); !errors.Is(err, ErrCreateBeforeStart) {
				t.Fatalf("base observation err=%v", err)
			}
			if created || fake.MutationCount("pr_create") != 0 {
				t.Fatal("unavailable or malformed base observation launched create")
			}
		})
	}
}

func TestCleanupUncertaintyNeverBecomesMutationSuccess(t *testing.T) {
	client, _, identity := fixture(t)
	uncertainGuard := mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		_, _ = start(ctx)
		return nil, ErrProcessCleanup
	})

	client.mutationGuard = uncertainGuard
	createClaim := testClaim("draft_pr", identity, "title", "body")
	if _, err := client.CreateDraftPullRequest(context.Background(), createClaim, identity, "title", "body"); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("create cleanup uncertainty=%v", err)
	}

	client.mutationGuard = fixtureGuard()
	created := createDraft(t, client, identity, "before", "before body")
	client.mutationGuard = uncertainGuard
	editClaim := testClaim("pr_edit", created.Identity, "after", "after body")
	if err := client.UpdatePullRequest(context.Background(), editClaim, created.Identity, "after", "after body"); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("update cleanup uncertainty=%v", err)
	}

	client.mutationGuard = fixtureGuard()
	if err := client.MarkReady(context.Background(), testClaim("pr_ready", created.Identity), created.Identity); err != nil {
		t.Fatal(err)
	}
	readyClaim := testClaim("pr_ready", created.Identity)
	client.mutationGuard = uncertainGuard
	if err := client.MarkReady(context.Background(), readyClaim, created.Identity); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("ready cleanup uncertainty=%v", err)
	}

	client.mutationGuard = fixtureGuard()
	mergeClaim := testClaim("merge", created.Identity, created.Identity.HeadOID, "merge")
	authorization := testAuthorization(created.Identity)
	client.mutationGuard = uncertainGuard
	if err := client.MergeExactHead(context.Background(), mergeClaim, created.Identity, created.Identity.HeadOID, "merge", authorization); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("guarded merge cleanup uncertainty=%v", err)
	}
}

func TestCreateAndFactoryUpdatePreserveCleanupQuarantineFatal(t *testing.T) {
	client, fake, identity := fixture(t)
	fatalGuard := mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		_, _ = start(ctx)
		return nil, ErrCleanupQuarantineFatal
	})
	client.mutationGuard = fatalGuard
	fatalCreate := identity
	fatalCreate.HeadRef += "-fatal"
	if _, err := client.CreateDraftPullRequest(context.Background(), testClaim("draft_pr", fatalCreate, "fatal", "fatal body"), fatalCreate, "fatal", "fatal body"); !errors.Is(err, ErrCleanupQuarantineFatal) {
		t.Fatalf("create lost cleanup-quarantine fatal result: %v", err)
	}

	client.mutationGuard = fixtureGuard()
	prior := createDraft(t, client, identity, "before", "before body").Identity
	expected := prior
	expected.HeadOID = strings.Repeat("b", 40)
	if err := fake.SetPullRequestHeadOIDForTest(prior.Number, expected.HeadOID); err != nil {
		t.Fatal(err)
	}
	client.mutationGuard = fatalGuard
	if err := client.UpdateFactoryPullRequest(context.Background(), testClaim("pr_edit", expected, "after", "after body"), prior, expected, "after", "after body"); !errors.Is(err, ErrCleanupQuarantineFatal) {
		t.Fatalf("factory update lost cleanup-quarantine fatal result: %v", err)
	}
}

func TestCleanupQuarantineIsNotACompletedProof(t *testing.T) {
	if (CleanupProof{Quarantined: true}).valid() {
		t.Fatal("quarantine was accepted as successful cleanup")
	}
	if (CleanupProof{Drained: true, Quarantined: true}).valid() {
		t.Fatal("contradictory drain/quarantine proof was accepted")
	}
}

func TestContradictoryCleanupProofLatchesStoreMutationGate(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "sf.sqlite")
	database, err := store.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "cleanup", Ticket: "SF-44"}
	if err := database.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "cleanup", Path: t.TempDir(), BaseRef: "main"}); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "cleanup", Type: domain.TicketBug, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "cleanup")
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, 1, "dev/cleanup/SF-44", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := store.EffectFence{SemanticKey: "cleanup-effect", Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}
	if _, err := database.PlanEffect(ctx, store.EffectPlan{SemanticKey: fence.SemanticKey, Ref: ref, Kind: "pr_edit", TicketVersion: fence.TicketVersion, Fence: fence.Fence, RequestDigest: "cleanup"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := database.ClaimEffect(ctx, fence)
	if err != nil {
		t.Fatal(err)
	}
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: contradictoryCleanupRunner{}, quarantiner: database}
	if _, err := database.ExternalMutationGuard().RunExternalMutation(ctx, claimed.ExternalClaim(), func(runCtx context.Context) ([]byte, error) {
		return client.run(runCtx, "auth", "status", "--json", "hosts")
	}); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("contradictory cleanup=%v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := store.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err := restarted.ExternalMutationGuard().RunExternalMutation(ctx, claimed.ExternalClaim(), func(context.Context) ([]byte, error) {
		t.Fatal("contradictory cleanup released the mutation gate")
		return nil, nil
	}); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("contradictory cleanup was not durably latched: %v", err)
	}
}

type quarantineRunner struct{}

func (quarantineRunner) Run(context.Context, string, []string, []string) ([]byte, error) {
	return []byte("{}"), nil
}
func (quarantineRunner) Cleanup(context.Context) (CleanupProof, error) {
	return CleanupProof{Quarantined: true}, nil
}

func TestQuarantinedRunnerBlocksGuardedMutation(t *testing.T) {
	client, _, identity := fixture(t)
	client.runner = quarantineRunner{}
	started := false
	client.mutationGuard = mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		started = true
		return start(ctx)
	})
	claim := testClaim("pr_edit", identity, "title", "body")
	if _, err := client.mutateExact(context.Background(), claim, identity, "pr", "edit", "1"); !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("quarantined runner err=%v", err)
	}
	if !started {
		t.Fatal("guarded callback did not run")
	}
}

func fixtureGuard() contracts.ExternalMutationGuard {
	return mutationGuardFunc(func(ctx context.Context, claim domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		if claim.Ref.Validate() != nil || claim.SemanticKey == "" || claim.Kind == "" || claim.RequestDigest == "" || claim.TicketVersion == 0 || claim.LeaderEpoch == 0 || claim.RunnerEpoch == 0 || claim.ClaimEpoch == 0 {
			return nil, ErrPolicyRefusal
		}
		return start(ctx)
	})
}

func TestChecksRejectsHeadDriftBetweenCheckObservations(t *testing.T) {
	_, _, identity := fixture(t)
	identity.Number = 7
	changed := identity
	changed.HeadOID = strings.Repeat("b", 40)
	open := mergeWire(identity, "OPEN", "CLEAN", nil, nil)
	mutated := mergeWire(changed, "OPEN", "CLEAN", nil, nil)
	pre, _ := json.Marshal([]map[string]any{open})
	post, _ := json.Marshal([]map[string]any{mutated})
	checks := `[{"name":"unit","state":"SUCCESS","workflow":"ci","link":"https://example.test/1","bucket":"test"}]`
	listCalls := 0
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if args[0] == "pr" && args[1] == "list" {
			listCalls++
			if listCalls == 1 {
				return pre, nil
			}
			return post, nil
		}
		if args[0] == "pr" && args[1] == "checks" {
			return []byte(checks), nil
		}
		return nil, errors.New("unexpected command")
	}), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil })}
	if _, err := client.RequiredChecks(context.Background(), identity); !errors.Is(err, ErrChecksFailed) {
		t.Fatalf("head drift checks=%v", err)
	}
}

func TestChecksCanonicalizesProductionExternalIdentityForStore(t *testing.T) {
	repository := contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}
	identity := contracts.PullRequestIdentity{
		Repository:     repository,
		Number:         7,
		HeadOwner:      "example",
		HeadRepository: "app",
		HeadRef:        "sf/dev/example/SF-44-random",
		HeadOID:        strings.Repeat("a", 40),
		BaseRef:        "main",
		BaseOID:        strings.Repeat("c", 40),
		FactoryOwned:   true,
	}
	observed, err := json.Marshal([]map[string]any{mergeWire(identity, "OPEN", "CLEAN", nil, nil)})
	if err != nil {
		t.Fatal(err)
	}
	wire := checkWire{Name: "unit", State: "SUCCESS", Workflow: "ci", Link: "https://example.test/actions/runs/1/jobs/2", Bucket: "pass"}
	longLinkPrefix := "https://example.test/"
	longWire := checkWire{Name: "lint", State: "PENDING", Link: longLinkPrefix + strings.Repeat("x", 2048-len(longLinkPrefix))}
	wireJSON, err := json.Marshal([]checkWire{wire, longWire})
	if err != nil {
		t.Fatal(err)
	}
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if args[0] == "pr" && args[1] == "list" {
			return observed, nil
		}
		if args[0] == "pr" && args[1] == "checks" {
			return wireJSON, nil
		}
		return nil, errors.New("unexpected command")
	}), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil })}

	checks, err := client.RequiredChecks(context.Background(), identity)
	if err != nil || len(checks) != 2 {
		t.Fatalf("checks=%+v err=%v", checks, err)
	}
	expected := canonicalCheckExternalID(wire)
	if checks[0].ExternalID != expected || len(expected) != len("sha256:")+sha256.Size*2 || strings.ContainsRune(expected, '\x00') {
		t.Fatalf("external identity=%q expected=%q", checks[0].ExternalID, expected)
	}
	if _, err := store.NormalizeCIObservationChecks(checks); err != nil {
		t.Fatalf("Store rejected canonical GitHub check identity: %v", err)
	}
	if len(longWire.Link) != 2048 || checks[1].ExternalID != canonicalCheckExternalID(longWire) {
		t.Fatalf("long link identity=%q link-length=%d", checks[1].ExternalID, len(longWire.Link))
	}
	stateChanged := wire
	stateChanged.State = "PENDING"
	if canonicalCheckExternalID(stateChanged) != expected {
		t.Fatal("check state changed stable external identity")
	}
	bucketChanged := wire
	bucketChanged.Bucket = "pending"
	if canonicalCheckExternalID(bucketChanged) != expected {
		t.Fatal("state-derived bucket changed stable external identity")
	}
	for name, changed := range map[string]checkWire{
		"workflow": {Name: wire.Name, State: wire.State, Workflow: "other", Link: wire.Link, Bucket: wire.Bucket},
		"link":     {Name: wire.Name, State: wire.State, Workflow: wire.Workflow, Link: wire.Link + "/3", Bucket: wire.Bucket},
	} {
		if canonicalCheckExternalID(changed) == expected {
			t.Fatalf("%s change retained external identity", name)
		}
	}
}

func TestChecksMergeAndApprovalPolicies(t *testing.T) {
	client, fake, identity := fixture(t)
	pr := createDraft(t, client, identity, "title", "body")
	claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash")
	if _, err := client.RequiredChecks(context.Background(), pr.Identity); !errors.Is(err, ErrChecksFailed) {
		t.Fatalf("empty server-required check set=%v", err)
	}
	if err := fake.SetChecks(pr.Identity.Number, contracts.RequiredCheck{Name: "unit", ExternalID: "1", State: "SUCCESS"}); err != nil {
		t.Fatal(err)
	}
	checkID := canonicalCheckExternalID(checkWire{Link: "1"})
	checks, err := client.WaitChecks(context.Background(), pr.Identity, []CheckIdentity{{Name: "unit", ExternalID: checkID}}, time.Millisecond, time.Millisecond)
	if err != nil || len(checks) != 1 {
		t.Fatalf("checks=%+v err=%v", checks, err)
	}
	if err := fake.SetChecks(pr.Identity.Number, contracts.RequiredCheck{Name: "unit", ExternalID: "wrong", State: "SUCCESS"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.WaitChecks(context.Background(), pr.Identity, []CheckIdentity{{Name: "unit", ExternalID: checkID}}, time.Millisecond, time.Millisecond); !errors.Is(err, ErrChecksFailed) {
		t.Fatalf("strict checks=%v", err)
	}
	if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
		t.Fatal(err)
	}
	pr.Draft = false
	if err := fake.SetResponse("pr_merge", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	authorization := testAuthorization(pr.Identity)
	err = client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "squash", authorization)
	if err != nil {
		t.Fatalf("guarded merge=%v", err)
	}
	if err := (ApprovalBinding{ReviewedHead: pr.Identity.HeadOID, CurrentHead: pr.Identity.HeadOID}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (ApprovalBinding{ReviewedHead: pr.Identity.HeadOID, CurrentHead: "changed"}).Validate(); !errors.Is(err, ErrApprovalInvalid) {
		t.Fatalf("approval invalidation=%v", err)
	}
}

func TestDraftAndNonOpenPRsCannotMergeOrBeAdopted(t *testing.T) {
	client, _, identity := fixture(t)
	pr := createDraft(t, client, identity, "title", "body")
	claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "merge")
	if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "merge", testAuthorization(pr.Identity)); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("draft merge=%v", err)
	}
	closedClient, closedFake, closedIdentity := fixture(t)
	closed := closedIdentity
	closed.Number = 1
	if err := closedFake.InjectPullRequestForTest(testkit.PullRequest{Identity: closed, Draft: true, Merged: true}); err != nil {
		t.Fatal(err)
	}
	closedClaim := testClaim("draft_pr", closedIdentity, "title", "body")
	if _, err := closedClient.CreateDraftPullRequest(context.Background(), closedClaim, closedIdentity, "title", "body"); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("merged draft adoption=%v", err)
	}
}

func TestMarkReadyRejectsMergedPRBeforeMutation(t *testing.T) {
	client, fake, identity := fixture(t)
	identity.Number = 1
	if err := fake.InjectPullRequestForTest(testkit.PullRequest{Identity: identity, Merged: true}); err != nil {
		t.Fatal(err)
	}
	durable := testClaim("pr_ready", identity)
	if err := client.MarkReady(context.Background(), durable, identity); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("merged ready=%v", err)
	}
	if fake.MutationCount("pr_ready") != 0 {
		t.Fatalf("merged PR was mutated")
	}
}

func TestMarkReadySynchronizeGapCompensatesChangedSource(t *testing.T) {
	client, _, identity := fixture(t)
	identity.Number = 1
	changed := identity
	changed.HeadOID = strings.Repeat("b", 40)
	oldWire, err := json.Marshal(mergeWire(identity, "OPEN", "CLEAN", nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	newWireValue := mergeWire(changed, "OPEN", "CLEAN", nil, nil)
	newWireValue["body"] = ownershipMarker(identity)
	newWire, err := json.Marshal(newWireValue)
	if err != nil {
		t.Fatal(err)
	}
	restoredWireValue := mergeWire(changed, "OPEN", "CLEAN", nil, nil)
	restoredWireValue["body"] = ownershipMarker(identity)
	restoredWireValue["isDraft"] = true
	restoredWire, err := json.Marshal(restoredWireValue)
	if err != nil {
		t.Fatal(err)
	}
	phase := 0
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if len(args) < 2 || args[0] != "pr" {
			return nil, errors.New("unexpected command")
		}
		switch args[1] {
		case "list":
			if phase == 0 {
				return []byte("[" + string(oldWire) + "]"), nil
			}
			if phase == 3 {
				return []byte("[" + string(restoredWire) + "]"), nil
			}
			return []byte("[" + string(newWire) + "]"), nil
		case "view":
			if phase == 0 {
				phase = 1
				return oldWire, nil
			}
			if phase == 3 {
				return restoredWire, nil
			}
			return newWire, nil
		case "ready":
			phase = 2
			return []byte("{}"), nil
		default:
			return nil, errors.New("unexpected command")
		}
	})
	claim := testClaim("pr_ready", identity)
	if err := client.MarkReady(context.Background(), claim, identity); !errors.Is(err, ErrPolicyRefusal) {
		t.Fatalf("changed-head ready=%v", err)
	}
}

func TestMarkReadyFinalHandoffRejectsChangedGuardedFields(t *testing.T) {
	cases := []struct {
		name string
		wire func(contracts.PullRequestIdentity) map[string]any
	}{
		{"closed", func(id contracts.PullRequestIdentity) map[string]any {
			return mergeWire(id, "CLOSED", "CLEAN", nil, nil)
		}},
		{"merged", func(id contracts.PullRequestIdentity) map[string]any {
			return mergeWire(id, "MERGED", "CLEAN", "2026-01-01T00:00:00Z", nil)
		}},
		{"head-changed", func(id contracts.PullRequestIdentity) map[string]any {
			id.HeadOID = strings.Repeat("b", 40)
			return mergeWire(id, "OPEN", "CLEAN", nil, nil)
		}},
		{"base-changed", func(id contracts.PullRequestIdentity) map[string]any {
			id.BaseRef = "release"
			return mergeWire(id, "OPEN", "CLEAN", nil, nil)
		}},
		{"auto-merge", func(id contracts.PullRequestIdentity) map[string]any {
			return mergeWire(id, "OPEN", "CLEAN", nil, map[string]any{"enabledAt": "now"})
		}},
		{"queued", func(id contracts.PullRequestIdentity) map[string]any {
			return mergeWire(id, "OPEN", "QUEUED", nil, nil)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			client, _, identity := fixture(t)
			identity.Number = 1
			initialWire := mergeWire(identity, "OPEN", "CLEAN", nil, nil)
			initialWire["isDraft"] = true
			initial, err := json.Marshal(initialWire)
			if err != nil {
				t.Fatal(err)
			}
			changed, err := json.Marshal(test.wire(identity))
			if err != nil {
				t.Fatal(err)
			}
			readyCalls := 0
			client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
				if len(args) >= 2 && args[0] == "pr" && args[1] == "list" {
					return []byte("[" + string(initial) + "]"), nil
				}
				if len(args) >= 2 && args[0] == "pr" && args[1] == "view" {
					return changed, nil
				}
				if len(args) >= 2 && args[0] == "pr" && args[1] == "ready" {
					readyCalls++
				}
				return []byte("{}"), nil
			})
			claim := testClaim("pr_ready", identity)
			if err := client.MarkReady(context.Background(), claim, identity); !errors.Is(err, ErrPolicyRefusal) {
				t.Fatalf("changed %s accepted: %v", test.name, err)
			}
			if readyCalls != 0 {
				t.Fatalf("changed %s launched ready %d times", test.name, readyCalls)
			}
		})
	}
}

func TestMergeRequiresFreshProtectedBranchProof(t *testing.T) {
	t.Run("unavailable verifier is never success", func(t *testing.T) {
		client, fake, identity := fixture(t)
		pr := createDraft(t, client, identity, "title", "body")
		claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "merge")
		client.verifyProtectedBranch = nil
		if err := fake.SetResponse("pr_merge", testkit.ResponseDropAfterCall); err != nil {
			t.Fatal(err)
		}
		if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "merge", testAuthorization(pr.Identity)); !errors.Is(err, ErrPolicyRefusal) {
			t.Fatalf("missing proof verifier=%v", err)
		}
	})
	t.Run("mismatched proof is never success", func(t *testing.T) {
		client, fake, identity := fixture(t)
		pr := createDraft(t, client, identity, "title", "body")
		claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "merge")
		client.verifyProtectedBranch = verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
			return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
		})
		if err := fake.SetResponse("pr_merge", testkit.ResponseDropAfterCall); err != nil {
			t.Fatal(err)
		}
		if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "merge", testAuthorization(pr.Identity)); !errors.Is(err, ErrPolicyRefusal) {
			t.Fatalf("mismatched proof=%v", err)
		}
	})
}

func TestMergeCrossBindsBaseAndRefusesMovedBaseDuringGuardedHandoff(t *testing.T) {
	t.Run("split local and GitHub base witness is refused before launch", func(t *testing.T) {
		client, fake, identity := fixture(t)
		pr := createDraft(t, client, identity, "title", "body")
		if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
			t.Fatal(err)
		}
		authorization := testAuthorization(pr.Identity)
		authorization.CurrentBaseHeadOID = strings.Repeat("d", 40)
		claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash", authorization.ReviewedBaseSHA, authorization.CurrentBaseSHA, authorization.ReviewedBaseHeadOID, authorization.CurrentBaseHeadOID)
		if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "squash", authorization); !errors.Is(err, ErrApprovalInvalid) {
			t.Fatalf("split base witness=%v", err)
		}
		if got := fake.MutationCount("pr_merge"); got != 0 {
			t.Fatalf("split base launched merge %d times", got)
		}
	})
	t.Run("base movement after preflight but before launch is fenced", func(t *testing.T) {
		client, fake, identity := fixture(t)
		pr := createDraft(t, client, identity, "title", "body")
		if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
			t.Fatal(err)
		}
		client.mutationGuard = mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
			if err := fake.SetBaseHeadOIDForTest(strings.Repeat("d", 40)); err != nil {
				return nil, err
			}
			return start(ctx)
		})
		claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash")
		if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "squash", testAuthorization(pr.Identity)); !errors.Is(err, ErrPolicyRefusal) {
			t.Fatalf("moved base handoff=%v", err)
		}
		if got := fake.MutationCount("pr_merge"); got != 0 {
			t.Fatalf("moved base launched merge %d times", got)
		}
	})
}

func TestMergeRequiresStrictServerProtectionWithoutBypass(t *testing.T) {
	for _, test := range []struct {
		name   string
		strict bool
		bypass int
		admin  bool
		rules  int
	}{
		{name: "non-strict", strict: false, admin: true},
		{name: "bypass allowance", strict: true, admin: true, bypass: 1},
		{name: "admin bypass", strict: true, admin: false},
		{name: "active ruleset", strict: true, admin: true, rules: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, fake, identity := fixture(t)
			pr := createDraft(t, client, identity, "title", "body")
			if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
				t.Fatal(err)
			}
			if err := fake.SetProtectionWitnessForTest(test.strict, test.admin, test.bypass, test.rules); err != nil {
				t.Fatal(err)
			}
			claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash")
			if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "squash", testAuthorization(pr.Identity)); !errors.Is(err, ErrGuardedMergeUnavailable) {
				t.Fatalf("protection=%v", err)
			}
			if got := fake.MutationCount("pr_merge"); got != 0 {
				t.Fatalf("unsafe protection launched merge %d times", got)
			}
		})
	}
	t.Run("force-push bypass allowance", func(t *testing.T) {
		client, fake, identity := fixture(t)
		pr := createDraft(t, client, identity, "title", "body")
		if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
			t.Fatal(err)
		}
		if err := fake.SetBypassForcePushAllowancesForTest(1); err != nil {
			t.Fatal(err)
		}
		if err := client.MergeExactHead(context.Background(), testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash"), pr.Identity, pr.Identity.HeadOID, "squash", testAuthorization(pr.Identity)); !errors.Is(err, ErrGuardedMergeUnavailable) {
			t.Fatalf("force-push bypass=%v", err)
		}
		if got := fake.MutationCount("pr_merge"); got != 0 {
			t.Fatalf("force-push bypass launched merge %d times", got)
		}
	})
}

func TestStrictProtectionTrustsOnlyAppliedRefRule(t *testing.T) {
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if strings.Contains(strings.Join(args, "\x00"), "graphql") {
			if !strings.Contains(strings.Join(args, "\x00"), "ref(qualifiedName:$qualifiedRef){branchProtectionRule") {
				return nil, errors.New("did not request applied ref rule")
			}
			// The actual ref rule is weak. The query assertion above ensures this
			// response cannot be replaced by an unordered duplicate-rule scan.
			return []byte(`{"data":{"repository":{"ref":{"branchProtectionRule":{"id":"applied","pattern":"main","requiresStrictStatusChecks":false,"isAdminEnforced":true,"bypassPullRequestAllowances":{"totalCount":0},"bypassForcePushAllowances":{"totalCount":0}}}}}}`), nil
		}
		return []byte(`[]`), nil
	})}
	if _, err := client.strictProtection(context.Background(), contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, "main"); !errors.Is(err, ErrGuardedMergeUnavailable) {
		t.Fatalf("weak applied rule=%v", err)
	}
}

func TestStrictProtectionRefusesNullAppliedRule(t *testing.T) {
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if strings.Contains(strings.Join(args, "\x00"), "graphql") {
			return []byte(`{"data":{"repository":{"ref":null}}}`), nil
		}
		return []byte(`[]`), nil
	})}
	if _, err := client.strictProtection(context.Background(), contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, "main"); !errors.Is(err, ErrGuardedMergeUnavailable) {
		t.Fatalf("null applied rule=%v", err)
	}
}

func TestStrictProtectionPinsRulesRESTToGitHubDespiteDefaultHost(t *testing.T) {
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if strings.Contains(strings.Join(args, "\x00"), "graphql") {
			return []byte(`{"data":{"repository":{"ref":{"branchProtectionRule":{"id":"rule","pattern":"main","requiresStrictStatusChecks":true,"isAdminEnforced":true,"bypassPullRequestAllowances":{"totalCount":0},"bypassForcePushAllowances":{"totalCount":0}}}}}}`), nil
		}
		// This runner models a machine whose implicit gh host is a GHE server:
		// only an explicit github.com request receives the empty GitHub ruleset.
		hostPinned := false
		for index, arg := range args {
			if arg == "--hostname" && index+1 < len(args) && args[index+1] == "github.com" {
				hostPinned = true
			}
		}
		if !hostPinned {
			return []byte(`[{"type":"ghe-rule"}]`), nil
		}
		return []byte(`[]`), nil
	})}
	if _, err := client.strictProtection(context.Background(), contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, "main"); err != nil {
		t.Fatalf("github-pinned rules request=%v", err)
	}
}

func exactRepositoryRuleset() testkit.FakeRuleset {
	return testkit.FakeRuleset{
		ID: 42, Target: "branch", Source: "example/app", SourceType: "Repository", Enforcement: "active",
		Conditions: &testkit.FakeRulesetConditions{RefName: &testkit.FakeRulesetRefCondition{Include: []string{"refs/heads/main"}, Exclude: []string{}}},
		Rules: []testkit.FakeRulesetRule{
			{Type: "pull_request", Parameters: map[string]any{"allowed_merge_methods": []any{"squash"}}},
			{Type: "required_status_checks", Parameters: map[string]any{"strict_required_status_checks_policy": true, "required_status_checks": []any{map[string]any{"context": "ci"}, map[string]any{"context": "test-immutability"}}}},
			{Type: "non_fast_forward"},
			{Type: "deletion"},
		},
		BypassActors: []any{},
	}
}

func TestStrictProtectionAcceptsExactRepositoryRuleset(t *testing.T) {
	client, fake, identity := fixture(t)
	if err := fake.SetRulesetsForTest(exactRepositoryRuleset()); err != nil {
		t.Fatal(err)
	}
	witness, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash")
	if err != nil || witness.Kind != "ruleset" || witness.ID != "42" || witness.ActiveRulesetCount != 1 || len(witness.Checks) != 2 || witness.ChecksDigest == "" {
		t.Fatalf("ruleset witness=%+v err=%v", witness, err)
	}
}

func TestObserveCIRequiredCheckPolicyAcceptsRulesetWithoutMergeMethod(t *testing.T) {
	client, fake, identity := fixture(t)
	if err := fake.SetRulesetsForTest(exactRepositoryRuleset()); err != nil {
		t.Fatal(err)
	}
	pr := createDraft(t, client, identity, "ruleset CI", "body")
	if err := fake.SetChecks(pr.Identity.Number,
		contracts.RequiredCheck{Name: "ci", ExternalID: "https://github.com/example/app/actions/runs/1", State: "SUCCESS"},
		contracts.RequiredCheck{Name: "test-immutability", ExternalID: "https://github.com/example/app/actions/runs/2", State: "SUCCESS"},
	); err != nil {
		t.Fatal(err)
	}
	policy, err := client.ObserveCIRequiredCheckPolicy(context.Background(), pr.Identity)
	if err != nil || len(policy.RequiredChecks) != 2 || policy.ProtectedBranchRef != "main" {
		t.Fatalf("ruleset CI policy observation=%+v err=%v", policy, err)
	}
}

func TestObserveCIRequiredCheckPolicyRejectsProtectionWitnessRace(t *testing.T) {
	client, fake, identity := fixture(t)
	if err := fake.SetRulesetsForTest(exactRepositoryRuleset()); err != nil {
		t.Fatal(err)
	}
	pr := createDraft(t, client, identity, "title", "body")
	if err := fake.SetChecks(pr.Identity.Number, contracts.RequiredCheck{Name: "ci", ExternalID: "ci", State: "SUCCESS"}); err != nil {
		t.Fatal(err)
	}
	original := client.runner
	client.runner = commandRunnerFunc(func(ctx context.Context, binary string, args, env []string) ([]byte, error) {
		if len(args) >= 2 && args[0] == "pr" && args[1] == "checks" {
			changed := exactRepositoryRuleset()
			changed.Rules[1].Parameters["required_status_checks"] = []any{map[string]any{"context": "ci"}}
			if err := fake.SetRulesetsForTest(changed); err != nil {
				return nil, err
			}
		}
		return original.Run(ctx, binary, args, env)
	})
	if _, err := client.ObserveCIRequiredCheckPolicy(context.Background(), pr.Identity); err == nil {
		t.Fatalf("protection witness race accepted: %v", err)
	}
}

func TestMergeBindsRulesetCheckDigestIntoIntent(t *testing.T) {
	client, fake, identity := fixture(t)
	if err := fake.SetRulesetsForTest(exactRepositoryRuleset()); err != nil {
		t.Fatal(err)
	}
	pr := createDraft(t, client, identity, "title", "body")
	if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
		t.Fatal(err)
	}
	var recorded domain.MergeIntent
	client.mergeIntents = intentRecorderFunc(func(_ context.Context, intent domain.MergeIntent) error {
		recorded = intent
		return nil
	})
	if err := client.MergeExactHead(context.Background(), testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash"), pr.Identity, pr.Identity.HeadOID, "squash", testAuthorization(pr.Identity)); err != nil {
		t.Fatal(err)
	}
	if recorded.ProtectionKind != "ruleset" || recorded.ProtectionRuleID != "42" || recorded.ProtectionChecksDigest == "" || recorded.ActiveRulesetCount != 1 || recorded.ValidateProtectionWitness() != nil {
		t.Fatalf("ruleset merge intent=%+v", recorded)
	}
}

func TestStrictProtectionRulesetFailsClosedForWeakOrAmbiguousPolicy(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testkit.FakeRuleset)
	}{
		{"bypass", func(rule *testkit.FakeRuleset) { rule.BypassActors = []any{map[string]any{"actor_id": 1}} }},
		{"wrong-ref", func(rule *testkit.FakeRuleset) { rule.Conditions.RefName.Include = []string{"refs/heads/release"} }},
		{"broad-ref", func(rule *testkit.FakeRuleset) { rule.Conditions.RefName.Include = []string{"refs/heads/*"} }},
		{"wrong-scope", func(rule *testkit.FakeRuleset) { rule.SourceType = "Organization" }},
		{"wrong-source", func(rule *testkit.FakeRuleset) { rule.Source = "other/app" }},
		{"wrong-method", func(rule *testkit.FakeRuleset) { rule.Rules[0].Parameters["allowed_merge_methods"] = []any{"merge"} }},
		{"malformed-method", func(rule *testkit.FakeRuleset) {
			rule.Rules[0].Parameters["allowed_merge_methods"] = []any{"squash", "octopus"}
		}},
		{"duplicate-pr", func(rule *testkit.FakeRuleset) { rule.Rules = append(rule.Rules, rule.Rules[0]) }},
		{"duplicate-check-rule", func(rule *testkit.FakeRuleset) { rule.Rules = append(rule.Rules, rule.Rules[1]) }},
		{"duplicate-non-fast-forward", func(rule *testkit.FakeRuleset) { rule.Rules = append(rule.Rules, rule.Rules[2]) }},
		{"deletion-parameters", func(rule *testkit.FakeRuleset) { rule.Rules[3].Parameters = map[string]any{"unexpected": true} }},
		{"non-strict-checks", func(rule *testkit.FakeRuleset) {
			rule.Rules[1].Parameters["strict_required_status_checks_policy"] = false
		}},
		{"unknown-rule", func(rule *testkit.FakeRuleset) {
			rule.Rules = append(rule.Rules, testkit.FakeRulesetRule{Type: "required_signatures", Parameters: map[string]any{}})
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			client, fake, identity := fixture(t)
			ruleset := exactRepositoryRuleset()
			test.mutate(&ruleset)
			if err := fake.SetRulesetsForTest(ruleset); err != nil {
				t.Fatal(err)
			}
			if _, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash"); !errors.Is(err, ErrGuardedMergeUnavailable) {
				t.Fatalf("unsafe ruleset accepted: %v", err)
			}
		})
	}

	t.Run("ambiguous-relevant-rulesets", func(t *testing.T) {
		client, fake, identity := fixture(t)
		first, second := exactRepositoryRuleset(), exactRepositoryRuleset()
		second.ID = 43
		if err := fake.SetRulesetsForTest(first, second); err != nil {
			t.Fatal(err)
		}
		if _, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash"); !errors.Is(err, ErrGuardedMergeUnavailable) {
			t.Fatalf("ambiguous rulesets accepted: %v", err)
		}
	})
	t.Run("applicable-active-organization-ruleset", func(t *testing.T) {
		client, fake, identity := fixture(t)
		repositoryRule, organizationRule := exactRepositoryRuleset(), exactRepositoryRuleset()
		organizationRule.ID = 43
		organizationRule.SourceType = "Organization"
		organizationRule.Source = "example"
		if err := fake.SetRulesetsForTest(repositoryRule, organizationRule); err != nil {
			t.Fatal(err)
		}
		if _, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash"); !errors.Is(err, ErrGuardedMergeUnavailable) {
			t.Fatalf("applicable active organization ruleset accepted: %v", err)
		}
	})
	t.Run("evaluate-organization-ruleset-is-audited", func(t *testing.T) {
		client, fake, identity := fixture(t)
		repositoryRule, organizationRule := exactRepositoryRuleset(), exactRepositoryRuleset()
		organizationRule.ID = 43
		organizationRule.SourceType = "Organization"
		organizationRule.Source = "example"
		organizationRule.Enforcement = "evaluate"
		if err := fake.SetRulesetsForTest(repositoryRule, organizationRule); err != nil {
			t.Fatal(err)
		}
		if _, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash"); !errors.Is(err, ErrGuardedMergeUnavailable) {
			t.Fatalf("applicable evaluate parent accepted: %v", err)
		}
	})
	t.Run("inactive-organization-ruleset", func(t *testing.T) {
		client, fake, identity := fixture(t)
		repositoryRule, organizationRule := exactRepositoryRuleset(), exactRepositoryRuleset()
		organizationRule.ID = 43
		organizationRule.SourceType = "Organization"
		organizationRule.Source = "example"
		organizationRule.Enforcement = "disabled"
		if err := fake.SetRulesetsForTest(repositoryRule, organizationRule); err != nil {
			t.Fatal(err)
		}
		if _, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash"); err != nil {
			t.Fatalf("inactive organization ruleset blocked exact witness: %v", err)
		}
	})
}

func TestEvaluateRulesetSummaryAuditFailsClosed(t *testing.T) {
	newEvaluateParent := func() testkit.FakeRuleset {
		ruleset := exactRepositoryRuleset()
		ruleset.ID, ruleset.SourceType, ruleset.Source, ruleset.Enforcement = 43, "Organization", "example", "evaluate"
		return ruleset
	}
	for _, test := range []struct {
		name   string
		mutate func(*testkit.FakeRuleset)
		refuse bool
	}{
		{"default-branch", func(rule *testkit.FakeRuleset) { rule.Conditions.RefName.Include = []string{"~DEFAULT_BRANCH"} }, true},
		{"all-branches", func(rule *testkit.FakeRuleset) { rule.Conditions.RefName.Include = []string{"~ALL"} }, true},
		{"wildcard", func(rule *testkit.FakeRuleset) { rule.Conditions.RefName.Include = []string{"refs/heads/release/*"} }, true},
		{"unknown-pattern", func(rule *testkit.FakeRuleset) { rule.Conditions.RefName.Include = []string{"refs/tags/v1"} }, true},
		{"malformed-include", func(rule *testkit.FakeRuleset) { rule.Conditions.RefName.Include = []string{"refs/heads/release "} }, true},
		{"malformed-exclude", func(rule *testkit.FakeRuleset) { rule.Conditions.RefName.Exclude = []string{"refs/heads/release\r"} }, true},
		{"unrelated-exact-ref", func(rule *testkit.FakeRuleset) { rule.Conditions.RefName.Include = []string{"refs/heads/release"} }, false},
		{"disabled", func(rule *testkit.FakeRuleset) { rule.Enforcement = "disabled" }, false},
		{"unknown-enforcement", func(rule *testkit.FakeRuleset) { rule.Enforcement = "future" }, true},
		{"unknown-source", func(rule *testkit.FakeRuleset) { rule.SourceType = "Future" }, true},
		{"non-branch-target", func(rule *testkit.FakeRuleset) { rule.Target = "tag" }, true},
		{"malformed-summary-metadata", func(rule *testkit.FakeRuleset) { rule.Name = "bad\x00name" }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, fake, identity := fixture(t)
			repositoryRule, parent := exactRepositoryRuleset(), newEvaluateParent()
			test.mutate(&parent)
			if err := fake.SetRulesetsForTest(repositoryRule, parent); err != nil {
				t.Fatal(err)
			}
			_, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash")
			if test.refuse && !errors.Is(err, ErrGuardedMergeUnavailable) {
				t.Fatalf("unsafe evaluate summary accepted: %v", err)
			}
			if !test.refuse && err != nil {
				t.Fatalf("safe evaluate summary refused: %v", err)
			}
		})
	}
	t.Run("full-page", func(t *testing.T) {
		client, fake, identity := fixture(t)
		rulesets := make([]testkit.FakeRuleset, 100)
		for index := range rulesets {
			rulesets[index] = newEvaluateParent()
			rulesets[index].ID = int64(index + 1)
			rulesets[index].Enforcement = "disabled"
		}
		if err := fake.SetRulesetsForTest(rulesets...); err != nil {
			t.Fatal(err)
		}
		if _, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash"); !errors.Is(err, ErrGuardedMergeUnavailable) {
			t.Fatalf("full summary page accepted: %v", err)
		}
	})
	t.Run("unknown-field", func(t *testing.T) {
		client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), runner: commandRunnerFunc(func(_ context.Context, _ string, _ []string, _ []string) ([]byte, error) {
			return []byte(`[{"id":42,"name":"exact","target":"branch","source":"example/app","source_type":"Repository","enforcement":"enabled","node_id":"RRS_fake_42","_links":{"self":{"href":"https://api.github.com/repos/example/app/rulesets/42"},"html":{"href":"https://github.com/example/app/rules/42"}},"created_at":"2023-07-15T08:43:03Z","updated_at":"2023-08-23T16:29:47Z","unexpected":true}]`), nil
		})}
		if _, err := client.auditEvaluateRulesets(context.Background(), contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, "main"); !errors.Is(err, ErrGuardedMergeUnavailable) {
			t.Fatalf("unknown summary field accepted: %v", err)
		}
	})
}

func TestStrictProtectionRequiresSelectedRulesetInventory(t *testing.T) {
	type testCase struct {
		name    string
		list    func(testkit.FakeRuleset) []map[string]any
		detail  func(testkit.FakeRuleset) testkit.FakeRuleset
		accepts bool
	}
	for _, test := range []testCase{
		{
			name: "selected-active-summary",
			list: func(rule testkit.FakeRuleset) []map[string]any {
				return []map[string]any{rulesetSummaryForTest(rule, "active")}
			},
			accepts: true,
		},
		{
			name: "selected-documented-enabled-summary",
			list: func(rule testkit.FakeRuleset) []map[string]any {
				return []map[string]any{rulesetSummaryForTest(rule, "enabled")}
			},
			accepts: true,
		},
		{
			name: "selected-summary-omitted",
			list: func(testkit.FakeRuleset) []map[string]any { return []map[string]any{} },
		},
		{
			name: "selected-summary-source-mismatch",
			list: func(rule testkit.FakeRuleset) []map[string]any {
				summary := rulesetSummaryForTest(rule, "active")
				summary["source"] = "other/app"
				return []map[string]any{summary}
			},
		},
		{
			name: "selected-summary-source-type-mismatch",
			list: func(rule testkit.FakeRuleset) []map[string]any {
				summary := rulesetSummaryForTest(rule, "active")
				summary["source_type"], summary["source"] = "Organization", "example"
				return []map[string]any{summary}
			},
		},
		{
			name: "selected-summary-disabled",
			list: func(rule testkit.FakeRuleset) []map[string]any {
				return []map[string]any{rulesetSummaryForTest(rule, "disabled")}
			},
		},
		{
			name: "selected-summary-evaluate",
			list: func(rule testkit.FakeRuleset) []map[string]any {
				return []map[string]any{rulesetSummaryForTest(rule, "evaluate")}
			},
		},
		{
			name: "enabled-list-detail-race",
			list: func(rule testkit.FakeRuleset) []map[string]any {
				return []map[string]any{rulesetSummaryForTest(rule, "enabled")}
			},
			detail: func(rule testkit.FakeRuleset) testkit.FakeRuleset {
				rule.Enforcement = "evaluate"
				rule.Conditions.RefName.Include = []string{"refs/heads/release"}
				return rule
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, fake, identity := fixture(t)
			if err := fake.SetRulesetsForTest(exactRepositoryRuleset()); err != nil {
				t.Fatal(err)
			}
			rule := fake.Snapshot().Rulesets[0]
			original := client.runner
			client.runner = commandRunnerFunc(func(ctx context.Context, binary string, args, env []string) ([]byte, error) {
				path := args[len(args)-1]
				if strings.HasSuffix(path, "/rulesets?includes_parents=true&targets=branch&per_page=100&page=1") {
					return json.Marshal(test.list(rule))
				}
				if test.detail != nil && strings.Contains(path, "/rulesets/42?includes_parents=true") {
					return json.Marshal(test.detail(rule))
				}
				return original.Run(ctx, binary, args, env)
			})
			_, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash")
			if test.accepts && err != nil {
				t.Fatalf("selected inventory witness refused: %v", err)
			}
			if !test.accepts && !errors.Is(err, ErrGuardedMergeUnavailable) {
				t.Fatalf("incomplete or mismatched inventory accepted: %v", err)
			}
		})
	}
}

func rulesetSummaryForTest(rule testkit.FakeRuleset, enforcement string) map[string]any {
	return map[string]any{"id": rule.ID, "name": rule.Name, "target": rule.Target, "source": rule.Source, "source_type": rule.SourceType, "enforcement": enforcement, "node_id": rule.NodeID, "_links": rule.Links, "created_at": rule.CreatedAt, "updated_at": rule.UpdatedAt}
}

func TestStrictProtectionRejectsMixedClassicAndRulesetWitness(t *testing.T) {
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		if strings.Contains(strings.Join(args, "\x00"), "graphql") {
			return []byte(`{"data":{"repository":{"ref":{"branchProtectionRule":{"id":"classic","pattern":"main","requiresStrictStatusChecks":true,"isAdminEnforced":true,"bypassPullRequestAllowances":{"totalCount":0},"bypassForcePushAllowances":{"totalCount":0}}}}}}`), nil
		}
		return []byte(`[{"type":"pull_request","ruleset_source_type":"Repository","ruleset_source":"example/app","ruleset_id":42,"parameters":{}}]`), nil
	})}
	if _, err := client.strictProtection(context.Background(), contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, "main", "squash"); !errors.Is(err, ErrGuardedMergeUnavailable) {
		t.Fatalf("mixed classic/ruleset witness accepted: %v", err)
	}
}

func TestStrictProtectionCanonicalizesIntegrationIDsAndRefusesMalformedValues(t *testing.T) {
	client, fake, identity := fixture(t)
	ruleset := exactRepositoryRuleset()
	ruleset.Rules[1].Parameters["required_status_checks"] = []any{map[string]any{"context": "lint", "integration_id": 2}, map[string]any{"context": "ci", "integration_id": 1}}
	if err := fake.SetRulesetsForTest(ruleset); err != nil {
		t.Fatal(err)
	}
	first, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash")
	if err != nil {
		t.Fatal(err)
	}
	ruleset.Rules[1].Parameters["required_status_checks"] = []any{map[string]any{"context": "ci", "integration_id": 1}, map[string]any{"context": "lint", "integration_id": 2}}
	if err := fake.SetRulesetsForTest(ruleset); err != nil {
		t.Fatal(err)
	}
	second, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash")
	if err != nil || !sameProtectionWitness(first, second) || first.ChecksDigest != second.ChecksDigest {
		t.Fatalf("integration check order changed first=%+v second=%+v err=%v", first, second, err)
	}
	for name, integration := range map[string]any{"string": "1", "zero": 0, "fraction": 1.5, "negative": -1} {
		t.Run(name, func(t *testing.T) {
			ruleset.Rules[1].Parameters["required_status_checks"] = []any{map[string]any{"context": "ci", "integration_id": integration}}
			if err := fake.SetRulesetsForTest(ruleset); err != nil {
				t.Fatal(err)
			}
			if _, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash"); !errors.Is(err, ErrGuardedMergeUnavailable) {
				t.Fatalf("malformed integration id accepted: %v", err)
			}
		})
	}
}

func TestRulesetDetailMetadataIsExplicitAndFailClosed(t *testing.T) {
	t.Run("malformed-documented-extra", func(t *testing.T) {
		client, fake, identity := fixture(t)
		ruleset := exactRepositoryRuleset()
		ruleset.CreatedAt = "not-a-timestamp"
		if err := fake.SetRulesetsForTest(ruleset); err != nil {
			t.Fatal(err)
		}
		if _, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash"); !errors.Is(err, ErrGuardedMergeUnavailable) {
			t.Fatalf("malformed timestamp accepted: %v", err)
		}
	})
	t.Run("authenticated-bypass", func(t *testing.T) {
		client, fake, identity := fixture(t)
		ruleset := exactRepositoryRuleset()
		ruleset.CurrentUserCanBypass = "always"
		if err := fake.SetRulesetsForTest(ruleset); err != nil {
			t.Fatal(err)
		}
		if _, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash"); !errors.Is(err, ErrGuardedMergeUnavailable) {
			t.Fatalf("current user bypass accepted: %v", err)
		}
	})
	t.Run("unknown-field", func(t *testing.T) {
		client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
			endpoint := args[len(args)-1]
			if strings.Contains(endpoint, "/rules/branches/") {
				return []byte(`[{"type":"pull_request","ruleset_source_type":"Repository","ruleset_source":"example/app","ruleset_id":42,"parameters":{}}]`), nil
			}
			if strings.Contains(endpoint, "/rulesets/42") {
				return []byte(`{"id":42,"name":"exact","target":"branch","source":"example/app","source_type":"Repository","enforcement":"active","conditions":{"ref_name":{"include":["refs/heads/main"],"exclude":[]}},"rules":[{"type":"pull_request","parameters":{"allowed_merge_methods":["squash"]}},{"type":"required_status_checks","parameters":{"strict_required_status_checks_policy":true,"required_status_checks":[{"context":"ci"}]}},{"type":"non_fast_forward"},{"type":"deletion"}],"bypass_actors":[],"node_id":"RRS_fake_42","_links":{"self":{"href":"https://api.github.com/repos/example/app/rulesets/42"},"html":{"href":"https://github.com/example/app/rules/42"}},"created_at":"2023-07-15T08:43:03Z","updated_at":"2023-08-23T16:29:47Z","current_user_can_bypass":"never","unexpected":true}`), nil
			}
			return nil, errors.New("unexpected command")
		})}
		if _, err := client.rulesetProtection(context.Background(), contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, "main", "squash", appliedRulesetRef{ID: 42, SourceType: "Repository", Source: "example/app"}, rulesetWire{}, false); !errors.Is(err, ErrGuardedMergeUnavailable) {
			t.Fatalf("unknown detail field accepted: %v", err)
		}
	})
}

func TestStrictProtectionRefusesFullRulesetPage(t *testing.T) {
	client, fake, identity := fixture(t)
	rulesets := make([]testkit.FakeRuleset, 100)
	for index := range rulesets {
		rulesets[index] = exactRepositoryRuleset()
		rulesets[index].ID = int64(index + 1)
		rulesets[index].Conditions.RefName.Include = []string{fmt.Sprintf("refs/heads/other-%d", index)}
	}
	if err := fake.SetRulesetsForTest(rulesets...); err != nil {
		t.Fatal(err)
	}
	if _, err := client.strictProtection(context.Background(), identity.Repository, identity.BaseRef, "squash"); !errors.Is(err, ErrGuardedMergeUnavailable) {
		t.Fatalf("full ruleset page accepted: %v", err)
	}
}

func TestMergeFinalHandoffRefusesChangedSafetyWitness(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testkit.FakeGH, contracts.PullRequestIdentity) error
	}{
		{"rule-removed", func(fake *testkit.FakeGH, _ contracts.PullRequestIdentity) error {
			return fake.SetProtectionWitnessForTest(false, true, 0, 0)
		}},
		{"ruleset-added", func(fake *testkit.FakeGH, _ contracts.PullRequestIdentity) error {
			return fake.SetProtectionWitnessForTest(true, true, 0, 1)
		}},
		{"head-moved", func(fake *testkit.FakeGH, identity contracts.PullRequestIdentity) error {
			return fake.SetPullRequestHeadOIDForTest(identity.Number, strings.Repeat("b", 40))
		}},
		{"queue-entered", func(fake *testkit.FakeGH, _ contracts.PullRequestIdentity) error {
			return fake.SetMergeQueuedForTest(true)
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			client, fake, identity := fixture(t)
			pr := createDraft(t, client, identity, "title", "body")
			if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
				t.Fatal(err)
			}
			client.mutationGuard = mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
				if err := test.mutate(fake, pr.Identity); err != nil {
					return nil, err
				}
				return start(ctx)
			})
			err := client.MergeExactHead(context.Background(), testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash"), pr.Identity, pr.Identity.HeadOID, "squash", testAuthorization(pr.Identity))
			if !errors.Is(err, ErrPolicyRefusal) && !errors.Is(err, ErrGuardedMergeUnavailable) {
				t.Fatalf("handoff %s err=%v", test.name, err)
			}
			if got := fake.MutationCount("pr_merge"); got != 0 {
				t.Fatalf("handoff %s launched merge %d times", test.name, got)
			}
		})
	}
}

func TestCleanupQuarantineWriteFailureLatchesProcess(t *testing.T) {
	var ran int
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: contradictoryCleanupRunner{}, cleanupLatched: &atomic.Bool{}, quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return errors.New("disk unavailable") })}
	if _, err := client.run(context.Background(), "auth", "status"); !errors.Is(err, ErrCleanupQuarantineFatal) {
		t.Fatalf("write failure=%v", err)
	}
	client.runner = commandRunnerFunc(func(context.Context, string, []string, []string) ([]byte, error) { ran++; return nil, nil })
	if _, err := client.run(context.Background(), "auth", "status"); !errors.Is(err, ErrCleanupQuarantineFatal) || ran != 0 {
		t.Fatalf("latched process ran=%d err=%v", ran, err)
	}
}

func TestMergeLostResponseReconcilesFromOriginalBaseWitness(t *testing.T) {
	client, fake, identity := fixture(t)
	pr := createDraft(t, client, identity, "title", "body")
	if err := client.MarkReady(context.Background(), testClaim("pr_ready", pr.Identity), pr.Identity); err != nil {
		t.Fatal(err)
	}
	if err := fake.SetResponse("pr_merge", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	var original string
	client.verifyProtectedBranch = verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
		original = originalBaseOID
		return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
	})
	claim := testClaim("merge", pr.Identity, pr.Identity.HeadOID, "squash")
	if err := client.MergeExactHead(context.Background(), claim, pr.Identity, pr.Identity.HeadOID, "squash", testAuthorization(pr.Identity)); err != nil {
		t.Fatalf("lost-response merge=%v", err)
	}
	if original != pr.Identity.BaseOID || fake.MutationCount("pr_merge") != 1 {
		t.Fatalf("lost-response original=%q mutations=%d", original, fake.MutationCount("pr_merge"))
	}
}

func TestOfficialGHArgvGolden(t *testing.T) {
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", BaseOID: strings.Repeat("c", 40), FactoryOwned: true}
	created := identity
	created.Number = 7
	createdMap := mergeWire(created, "OPEN", "CLEAN", nil, nil)
	createdMap["isDraft"] = true
	createdWire, err := json.Marshal(createdMap)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]string
	listCalls := 0
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		got = append(got, append([]string(nil), args...))
		switch args[0] + " " + args[1] {
		case "pr list":
			listCalls++
			if listCalls <= 2 {
				return []byte("[]"), nil
			}
			return []byte("[" + string(createdWire) + "]"), nil
		case "pr create":
			return []byte("https://github.com/example/app/pull/7\n"), nil
		case "api repos/example/app/git/ref/heads/sf/dev/example/SF-44-random":
			return []byte(`{"object":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`), nil
		case "api repos/example/app/git/ref/heads/main":
			return []byte(`{"object":{"sha":"cccccccccccccccccccccccccccccccccccccccc"}}`), nil
		default:
			return nil, errors.New("unexpected command")
		}
	}), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), mutationGuard: mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		return start(ctx)
	}), validateClaimFn: func(context.Context, domain.ExternalEffectClaim) error { return nil }}
	claim := testClaim("draft_pr", identity, "title", "body")
	if _, err := client.CreateDraftPullRequest(context.Background(), claim, identity, "title", "body"); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"pr", "list", "--repo", "example/app", "--state", "all", "--limit", "100", "--json", prFields},
		{"api", "repos/example/app/git/ref/heads/main"},
		{"api", "repos/example/app/git/ref/heads/sf/dev/example/SF-44-random"},
		{"pr", "list", "--repo", "example/app", "--state", "all", "--limit", "100", "--json", prFields},
		{"pr", "create", "--repo", "example/app", "--head", "example:sf/dev/example/SF-44-random", "--base", "main", "--draft", "--title", "title", "--body", "body\n\n" + ownershipMarker(identity)},
		{"pr", "list", "--repo", "example/app", "--state", "all", "--limit", "100", "--json", prFields},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("official gh argv\n got: %#v\nwant: %#v", got, want)
	}
}

func TestOfficialMergeArgvGoldenAndProof(t *testing.T) {
	identity := contracts.PullRequestIdentity{Number: 7, Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"}, HeadOwner: "example", HeadRepository: "app", HeadRef: "sf/dev/example/SF-44-random", HeadOID: strings.Repeat("a", 40), BaseRef: "main", FactoryOwned: true}
	wire := mergeWire(identity, "MERGED", "CLEAN", "2026-01-01T00:00:00Z", nil)
	wire["mergeCommit"] = map[string]string{"oid": strings.Repeat("b", 40)}
	payload, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]string
	verified := false
	viewCalls := 0
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) {
		got = append(got, append([]string(nil), args...))
		if args[0] == "pr" && args[1] == "merge" {
			return nil, nil
		}
		if args[0] == "api" {
			if strings.Contains(strings.Join(args, "\x00"), "branchProtectionRule") {
				return []byte(`{"data":{"repository":{"ref":{"branchProtectionRule":{"id":"rule-main","pattern":"main","requiresStrictStatusChecks":true,"isAdminEnforced":true,"bypassPullRequestAllowances":{"totalCount":0},"bypassForcePushAllowances":{"totalCount":0}}}}}}`), nil
			}
			if len(args) == 6 && args[1] == "--hostname" && args[2] == "github.com" && args[3] == "--method" && args[4] == "GET" {
				return []byte(`[]`), nil
			}
			if len(args) == 2 && args[1] == "repos/example/app/git/ref/heads/sf/dev/example/SF-44-random" {
				return []byte(`{"object":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`), nil
			}
			if len(args) == 2 && args[1] == "repos/example/app/git/ref/heads/main" {
				return []byte(`{"object":{"sha":"cccccccccccccccccccccccccccccccccccccccc"}}`), nil
			}
			return []byte(`{"data":{"repository":{"pullRequest":{"mergeQueueEntry":null}}}}`), nil
		}
		if args[0] == "pr" && args[1] == "list" {
			open := mergeWire(identity, "OPEN", "CLEAN", nil, nil)
			return json.Marshal([]map[string]any{open})
		}
		if args[0] == "pr" && args[1] == "view" {
			viewCalls++
			if viewCalls < 2 {
				open := mergeWire(identity, "OPEN", "CLEAN", nil, nil)
				return json.Marshal(open)
			}
			return payload, nil
		}
		return nil, errors.New("unexpected command")
	}), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil }), mutationGuard: mutationGuardFunc(func(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
		return start(ctx)
	}), validateClaimFn: func(context.Context, domain.ExternalEffectClaim) error { return nil }, mergeIntents: intentRecorderFunc(func(context.Context, domain.MergeIntent) error { return nil }), verifyProtectedBranch: verifierFunc(func(_ context.Context, repository contracts.RepositoryIdentity, baseRef, mergeCommit, originalBaseOID string) (contracts.ProtectedBranchObservation, error) {
		verified = repository == identity.Repository && baseRef == "main" && mergeCommit == strings.Repeat("b", 40)
		return contracts.ProtectedBranchObservation{Repository: repository, BaseRef: baseRef, MergeCommit: mergeCommit, OriginalBaseOID: originalBaseOID, BaseHeadOID: strings.Repeat("d", 40), Contains: true}, nil
	})}
	claim := testClaim("merge", identity, identity.HeadOID, "squash")
	authorization := testAuthorization(identity)
	if err := client.MergeExactHead(context.Background(), claim, identity, identity.HeadOID, "squash", authorization); err != nil || !verified {
		t.Fatalf("guarded merge verified=%v err=%v", verified, err)
	}
	queue := []string{"api", "--hostname", "github.com", "graphql", "-f", "query=query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){pullRequest(number:$number){mergeQueueEntry{position}}}}", "-F", "owner=example", "-F", "name=app", "-F", "number=7"}
	protection := []string{"api", "--hostname", "github.com", "graphql", "-f", "query=query($owner:String!,$name:String!,$qualifiedRef:String!){repository(owner:$owner,name:$name){ref(qualifiedName:$qualifiedRef){branchProtectionRule{id pattern requiresStrictStatusChecks isAdminEnforced requiredStatusCheckContexts bypassPullRequestAllowances(first:1){totalCount} bypassForcePushAllowances(first:1){totalCount}}}}}", "-F", "owner=example", "-F", "name=app", "-F", "qualifiedRef=refs/heads/main"}
	rules := []string{"api", "--hostname", "github.com", "--method", "GET", "repos/example/app/rules/branches/main?per_page=100&page=1"}
	rulesetAudit := []string{"api", "--hostname", "github.com", "--method", "GET", "repos/example/app/rulesets?includes_parents=true&targets=branch&per_page=100&page=1"}
	view := []string{"pr", "view", "7", "--repo", "example/app", "--json", prFields}
	want := [][]string{{"pr", "list", "--repo", "example/app", "--state", "all", "--limit", "100", "--json", prFields}, queue, protection, rules, rulesetAudit, view, queue, protection, rules, rulesetAudit, {"pr", "merge", "7", "--repo", "example/app", "--match-head-commit", identity.HeadOID, "--squash"}, view}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("official merge argv\n got: %#v\nwant: %#v", got, want)
	}
}

func mergeWire(identity contracts.PullRequestIdentity, state, mergeState string, mergedAt, autoMerge any) map[string]any {
	return map[string]any{
		"number": identity.Number, "title": "title", "body": ownershipMarker(identity),
		"headRepositoryOwner": map[string]string{"login": identity.HeadOwner},
		"headRepository":      map[string]string{"nameWithOwner": identity.HeadOwner + "/" + identity.HeadRepository},
		"headRefName":         identity.HeadRef, "headRefOid": identity.HeadOID, "baseRefName": identity.BaseRef, "baseRefOid": strings.Repeat("c", 40),
		"isDraft": false, "mergedAt": mergedAt, "mergeCommit": nil, "state": state, "mergeStateStatus": mergeState, "autoMergeRequest": autoMerge,
	}
}

func TestUpdateAndReadyReconcileOnlyExactObservedState(t *testing.T) {
	client, fake, identity := fixture(t)
	pr := createDraft(t, client, identity, "before", "before body")
	if err := fake.SetResponse("pr_edit", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	updateClaim := testClaim("pr_edit", pr.Identity, "after", "after body")
	if err := client.UpdatePullRequest(context.Background(), updateClaim, pr.Identity, "after", "after body"); err != nil {
		t.Fatalf("update reconciliation=%v", err)
	}
	if err := fake.SetResponse("pr_ready", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	durable := testClaim("pr_ready", pr.Identity)
	if err := client.MarkReady(context.Background(), durable, pr.Identity); err != nil {
		t.Fatalf("ready reconciliation=%v", err)
	}
}

func TestChecksAllowExtrasButFailureDominatesPending(t *testing.T) {
	actual := []contracts.RequiredCheck{{Name: "required", ExternalID: "one", State: "SUCCESS"}, {Name: "extra", ExternalID: "two", State: "PENDING"}}
	if err := evaluateChecks(actual, []CheckIdentity{{Name: "required", ExternalID: "one"}}); !errors.Is(err, ErrChecksPending) {
		t.Fatalf("extra pending=%v", err)
	}
	actual[1].State = "FAILURE"
	if err := evaluateChecks(actual, []CheckIdentity{{Name: "required", ExternalID: "one"}}); !errors.Is(err, ErrChecksFailed) {
		t.Fatalf("failure precedence=%v", err)
	}
}

func TestRequiredChecksMatchProtectionRequiresExactConfiguredSet(t *testing.T) {
	protection := strictProtectionWitness{Kind: "ruleset", Checks: []string{"lint\x00-", "unit\x00-"}}
	if !requiredChecksMatchProtection([]contracts.RequiredCheck{{Name: "unit", ExternalID: "run-1"}, {Name: "lint", ExternalID: "run-2"}}, protection) {
		t.Fatal("exact configured check set was refused")
	}
	if requiredChecksMatchProtection([]contracts.RequiredCheck{{Name: "lint", ExternalID: "run-2"}}, protection) || requiredChecksMatchProtection([]contracts.RequiredCheck{{Name: "lint", ExternalID: "run-2"}, {Name: "unit", ExternalID: "run-1"}, {Name: "extra", ExternalID: "run-3"}}, protection) {
		t.Fatal("subset or extra check set was accepted")
	}
}

func TestObserveCIRequiredCheckPolicyAcceptsClassicProtectionContexts(t *testing.T) {
	client, fake, identity := fixture(t)
	if err := fake.SetRequiredStatusCheckContextsForTest("lint", "unit"); err != nil {
		t.Fatal(err)
	}
	pr := createDraft(t, client, identity, "title", "body")
	if err := fake.SetChecks(pr.Identity.Number,
		contracts.RequiredCheck{Name: "lint", ExternalID: "https://github.com/example/app/actions/runs/1", State: "SUCCESS"},
		contracts.RequiredCheck{Name: "unit", ExternalID: "https://github.com/example/app/actions/runs/2", State: "SUCCESS"},
	); err != nil {
		t.Fatal(err)
	}
	policy, err := client.ObserveCIRequiredCheckPolicy(context.Background(), pr.Identity)
	if err != nil || len(policy.RequiredChecks) != 2 {
		t.Fatalf("classic policy observation=%+v err=%v", policy, err)
	}
}

func TestRequiredChecksMatchProtectionRejectsUnprovableRulesetIntegration(t *testing.T) {
	protection := strictProtectionWitness{Kind: "ruleset", Checks: []string{"unit\x0042"}}
	if requiredChecksMatchProtection([]contracts.RequiredCheck{{Name: "unit", ExternalID: "https://github.com/acme/app/actions/runs/9"}}, protection) {
		t.Fatal("ruleset integration requirement was accepted from an unrelated run URL")
	}
}

func TestWaitChecksBoundsBackgroundContext(t *testing.T) {
	client, fake, identity := fixture(t)
	pr := createDraft(t, client, identity, "title", "body")
	if err := fake.SetChecks(pr.Identity.Number, contracts.RequiredCheck{Name: "unit", ExternalID: "one", State: "PENDING"}); err != nil {
		t.Fatal(err)
	}
	client.runner = commandRunnerFunc(func(_ context.Context, _ string, args, _ []string) ([]byte, error) { return fake.Run(args) })
	old := maxGHDeadline
	maxGHDeadline = 300 * time.Millisecond
	t.Cleanup(func() { maxGHDeadline = old })
	checkID := canonicalCheckExternalID(checkWire{Link: "one"})
	if _, err := client.WaitChecks(context.Background(), pr.Identity, []CheckIdentity{{Name: "unit", ExternalID: checkID}}, time.Millisecond, time.Millisecond); !errors.Is(err, ErrChecksPending) {
		t.Fatalf("bounded background polling=%v", err)
	}
}

func TestWaitChecksPreservesCancellationAsPending(t *testing.T) {
	client, _, identity := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.WaitChecks(ctx, identity, nil, time.Millisecond, time.Millisecond); !errors.Is(err, ErrChecksPending) {
		t.Fatalf("cancelled checks=%v", err)
	}
}

func TestStrictJSONBoundedSanitizedCommandBoundary(t *testing.T) {
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: filepath.Join(t.TempDir(), "gh-config"), runner: commandRunnerFunc(func(context.Context, string, []string, []string) ([]byte, error) {
		return []byte(`{"unknown":true}`), nil
	}), quarantiner: cleanupQuarantinerFunc(func(context.Context) error { return nil })}
	var value struct{}
	if err := client.json(context.Background(), &value, "repo", "view"); !errors.Is(err, ErrMalformedResponse) {
		t.Fatalf("unknown json=%v", err)
	}
	client.runner = commandRunnerFunc(func(context.Context, string, []string, []string) ([]byte, error) {
		return make([]byte, maxResponse+1), nil
	})
	if _, err := client.run(context.Background(), "repo", "view"); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("oversized=%v", err)
	}
	client.runner = commandRunnerFunc(func(context.Context, string, []string, []string) ([]byte, error) {
		return []byte("secret-token-in-output"), errors.New("failure")
	})
	if _, err := client.run(context.Background(), "repo", "view"); err == nil || err.Error() != "gh command failed" {
		t.Fatalf("sanitized error=%v", err)
	}
}

func TestRunBoundedKillsProcessGroupOnDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := runBounded(ctx, "/bin/sh", []string{"-c", "sleep 5 & wait"}, []string{"PATH=/usr/bin:/bin"})
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("stuck process group err=%v elapsed=%s", err, time.Since(started))
	}
}

func TestRunBoundedClosesRetainedPipeFromEscapedDescendant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := filepath.Join(t.TempDir(), "escaped-child-ready")
	script := "import os,time\nif os.fork()==0:\n if os.fork()==0:\n  os.setsid(); print('escaped-child-pid='+str(os.getpid()), flush=True); ready=os.environ['SF_TEST_READY']; open(ready+'.tmp','w').write(str(os.getpid())); os.replace(ready+'.tmp',ready); time.sleep(5)\n os._exit(0)\ntime.sleep(5)"
	type boundedResult struct {
		output []byte
		err    error
	}
	done := make(chan boundedResult, 1)
	go func() {
		output, err := runBounded(ctx, "/usr/bin/python3", []string{"-c", script}, []string{"PATH=/usr/bin:/bin", "SF_TEST_READY=" + ready})
		done <- boundedResult{output: output, err: err}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		select {
		case result := <-done:
			t.Fatalf("hostile fixture exited before readiness: err=%v output=%q", result.err, result.output)
		default:
		}
		if time.Now().After(deadline) {
			cancel()
			result := <-done
			t.Fatalf("hostile fixture did not become ready: err=%v output=%q", result.err, result.output)
		}
		time.Sleep(10 * time.Millisecond)
	}
	readyPID, err := os.ReadFile(ready)
	if err != nil {
		cancel()
		result := <-done
		t.Fatalf("read escaped child pid: %v; fixture err=%v output=%q", err, result.err, result.output)
	}
	escapedPID, err := strconv.Atoi(strings.TrimSpace(string(readyPID)))
	if err != nil || escapedPID <= 0 {
		cancel()
		result := <-done
		t.Fatalf("parse escaped child pid %q: %v; fixture err=%v output=%q", readyPID, err, result.err, result.output)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(escapedPID, syscall.SIGKILL)
	})
	cleanupDeadline := time.NewTimer(2 * time.Second)
	defer cleanupDeadline.Stop()
	cancel()
	var result boundedResult
	select {
	case result = <-done:
	case <-cleanupDeadline.C:
		t.Fatal("escaped retained pipe cleanup exceeded 2 seconds after cancellation")
	}
	output, err := result.output, result.err
	if !errors.Is(err, ErrProcessCleanup) {
		t.Fatalf("escaped retained pipe err=%v output=%q", err, output)
	}
	if !strings.Contains(string(output), "escaped-child-pid="+strconv.Itoa(escapedPID)) {
		t.Fatalf("escaped child pid missing from output: pid=%d output=%q", escapedPID, output)
	}
}
