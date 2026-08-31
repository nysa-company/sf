package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/testkit"
)

func ciAuthorityPublishedFixture(t *testing.T) (*Store, Ticket, domain.Fence) {
	t.Helper()
	db, ctx, ticket, fence := publicationLifecycleFixture(t)
	worktree, err := db.Worktree(ctx, ticket.Ref)
	if err != nil {
		t.Fatalf("CI fixture load worktree: %v", err)
	}
	// The publication lifecycle is still in publishing here: its immutable
	// publication witness is recorded immediately below.  Use the same
	// candidate-only authenticated reader as the publication path rather than
	// requiring the strict current-publication witness prematurely.
	candidate, err := db.RecoverableCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatalf("CI fixture load candidate: %v", err)
	}
	pr := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, Number: 101, HeadOwner: "acme", HeadRepository: "app", HeadRef: worktree.Branch, HeadOID: candidate.Snapshot.HeadSHA, BaseRef: "main", BaseOID: candidate.Snapshot.BaseSHA, FactoryOwned: true}
	value := PublishedCandidateEvidence{Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence, Candidate: candidate, ConfigGeneration: ticket.ConfigGeneration, ConfigDigest: ticket.ConfigDigest, ConfigSnapshotDigest: sha256Digest(ticket.ConfigSnapshot), Worktree: worktree, RemoteBranchRef: worktree.Branch, RemoteBranchOID: candidate.Snapshot.HeadSHA, RemoteBaseOID: candidate.Snapshot.BaseSHA, PullRequest: pr, PullRequestState: "OPEN", PullRequestDraft: true, PullRequestObservedAt: time.Now().UTC(), CreatedAt: time.Now().UTC()}
	value.PushEffect = PublicationEffectEvidence{SemanticKey: "ci-push", Kind: PublicationPushEffectKind, RequestDigest: strings.Repeat("1", 64), ClaimEpoch: 1, ObservedIdentity: CanonicalPublicationPushObservation(value.RemoteBranchRef, value.RemoteBranchOID)}
	value.PRCreateOrUpdateEffect = PublicationEffectEvidence{SemanticKey: "ci-pr", Kind: PublicationPRCreateEffectKind, RequestDigest: "sha256:" + strings.Repeat("2", 64), ClaimEpoch: 1, ObservedIdentity: CanonicalPublicationPRObservation(pr, "OPEN", true)}
	for _, effect := range []PublicationEffectEvidence{value.PushEffect, value.PRCreateOrUpdateEffect} {
		if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: effect.SemanticKey, Ref: ticket.Ref, Kind: effect.Kind, TicketVersion: ticket.Version, Fence: fence, RequestDigest: effect.RequestDigest}); err != nil {
			t.Fatalf("CI fixture plan %s effect: %v", effect.SemanticKey, err)
		}
		claim, err := db.ClaimEffect(ctx, EffectFence{SemanticKey: effect.SemanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence})
		if err != nil {
			t.Fatalf("CI fixture claim %s effect: %v", effect.SemanticKey, err)
		}
		if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: effect.SemanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: claim.Effect.LeaderEpoch, RunnerEpoch: claim.Effect.RunnerEpoch, ClaimEpoch: claim.Effect.ClaimEpoch}}, effect.ObservedIdentity); err != nil {
			t.Fatalf("CI fixture confirm %s effect: %v", effect.SemanticKey, err)
		}
	}
	if err := db.RecordPublishedCandidate(ctx, value); err != nil {
		t.Fatalf("CI fixture record publication: %v", err)
	}
	if _, err := db.TransitionPublishedCandidate(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePublishing, To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: fence}); err != nil {
		t.Fatalf("CI fixture transition publication: %v", err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatalf("CI fixture reload ticket: %v", err)
	}
	return db, ticket, fence
}

func TestCIObservationCanonicalizesAndClassifiesChecks(t *testing.T) {
	checks, err := NormalizeCIObservationChecks([]contracts.RequiredCheck{{Name: " z", ExternalID: "2", State: "SUCCESS"}, {Name: "a", ExternalID: "1", State: "IN_PROGRESS"}})
	if err != nil || len(checks) != 2 || checks[0].CanonicalName != "a" || checks[0].NormalizedState != "pending" || checks[1].NormalizedState != "success" {
		t.Fatalf("checks=%+v err=%v", checks, err)
	}
	if _, err := NormalizeCIObservationChecks([]contracts.RequiredCheck{{Name: "lint", ExternalID: "same", State: "success"}, {Name: "lint", ExternalID: "same", State: "failure"}}); !errors.Is(err, ErrCIObservation) {
		t.Fatalf("duplicate check err=%v", err)
	}
	for _, state := range []string{"SKIPPED", "NEUTRAL"} {
		checks, err := NormalizeCIObservationChecks([]contracts.RequiredCheck{{Name: "lint", ExternalID: state, State: state}})
		if err != nil || checks[0].NormalizedState != strings.ToLower(state) {
			t.Fatalf("terminal green state %s checks=%+v err=%v", state, checks, err)
		}
	}
}

// TestCIPollerAuthorityE2E exercises the production poller's Store sequence
// against FakeGH's durable state. The localruntime worker is deliberately a
// thin caller of these Store APIs; this fixture keeps the public candidate and
// recovery authorities real without fabricating any evidence row.
func TestCIPollerAuthorityE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		state  string
		setup  func(*Store, Ticket, domain.Fence)
		want   domain.State
		resume domain.State
	}{
		{name: "pending_replay", state: "PENDING", want: domain.StateWaitingCI},
		{name: "green_reviewing", state: "SUCCESS", want: domain.StateReviewing},
		{name: "red_consumes_one_budget", state: "FAILURE", want: domain.StateBuilding},
		{name: "red_exhausted_pauses", state: "FAILURE", setup: func(db *Store, ticket Ticket, fence domain.Fence) {
			for _, id := range []string{"prior-one", "prior-two"} {
				if _, err := db.ConsumeBudget(t.Context(), BudgetUse{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Kind: "correction", RequestID: id}); err != nil {
					t.Fatal(err)
				}
			}
		}, want: domain.StatePaused, resume: domain.StateWaitingCI},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, ticket, fence := ciAuthorityPublishedFixture(t)
			defer db.Close()
			publication, err := db.LoadPublishedCandidate(t.Context(), ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			gh, err := testkit.NewFakeGH(t.TempDir()+"/fake-gh.json", publication.PullRequest.Repository)
			if err != nil {
				t.Fatal(err)
			}
			if err := gh.SetAuthenticated(true); err != nil {
				t.Fatal(err)
			}
			if err := gh.InjectPullRequestForTest(testkit.PullRequest{Identity: publication.PullRequest, Draft: true}); err != nil {
				t.Fatal(err)
			}
			if err := gh.SetChecks(publication.PullRequest.Number, contracts.RequiredCheck{Name: "lint", ExternalID: "check-lint", State: test.state}); err != nil {
				t.Fatal(err)
			}
			if test.setup != nil {
				test.setup(db, ticket, fence)
			}
			if err := pollCIAuthority(t.Context(), db, ticket, fence, gh); err != nil {
				t.Fatal(err)
			}
			current, err := db.Ticket(t.Context(), ticket.Ref)
			if err != nil || current.State != test.want || current.ResumeState != test.resume {
				t.Fatalf("ticket=%+v err=%v", current, err)
			}
			if test.name == "pending_replay" {
				// A second bounded poll reads the durable pending chain rather than
				// replaying an external action or reconstructing publication state.
				if err := pollCIAuthority(t.Context(), db, current, domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: current.RunnerEpoch}, gh); err != nil {
					t.Fatal(err)
				}
				current, err = db.Ticket(t.Context(), ticket.Ref)
				if err != nil || current.State != domain.StateWaitingCI || current.Version != ticket.Version+2 {
					t.Fatalf("pending replay ticket=%+v err=%v", current, err)
				}
			}
			if test.want == domain.StateBuilding {
				var uses int
				if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&uses); err != nil || uses != 1 {
					t.Fatalf("correction uses=%d err=%v", uses, err)
				}
			}
			if test.want == domain.StatePaused && current.BlockedCode != "" {
				t.Fatalf("ci red exhaustion must use normative pause, got blocked code %q", current.BlockedCode)
			}
			if test.want == domain.StatePaused {
				events, err := db.Events(t.Context(), ticket.Ref.Channel, 0, 100)
				if err != nil || len(events) == 0 || events[len(events)-1].Trigger != "checks_red" || !strings.Contains(events[len(events)-1].Payload, `"code":"ci_red_exhausted"`) {
					t.Fatalf("typed ci exhaustion event=%+v err=%v", events, err)
				}
			}
		})
	}
}

func TestCIPollerAuthorityPendingRestartReplaysDurableState(t *testing.T) {
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	publication, err := db.LoadPublishedCandidate(t.Context(), ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	ghPath := filepath.Join(t.TempDir(), "fake-gh.json")
	gh, err := testkit.NewFakeGH(ghPath, publication.PullRequest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := gh.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	if err := gh.InjectPullRequestForTest(testkit.PullRequest{Identity: publication.PullRequest, Draft: true}); err != nil {
		t.Fatal(err)
	}
	if err := gh.SetChecks(publication.PullRequest.Number, contracts.RequiredCheck{Name: "lint", ExternalID: "check-lint", State: "PENDING"}); err != nil {
		t.Fatal(err)
	}
	if err := pollCIAuthority(t.Context(), db, ticket, fence, gh); err != nil {
		t.Fatal(err)
	}
	pending, err := db.Ticket(t.Context(), ticket.Ref)
	if err != nil || pending.State != domain.StateWaitingCI {
		t.Fatalf("pending ticket=%+v err=%v", pending, err)
	}
	restartDir := t.TempDir()
	// Backup intentionally rejects group/world-accessible parents because it
	// may contain sealed authority rows. Go's test temp root is 0755 on macOS,
	// so make this fixture's dedicated parent satisfy that production contract.
	if err := os.Chmod(restartDir, 0o700); err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(restartDir, "restart.sqlite")
	if err := db.Backup(t.Context(), databasePath); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(t.Context(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	leader, err := db.AcquireLeader(t.Context(), domain.ChannelDev, "ci-poller-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(t.Context(), domain.ChannelDev, leader); err != nil || changed != 1 {
		t.Fatalf("pending CI signed runner recovery changed=%d err=%v", changed, err)
	}
	if err := db.RebindRecoveredPublishedCandidates(t.Context(), domain.ChannelDev, leader); err != nil {
		t.Fatal(err)
	}
	recovered, err := db.Ticket(t.Context(), ticket.Ref)
	if err != nil || recovered.State != domain.StateWaitingCI || recovered.RunnerEpoch == pending.RunnerEpoch {
		t.Fatalf("recovered ticket=%+v err=%v", recovered, err)
	}
	restartedGH, err := testkit.OpenFakeGH(ghPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := pollCIAuthority(t.Context(), db, recovered, domain.Fence{LeaderEpoch: leader, RunnerEpoch: recovered.RunnerEpoch}, restartedGH); err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(t.Context(), ticket.Ref)
	if err != nil || current.State != domain.StateWaitingCI || current.Version != recovered.Version+1 {
		t.Fatalf("replayed ticket=%+v err=%v", current, err)
	}
}

func pollCIAuthority(ctx context.Context, db *Store, ticket Ticket, fence domain.Fence, gh interface {
	contracts.CIRequiredCheckPolicyObserver
	RequiredChecks(context.Context, contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error)
}) error {
	if err := db.RecordCIRequiredCheckPolicyFromObserver(ctx, ticket.Ref, gh); err != nil {
		return fmt.Errorf("record fresh CI policy: %w", err)
	}
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		return fmt.Errorf("load current published candidate: %w", err)
	}
	checks, err := gh.RequiredChecks(ctx, publication.PullRequest)
	if err != nil {
		return fmt.Errorf("read live required checks: %w", err)
	}
	normalized, err := NormalizeCIObservationChecks(checks)
	if err != nil {
		return fmt.Errorf("normalize required checks: %w", err)
	}
	classification := "green"
	for _, check := range normalized {
		if check.NormalizedState == "failure" || check.NormalizedState == "cancelled" {
			classification = "red"
			break
		}
		if check.NormalizedState == "pending" {
			classification = "pending"
		}
	}
	observation := CIObservation{Ref: ticket.Ref, CandidateGeneration: publication.Candidate.Snapshot.Generation, CandidateHeadSHA: publication.Candidate.Snapshot.HeadSHA, CandidateTreeSHA: publication.Candidate.Snapshot.TreeSHA, PublicationWitnessDigest: publication.WitnessDigest, PullRequest: publication.PullRequest, ObservedTicketVersion: ticket.Version, ObservedFence: fence, ObservedAt: time.Now().UTC(), RequiredChecks: normalized, Classification: classification}
	if err := db.RecordAuthenticatedCIObservation(ctx, observation); err != nil {
		return fmt.Errorf("record authenticated CI observation: %w", err)
	}
	stored, err := db.LoadCIObservation(ctx, ticket.Ref)
	if err != nil {
		return fmt.Errorf("load authenticated CI observation: %w", err)
	}
	request := CIObservationTransition{Ref: ticket.Ref, ObservationDigest: stored.ObservationDigest, ExpectedVersion: ticket.Version, Fence: fence}
	if stored.Classification == "red" {
		id := "ci-red/" + strings.TrimPrefix(stored.ObservationDigest, "sha256:")
		if _, err := db.ConsumeBudget(ctx, BudgetUse{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Kind: "correction", RequestID: id}); err == nil {
			request.CorrectionBudget = &CorrectionBudgetAuthority{Ref: ticket.Ref, RequestID: id, TicketVersion: ticket.Version, Fence: fence}
		} else if !errors.Is(err, ErrBudgetExhausted) {
			return err
		}
	}
	_, err = db.ConsumeAuthenticatedCIObservation(ctx, request)
	if err != nil {
		return fmt.Errorf("consume authenticated CI observation: %w", err)
	}
	return nil
}

func TestCIObservationRecordLoadAndRedExhaustedReplay(t *testing.T) {
	db, ticket, publicationFence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	observation := CIObservation{Ref: ticket.Ref, CandidateGeneration: publication.Candidate.Snapshot.Generation, CandidateHeadSHA: publication.Candidate.Snapshot.HeadSHA, CandidateTreeSHA: publication.Candidate.Snapshot.TreeSHA, PublicationWitnessDigest: publication.WitnessDigest, PullRequest: publication.PullRequest, ObservedTicketVersion: ticket.Version, ObservedFence: domain.Fence{LeaderEpoch: publicationFence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch}, ObservedAt: time.Now().UTC(), RequiredChecks: []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-1", NormalizedState: "failure", FailingDiagnosticText: "lint failed"}}, Classification: "red"}
	if err := recordCIAuthorityPolicy(db, ctx, publication, ticket, []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-1"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordCIObservation(ctx, observation); err != nil {
		t.Fatal(err)
	}
	loaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
	if err != nil || loaded.Classification != "red" || loaded.RequiredChecks[0].FailingDiagnosticDigest == "" {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	if err := db.RecordCIObservation(ctx, observation); err != nil {
		t.Fatalf("exact observation replay=%v", err)
	}
	result, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence})
	if err != nil || result.Version != ticket.Version+1 {
		t.Fatalf("red exhausted result=%+v err=%v", result, err)
	}
	if current, err := db.Ticket(ctx, ticket.Ref); err != nil || current.State != domain.StatePaused || current.ResumeState != domain.StateWaitingCI {
		t.Fatalf("paused ticket=%+v err=%v", current, err)
	}
	replay, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence})
	if err != nil || replay != result {
		t.Fatalf("lost response replay=%+v/%+v err=%v", replay, result, err)
	}
}

func ciAuthorityObservationFor(publication PublishedCandidateEvidence, ticket Ticket, fence domain.Fence, classification string, observedAt time.Time, checks []CIObservationCheck) CIObservation {
	return CIObservation{
		Ref:                      ticket.Ref,
		CandidateGeneration:      publication.Candidate.Snapshot.Generation,
		CandidateHeadSHA:         publication.Candidate.Snapshot.HeadSHA,
		CandidateTreeSHA:         publication.Candidate.Snapshot.TreeSHA,
		PublicationWitnessDigest: publication.WitnessDigest,
		PullRequest:              publication.PullRequest,
		ObservedTicketVersion:    ticket.Version,
		ObservedFence:            domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: ticket.RunnerEpoch},
		ObservedAt:               observedAt,
		RequiredChecks:           checks,
		Classification:           classification,
	}
}

func ciAuthorityPolicyFor(publication PublishedCandidateEvidence, ticket Ticket, checks []CIObservationCheck) CIRequiredCheckPolicy {
	return CIRequiredCheckPolicy{
		Ref:                      ticket.Ref,
		CandidateGeneration:      publication.Candidate.Snapshot.Generation,
		CandidateHeadSHA:         publication.Candidate.Snapshot.HeadSHA,
		CandidateTreeSHA:         publication.Candidate.Snapshot.TreeSHA,
		PublicationWitnessDigest: publication.WitnessDigest,
		ProtectedBranchRef:       publication.PullRequest.BaseRef,
		ProtectedBranchOID:       publication.PullRequest.BaseOID,
		PolicySourceDigest:       strings.Repeat("a", 64),
		AuthenticatedPrincipal:   "ci-observer",
		RequiredChecks:           checks,
	}
}

type fakeCIAuthorityPolicyObserver struct {
	value contracts.CIRequiredCheckPolicyObservation
}

func (f fakeCIAuthorityPolicyObserver) ObserveCIRequiredCheckPolicy(_ context.Context, want contracts.PullRequestIdentity) (contracts.CIRequiredCheckPolicyObservation, error) {
	if f.value.PullRequest != want {
		return contracts.CIRequiredCheckPolicyObservation{}, ErrCIObservation
	}
	return f.value, nil
}

func recordCIAuthorityPolicy(db *Store, ctx context.Context, publication PublishedCandidateEvidence, ticket Ticket, checks []CIObservationCheck) error {
	observedChecks := make([]contracts.RequiredCheck, len(checks))
	for i, check := range checks {
		observedChecks[i] = contracts.RequiredCheck{Name: check.CanonicalName, ExternalID: check.ExternalID, State: "SUCCESS"}
	}
	value := contracts.CIRequiredCheckPolicyObservation{
		PullRequest: publication.PullRequest, ProtectedBranchRef: publication.PullRequest.BaseRef,
		ProtectedBranchOID: publication.PullRequest.BaseOID, PolicySourceDigest: strings.Repeat("a", 64),
		AuthenticatedPrincipal: "ci-observer", RequiredChecks: observedChecks, ObservedAt: time.Now().UTC(),
	}
	return db.RecordCIRequiredCheckPolicyFromObserver(ctx, ticket.Ref, fakeCIAuthorityPolicyObserver{value: value})
}

func ciAuthorityLintPolicy() []CIObservationCheck {
	return []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint"}}
}

func TestCIRequiredCheckPolicyRejectsRawCallerInjection(t *testing.T) {
	db, ticket, _ := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordCIRequiredCheckPolicy(ctx, ciAuthorityPolicyFor(publication, ticket, ciAuthorityLintPolicy())); !errors.Is(err, ErrCIObservation) {
		t.Fatalf("raw policy caller accepted: %v", err)
	}
}

func TestCIRequiredCheckPolicyAuthenticatesRunnerRecoveryBaseline(t *testing.T) {
	db, ticket, _ := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "ci-policy-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := recordCIAuthorityPolicy(db, ctx, publication, current, ciAuthorityLintPolicy()); err != nil {
		t.Fatalf("recovered waiting-ci policy=%v", err)
	}
}

func TestCIObservationRequiresExactPersistedPolicySet(t *testing.T) {
	db, ticket, publicationFence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	policyChecks := []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint"}, {CanonicalName: "unit", ExternalID: "check-unit"}}
	if err := recordCIAuthorityPolicy(db, ctx, publication, ticket, policyChecks); err != nil {
		t.Fatal(err)
	}
	for name, checks := range map[string][]CIObservationCheck{
		"omitted": {{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "success"}},
		"extra":   {{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "success"}, {CanonicalName: "unit", ExternalID: "check-unit", NormalizedState: "success"}, {CanonicalName: "security", ExternalID: "check-security", NormalizedState: "success"}},
	} {
		t.Run(name, func(t *testing.T) {
			observation := ciAuthorityObservationFor(publication, ticket, publicationFence, "green", time.Now().UTC(), checks)
			if err := db.RecordCIObservation(ctx, observation); !errors.Is(err, ErrCIObservation) {
				t.Fatalf("policy mismatch accepted: %v", err)
			}
		})
	}
}

func TestCIObservationPendingAndGreenReducers(t *testing.T) {
	for _, classification := range []string{"pending", "green"} {
		classification := classification
		t.Run(classification, func(t *testing.T) {
			db, ticket, publicationFence := ciAuthorityPublishedFixture(t)
			defer db.Close()
			ctx := t.Context()
			publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			checks := ciAuthorityLintPolicy()
			if err := recordCIAuthorityPolicy(db, ctx, publication, ticket, checks); err != nil {
				t.Fatal(err)
			}
			observation := ciAuthorityObservationFor(publication, ticket, publicationFence, classification, time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: map[string]string{"pending": "in_progress", "green": "success"}[classification]}})
			if err := db.RecordCIObservation(ctx, observation); err != nil {
				t.Fatal(err)
			}
			loaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			result, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence})
			if err != nil || result.Version != ticket.Version+1 {
				t.Fatalf("consume result=%+v err=%v", result, err)
			}
			current, err := db.Ticket(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			want := domain.StateReviewing
			if classification == "pending" {
				want = domain.StateWaitingCI
			}
			if current.State != want {
				t.Fatalf("state=%s want=%s", current.State, want)
			}
		})
	}
}

func TestCIObservationBindsPolicyWitnessBeforeDigest(t *testing.T) {
	db, ticket, publicationFence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	checks := ciAuthorityLintPolicy()
	if err := recordCIAuthorityPolicy(db, ctx, publication, ticket, checks); err != nil {
		t.Fatal(err)
	}
	observation := ciAuthorityObservationFor(publication, ticket, publicationFence, "green", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "success"}})
	if err := db.RecordCIObservation(ctx, observation); err != nil {
		t.Fatalf("empty caller digests rejected: %v", err)
	}
	loaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
	if err != nil || loaded.PolicyWitnessDigest == "" || loaded.ObservationDigest == "" {
		t.Fatalf("bound observation=%+v err=%v", loaded, err)
	}
	tampered := observation
	tampered.ObservationDigest = strings.Repeat("f", 64)
	if err := db.RecordCIObservation(ctx, tampered); !errors.Is(err, ErrCIObservation) {
		t.Fatalf("tampered observation digest accepted: %v", err)
	}
}

func TestCIObservationMalformedBudgetPausesRed(t *testing.T) {
	for name, authority := range map[string]CorrectionBudgetAuthority{
		"missing-request": {},
		"stale-fence":     {RequestID: "stale", TicketVersion: 999, Fence: domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}},
	} {
		name, authority := name, authority
		t.Run(name, func(t *testing.T) {
			db, ticket, publicationFence := ciAuthorityPublishedFixture(t)
			defer db.Close()
			ctx := t.Context()
			publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			checks := ciAuthorityLintPolicy()
			if err := recordCIAuthorityPolicy(db, ctx, publication, ticket, checks); err != nil {
				t.Fatal(err)
			}
			observation := ciAuthorityObservationFor(publication, ticket, publicationFence, "red", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "failure", FailingDiagnosticText: "failed"}})
			if err := db.RecordCIObservation(ctx, observation); err != nil {
				t.Fatal(err)
			}
			loaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			authority.Ref = ticket.Ref
			result, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, CorrectionBudget: &authority})
			if err != nil || result.Version != ticket.Version+1 {
				t.Fatalf("malformed budget result=%+v err=%v", result, err)
			}
			current, err := db.Ticket(ctx, ticket.Ref)
			if err != nil || current.State != domain.StatePaused || current.ResumeState != domain.StateWaitingCI {
				t.Fatalf("ticket=%+v err=%v", current, err)
			}
		})
	}
}

func TestCIObservationValidRepairBudgetRequiresSuccessorCompletion(t *testing.T) {
	db, ticket, publicationFence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	checks := ciAuthorityLintPolicy()
	if err := recordCIAuthorityPolicy(db, ctx, publication, ticket, checks); err != nil {
		t.Fatal(err)
	}
	red := ciAuthorityObservationFor(publication, ticket, publicationFence, "red", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "failure", FailingDiagnosticText: "failed"}})
	if err := db.RecordCIObservation(ctx, red); err != nil {
		t.Fatal(err)
	}
	loaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	requestID := "repair-request"
	if _, err := db.ConsumeBudget(ctx, BudgetUse{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, Kind: "correction", RequestID: requestID}); err != nil {
		t.Fatal(err)
	}
	authority := CorrectionBudgetAuthority{Ref: ticket.Ref, RequestID: requestID, TicketVersion: ticket.Version, Fence: loaded.ObservedFence}
	if _, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, CorrectionBudget: &authority}); err != nil {
		t.Fatal(err)
	}
	building, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || building.State != domain.StateBuilding {
		t.Fatalf("building ticket=%+v err=%v", building, err)
	}
	var bindingCount, invalidationCount int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, publication.Candidate.Snapshot.Generation+1).Scan(&bindingCount); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM invalidation_receipts WHERE channel=? AND project_id=? AND ticket_id=? AND generation=? AND kind IN ('github_checks','final_review','approval')`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, publication.Candidate.Snapshot.Generation).Scan(&invalidationCount); err != nil {
		t.Fatal(err)
	}
	if bindingCount != 1 || invalidationCount != 3 {
		t.Fatalf("repair authority cardinality binding=%d invalidations=%d", bindingCount, invalidationCount)
	}

	builderQualification, _ := setupProviderPair(t, db, ctx)
	worktree, err := db.Worktree(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	buildFence := domain.Fence{LeaderEpoch: publicationFence.LeaderEpoch, RunnerEpoch: building.RunnerEpoch}
	builderRequest := supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: building.Version, Fence: buildFence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(builderQualification), ConfigDigest: building.ConfigDigest, Capacity: 1, At: time.Now().UTC()})
	builderRequest.WorktreeIdentity = string(worktree.IdentityJSON)
	builderRequest.Input.WorktreeIdentity = string(worktree.IdentityJSON)
	builderRequest.BaseSHA = worktree.BaseSHA
	builderRequest.Input.BaseSHA = worktree.BaseSHA
	claim, err := db.BeginProviderAttempt(ctx, builderRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "repair", ProcessStartIdentity: fmt.Sprintf("repair-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	builderRaw := []byte(`{"schema":"sf.builder/v1","summary":"repair","changed_files":["internal/repair.go"],"commands":[["go","test"]]}`)
	if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), building.Version, buildFence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: builderRaw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: building.Type}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	key := ProviderAttemptResultKey{AttemptID: claim.ID, Ref: ticket.Ref, Phase: domain.PhaseBuild, Attempt: claim.Attempt}
	_, parsed, err := db.LoadHistoricalProviderAttemptResult(ctx, key)
	if err != nil || parsed.Builder == nil {
		t.Fatalf("builder result=%+v err=%v", parsed, err)
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := publication.Candidate.Commit.ParentOID
	policyDigest := sha256Digest([]byte("repair-policy"))
	command := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePostbuildCandidate, ticket.Ref, building.Version, buildFence, key, publication.Candidate.Snapshot.VerificationIntentDigest, publication.Candidate.Snapshot.ProofDigest, checkpoint, "sha256:"+policyDigest, 0)
	successor := domain.CandidateSnapshot{Generation: publication.Candidate.Snapshot.Generation + 1, BaseSHA: publication.Candidate.Snapshot.BaseSHA, HeadSHA: strings.Repeat("7", 40), TreeSHA: strings.Repeat("8", 40), SourceDigest: publication.Candidate.Snapshot.SourceDigest, VerificationIntentDigest: publication.Candidate.Snapshot.VerificationIntentDigest, ProofDigest: publication.Candidate.Snapshot.ProofDigest, CommandPolicyDigest: policyDigest, BuilderEvidenceDigest: builderDigest}
	if _, err := db.RecordCandidate(ctx, CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: building.Version, Fence: buildFence, Snapshot: successor, BuilderResult: key, Commit: CommitObservation{CommitOID: successor.HeadSHA, ParentOID: checkpoint, TreeOID: successor.TreeSHA}, Reason: "CI repair", CommandResult: command}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionCandidate(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: building.Version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: buildFence, EventPayload: "{}"}, successor); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("successor published without repair completion: %v", err)
	}
	completion := CandidateRepairCompletion{Ref: ticket.Ref, TargetGeneration: successor.Generation, BuilderResultAttemptID: claim.ID, BuilderResultAttempt: claim.Attempt, BuilderBindingTicketVersion: building.Version, BuilderBindingFence: buildFence, FinalCandidateHeadSHA: successor.HeadSHA, FinalCandidateTreeSHA: successor.TreeSHA, CompletedAt: time.Now().UTC()}
	tampered := completion
	tampered.FinalCandidateTreeSHA = strings.Repeat("9", 40)
	if err := db.RecordCandidateRepairCompletion(ctx, tampered); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("tampered repair completion=%v", err)
	}
	// A leader-only change cannot rebind a builder completion. The original
	// immutable fence must have a complete signed recovery ledger.
	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "repair-completion-restart")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RecordCandidateRepairCompletion(ctx, completion); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("leader-only repair completion=%v", err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); err != nil || changed != 1 {
		t.Fatalf("signed repair runner recovery changed=%d err=%v", changed, err)
	}
	recovered, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || recovered.State != domain.StateBuilding || recovered.Version <= building.Version || recovered.RunnerEpoch <= buildFence.RunnerEpoch {
		t.Fatalf("recovered builder ticket=%+v err=%v", recovered, err)
	}
	if err := db.RecordCandidateRepairCompletion(ctx, completion); err != nil {
		t.Fatal(err)
	}
	if err := db.RecordCandidateRepairCompletion(ctx, completion); err != nil {
		t.Fatalf("exact recovered completion replay=%v", err)
	}
	recoveredCandidate, err := db.latestCandidateFrom(ctx, db.db, ticket.Ref, false)
	if err != nil || recoveredCandidate.Snapshot != successor || recoveredCandidate.TicketVersion != building.Version || recoveredCandidate.Fence != buildFence {
		t.Fatalf("recovered repair candidate=%+v err=%v", recoveredCandidate, err)
	}
	if err := validateRunnerRecoveryLedger(ctx, db.db, ticket.Ref, building.Version, buildFence.RunnerEpoch, buildFence.LeaderEpoch, recovered.Version, recovered.RunnerEpoch, newLeader); err != nil {
		t.Fatalf("recovered repair ledger=%v", err)
	}
	builderResult, _, err := db.LoadHistoricalProviderAttemptResult(ctx, recoveredCandidate.BuilderResult)
	if err != nil || providerResultReachesHistoricalFence(ctx, db.db, recoveredCandidate.BuilderResult, builderResult, recoveredCandidate.TicketVersion, recoveredCandidate.Fence) != nil {
		t.Fatalf("recovered repair builder authority result=%+v err=%v", builderResult, err)
	}
	if err := db.reauthenticateStoredCandidateCommandHistoricalFrom(ctx, db.db, ticket.Ref, recoveredCandidate); err != nil {
		t.Fatalf("recovered repair candidate command=%v", err)
	}
	if _, err := db.TransitionCandidate(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: recovered.Version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: recovered.RunnerEpoch}, EventPayload: "{}"}, successor); err != nil {
		t.Fatal(err)
	}
	var completionCount int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, successor.Generation).Scan(&completionCount); err != nil {
		t.Fatal(err)
	}
	if completionCount != 1 {
		t.Fatalf("completion cardinality=%d", completionCount)
	}
}

func TestCIObservationFutureAdmissionDoesNotPoisonNextValidRecord(t *testing.T) {
	db, ticket, publicationFence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	checks := ciAuthorityLintPolicy()
	if err := recordCIAuthorityPolicy(db, ctx, publication, ticket, checks); err != nil {
		t.Fatal(err)
	}
	future := ciAuthorityObservationFor(publication, ticket, publicationFence, "green", time.Now().UTC().Add(48*time.Hour), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "success"}})
	if err := db.RecordCIObservation(ctx, future); !errors.Is(err, ErrCIObservation) {
		t.Fatalf("future observation accepted: %v", err)
	}
	valid := ciAuthorityObservationFor(publication, ticket, publicationFence, "green", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "success"}})
	if err := db.RecordCIObservation(ctx, valid); err != nil {
		t.Fatalf("valid observation poisoned by future attempt: %v", err)
	}
}

func TestCIObservationOldTransitionReplayAfterLaterObservation(t *testing.T) {
	db, ticket, publicationFence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	checks := ciAuthorityLintPolicy()
	if err := recordCIAuthorityPolicy(db, ctx, publication, ticket, checks); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE ci_required_check_policies SET required_set_digest=? WHERE channel=? AND project_id=? AND ticket_id=?`, strings.Repeat("0", 64), ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err == nil {
		t.Fatal("policy tamper was accepted")
	}
	first := ciAuthorityObservationFor(publication, ticket, publicationFence, "pending", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "pending"}})
	if err := db.RecordCIObservation(ctx, first); err != nil {
		t.Fatal(err)
	}
	firstLoaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	firstResult, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: ticket.Ref, ObservationDigest: firstLoaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: firstLoaded.ObservedFence})
	if err != nil {
		t.Fatal(err)
	}
	laterTicket, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	later := ciAuthorityObservationFor(publication, laterTicket, publicationFence, "pending", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "pending"}})
	if err := db.RecordCIObservation(ctx, later); err != nil {
		t.Fatal(err)
	}
	laterLoaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: ticket.Ref, ObservationDigest: laterLoaded.ObservationDigest, ExpectedVersion: laterTicket.Version, Fence: laterLoaded.ObservedFence}); err != nil {
		t.Fatal(err)
	}
	replay, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: ticket.Ref, ObservationDigest: firstLoaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: firstLoaded.ObservedFence})
	if err != nil || replay != firstResult {
		t.Fatalf("old replay=%+v/%+v err=%v", replay, firstResult, err)
	}
	var events, evidence int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='checks_pending'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&evidence); err != nil {
		t.Fatal(err)
	}
	if events != 2 || evidence != 2 {
		t.Fatalf("replay changed cardinality events=%d evidence=%d", events, evidence)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE ci_transition_evidence SET transition_digest=? WHERE channel=? AND project_id=? AND ticket_id=?`, "tampered", ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err == nil {
		t.Fatal("transition evidence tamper was accepted")
	}
}

func TestCIObservationPublicationChainGapFailsClosed(t *testing.T) {
	db, ticket, publicationFence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	checks := ciAuthorityLintPolicy()
	if err := recordCIAuthorityPolicy(db, ctx, publication, ticket, checks); err != nil {
		t.Fatal(err)
	}
	observation := ciAuthorityObservationFor(publication, ticket, publicationFence, "pending", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "pending"}})
	if err := db.RecordCIObservation(ctx, observation); err != nil {
		t.Fatal(err)
	}
	loaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `DROP TRIGGER ci_transition_evidence_immutable_delete`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `DELETE FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := db.LoadCurrentCIObservation(ctx, ticket.Ref); !errors.Is(err, ErrCIObservation) {
		t.Fatalf("publication chain gap accepted: %v", err)
	}
}

func TestCIObservationLatestTamperAndCardinalityFailClosed(t *testing.T) {
	for name, tamper := range map[string]string{
		"row":   `UPDATE ci_observations SET diagnostic_text='tampered' WHERE observation_digest=?`,
		"check": `DELETE FROM ci_observation_checks WHERE observation_digest=?`,
	} {
		name, tamper := name, tamper
		t.Run(name, func(t *testing.T) {
			db, ticket, publicationFence := ciAuthorityPublishedFixture(t)
			defer db.Close()
			ctx := t.Context()
			publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			checks := ciAuthorityLintPolicy()
			if err := recordCIAuthorityPolicy(db, ctx, publication, ticket, checks); err != nil {
				t.Fatal(err)
			}
			observation := ciAuthorityObservationFor(publication, ticket, publicationFence, "green", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "success"}})
			if err := db.RecordCIObservation(ctx, observation); err != nil {
				t.Fatal(err)
			}
			loaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.db.ExecContext(ctx, `DROP TRIGGER ci_observations_immutable_update`); err != nil {
				t.Fatal(err)
			}
			if name == "check" {
				if _, err := db.db.ExecContext(ctx, `DROP TRIGGER ci_observation_checks_immutable_delete`); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.db.ExecContext(ctx, tamper, loaded.ObservationDigest); err != nil {
				t.Fatal(err)
			}
			if _, err := db.LoadCurrentCIObservation(ctx, ticket.Ref); !errors.Is(err, ErrCIObservation) {
				t.Fatalf("tampered latest accepted: %v", err)
			}
		})
	}
}

func TestCIObservationCheckAndAggregateDiagnosticCaps(t *testing.T) {
	db, ticket, publicationFence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	publication, err := db.LoadPublishedCandidate(t.Context(), ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	base := ciAuthorityObservationFor(publication, ticket, publicationFence, "pending", time.Now().UTC(), nil)
	tooMany := make([]CIObservationCheck, maxCIChecks+1)
	for i := range tooMany {
		tooMany[i] = CIObservationCheck{CanonicalName: fmt.Sprintf("check-%d", i), ExternalID: fmt.Sprintf("id-%d", i), NormalizedState: "pending"}
	}
	base.RequiredChecks = tooMany
	if _, err := canonicalCIObservation(base); !errors.Is(err, ErrCIObservation) {
		t.Fatalf("check count cap accepted: %v", err)
	}
	tooMuchDiagnostic := make([]CIObservationCheck, 17)
	for i := range tooMuchDiagnostic {
		tooMuchDiagnostic[i] = CIObservationCheck{CanonicalName: fmt.Sprintf("failure-%d", i), ExternalID: fmt.Sprintf("failure-id-%d", i), NormalizedState: "failure", FailingDiagnosticText: strings.Repeat("x", maxCIDiagnosticText)}
	}
	base.RequiredChecks = tooMuchDiagnostic
	if _, err := canonicalCIObservation(base); !errors.Is(err, ErrCIObservation) {
		t.Fatalf("aggregate diagnostic cap accepted: %v", err)
	}
}
