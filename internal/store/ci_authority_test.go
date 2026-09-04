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
		{name: "green_skipped_reviewing", state: "SKIPPED", want: domain.StateReviewing},
		{name: "green_neutral_reviewing", state: "NEUTRAL", want: domain.StateReviewing},
		{name: "red_consumes_one_budget", state: "FAILURE", want: domain.StateBuilding},
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

func TestCIPendingChainRecoveryAcrossCardinalities(t *testing.T) {
	for _, pendingCount := range []int{0, 1, 3} {
		t.Run(fmt.Sprintf("pending_%d", pendingCount), func(t *testing.T) {
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
			if err := gh.SetChecks(publication.PullRequest.Number, contracts.RequiredCheck{Name: "lint", ExternalID: "check-lint", State: "PENDING"}); err != nil {
				t.Fatal(err)
			}
			initialVersion := ticket.Version
			for index := 0; index < pendingCount; index++ {
				if err := pollCIAuthority(t.Context(), db, ticket, fence, gh); err != nil {
					t.Fatalf("pending observation %d: %v", index+1, err)
				}
				ticket, err = db.Ticket(t.Context(), ticket.Ref)
				if err != nil {
					t.Fatal(err)
				}
				fence.RunnerEpoch = ticket.RunnerEpoch
			}
			leader, err := db.AcquireLeader(t.Context(), ticket.Ref.Channel, fmt.Sprintf("ci-pending-recovery-%d", pendingCount))
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := db.FenceRecoveredRunners(t.Context(), ticket.Ref.Channel, leader); err != nil || changed != 1 {
				t.Fatalf("pending=%d recovery changed=%d err=%v", pendingCount, changed, err)
			}
			recovered, err := db.Ticket(t.Context(), ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.LoadPublishedCandidate(t.Context(), ticket.Ref); err != nil {
				t.Fatalf("pending=%d recovered publication load before red consume: %v", pendingCount, err)
			}
			retryFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: recovered.RunnerEpoch}
			if err := gh.SetChecks(publication.PullRequest.Number, contracts.RequiredCheck{Name: "lint", ExternalID: "check-lint", State: "FAILURE"}); err != nil {
				t.Fatal(err)
			}
			if err := pollCIAuthority(t.Context(), db, recovered, retryFence, gh); err != nil {
				t.Fatalf("pending=%d post-recovery red observation: %v", pendingCount, err)
			}
			current, err := db.Ticket(t.Context(), ticket.Ref)
			if err != nil || current.Version != initialVersion+uint64(pendingCount)+2 || current.State != domain.StateBuilding {
				t.Fatalf("pending=%d recovered chain ticket=%+v err=%v", pendingCount, current, err)
			}
			var uses, bindings int
			if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&uses); err != nil {
				t.Fatal(err)
			}
			if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&bindings); err != nil {
				t.Fatal(err)
			}
			if uses != 1 || bindings != 1 {
				t.Fatalf("pending=%d atomic red cardinality uses=%d bindings=%d", pendingCount, uses, bindings)
			}
		})
	}
}

func TestCIPollRetryEpochUsesArbitraryPendingChainAcrossRecovery(t *testing.T) {
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
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
	if err := gh.SetChecks(publication.PullRequest.Number, contracts.RequiredCheck{Name: "lint", ExternalID: "check-lint", State: "PENDING"}); err != nil {
		t.Fatal(err)
	}
	// Several durable pending observations deliberately occupy versions between
	// publication and exhaustion; retry authorization must discover these
	// versions rather than assuming a fixed +2/+3 layout.
	for pending := 0; pending < 3; pending++ {
		if err := pollCIAuthority(ctx, db, ticket, fence, gh); err != nil {
			t.Fatalf("pending observation %d: %v", pending+1, err)
		}
		ticket, err = db.Ticket(ctx, ticket.Ref)
		if err != nil {
			t.Fatal(err)
		}
		fence.RunnerEpoch = ticket.RunnerEpoch
	}
	baselineVersion := ticket.Version
	start := time.Now().UTC().Truncate(time.Microsecond)
	admission, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, start)
	if err != nil || !admission.Due {
		t.Fatalf("pending-chain first admission=%+v err=%v", admission, err)
	}
	next := admission.NextPoll
	for attempt := 2; attempt <= ciPollMaxAttempts; attempt++ {
		admission, err = db.AdmitCIPoll(ctx, ticket.Ref, fence, next)
		if err != nil || !admission.Due || admission.Attempt != attempt {
			t.Fatalf("pending-chain attempt %d admission=%+v err=%v", attempt, admission, err)
		}
		next = admission.NextPoll
	}
	if exhausted, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, next); err != nil || !exhausted.Expired {
		t.Fatalf("pending-chain exhaustion=%+v err=%v", exhausted, err)
	}
	paused, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || paused.Version != baselineVersion+1 || paused.State != domain.StatePaused {
		t.Fatalf("pending-chain paused ticket=%+v err=%v", paused, err)
	}
	if _, err := db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateWaitingCI, Trigger: "operator_resume", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatalf("pending-chain resume: %v", err)
	}
	// Take over after resume but before retry admission. This exercises the
	// lost-response/recovery path with the arbitrary pending chain intact.
	leader, err := db.AcquireLeader(ctx, ticket.Ref.Channel, "ci-poll-pending-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, ticket.Ref.Channel, leader); err != nil || changed != 1 {
		t.Fatalf("pending-chain runner recovery changed=%d err=%v", changed, err)
	}
	recovered, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || recovered.Version != paused.Version+2 || recovered.RunnerEpoch != fence.RunnerEpoch+1 {
		t.Fatalf("pending-chain recovered ticket=%+v err=%v", recovered, err)
	}
	retry, err := db.AdmitCIPoll(ctx, ticket.Ref, domain.Fence{LeaderEpoch: leader, RunnerEpoch: recovered.RunnerEpoch}, next)
	if err != nil || !retry.Due || retry.Attempt != 1 {
		t.Fatalf("pending-chain recovered retry=%+v err=%v", retry, err)
	}
}

func TestCandidateRepairCIHistoryAuthenticatesPollRetryEpochAndRejectsTamper(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Store, domain.TicketRef)
	}{
		{name: "valid"},
		{
			name: "retry digest",
			mutate: func(t *testing.T, db *Store, ref domain.TicketRef) {
				t.Helper()
				if _, err := db.db.ExecContext(t.Context(), `UPDATE ci_poll_retry_epochs SET retry_digest=? WHERE channel=? AND project_id=? AND ticket_id=?`, "sha256:"+strings.Repeat("f", 64), ref.Channel, ref.Project, ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "retry deadline",
			mutate: func(t *testing.T, db *Store, ref domain.TicketRef) {
				t.Helper()
				var raw string
				if err := db.db.QueryRowContext(t.Context(), `SELECT deadline_at FROM ci_poll_retry_epochs WHERE channel=? AND project_id=? AND ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				deadline, err := time.Parse(time.RFC3339Nano, raw)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.db.ExecContext(t.Context(), `UPDATE ci_poll_retry_epochs SET deadline_at=? WHERE channel=? AND project_id=? AND ticket_id=?`, deadline.Add(time.Second).Format(time.RFC3339Nano), ref.Channel, ref.Project, ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, waiting, fence := ciAuthorityPublishedFixture(t)
			defer db.Close()
			ctx := t.Context()
			publication, err := db.LoadPublishedCandidate(ctx, waiting.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if err := recordCIAuthorityPolicy(db, ctx, publication, waiting, ciAuthorityLintPolicy()); err != nil {
				t.Fatal(err)
			}

			start := time.Now().UTC().Truncate(time.Microsecond)
			admission, err := db.AdmitCIPoll(ctx, waiting.Ref, fence, start)
			if err != nil || !admission.Due {
				t.Fatalf("initial poll admission=%+v err=%v", admission, err)
			}
			next := admission.NextPoll
			for attempt := 2; attempt <= ciPollMaxAttempts; attempt++ {
				admission, err = db.AdmitCIPoll(ctx, waiting.Ref, fence, next)
				if err != nil || !admission.Due || admission.Attempt != attempt {
					t.Fatalf("poll attempt %d admission=%+v err=%v", attempt, admission, err)
				}
				next = admission.NextPoll
			}
			if exhausted, err := db.AdmitCIPoll(ctx, waiting.Ref, fence, next); err != nil || !exhausted.Expired {
				t.Fatalf("poll exhaustion=%+v err=%v", exhausted, err)
			}
			paused, err := db.Ticket(ctx, waiting.Ref)
			if err != nil || paused.State != domain.StatePaused {
				t.Fatalf("paused ticket=%+v err=%v", paused, err)
			}
			if _, err := db.TransitionPublishedResume(ctx, Transition{Ref: waiting.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateWaitingCI, Trigger: "operator_resume", Fence: fence, EventPayload: `{}`}); err != nil {
				t.Fatal(err)
			}
			resumed, err := db.Ticket(ctx, waiting.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if retry, err := db.AdmitCIPoll(ctx, waiting.Ref, fence, next); err != nil || !retry.Due || retry.Attempt != 1 {
				t.Fatalf("retry epoch admission=%+v err=%v", retry, err)
			}

			red := ciAuthorityObservationFor(publication, resumed, fence, "red", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "failure", FailingDiagnosticText: "retry epoch remains red"}})
			if err := db.recordCIObservation(ctx, red); err != nil {
				t.Fatal(err)
			}
			red, err = db.LoadCurrentCIObservation(ctx, waiting.Ref)
			if err != nil {
				t.Fatal(err)
			}
			authority := redCICorrectionAuthority(t, resumed, red)
			if _, err := db.ConsumeCIObservation(ctx, CIObservationTransition{Ref: waiting.Ref, ObservationDigest: red.ObservationDigest, ExpectedVersion: resumed.Version, Fence: red.ObservedFence, CorrectionBudget: &authority}); err != nil {
				t.Fatal(err)
			}
			building, err := db.Ticket(ctx, waiting.Ref)
			if err != nil || building.State != domain.StateBuilding {
				t.Fatalf("repair ticket=%+v err=%v", building, err)
			}
			buildFence := domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: building.RunnerEpoch}
			if _, err := db.CandidateRepairBuildContext(ctx, waiting.Ref, building.Version, buildFence); err != nil {
				t.Fatalf("valid retry epoch repair context: %v", err)
			}
			if test.mutate == nil {
				return
			}
			if _, err := db.db.ExecContext(ctx, `DROP TRIGGER ci_poll_retry_epochs_immutable_update`); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, db, waiting.Ref)
			if _, err := db.CandidateRepairBuildContext(ctx, waiting.Ref, building.Version, buildFence); err == nil {
				t.Fatal("candidate repair accepted tampered CI poll retry epoch")
			}
		})
	}
}

func TestCIPollAdmissionBackoffCapAndRestart(t *testing.T) {
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	ctx := t.Context()
	start := time.Now().UTC().Truncate(time.Microsecond)
	first, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, start)
	if err != nil || !first.Due || first.Attempt != 1 || !first.NextPoll.After(start) {
		t.Fatalf("first CI poll admission=%+v err=%v", first, err)
	}
	notDue, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, start.Add(time.Second))
	if err != nil || notDue.Due || notDue.Expired || !notDue.NextPoll.Equal(first.NextPoll) {
		t.Fatalf("backoff must suppress eager scheduler tick admission=%+v err=%v", notDue, err)
	}
	restartDir := t.TempDir()
	if err := os.Chmod(restartDir, 0o700); err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(restartDir, "ci-schedule.sqlite")
	if err := db.Backup(ctx, databasePath); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// The durable schedule is independent of process lifetime; same leader and
	// runner are still current because this is a direct Store restart fixture.
	second, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, first.NextPoll)
	if err != nil || !second.Due || second.Attempt != 2 || second.NextPoll.Sub(first.NextPoll) != ciPollBackoff(2) {
		t.Fatalf("restarted CI backoff admission=%+v err=%v", second, err)
	}
	next := second.NextPoll
	for attempt := 3; attempt <= ciPollMaxAttempts; attempt++ {
		admission, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, next)
		if err != nil || !admission.Due || admission.Attempt != attempt {
			t.Fatalf("CI attempt %d admission=%+v err=%v", attempt, admission, err)
		}
		next = admission.NextPoll
	}
	exhausted, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, next)
	if err != nil || !exhausted.Expired || exhausted.PauseCode != "ci_poll_attempts_exhausted" {
		t.Fatalf("CI poll cap=%+v err=%v", exhausted, err)
	}
	paused, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || paused.State != domain.StatePaused || paused.ResumeState != domain.StateWaitingCI {
		t.Fatalf("CI poll cap ticket=%+v err=%v", paused, err)
	}
	if _, err := db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateWaitingCI, Trigger: "operator_resume", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatalf("resume one bounded CI retry window: %v", err)
	}
	// The resume itself is exact durable authority for epoch 2. Its attempt
	// number restarts at one, while the immutable attempt ledger remains 1..24.
	retry, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, next)
	if err != nil || !retry.Due || retry.Attempt != 1 {
		t.Fatalf("retry epoch first admission=%+v err=%v", retry, err)
	}
	// Simulate a caller losing the successful admission response. The durable
	// retry-epoch insert and attempt must make the replay non-due, rather than
	// minting a second retry epoch or a second external read authorization.
	lostResponseReplay, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, retry.NextPoll.Add(-time.Nanosecond))
	if err != nil || lostResponseReplay.Due || lostResponseReplay.Attempt != 0 {
		t.Fatalf("retry epoch lost-response replay=%+v err=%v", lostResponseReplay, err)
	}
	var epochs, durableAttempts int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_poll_retry_epochs`).Scan(&epochs); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ci_poll_attempts`).Scan(&durableAttempts); err != nil || epochs != 1 || durableAttempts != ciPollMaxAttempts+1 {
		t.Fatalf("retry epoch idempotency epochs=%d attempts=%d err=%v", epochs, durableAttempts, err)
	}
	next = retry.NextPoll
	for attempt := 2; attempt <= ciPollMaxAttempts; attempt++ {
		admission, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, next)
		if err != nil || !admission.Due || admission.Attempt != attempt {
			t.Fatalf("retry epoch attempt %d admission=%+v err=%v", attempt, admission, err)
		}
		next = admission.NextPoll
	}
	terminal, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, next)
	if err != nil || !terminal.Expired || terminal.PauseCode != "ci_poll_retry_attempts_exhausted" {
		t.Fatalf("retry terminal admission=%+v err=%v", terminal, err)
	}
	events, err := db.Events(ctx, ticket.Ref.Channel, 0, 100)
	if err != nil || len(events) == 0 || !strings.Contains(events[len(events)-1].Payload, "submit a fresh ticket") {
		t.Fatalf("retry terminal action events=%+v err=%v", events, err)
	}
}

func TestCIPollRetryEpochSurvivesLeaderRecoveryBeforeFirstRetry(t *testing.T) {
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	start := time.Now().UTC().Truncate(time.Microsecond)
	admission, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, start)
	if err != nil || !admission.Due {
		t.Fatalf("first admission=%+v err=%v", admission, err)
	}
	next := admission.NextPoll
	for attempt := 2; attempt <= ciPollMaxAttempts; attempt++ {
		admission, err = db.AdmitCIPoll(ctx, ticket.Ref, fence, next)
		if err != nil || !admission.Due {
			t.Fatalf("initial attempt %d admission=%+v err=%v", attempt, admission, err)
		}
		next = admission.NextPoll
	}
	if exhausted, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, next); err != nil || !exhausted.Expired {
		t.Fatalf("initial exhaustion=%+v err=%v", exhausted, err)
	}
	paused, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPublishedResume(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StateWaitingCI, Trigger: "operator_resume", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatalf("resume retry epoch: %v", err)
	}
	// Crash after the exact resume but before the first retry admission. The
	// recovered runner must traverse the signed ledger and preserve the one
	// remaining retry epoch, not reinterpret version-1 as a fresh resume.
	leader, err := db.AcquireLeader(ctx, ticket.Ref.Channel, "ci-poll-retry-recovery")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := db.FenceRecoveredRunners(ctx, ticket.Ref.Channel, leader); err != nil || changed != 1 {
		t.Fatalf("fence resumed CI poller changed=%d err=%v", changed, err)
	}
	recovered, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || recovered.Version != paused.Version+2 || recovered.RunnerEpoch != fence.RunnerEpoch+1 {
		t.Fatalf("recovered resumed ticket=%+v err=%v", recovered, err)
	}
	retryFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: recovered.RunnerEpoch}
	retry, err := db.AdmitCIPoll(ctx, ticket.Ref, retryFence, next)
	if err != nil || !retry.Due || retry.Attempt != 1 {
		t.Fatalf("recovered first retry admission=%+v err=%v", retry, err)
	}
}

func TestCIPollAdmissionDeadlinePausesWithExplicitAction(t *testing.T) {
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	start := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := db.AdmitCIPoll(t.Context(), ticket.Ref, fence, start); err != nil {
		t.Fatal(err)
	}
	expired, err := db.AdmitCIPoll(t.Context(), ticket.Ref, fence, start.Add(ciPollDeadline))
	if err != nil || !expired.Expired || expired.PauseCode != "ci_poll_deadline_exhausted" {
		t.Fatalf("CI deadline admission=%+v err=%v", expired, err)
	}
	events, err := db.Events(t.Context(), ticket.Ref.Channel, 0, 100)
	if err != nil || len(events) == 0 || events[len(events)-1].Trigger != "ci_poll_exhausted" || !strings.Contains(events[len(events)-1].Payload, `"next_action"`) {
		t.Fatalf("CI deadline explicit next action events=%+v err=%v", events, err)
	}
}

func TestCIPollAdmissionRejectsTamperedDeadline(t *testing.T) {
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	ctx := t.Context()
	start := time.Now().UTC().Truncate(time.Microsecond)
	if _, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, start); err != nil {
		t.Fatal(err)
	}
	// Bypass the append-only trigger only to model a storage tamper. Admission
	// must still enforce the canonical first+3h invariant.
	if _, err := db.db.ExecContext(ctx, `DROP TRIGGER ci_poll_schedules_immutable_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE ci_poll_schedules SET deadline_at=? WHERE channel=? AND project_id=? AND ticket_id=?`, start.Add(ciPollDeadline+time.Second).Format(time.RFC3339Nano), ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AdmitCIPoll(ctx, ticket.Ref, fence, start.Add(time.Second)); !errors.Is(err, ErrCIObservation) {
		t.Fatalf("tampered CI deadline accepted: %v", err)
	}
}

func pollCIAuthority(ctx context.Context, db *Store, ticket Ticket, fence domain.Fence, gh interface {
	contracts.CIRequiredCheckPolicyObserver
	contracts.CIRequiredChecksObserver
}) error {
	publication, err := db.LoadPublishedCandidate(ctx, ticket.Ref)
	if err != nil {
		return fmt.Errorf("load current published candidate: %w", err)
	}
	// FakeGH independently models the protected base tip. Most CI fixtures
	// create a PR with a deterministic historical BaseOID, so align the fake's
	// live protected ref to that authenticated publication before observing
	// policy. Deliberate base-move tests use a remote without this setup path or
	// mutate it after this point.
	if setter, ok := gh.(interface{ SetBaseHeadOIDForTest(string) error }); ok {
		if err := setter.SetBaseHeadOIDForTest(publication.PullRequest.BaseOID); err != nil {
			return fmt.Errorf("align fake protected base: %w", err)
		}
	}
	if err := db.RecordCIRequiredCheckPolicyFromObserver(ctx, ticket.Ref, gh); err != nil {
		return fmt.Errorf("record fresh CI policy: %w", err)
	}
	if err := db.RecordCIObservationFromObserver(ctx, ticket.Ref, ticket.Version, fence, gh); err != nil {
		return fmt.Errorf("record authenticated CI observation: %w", err)
	}
	stored, err := db.LoadCIObservation(ctx, ticket.Ref)
	if err != nil {
		return fmt.Errorf("load authenticated CI observation: %w", err)
	}
	request := CIObservationTransition{Ref: ticket.Ref, ObservationDigest: stored.ObservationDigest, ExpectedVersion: ticket.Version, Fence: fence}
	if stored.Classification == "red" {
		id := "ci-red/" + strings.TrimPrefix(stored.ObservationDigest, "sha256:")
		request.CorrectionBudget = &CorrectionBudgetAuthority{Ref: ticket.Ref, RequestID: id, TicketVersion: ticket.Version, Fence: fence}
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
	if err := db.recordCIObservation(ctx, observation); err != nil {
		t.Fatal(err)
	}
	loaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
	if err != nil || loaded.Classification != "red" || loaded.RequiredChecks[0].FailingDiagnosticDigest == "" {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	if err := db.recordCIObservation(ctx, observation); err != nil {
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

type ciRequiredChecksObserverFunc func(context.Context, contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error)

func (f ciRequiredChecksObserverFunc) RequiredChecks(ctx context.Context, identity contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
	return f(ctx, identity)
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

func redCIConsumptionFixture(t *testing.T) (*Store, Ticket, PublishedCandidateEvidence, CIObservation) {
	t.Helper()
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	publication, err := db.LoadPublishedCandidate(t.Context(), ticket.Ref)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	checks := []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "failure", FailingDiagnosticText: "lint failed"}}
	if err := recordCIAuthorityPolicy(db, t.Context(), publication, ticket, []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint"}}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	observation := ciAuthorityObservationFor(publication, ticket, fence, "red", time.Now().UTC(), checks)
	if err := db.recordCIObservation(t.Context(), observation); err != nil {
		db.Close()
		t.Fatal(err)
	}
	loaded, err := db.LoadCurrentCIObservation(t.Context(), ticket.Ref)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db, ticket, publication, loaded
}

func redCICorrectionAuthority(t *testing.T, ticket Ticket, observation CIObservation) CorrectionBudgetAuthority {
	t.Helper()
	requestID, ok := ciRedCorrectionRequestID(observation.ObservationDigest)
	if !ok {
		t.Fatalf("invalid red observation digest %q", observation.ObservationDigest)
	}
	return CorrectionBudgetAuthority{Ref: ticket.Ref, RequestID: requestID, TicketVersion: ticket.Version, Fence: observation.ObservedFence}
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

func TestCIObservationRejectsRawGreenAndRedCallerInjection(t *testing.T) {
	for _, classification := range []string{"green", "red"} {
		classification := classification
		t.Run(classification, func(t *testing.T) {
			db, ticket, fence := ciAuthorityPublishedFixture(t)
			defer db.Close()
			publication, err := db.LoadPublishedCandidate(t.Context(), ticket.Ref)
			if err != nil {
				t.Fatal(err)
			}
			if err := recordCIAuthorityPolicy(db, t.Context(), publication, ticket, ciAuthorityLintPolicy()); err != nil {
				t.Fatal(err)
			}
			state := "success"
			if classification == "red" {
				state = "failure"
			}
			input := ciAuthorityObservationFor(publication, ticket, fence, classification, time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: state}})
			for name, record := range map[string]func(context.Context, CIObservation) error{
				"raw":                   db.RecordCIObservation,
				"descriptive-alias-raw": db.RecordAuthenticatedCIObservation,
			} {
				if err := record(t.Context(), input); !errors.Is(err, ErrCIObservation) {
					t.Fatalf("%s accepted caller-assembled %s observation: %v", name, classification, err)
				}
			}
			if _, err := db.LoadCurrentCIObservation(t.Context(), ticket.Ref); !errors.Is(err, ErrNotFound) {
				t.Fatalf("raw %s observation left durable evidence: %v", classification, err)
			}
		})
	}
}

func TestRecordCIObservationFromObserverAuthenticatesExactPublication(t *testing.T) {
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	publication, err := db.LoadPublishedCandidate(t.Context(), ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := recordCIAuthorityPolicy(db, t.Context(), publication, ticket, ciAuthorityLintPolicy()); err != nil {
		t.Fatal(err)
	}
	observer := ciRequiredChecksObserverFunc(func(_ context.Context, want contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
		if want != publication.PullRequest {
			return nil, errors.New("observer received the wrong pull request")
		}
		return []contracts.RequiredCheck{{Name: "lint", ExternalID: "check-lint", State: "FAILURE"}}, nil
	})
	if err := db.RecordCIObservationFromObserver(t.Context(), ticket.Ref, ticket.Version, fence, observer); err != nil {
		t.Fatal(err)
	}
	stored, err := db.LoadCurrentCIObservation(t.Context(), ticket.Ref)
	if err != nil || stored.Classification != "red" || len(stored.RequiredChecks) != 1 || stored.RequiredChecks[0].NormalizedState != "failure" {
		t.Fatalf("stored observer observation=%+v err=%v", stored, err)
	}
}

func TestRecordCIObservationFromObserverRejectsStaleFenceBeforeRead(t *testing.T) {
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	called := false
	observer := ciRequiredChecksObserverFunc(func(context.Context, contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
		called = true
		return []contracts.RequiredCheck{{Name: "lint", ExternalID: "check-lint", State: "SUCCESS"}}, nil
	})
	fence.LeaderEpoch++
	if err := db.RecordCIObservationFromObserver(t.Context(), ticket.Ref, ticket.Version, fence, observer); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale observer fence=%v", err)
	}
	if called {
		t.Fatal("observer ran before the initial fence check")
	}
}

func TestRecordCIObservationFromObserverRejectsPublicationRecoveryRace(t *testing.T) {
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	publication, err := db.LoadPublishedCandidate(t.Context(), ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := recordCIAuthorityPolicy(db, t.Context(), publication, ticket, ciAuthorityLintPolicy()); err != nil {
		t.Fatal(err)
	}
	var recoveryErr error
	observer := ciRequiredChecksObserverFunc(func(ctx context.Context, want contracts.PullRequestIdentity) ([]contracts.RequiredCheck, error) {
		if want != publication.PullRequest {
			return nil, errors.New("observer received the wrong pull request")
		}
		leader, err := db.AcquireLeader(ctx, ticket.Ref.Channel, "ci-observation-race")
		if err == nil {
			_, err = db.FenceRecoveredRunners(ctx, ticket.Ref.Channel, leader)
		}
		if err == nil {
			err = db.RebindRecoveredPublishedCandidates(ctx, ticket.Ref.Channel, leader)
		}
		recoveryErr = err
		return []contracts.RequiredCheck{{Name: "lint", ExternalID: "check-lint", State: "SUCCESS"}}, nil
	})
	err = db.RecordCIObservationFromObserver(t.Context(), ticket.Ref, ticket.Version, fence, observer)
	if recoveryErr != nil {
		t.Fatalf("fixture publication recovery: %v", recoveryErr)
	}
	if err == nil {
		t.Fatal("observation survived a publication/fence recovery during the external read")
	}
	if _, loadErr := db.LoadCurrentCIObservation(t.Context(), ticket.Ref); !errors.Is(loadErr, ErrNotFound) {
		t.Fatalf("racing observation left durable evidence: %v", loadErr)
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
			if err := db.recordCIObservation(ctx, observation); !errors.Is(err, ErrCIObservation) {
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
			if err := db.recordCIObservation(ctx, observation); err != nil {
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
	if err := db.recordCIObservation(ctx, observation); err != nil {
		t.Fatalf("empty caller digests rejected: %v", err)
	}
	loaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
	if err != nil || loaded.PolicyWitnessDigest == "" || loaded.ObservationDigest == "" {
		t.Fatalf("bound observation=%+v err=%v", loaded, err)
	}
	tampered := observation
	tampered.ObservationDigest = strings.Repeat("f", 64)
	if err := db.recordCIObservation(ctx, tampered); !errors.Is(err, ErrCIObservation) {
		t.Fatalf("tampered observation digest accepted: %v", err)
	}
}

func TestCIRequiredPolicyAllowsRerunRunIdentity(t *testing.T) {
	db, ticket, fence := ciAuthorityPublishedFixture(t)
	defer db.Close()
	publication, err := db.LoadPublishedCandidate(t.Context(), ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	observe := func(run string) error {
		return db.RecordCIRequiredCheckPolicyFromObserver(t.Context(), ticket.Ref, fakeCIAuthorityPolicyObserver{value: contracts.CIRequiredCheckPolicyObservation{
			PullRequest: publication.PullRequest, ProtectedBranchRef: publication.PullRequest.BaseRef, ProtectedBranchOID: publication.PullRequest.BaseOID,
			PolicySourceDigest: strings.Repeat("a", 64), AuthenticatedPrincipal: "ci-observer", RequiredChecks: []contracts.RequiredCheck{{Name: "lint", ExternalID: run, State: "PENDING"}}, ObservedAt: time.Now().UTC(),
		}})
	}
	if err := observe("https://github.com/acme/app/actions/runs/1"); err != nil {
		t.Fatal(err)
	}
	if err := observe("https://github.com/acme/app/actions/runs/2"); err != nil {
		t.Fatalf("same required context with rerun identity must not conflict: %v", err)
	}
	if _, err := db.Ticket(t.Context(), ticket.Ref); err != nil || fence.RunnerEpoch != ticket.RunnerEpoch {
		t.Fatalf("rerun policy unexpectedly mutated ticket err=%v", err)
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
			if err := db.recordCIObservation(ctx, observation); err != nil {
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

func TestCIObservationAtomicBudgetRollbackBeforeTransition(t *testing.T) {
	db, ticket, _, loaded := redCIConsumptionFixture(t)
	defer db.Close()
	faultErr := errors.New("injected CI transition failure")
	db.SetCIConsumeFaultForTest(func(stage string) error {
		if stage == "after_correction_budget" {
			return faultErr
		}
		return nil
	})
	authority := redCICorrectionAuthority(t, ticket, loaded)
	_, err := db.ConsumeCIObservation(t.Context(), CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, CorrectionBudget: &authority})
	if !errors.Is(err, faultErr) {
		t.Fatalf("faulted atomic CI consume err=%v", err)
	}
	var uses, transitions, bindings int
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&uses); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&transitions); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(t.Context(), ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if uses != 0 || transitions != 0 || bindings != 0 || current.State != domain.StateWaitingCI || current.Version != ticket.Version {
		t.Fatalf("atomic rollback left durable state uses=%d transitions=%d bindings=%d ticket=%+v", uses, transitions, bindings, current)
	}
}

func TestCIObservationAtomicBudgetCommittedLostResponseReplay(t *testing.T) {
	db, ticket, _, loaded := redCIConsumptionFixture(t)
	defer db.Close()
	authority := redCICorrectionAuthority(t, ticket, loaded)
	request := CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, CorrectionBudget: &authority}
	first, err := db.ConsumeCIObservation(t.Context(), request)
	if err != nil {
		t.Fatalf("atomic CI consume=%+v err=%v", first, err)
	}
	replay, err := db.ConsumeCIObservation(t.Context(), request)
	if err != nil || replay != first {
		t.Fatalf("atomic CI lost-response replay=%+v/%+v err=%v", replay, first, err)
	}
	var uses, budgetEvents, transitions, bindings int
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&uses); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='budget_correction'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&budgetEvents); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM ci_transition_evidence WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&transitions); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if uses != 1 || budgetEvents != 1 || transitions != 1 || bindings != 1 {
		t.Fatalf("atomic CI replay cardinality uses=%d budgetEvents=%d transitions=%d bindings=%d", uses, budgetEvents, transitions, bindings)
	}
}

func TestCIObservationAtomicBudgetExhaustionHasNoDanglingUse(t *testing.T) {
	db, ticket, _, loaded := redCIConsumptionFixture(t)
	defer db.Close()
	// The correction limit is ticket-global. These non-CI correction uses are
	// legitimate members of that shared budget domain, but CI-red lineage must
	// not misclassify them as candidate-repair orphans.
	for _, requestID := range []string{"final-review/1/one", "final-review/2/two"} {
		if _, err := db.ConsumeBudget(t.Context(), BudgetUse{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, Kind: "correction", RequestID: requestID}); err != nil {
			t.Fatal(err)
		}
	}
	authority := redCICorrectionAuthority(t, ticket, loaded)
	if _, err := db.ConsumeCIObservation(t.Context(), CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, CorrectionBudget: &authority}); err != nil {
		t.Fatalf("exhausted atomic CI consume err=%v", err)
	}
	var uses int
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&uses); err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(t.Context(), ticket.Ref)
	if err != nil || uses != 2 || current.State != domain.StatePaused || current.ResumeState != domain.StateWaitingCI || current.Version != ticket.Version+1 {
		t.Fatalf("exhausted atomic CI state uses=%d ticket=%+v err=%v", uses, current, err)
	}
	var bindings int
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	if bindings != 0 {
		t.Fatalf("exhausted atomic CI created repair binding count=%d", bindings)
	}
}

func TestCIObservationSharesGlobalBudgetWithNonCICorrectionDomain(t *testing.T) {
	db, ticket, _, loaded := redCIConsumptionFixture(t)
	defer db.Close()
	if _, err := db.ConsumeBudget(t.Context(), BudgetUse{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, Kind: "correction", RequestID: "final-review/1/prior"}); err != nil {
		t.Fatal(err)
	}
	authority := redCICorrectionAuthority(t, ticket, loaded)
	if _, err := db.ConsumeCIObservation(t.Context(), CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, CorrectionBudget: &authority}); err != nil {
		t.Fatal(err)
	}
	var uses, bindings int
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&uses); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM candidate_repair_bindings WHERE channel=? AND project_id=? AND ticket_id=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&bindings); err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(t.Context(), ticket.Ref)
	if err != nil || uses != 2 || bindings != 1 || current.State != domain.StateBuilding {
		t.Fatalf("cross-domain correction budget uses=%d bindings=%d ticket=%+v err=%v", uses, bindings, current, err)
	}
}

func TestCIObservationRejectsWrongCorrectionBudgetDomainWithoutSpending(t *testing.T) {
	db, ticket, _, loaded := redCIConsumptionFixture(t)
	defer db.Close()
	authority := CorrectionBudgetAuthority{Ref: ticket.Ref, RequestID: "final-review/not-ci", TicketVersion: ticket.Version, Fence: loaded.ObservedFence}
	if _, err := db.ConsumeCIObservation(t.Context(), CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, CorrectionBudget: &authority}); err != nil {
		t.Fatal(err)
	}
	var uses int
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&uses); err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(t.Context(), ticket.Ref)
	if err != nil || uses != 0 || current.State != domain.StatePaused || current.ResumeState != domain.StateWaitingCI {
		t.Fatalf("wrong-domain correction authority spent budget uses=%d ticket=%+v err=%v", uses, current, err)
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
	if err := db.recordCIObservation(ctx, red); err != nil {
		t.Fatal(err)
	}
	loaded, err := db.LoadCurrentCIObservation(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	authority := redCICorrectionAuthority(t, ticket, loaded)
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
	predecessorHead := publication.Candidate.Snapshot.HeadSHA
	policyDigest := sha256Digest([]byte("repair-policy"))
	command := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePostbuildCandidate, ticket.Ref, building.Version, buildFence, key, publication.Candidate.Snapshot.VerificationIntentDigest, publication.Candidate.Snapshot.ProofDigest, checkpoint, "sha256:"+policyDigest, 0)
	successor := domain.CandidateSnapshot{Generation: publication.Candidate.Snapshot.Generation + 1, BaseSHA: publication.Candidate.Snapshot.BaseSHA, HeadSHA: strings.Repeat("7", 40), TreeSHA: strings.Repeat("8", 40), SourceDigest: publication.Candidate.Snapshot.SourceDigest, VerificationIntentDigest: publication.Candidate.Snapshot.VerificationIntentDigest, ProofDigest: publication.Candidate.Snapshot.ProofDigest, CommandPolicyDigest: policyDigest, BuilderEvidenceDigest: builderDigest}
	if _, err := db.RecordCandidate(ctx, CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: building.Version, Fence: buildFence, Snapshot: successor, BuilderResult: key, Commit: CommitObservation{CommitOID: successor.HeadSHA, ParentOID: predecessorHead, TreeOID: successor.TreeSHA}, Reason: "CI repair", CommandResult: command}); err != nil {
		t.Fatal(err)
	}
	completion := CandidateRepairCompletion{Ref: ticket.Ref, TargetGeneration: successor.Generation}
	var completedAt string
	if err := db.db.QueryRowContext(ctx, `SELECT builder_result_attempt_id,builder_result_attempt,builder_binding_ticket_version,builder_binding_leader_epoch,builder_binding_runner_epoch,final_candidate_head_sha,final_candidate_tree_sha,completion_digest,completed_at FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, successor.Generation).Scan(&completion.BuilderResultAttemptID, &completion.BuilderResultAttempt, &completion.BuilderBindingTicketVersion, &completion.BuilderBindingFence.LeaderEpoch, &completion.BuilderBindingFence.RunnerEpoch, &completion.FinalCandidateHeadSHA, &completion.FinalCandidateTreeSHA, &completion.CompletionDigest, &completedAt); err != nil {
		t.Fatalf("atomic repair completion: %v", err)
	}
	completion.CompletedAt, err = time.Parse(time.RFC3339Nano, completedAt)
	if err != nil || completion.BuilderResultAttemptID != claim.ID || completion.BuilderResultAttempt != claim.Attempt || completion.BuilderBindingTicketVersion != building.Version || completion.BuilderBindingFence != buildFence || completion.FinalCandidateHeadSHA != successor.HeadSHA || completion.FinalCandidateTreeSHA != successor.TreeSHA || completion.CompletionDigest != candidateRepairCompletionDigest(completion) {
		t.Fatalf("atomic repair completion=%+v parseErr=%v", completion, err)
	}
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
	// This is the production workflow replay: after startup fencing, the Worker
	// authenticates the reusable Builder result and appends a current binding
	// before it asks TransitionCandidate to consume phase_pass.
	recoveredFence := domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: recovered.RunnerEpoch}
	builderResultForRecovery, _, err := db.LoadHistoricalProviderAttemptResult(ctx, key)
	repairRecoveryErr := candidateRepairBuilderResultReachesFence(ctx, db.db, key, builderResultForRecovery, recovered.Version, recoveredFence)
	if err != nil || repairRecoveryErr != nil {
		t.Fatalf("repaired Builder completion recovery result=%+v loadErr=%v recoveryErr=%v", builderResultForRecovery, err, repairRecoveryErr)
	}
	reusableBuilder, err := db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: ticket.Ref, Phase: domain.PhaseBuild, Role: "builder", ExpectedVersion: recovered.Version, Fence: recoveredFence})
	if err != nil || !reusableBuilder.Recovered || reusableBuilder.Key != key {
		t.Fatalf("reusable repaired Builder=%+v err=%v", reusableBuilder, err)
	}
	if _, found, sourceErr := db.OperatorSourceResumeProof(ctx, ticket.Ref, recovered.Version, recoveredFence); sourceErr != nil || found {
		t.Fatalf("candidate repair misclassified as source resume found=%v err=%v", found, sourceErr)
	}
	// Exercise the same conn-scoped subproofs RecordCandidate consumes before
	// asking the public boundary to append the recovered binding. The rollback
	// keeps this an authentication-only diagnostic and catches accidental use of
	// Store.db from inside the mutation transaction.
	conn, err := db.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	recoveredEvidence := CandidateEvidence{Ref: ticket.Ref, ExpectedVersion: recovered.Version, Fence: recoveredFence, Snapshot: successor, BuilderResult: key, Commit: CommitObservation{CommitOID: successor.HeadSHA, ParentOID: predecessorHead, TreeOID: successor.TreeSHA}, Reason: "recovered immutable candidate evidence", CommandResult: command}
	repairAuthority, err := db.candidateRepairBuildAuthorityAt(ctx, conn, ticket.Ref, recovered.Version, recoveredFence)
	if err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		conn.Close()
		t.Fatalf("recovered repair authority: %v", err)
	}
	_, commandBinding, err := authenticateCandidateCommandEvidence(ctx, conn, recoveredEvidence, builderResultForRecovery, publication.Candidate.Snapshot.VerificationIntentDigest, publication.Candidate.Snapshot.ProofDigest, checkpoint)
	if err == nil {
		err = db.ensureCandidateBinding(ctx, conn, recoveredEvidence, successor.Generation, builderResultForRecovery)
	}
	if err == nil {
		err = ensureCandidateCommandBinding(ctx, conn, ticket.Ref, successor.Generation, commandBinding)
	}
	if err == nil {
		err = ensureCandidateRepairCompletionAt(ctx, conn, recoveredEvidence, successor.Generation, builderResultForRecovery, repairAuthority)
	}
	_, _ = conn.ExecContext(ctx, "ROLLBACK")
	_ = conn.Close()
	if err != nil {
		t.Fatalf("recovered conn-scoped candidate proof: %v", err)
	}
	if _, err := db.RecordCandidate(ctx, recoveredEvidence); err != nil {
		t.Fatalf("rebind repaired candidate after recovery: %v", err)
	}
	recoveredCandidate, err := db.latestCandidateFrom(ctx, db.db, ticket.Ref, false)
	if err != nil || recoveredCandidate.Snapshot != successor || recoveredCandidate.TicketVersion != recovered.Version || recoveredCandidate.Fence != recoveredFence {
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
	if err := db.reauthenticateStoredCandidateCheckpointFrom(ctx, db.db, ticket.Ref, recoveredCandidate); err != nil {
		t.Fatalf("recovered repair candidate checkpoint=%v", err)
	}
	originalCandidate := recoveredCandidate
	originalCandidate.TicketVersion = building.Version
	originalCandidate.Fence = buildFence
	if err := providerResultReachesHistoricalFence(ctx, db.db, originalCandidate.BuilderResult, builderResult, originalCandidate.TicketVersion, originalCandidate.Fence); err != nil {
		t.Fatalf("original repair Builder historical fence=%v", err)
	}
	if err := candidateRepairBuilderResultReachesFence(ctx, db.db, originalCandidate.BuilderResult, builderResult, originalCandidate.TicketVersion, originalCandidate.Fence); err != nil {
		t.Fatalf("original repair Builder completion fence=%v", err)
	}
	if _, err := db.candidateRepairBuildAuthorityAt(ctx, db.db, ticket.Ref, building.Version, buildFence); err != nil {
		t.Fatalf("original repair authority=%v", err)
	}
	if _, err := completedCandidateRepairContextAt(ctx, db.db, originalCandidate, builderResult); err != nil {
		t.Fatalf("original completed repair context=%v", err)
	}
	if err := db.reauthenticateStoredCandidateCommandHistoricalFrom(ctx, db.db, ticket.Ref, originalCandidate); err != nil {
		t.Fatalf("original repair candidate command=%v", err)
	}
	if _, err := db.TransitionCandidate(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: recovered.Version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: recoveredFence, EventPayload: "{}"}, successor); err != nil {
		t.Fatal(err)
	}
	var bindingCountAfterTransition int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, successor.Generation).Scan(&bindingCountAfterTransition); err != nil {
		t.Fatal(err)
	}
	if bindingCountAfterTransition != 2 {
		t.Fatalf("recovered phase_pass changed exact binding cardinality: %d", bindingCountAfterTransition)
	}
	publishing, err := db.Ticket(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AuthenticatePublishingRecovery(ctx, ticket.Ref, recoveredCandidate, publishing.Version, domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: publishing.RunnerEpoch}); err != nil {
		t.Fatalf("successor repair candidate publication authority=%v", err)
	}
	var completionCount int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket, successor.Generation).Scan(&completionCount); err != nil {
		t.Fatal(err)
	}
	if completionCount != 1 {
		t.Fatalf("completion cardinality=%d", completionCount)
	}

	// Generation one remains an immutable publication witness while the
	// corrected generation is still candidate-only. Repeated restarts and
	// publication controls must classify that row as historical context rather
	// than rebinding or authorizing it as the current generation.
	for recovery := 1; recovery <= 2; recovery++ {
		newLeader, err = db.AcquireLeader(ctx, domain.ChannelDev, "successor-publication-recovery")
		if err != nil {
			t.Fatalf("successor recovery %d leader: %v", recovery, err)
		}
		if changed, fenceErr := db.FenceRecoveredRunners(ctx, domain.ChannelDev, newLeader); fenceErr != nil || changed != 1 {
			t.Fatalf("successor recovery %d fence changed=%d err=%v", recovery, changed, fenceErr)
		}
		latestAfterFence, candidateErr := db.latestCandidateFrom(ctx, db.db, ticket.Ref, false)
		if candidateErr != nil || db.reauthenticateStoredCandidateCheckpointFrom(ctx, db.db, ticket.Ref, latestAfterFence) != nil {
			t.Fatalf("successor recovery %d historical candidate checkpoint=%+v err=%v", recovery, latestAfterFence, candidateErr)
		}
		if err := db.RebindRecoveredPublishedCandidates(ctx, domain.ChannelDev, newLeader); err != nil {
			t.Fatalf("successor recovery %d startup publication proof: %v", recovery, err)
		}
	}
	publishing, err = db.Ticket(ctx, ticket.Ref)
	if err != nil || publishing.State != domain.StatePublishing {
		t.Fatalf("multi-recovered successor publishing=%+v err=%v", publishing, err)
	}
	publishingFence := domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: publishing.RunnerEpoch}
	blocked, err := db.TransitionPublishedBlock(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: publishing.Version, From: domain.StatePublishing, To: domain.StateBlocked,
		ResumeState: domain.StatePublishing, Trigger: "typed_blocker", Fence: publishingFence,
		EventPayload: `{"code":"publication_retry_required"}`,
	})
	if err != nil {
		t.Fatalf("successor candidate-only block: %v", err)
	}
	resumedBlock, err := db.TransitionPublishedResume(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: blocked.Version, From: domain.StateBlocked, To: domain.StatePublishing,
		Trigger: "operator_recover", Fence: publishingFence, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatalf("successor candidate-only block recovery: %v", err)
	}
	publishing, err = db.Ticket(ctx, ticket.Ref)
	if err != nil || publishing.Version != resumedBlock.Version {
		t.Fatalf("successor block recovery ticket=%+v result=%+v err=%v", publishing, resumedBlock, err)
	}
	paused, err := db.TransitionPublishedPause(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: publishing.Version, From: domain.StatePublishing, To: domain.StatePaused,
		ResumeState: domain.StatePublishing, Trigger: "retry_or_correction_exhausted", Fence: publishingFence, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatalf("successor candidate-only semantic pause: %v", err)
	}
	resumedPause, err := db.TransitionPublishedResume(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePublishing,
		Trigger: "operator_retry", Fence: publishingFence, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatalf("successor candidate-only semantic resume: %v", err)
	}
	publishing, err = db.Ticket(ctx, ticket.Ref)
	if err != nil || publishing.Version != resumedPause.Version {
		t.Fatalf("successor semantic resume ticket=%+v result=%+v err=%v", publishing, resumedPause, err)
	}
	stopped, pausedControl := postPublicationPauseAt(t, db, publishing, publishingFence)
	resumedResult, err := db.TransitionPublishedResume(ctx, Transition{
		Ref: ticket.Ref, ExpectedVersion: pausedControl.Version, From: domain.StatePaused, To: domain.StatePublishing,
		Trigger: "operator_resume", Fence: domain.Fence{LeaderEpoch: publishingFence.LeaderEpoch, RunnerEpoch: pausedControl.RunnerEpoch}, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatalf("successor paused leader-takeover resume: %v", err)
	}
	resumed, err := db.Ticket(ctx, ticket.Ref)
	if err != nil || resumed.Version != resumedResult.Version || resumed.State != domain.StatePublishing {
		t.Fatalf("successor paused leader-takeover ticket=%+v result=%+v err=%v", resumed, resumedResult, err)
	}

	var path string
	if err := db.db.QueryRowContext(ctx, `SELECT file FROM pragma_database_list WHERE name='main'`).Scan(&path); err != nil || path == "" {
		t.Fatalf("successor database path=%q err=%v", path, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	restartLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "successor-publication-rearm")
	if err != nil {
		t.Fatal(err)
	}
	if changed, fenceErr := db.FenceRecoveredRunners(ctx, domain.ChannelDev, restartLeader); fenceErr != nil || changed != 1 {
		t.Fatalf("successor rearm fence changed=%d err=%v", changed, fenceErr)
	}
	if err := db.RebindRecoveredPublishedCandidates(ctx, domain.ChannelDev, restartLeader); err != nil {
		t.Fatalf("successor rearm startup publication proof: %v", err)
	}
	stopped, err = db.StoppedRuntimeTicket(ctx, resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := db.PostPublicationRearmProof(ctx, resumed.Ref, stopped)
	if err != nil || capability == nil {
		t.Fatalf("successor candidate-only rearm capability=%v err=%v", capability, err)
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
	if err := db.recordCIObservation(ctx, future); !errors.Is(err, ErrCIObservation) {
		t.Fatalf("future observation accepted: %v", err)
	}
	valid := ciAuthorityObservationFor(publication, ticket, publicationFence, "green", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "check-lint", NormalizedState: "success"}})
	if err := db.recordCIObservation(ctx, valid); err != nil {
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
	if err := db.recordCIObservation(ctx, first); err != nil {
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
	if err := db.recordCIObservation(ctx, later); err != nil {
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
	if err := db.recordCIObservation(ctx, observation); err != nil {
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
			if err := db.recordCIObservation(ctx, observation); err != nil {
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
