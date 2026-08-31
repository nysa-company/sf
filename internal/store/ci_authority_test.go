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
	if err := db.RecordCIObservation(t.Context(), observation); err != nil {
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
	authority := CorrectionBudgetAuthority{Ref: ticket.Ref, RequestID: "atomic-rollback", TicketVersion: ticket.Version, Fence: loaded.ObservedFence}
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
	authority := CorrectionBudgetAuthority{Ref: ticket.Ref, RequestID: "atomic-commit", TicketVersion: ticket.Version, Fence: loaded.ObservedFence}
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
	// These rows model a legacy/crash orphan: even with their generic budget
	// evidence present, they have no exact red transition and repair binding.
	// The new Store reducer must reject them rather than treating them as an
	// exhausted authority or allocating a third use.
	for _, requestID := range []string{"existing-one", "existing-two"} {
		if _, err := db.ConsumeBudget(t.Context(), BudgetUse{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, Kind: "correction", RequestID: requestID}); err != nil {
			t.Fatal(err)
		}
	}
	authority := CorrectionBudgetAuthority{Ref: ticket.Ref, RequestID: "should-not-allocate", TicketVersion: ticket.Version, Fence: loaded.ObservedFence}
	if _, err := db.ConsumeCIObservation(t.Context(), CIObservationTransition{Ref: ticket.Ref, ObservationDigest: loaded.ObservationDigest, ExpectedVersion: ticket.Version, Fence: loaded.ObservedFence, CorrectionBudget: &authority}); !errors.Is(err, ErrCIObservation) {
		t.Fatalf("orphan exhausted atomic CI consume err=%v", err)
	}
	var uses int
	if err := db.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM ticket_budget_uses WHERE channel=? AND project_id=? AND ticket_id=? AND kind='correction'`, ticket.Ref.Channel, ticket.Ref.Project, ticket.Ref.Ticket).Scan(&uses); err != nil {
		t.Fatal(err)
	}
	current, err := db.Ticket(t.Context(), ticket.Ref)
	if err != nil || uses != 2 || current.State != domain.StateWaitingCI || current.Version != ticket.Version {
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
