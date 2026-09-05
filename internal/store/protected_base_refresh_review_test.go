package store

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

func TestProtectedBaseRefreshRequiresFreshFinalReview(t *testing.T) {
	t.Run("predecessor review becomes absence and fresh review succeeds", func(t *testing.T) {
		fixture, predecessor := protectedBaseRefreshReviewFixture(t)
		defer fixture.db.Close()

		request := LatestReusableProviderAttemptRequest{
			Ref: fixture.ticket.Ref, Phase: domain.PhaseReview, Role: "reviewer",
			ExpectedVersion: fixture.ticket.Version, Fence: fixture.fence,
		}
		if _, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, request); !errors.Is(err, ErrNotFound) {
			t.Fatalf("pre-refresh review must not be reused: %v", err)
		}

		fresh := completeFinalReview(t, fixture)
		if fresh.Attempt != predecessor.Attempt+1 || fresh.ExpectedVersion != fixture.ticket.Version {
			t.Fatalf("fresh review claim=%+v predecessor=%+v candidate=%+v", fresh, predecessor, fixture.candidate.Snapshot)
		}
		reused, err := fixture.db.LatestReusableProviderAttempt(fixture.ctx, request)
		if err != nil || reused.Key.AttemptID != fresh.ID || reused.Parsed.Reviewer == nil || reused.Parsed.Reviewer.ReviewedHead != fixture.candidate.Snapshot.HeadSHA {
			t.Fatalf("fresh review reusable=%+v err=%v", reused, err)
		}
	})

	t.Run("unrelated predecessor review is refused", func(t *testing.T) {
		fixture, predecessor := protectedBaseRefreshReviewFixture(t)
		defer fixture.db.Close()

		validation, digest, err := phaseartifact.CanonicalValidation(phaseartifact.Validation{
			TicketType: fixture.ticket.Type, ExpectedReviewedHead: strings.Repeat("d", 40),
			ExpectedProofDigest: fixture.candidate.Snapshot.ProofDigest,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `DROP TRIGGER provider_attempt_results_immutable_update`); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.db.ExecContext(fixture.ctx, `UPDATE provider_attempt_results SET validation=?,validation_sha256=? WHERE provider_attempt_id=?`, validation, digest, predecessor.ID); err != nil {
			t.Fatal(err)
		}

		_, err = fixture.db.LatestReusableProviderAttempt(fixture.ctx, LatestReusableProviderAttemptRequest{
			Ref: fixture.ticket.Ref, Phase: domain.PhaseReview, Role: "reviewer",
			ExpectedVersion: fixture.ticket.Version, Fence: fixture.fence,
		})
		if !errors.Is(err, ErrEvidenceConflict) && !errors.Is(err, ErrProviderAttempt) {
			t.Fatalf("unrelated predecessor review err=%v, want authenticated evidence refusal", err)
		}
		var attempts int
		if err := fixture.db.db.QueryRowContext(fixture.ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='review'`, fixture.ticket.Ref.Channel, fixture.ticket.Ref.Project, fixture.ticket.Ref.Ticket).Scan(&attempts); err != nil || attempts != 1 {
			t.Fatalf("review attempts=%d err=%v", attempts, err)
		}
	})
}

func protectedBaseRefreshReviewFixture(t *testing.T) (finalReviewFixture, ProviderAttemptClaim) {
	t.Helper()
	fixture := finalReviewLifecycleFixture(t)
	predecessor := completeFinalReview(t, fixture)
	if _, err := fixture.db.TransitionFinalReview(fixture.ctx, Transition{
		Ref: fixture.ticket.Ref, ExpectedVersion: fixture.ticket.Version,
		From: domain.StateReviewing, To: domain.StateWaitingApproval,
		Trigger: "review_pass", Fence: fixture.fence, EventPayload: `{}`,
	}); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	oldPublication, err := fixture.db.LoadHistoricalPublishedCandidate(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	waiting, err := fixture.db.Ticket(fixture.ctx, fixture.ticket.Ref)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: fixture.fence.LeaderEpoch, RunnerEpoch: waiting.RunnerEpoch}
	newBase := strings.Repeat("9", 40)
	proof, err := fixture.db.ProtectedBaseRefreshProofIntent(fixture.ctx, waiting.Ref, waiting.Version, fence, newBase)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	protectedBaseRefreshReviewConfirmEffect(t, fixture.db, waiting, fence, proof.Intent, "git/protected-ref-fetch", proof.ContextDigest, proof.ObservedIdentity)
	reservation, err := fixture.db.ReserveProtectedBaseRefresh(fixture.ctx, waiting.Ref, waiting.Version, fence, newBase)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	claim, err := fixture.db.IssueGitMutationClaim(fixture.ctx, reservation.Mutation)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	lease, err := fixture.db.AcquireGitMutation(fixture.ctx, claim)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	preparedCommit, preparedTree := strings.Repeat("8", 40), strings.Repeat("7", 40)
	recorder, ok := lease.(contracts.GitBaseRefreshPreparationLease)
	if !ok {
		fixture.db.Close()
		t.Fatal("missing base-refresh preparation lease")
	}
	if err := recorder.RecordBaseRefreshPreparation(fixture.ctx, preparedCommit, preparedTree, [2]string{claim.ExpectedHeadOID, claim.ExpectedBaseOID}); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	if _, err := fixture.db.ConfirmEffect(fixture.ctx, EffectFence{
		SemanticKey: claim.SemanticKey, Ref: waiting.Ref, TicketVersion: waiting.Version,
		Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch},
	}, preparedCommit); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	worktree, err := fixture.db.Worktree(fixture.ctx, waiting.Ref)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	var identity gitboundary.Identity
	if err := json.Unmarshal(worktree.IdentityJSON, &identity); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	identity.BaseHead = newBase
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	completion, err := fixture.db.CompleteProtectedBaseRefresh(fixture.ctx, claim, identityJSON)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	building, err := fixture.db.Ticket(fixture.ctx, waiting.Ref)
	if err != nil || building.State != domain.StateBuilding {
		fixture.db.Close()
		t.Fatalf("refreshed ticket=%+v err=%v", building, err)
	}
	buildFence := domain.Fence{LeaderEpoch: completion.Fence.LeaderEpoch, RunnerEpoch: building.RunnerEpoch}
	builderKey, builder := completeCandidateRepairBuilderBeforeCandidate(t, fixture.db, building, buildFence)
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(builder)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	verification, err := fixture.db.HistoricalVerification(fixture.ctx, waiting.Ref)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	policyDigest := sha256Digest([]byte("protected-base-refresh-review-policy"))
	command := completeEvidenceRepositoryCommand(t, fixture.db, fixture.ctx, RepositoryCommandPurposePostbuildCandidate, waiting.Ref, building.Version, buildFence, builderKey, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Revision.CheckpointID, "sha256:"+policyDigest, 0)
	successor := domain.CandidateSnapshot{
		Generation: fixture.candidate.Snapshot.Generation + 1, BaseSHA: newBase,
		HeadSHA: strings.Repeat("6", 40), TreeSHA: strings.Repeat("5", 40),
		SourceDigest:             fixture.candidate.Snapshot.SourceDigest,
		VerificationIntentDigest: fixture.candidate.Snapshot.VerificationIntentDigest,
		ProofDigest:              fixture.candidate.Snapshot.ProofDigest, CommandPolicyDigest: policyDigest,
		BuilderEvidenceDigest: builderDigest,
	}
	if _, err := fixture.db.RecordCandidate(fixture.ctx, CandidateEvidence{
		Ref: waiting.Ref, ExpectedVersion: building.Version, Fence: buildFence,
		Snapshot: successor, BuilderResult: builderKey,
		Commit: CommitObservation{CommitOID: successor.HeadSHA, ParentOID: preparedCommit, TreeOID: successor.TreeSHA},
		Reason: "protected base refresh", CommandResult: command,
	}); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	if _, err := fixture.db.TransitionCandidate(fixture.ctx, Transition{
		Ref: waiting.Ref, ExpectedVersion: building.Version, From: domain.StateBuilding,
		To: domain.StatePublishing, Trigger: "phase_pass", Fence: buildFence, EventPayload: `{}`,
	}, successor); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	publishing, err := fixture.db.Ticket(fixture.ctx, waiting.Ref)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	currentCandidate, err := fixture.db.RecoverableCandidate(fixture.ctx, waiting.Ref)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	currentWorktree, err := fixture.db.Worktree(fixture.ctx, waiting.Ref)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	pr := oldPublication.PullRequest
	pr.HeadOID, pr.BaseOID = successor.HeadSHA, successor.BaseSHA
	publication := PublishedCandidateEvidence{
		Ref: waiting.Ref, TicketVersion: publishing.Version, Fence: buildFence,
		Candidate: currentCandidate, ConfigGeneration: publishing.ConfigGeneration,
		ConfigDigest: publishing.ConfigDigest, ConfigSnapshotDigest: sha256Digest(publishing.ConfigSnapshot),
		Worktree: currentWorktree, RemoteBranchRef: currentWorktree.Branch,
		RemoteBranchOID: successor.HeadSHA, RemoteBaseOID: successor.BaseSHA,
		PullRequest: pr, PullRequestState: "OPEN", PullRequestDraft: true,
		PullRequestObservedAt: time.Now().UTC(), CreatedAt: time.Now().UTC(),
	}
	publication.PushEffect = PublicationEffectEvidence{
		SemanticKey: "refresh-review-push-" + string(waiting.Ref.Ticket), Kind: PublicationPushEffectKind,
		RequestDigest: strings.Repeat("3", 64), ClaimEpoch: 1,
		ObservedIdentity: CanonicalPublicationPushObservation(publication.RemoteBranchRef, publication.RemoteBranchOID),
	}
	publication.PRCreateOrUpdateEffect = PublicationEffectEvidence{
		SemanticKey: "refresh-review-pr-" + string(waiting.Ref.Ticket), Kind: PublicationPRUpdateEffectKind,
		RequestDigest: "sha256:" + strings.Repeat("4", 64), ClaimEpoch: 1,
		ObservedIdentity: CanonicalPublicationPRObservation(pr, "OPEN", true),
	}
	protectedBaseRefreshReviewConfirmPublicationEffect(t, fixture.db, publishing, buildFence, publication.PushEffect)
	protectedBaseRefreshReviewConfirmPublicationEffect(t, fixture.db, publishing, buildFence, publication.PRCreateOrUpdateEffect)
	if err := fixture.db.RecordPublishedCandidate(fixture.ctx, publication); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	if _, err := fixture.db.TransitionPublishedCandidate(fixture.ctx, Transition{
		Ref: waiting.Ref, ExpectedVersion: publishing.Version, From: domain.StatePublishing,
		To: domain.StateWaitingCI, Trigger: "effects_confirmed", Fence: buildFence, EventPayload: `{}`,
	}); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	waitingCI, err := fixture.db.Ticket(fixture.ctx, waiting.Ref)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	currentPublication, err := fixture.db.LoadPublishedCandidate(fixture.ctx, waiting.Ref)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	policy := CIRequiredCheckPolicy{
		Ref: waiting.Ref, CandidateGeneration: successor.Generation,
		CandidateHeadSHA: successor.HeadSHA, CandidateTreeSHA: successor.TreeSHA,
		PublicationWitnessDigest: currentPublication.WitnessDigest,
		ProtectedBranchRef:       pr.BaseRef, ProtectedBranchOID: pr.BaseOID,
		PolicySourceDigest: strings.Repeat("b", 64), AuthenticatedPrincipal: "refresh-review",
		RequiredChecks: []CIObservationCheck{{CanonicalName: "unit", ExternalID: "run-refresh-review"}},
		authenticated:  true,
	}
	canonicalPolicy, err := canonicalCIPolicy(policy)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	if err := fixture.db.RecordCIRequiredCheckPolicy(fixture.ctx, policy); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	observation := CIObservation{
		Ref: waiting.Ref, CandidateGeneration: successor.Generation,
		CandidateHeadSHA: successor.HeadSHA, CandidateTreeSHA: successor.TreeSHA,
		PublicationWitnessDigest: currentPublication.WitnessDigest,
		PolicyWitnessDigest:      canonicalPolicy.PolicyWitnessDigest, PullRequest: pr,
		ObservedTicketVersion: waitingCI.Version, ObservedFence: buildFence, ObservedAt: time.Now().UTC(),
		RequiredChecks: []CIObservationCheck{{CanonicalName: "unit", ExternalID: "run-refresh-review", NormalizedState: "success"}},
		Classification: "green",
	}
	canonicalObservation, err := canonicalCIObservation(observation)
	if err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	if err := fixture.db.recordCIObservation(fixture.ctx, observation); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	if _, err := fixture.db.ConsumeCIObservation(fixture.ctx, CIObservationTransition{
		Ref: waiting.Ref, ObservationDigest: canonicalObservation.ObservationDigest,
		ExpectedVersion: waitingCI.Version, Fence: buildFence,
	}); err != nil {
		fixture.db.Close()
		t.Fatal(err)
	}
	reviewing, err := fixture.db.Ticket(fixture.ctx, waiting.Ref)
	if err != nil || reviewing.State != domain.StateReviewing {
		fixture.db.Close()
		t.Fatalf("second-generation review ticket=%+v err=%v", reviewing, err)
	}
	return finalReviewFixture{
		db: fixture.db, ctx: fixture.ctx, ticket: reviewing,
		fence:     domain.Fence{LeaderEpoch: buildFence.LeaderEpoch, RunnerEpoch: reviewing.RunnerEpoch},
		candidate: currentCandidate,
	}, predecessor
}

func protectedBaseRefreshReviewConfirmEffect(t *testing.T, db *Store, ticket Ticket, fence domain.Fence, intent GitMutationIntent, kind, requestDigest, observed string) {
	t.Helper()
	if _, err := db.PlanEffect(t.Context(), EffectPlan{SemanticKey: intent.SemanticKey, Ref: ticket.Ref, Kind: kind, TicketVersion: ticket.Version, Fence: fence, RequestDigest: requestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueGitMutationClaim(t.Context(), intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmEffect(t.Context(), EffectFence{
		SemanticKey: claim.SemanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version,
		Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch},
	}, observed); err != nil {
		t.Fatal(err)
	}
}

func protectedBaseRefreshReviewConfirmPublicationEffect(t *testing.T, db *Store, ticket Ticket, fence domain.Fence, effect PublicationEffectEvidence) {
	t.Helper()
	if _, err := db.PlanEffect(t.Context(), EffectPlan{SemanticKey: effect.SemanticKey, Ref: ticket.Ref, Kind: effect.Kind, TicketVersion: ticket.Version, Fence: fence, RequestDigest: effect.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.ClaimEffect(t.Context(), EffectFence{SemanticKey: effect.SemanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version, Fence: fence})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Effect.ClaimEpoch != effect.ClaimEpoch {
		t.Fatalf("effect claim=%d want=%d", claim.Effect.ClaimEpoch, effect.ClaimEpoch)
	}
	if _, err := db.ConfirmEffect(t.Context(), EffectFence{
		SemanticKey: effect.SemanticKey, Ref: ticket.Ref, TicketVersion: ticket.Version,
		Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ClaimEpoch: claim.Effect.ClaimEpoch},
	}, effect.ObservedIdentity); err != nil {
		t.Fatal(err)
	}
}
