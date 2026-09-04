package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

func sameJSON(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func supervisedOperatorSource(t *testing.T, database *Store, ctx context.Context, request ProviderAttemptRequest) ProviderAttemptRequest {
	t.Helper()
	request = supervised(t, request)
	worktree, err := database.Worktree(ctx, request.Ref)
	if err != nil {
		t.Fatal(err)
	}
	request.WorktreeIdentity = string(worktree.IdentityJSON)
	request.Input.WorktreeIdentity = request.WorktreeIdentity
	return request
}

func TestOperatorSourceResumeEventRequiresCanonicalBoundedPaths(t *testing.T) {
	for _, paths := range [][]string{
		nil,
		{"."},
		{"../escape.go"},
		{"internal/feature.go", "internal/feature.go"},
		{"z.go", "a.go"},
		{strings.Repeat("x", 4097)},
	} {
		if validOperatorSourceChangedFiles(paths) {
			t.Fatalf("accepted paths=%q", paths)
		}
	}
	if !validOperatorSourceChangedFiles([]string{"cmd/sf/main.go", "internal/feature.go"}) {
		t.Fatal("rejected canonical source paths")
	}
}

func TestOperatorSourceResumeEventRejectsOutOfPlanAndOwnedPaths(t *testing.T) {
	if validChangedPathsForSourceProof([]string{"internal/feature.go"}, []string{"cmd"}, []string{"verify.go"}) {
		t.Fatal("accepted out-of-plan source edit")
	}
	if validChangedPathsForSourceProof([]string{"internal/verify.go"}, []string{"internal"}, []string{"internal/verify.go"}) {
		t.Fatal("accepted verification-owned edit")
	}
}

func TestOperatorSourceResumeTransitionAndProofBindExactEvidence(t *testing.T) {
	database, ctx, leader, resumed, source := operatorSourceResumeResumedFixture(t)
	if fresh, freshErr := database.OperatorSourceResumeRequiresFreshVerification(ctx, resumed.Ref, resumed.Version); freshErr != nil || !fresh {
		t.Fatalf("source transition did not require fresh verification: fresh=%v err=%v", fresh, freshErr)
	}
	openExactRuntimeAdmission(t, database, resumed.Ref)
	current, err := database.Ticket(ctx, resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	proof, found, err := database.OperatorSourceResumeProof(ctx, resumed.Ref, current.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch})
	if err != nil || !found || proof.Worktree.Path == "" || proof.Verification.Checkpoint.CommitOID == "" || proof.Plan.Digest == "" || proof.Operator != "sofia" || proof.SourceCommit.CommitOID != source.CommitOID || proof.Remote.CandidatePresent {
		t.Fatalf("proof=%+v found=%v err=%v", proof, found, err)
	}
}

func TestOperatorSourceResumeEndpointRejectsUnboundVerificationProjection(t *testing.T) {
	database, ctx, leader, resumed, _ := operatorSourceResumeResumedFixture(t)
	openExactRuntimeAdmission(t, database, resumed.Ref)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch}
	if err := database.write(ctx, func(conn *sql.Conn) error {
		return evidenceEvent(ctx, conn, resumed.Ref, resumed.Version, "verification_recorded", map[string]any{"revision": 2})
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := database.OperatorSourceResumeProof(ctx, resumed.Ref, resumed.Version, fence); !errors.Is(err, ErrEvidenceConflict) || found {
		t.Fatalf("unbound verification projection did not fail closed: found=%v err=%v", found, err)
	}
}

func TestOperatorSourceResumeEndpointRejectsUnexpectedProjection(t *testing.T) {
	database, ctx, leader, resumed, _ := operatorSourceResumeResumedFixture(t)
	openExactRuntimeAdmission(t, database, resumed.Ref)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch}
	if err := database.write(ctx, func(conn *sql.Conn) error {
		return evidenceEvent(ctx, conn, resumed.Ref, resumed.Version, "unexpected_projection", map[string]any{"unexpected": true})
	}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := database.OperatorSourceResumeProof(ctx, resumed.Ref, resumed.Version, fence); !errors.Is(err, ErrEvidenceConflict) || found {
		t.Fatalf("unexpected same-state projection did not fail closed: found=%v err=%v", found, err)
	}
}

func TestOperatorSourceResumeVerificationProjectionSurvivesRecoveryRebind(t *testing.T) {
	database, ctx, firstLeader, resumed, source := operatorSourceResumeResumedFixture(t)
	openExactRuntimeAdmission(t, database, resumed.Ref)
	artifact := recordOperatorSourceFreshVerificationAtEndpoint(t, database, ctx, firstLeader, resumed, source)
	firstFence := domain.Fence{LeaderEpoch: firstLeader, RunnerEpoch: resumed.RunnerEpoch}
	if proof, found, err := database.OperatorSourceResumeProof(ctx, resumed.Ref, resumed.Version, firstFence); err != nil || !found || proof.Version != resumed.Version {
		t.Fatalf("authenticated endpoint projection proof=%+v found=%v err=%v", proof, found, err)
	}

	if err := database.restoreRuntimeControls(ctx); err != nil {
		t.Fatal(err)
	}
	secondLeader, err := database.AcquireLeader(ctx, domain.ChannelDev, "source-resume-verification-rebind")
	if err != nil || secondLeader <= firstLeader {
		t.Fatalf("second leader=%d first=%d err=%v", secondLeader, firstLeader, err)
	}
	if changed, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, secondLeader); err != nil || changed != 1 {
		t.Fatalf("fence changed=%d err=%v", changed, err)
	}
	fenced, err := database.Ticket(ctx, resumed.Ref)
	if err != nil || fenced.State != domain.StateVerifying || fenced.Version != resumed.Version+1 || fenced.RunnerEpoch != resumed.RunnerEpoch+1 {
		t.Fatalf("fenced=%+v resumed=%+v err=%v", fenced, resumed, err)
	}
	openExactRuntimeAdmission(t, database, fenced.Ref)
	artifact.ExpectedVersion = fenced.Version
	artifact.Fence = domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: fenced.RunnerEpoch}
	if _, err := database.RecordVerification(ctx, artifact); err != nil {
		t.Fatalf("rebind fresh verification: %v", err)
	}
	if proof, found, err := database.OperatorSourceResumeProof(ctx, fenced.Ref, fenced.Version, artifact.Fence); err != nil || !found || proof.Version != fenced.Version {
		t.Fatalf("recovered projection proof=%+v found=%v err=%v", proof, found, err)
	}
	if _, err := database.TransitionVerification(ctx, Transition{Ref: fenced.Ref, ExpectedVersion: fenced.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "phase_pass", Fence: artifact.Fence, EventPayload: `{}`}); err != nil {
		t.Fatalf("transition recovered verification: %v", err)
	}
}

// operatorSourceResumeResumedFixture ends at the dedicated fresh-verification
// endpoint, before any new Reviewer has been claimed.  Recovery tests use the
// same real Store control lineage instead of fabricating events or counters.
func operatorSourceResumeResumedFixture(t *testing.T) (*Store, context.Context, uint64, Ticket, contracts.OperatorSourceCommit) {
	t.Helper()
	database, ctx, leader, building := operatorSourceResumeBuildingFixture(t)
	take, err := json.Marshal(map[string]any{"intent": "take", "operator": "sofia", "operator_uid": uint32(501)})
	if err != nil {
		t.Fatal(err)
	}
	stopping, err := database.TransitionAndInvalidateRunner(ctx, Transition{Ref: building.Ref, ExpectedVersion: building.Version, From: domain.StateBuilding, To: domain.StateStopping, ResumeState: domain.StateBuilding, Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: building.RunnerEpoch}, EventPayload: string(take)})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := database.Ticket(ctx, building.Ref)
	if err != nil || stopped.Version != stopping.Version {
		t.Fatalf("stopped=%+v transition=%+v err=%v", stopped, stopping, err)
	}
	worktree, err := database.Worktree(ctx, building.Ref)
	if err != nil {
		t.Fatal(err)
	}
	baseline := TakeoverRemoteBaseline{Registered: true, WorktreePath: worktree.Path, WorktreeBranch: worktree.Branch, WorktreeIdentity: sha256Digest(worktree.IdentityJSON), BaseOID: worktree.BaseSHA}
	drain, err := json.Marshal(map[string]any{"drained": true, "intent": "take", "remote": baseline})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteControlTransition(ctx, Transition{Ref: building.Ref, ExpectedVersion: stopped.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StateBuilding, Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopped.RunnerEpoch}, EventPayload: string(drain)}); err != nil {
		t.Fatal(err)
	}
	paused, err := database.Ticket(ctx, building.Ref)
	if err != nil {
		t.Fatal(err)
	}
	source := contracts.OperatorSourceCommit{CommitOID: strings.Repeat("e", 40), ParentOID: strings.Repeat("c", 40), TreeOID: strings.Repeat("f", 40), Changes: []contracts.OperatorSourceChange{{Status: "M", Path: "src/feature.go"}}}
	resumed, err := database.TransitionOperatorSourceResume(ctx, OperatorSourceResume{Ref: building.Ref, ExpectedVersion: paused.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: paused.RunnerEpoch}, Operator: "sofia", SourceCommit: source, Remote: baseline})
	if err != nil || resumed.Version != paused.Version+1 {
		t.Fatalf("source transition=%+v err=%v", resumed, err)
	}
	resumedTicket, err := database.Ticket(ctx, building.Ref)
	if err != nil || resumedTicket.State != domain.StateVerifying || resumedTicket.ResumeState != "" {
		t.Fatalf("source transition did not enter fresh verification: ticket=%+v err=%v", resumedTicket, err)
	}
	return database, ctx, leader, resumedTicket, source
}

func TestOperatorSourceResumeRecoveryKeepsFreshVerificationAcrossRestarts(t *testing.T) {
	database, ctx, firstLeader, resumed, source := operatorSourceResumeResumedFixture(t)
	openExactRuntimeAdmission(t, database, resumed.Ref)

	// Crash before a fresh Reviewer claim. restoreRuntimeControls is the same
	// durable-open -> sealed operation Open performs before startup fencing.
	if err := database.restoreRuntimeControls(ctx); err != nil {
		t.Fatal(err)
	}
	secondLeader, err := database.AcquireLeader(ctx, domain.ChannelDev, "source-resume-recovery-one")
	if err != nil || secondLeader <= firstLeader {
		t.Fatalf("second leader=%d first=%d err=%v", secondLeader, firstLeader, err)
	}
	if changed, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, secondLeader); err != nil || changed != 1 {
		t.Fatalf("first fence changed=%d err=%v", changed, err)
	}
	first, err := database.Ticket(ctx, resumed.Ref)
	if err != nil || first.State != domain.StateVerifying {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	if fresh, err := database.OperatorSourceResumeRequiresFreshVerification(ctx, first.Ref, first.Version); err != nil || !fresh {
		t.Fatalf("first fresh=%v err=%v", fresh, err)
	}
	proof, found, err := database.OperatorSourceResumeProof(ctx, first.Ref, first.Version, domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: first.RunnerEpoch})
	if err != nil || !found || proof.SourceCommit.CommitOID != source.CommitOID || proof.Fence.LeaderEpoch != secondLeader {
		t.Fatalf("first proof=%+v found=%v err=%v", proof, found, err)
	}
	firstStep, found, err := loadLatestRunnerRecovery(ctx, database.db, first.Ref)
	if err != nil || !found || firstStep.PriorTicketVersion != resumed.Version || firstStep.PriorRunnerEpoch != resumed.RunnerEpoch || firstStep.PriorLeaderEpoch != firstLeader || firstStep.LeaderEpoch != secondLeader {
		t.Fatalf("first recovery=%+v found=%v err=%v", firstStep, found, err)
	}

	// A second crash must extend the same signed chain, not lose the source
	// marker and fall back to normal worktree recovery.
	if err := database.restoreRuntimeControls(ctx); err != nil {
		t.Fatal(err)
	}
	thirdLeader, err := database.AcquireLeader(ctx, domain.ChannelDev, "source-resume-recovery-two")
	if err != nil || thirdLeader <= secondLeader {
		t.Fatalf("third leader=%d second=%d err=%v", thirdLeader, secondLeader, err)
	}
	if changed, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, thirdLeader); err != nil || changed != 1 {
		t.Fatalf("second fence changed=%d err=%v", changed, err)
	}
	second, err := database.Ticket(ctx, resumed.Ref)
	if err != nil || second.State != domain.StateVerifying {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	if fresh, err := database.OperatorSourceResumeRequiresFreshVerification(ctx, second.Ref, second.Version); err != nil || !fresh {
		t.Fatalf("second fresh=%v err=%v", fresh, err)
	}
	proof, found, err = database.OperatorSourceResumeProof(ctx, second.Ref, second.Version, domain.Fence{LeaderEpoch: thirdLeader, RunnerEpoch: second.RunnerEpoch})
	if err != nil || !found || proof.SourceCommit.CommitOID != source.CommitOID || proof.Fence.LeaderEpoch != thirdLeader {
		t.Fatalf("second proof=%+v found=%v err=%v", proof, found, err)
	}
	secondStep, found, err := loadLatestRunnerRecovery(ctx, database.db, second.Ref)
	if err != nil || !found || secondStep.PriorTicketVersion != first.Version || secondStep.PriorRunnerEpoch != first.RunnerEpoch || secondStep.PriorLeaderEpoch != secondLeader || secondStep.LeaderEpoch != thirdLeader {
		t.Fatalf("second recovery=%+v found=%v err=%v", secondStep, found, err)
	}
}

func TestOperatorSourceResumeRecoveryBeforeRearmStillRequiresFreshVerification(t *testing.T) {
	database, ctx, firstLeader, resumed, source := operatorSourceResumeResumedFixture(t)

	// TransitionOperatorSourceResume commits before the runtime controller can
	// rearm.  Model process death in that exact window: startup seals the durable
	// control, acquires the next daemon leader, and fences before any admission
	// capability is opened.  The source endpoint must be the only predecessor;
	// a generic worktree fallback would either lose the source marker or admit the
	// historical Reviewer.
	if err := database.restoreRuntimeControls(ctx); err != nil {
		t.Fatal(err)
	}
	secondLeader, err := database.AcquireLeader(ctx, domain.ChannelDev, "source-resume-before-rearm")
	if err != nil || secondLeader <= firstLeader {
		t.Fatalf("second leader=%d first=%d err=%v", secondLeader, firstLeader, err)
	}
	if changed, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, secondLeader); err != nil || changed != 1 {
		t.Fatalf("fence changed=%d err=%v", changed, err)
	}
	fenced, err := database.Ticket(ctx, resumed.Ref)
	if err != nil || fenced.State != domain.StateVerifying || fenced.Version != resumed.Version+1 || fenced.RunnerEpoch != resumed.RunnerEpoch+1 {
		t.Fatalf("fenced ticket=%+v resumed=%+v err=%v", fenced, resumed, err)
	}
	fresh, err := database.OperatorSourceResumeRequiresFreshVerification(ctx, fenced.Ref, fenced.Version)
	if err != nil || !fresh {
		t.Fatalf("fresh verification requirement=%v err=%v", fresh, err)
	}
	recovery, found, err := loadLatestRunnerRecovery(ctx, database.db, fenced.Ref)
	if err != nil || !found || recovery.PriorLeaderEpoch != firstLeader || recovery.LeaderEpoch != secondLeader {
		t.Fatalf("source recovery=%+v found=%v err=%v", recovery, found, err)
	}
	proof, found, err := database.OperatorSourceResumeProof(ctx, fenced.Ref, fenced.Version, domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: fenced.RunnerEpoch})
	if err != nil || found || proof.SourceCommit.CommitOID != "" {
		t.Fatalf("pre-rearm proof=%+v found=%v err=%v", proof, found, err)
	}
	if source.CommitOID == "" {
		t.Fatal("fixture source commit is empty")
	}
}

func TestOperatorSourceResumeRecoveryRejectsTamperedEventOrLedger(t *testing.T) {
	for _, mutate := range []struct {
		name  string
		apply func(*Store, context.Context, Ticket)
	}{
		{
			name: "source resume event",
			apply: func(database *Store, ctx context.Context, resumed Ticket) {
				if _, err := database.db.ExecContext(ctx, `UPDATE events SET payload='{}' WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='operator_resume' AND from_state='paused' AND to_state='verifying'`, resumed.Ref.Channel, resumed.Ref.Project, resumed.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "recovery ledger",
			apply: func(database *Store, ctx context.Context, resumed Ticket) {
				// Establish one valid recovery first, then bypass only the test-only
				// append-only trigger to simulate durable corruption.
				if err := database.restoreRuntimeControls(ctx); err != nil {
					t.Fatal(err)
				}
				leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "source-resume-tamper-first")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); err != nil {
					t.Fatal(err)
				}
				if _, err := database.db.ExecContext(ctx, `DROP TRIGGER runner_recovery_ledger_immutable_update`); err != nil {
					t.Fatal(err)
				}
				if _, err := database.db.ExecContext(ctx, `UPDATE runner_recovery_ledger SET prior_leader_epoch=prior_leader_epoch+1 WHERE channel=? AND project_id=? AND ticket_id=?`, resumed.Ref.Channel, resumed.Ref.Project, resumed.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			database, ctx, _, resumed, _ := operatorSourceResumeResumedFixture(t)
			openExactRuntimeAdmission(t, database, resumed.Ref)
			mutate.apply(database, ctx, resumed)
			if err := database.restoreRuntimeControls(ctx); err != nil {
				t.Fatal(err)
			}
			leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "source-resume-tamper")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, leader); !errors.Is(err, ErrPublicationEvidence) {
				t.Fatalf("fence error=%v", err)
			}
		})
	}
}

func TestOperatorSourceResumeHistoryRejectsMalformedPayloadAfterControlReplacement(t *testing.T) {
	database, ctx, leader, resumed, _ := operatorSourceResumeResumedFixture(t)
	openExactRuntimeAdmission(t, database, resumed.Ref)

	// Replace the source handoff's runtime-control row through a second real
	// take/drain cycle. Historical source detection must not depend on the current
	// control pointer continuing to name the first take.
	take, err := json.Marshal(map[string]any{"intent": "take", "operator": "sofia", "operator_uid": uint32(501)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: resumed.Ref, ExpectedVersion: resumed.Version,
		From: domain.StateVerifying, To: domain.StateStopping, ResumeState: domain.StateVerifying,
		Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch}, EventPayload: string(take),
	}); err != nil {
		t.Fatalf("replace source runtime control: %v", err)
	}
	stopping, err := database.Ticket(ctx, resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := database.Worktree(ctx, resumed.Ref)
	if err != nil {
		t.Fatal(err)
	}
	remote := TakeoverRemoteBaseline{Registered: true, WorktreePath: worktree.Path, WorktreeBranch: worktree.Branch, WorktreeIdentity: sha256Digest(worktree.IdentityJSON), BaseOID: worktree.BaseSHA}
	drain, err := json.Marshal(map[string]any{"drained": true, "intent": "take", "remote": remote})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteControlTransition(ctx, Transition{
		Ref: resumed.Ref, ExpectedVersion: stopping.Version,
		From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StateVerifying,
		Trigger: "process_and_effects_drained", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopping.RunnerEpoch}, EventPayload: string(drain),
	}); err != nil {
		t.Fatalf("complete replacement control: %v", err)
	}

	// Simulate privileged durable corruption of the original source payload.
	// Its unique Building -> stopping -> paused -> Verifying shape remains, so it
	// is malformed authority rather than clean absence.
	if _, err := database.db.ExecContext(ctx, `UPDATE events SET payload='{}' WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_resume' AND from_state='paused' AND to_state='verifying'`, resumed.Ref.Channel, resumed.Ref.Project, resumed.Ref.Ticket, resumed.Version); err != nil {
		t.Fatal(err)
	}
	if present, err := operatorSourceResumeHistoryPresent(ctx, database.db, resumed.Ref); err != nil || !present {
		t.Fatalf("structural source history present=%v err=%v", present, err)
	}
	found, err := validateOperatorSourceResumeRecoveryTarget(ctx, database.db, resumed.Ref, resumed.Version, resumed.RunnerEpoch, leader)
	if !found || !errors.Is(err, ErrPublicationEvidence) {
		t.Fatalf("malformed historical source target found=%v err=%v", found, err)
	}
}

func TestOperatorSourceResumePreparedCheckpointRejectsUnboundDigest(t *testing.T) {
	database, ctx, leader, resumed, source := operatorSourceResumeResumedFixture(t)
	// The source-resume proof becomes a runnable scheduler capability only after
	// the sealed control endpoint is explicitly rearmed.  This test is about an
	// unbound prepared commit, not about bypassing that admission boundary.
	openExactRuntimeAdmission(t, database, resumed.Ref)
	sourceProof, found, err := database.OperatorSourceResumeProof(ctx, resumed.Ref, resumed.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch})
	if err != nil || !found || !sameJSON(sourceProof.SourceCommit, source) {
		t.Fatalf("source proof=%+v found=%v err=%v", sourceProof, found, err)
	}
	project, err := database.Project(ctx, resumed.Ref.Channel, resumed.Ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	intent := GitMutationIntent{EffectFence: EffectFence{Ref: resumed.Ref, TicketVersion: resumed.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch}}, RequestDigest: "sha256:" + strings.Repeat("a", 64), Repository: project.Path, Worktree: sourceProof.Worktree.Path, Branch: sourceProof.Worktree.Branch, Operation: "commit", BaseRef: project.BaseRef, ExpectedBaseOID: sourceProof.Worktree.BaseSHA, ExpectedHeadOID: source.CommitOID}
	intent.SemanticKey = CanonicalGitMutationSemanticKey(intent)
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: intent.SemanticKey, Ref: intent.Ref, Kind: "git/commit", TicketVersion: intent.TicketVersion, Fence: intent.Fence, RequestDigest: intent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := database.IssueGitMutationClaim(ctx, intent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := database.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	commit, tree := strings.Repeat("b", 40), strings.Repeat("c", 40)
	if err := lease.(contracts.GitMutationRecoveryFactsLease).RecordPreparedCommit(ctx, commit, tree); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ConfirmPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: commit, ParentOID: source.CommitOID, TreeOID: tree}); err != nil {
		t.Fatal(err)
	}
	// The proof reader must remain connection-scoped. A prior implementation
	// held one result set while reserving/querying another Store connection and
	// could self-deadlock when SQLite was intentionally constrained to one.
	database.db.SetMaxOpenConns(1)
	proofCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	got, found, err := database.OperatorSourceResumePreparedCheckpoint(proofCtx, resumed.Ref, resumed.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch})
	if err == nil || found || got != (CommitObservation{}) {
		t.Fatalf("accepted unbound prepared checkpoint=%+v found=%v err=%v", got, found, err)
	}
}

// This is the Store half of the S→F→G response-loss contract.  It constructs
// a fresh Reviewer checkpoint F and a Builder's prepared candidate G without
// calling RecordCandidate, then proves that the only recovery result carries
// the exact historical Builder/post-build command and the live recovery fence.
// Runtime integration uses this witness to observe G instead of rerunning
// either repository mutation.
func TestOperatorSourceResumePreparedCandidateWitnessBindsSFG(t *testing.T) {
	database, ctx, leader, resumed, source := operatorSourceResumeResumedFixture(t)
	openExactRuntimeAdmission(t, database, resumed.Ref)
	pair, err := database.ProviderPair(ctx, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	sourceProof, found, err := database.OperatorSourceResumeProof(ctx, resumed.Ref, resumed.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch})
	if err != nil || !found {
		t.Fatalf("source proof=%+v found=%v err=%v", sourceProof, found, err)
	}
	plan, err := database.Plan(ctx, resumed.Ref)
	if err != nil || plan.Document.Planner == nil {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	planIdentity, err := workflowprompt.NewPlanIdentity(*plan.Document.Planner)
	if err != nil {
		t.Fatal(err)
	}
	verificationArtifact := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: planIdentity.Digest, ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"verify"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: sha256Digest([]byte("fresh source-resume verification"))}
	verificationRaw, err := json.Marshal(verificationArtifact)
	if err != nil {
		t.Fatal(err)
	}
	verificationFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch}
	reviewer, err := database.BeginProviderAttempt(ctx, supervisedOperatorSource(t, database, ctx, ProviderAttemptRequest{Ref: resumed.Ref, ExpectedVersion: resumed.Version, Fence: verificationFence, Phase: domain.PhaseVerification, Role: "reviewer", Binding: runtime(pair.Reviewer), ConfigDigest: resumed.ConfigDigest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RecordProviderLaunch(ctx, reviewer, contracts.ProviderLaunch{PID: int(reviewer.ID), PGID: int(reviewer.ID), BootIdentity: "source-resume", ProcessStartIdentity: "fresh-review", Worktree: reviewer.Worktree}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteProviderAttemptSuccess(ctx, reviewer, proof(t, reviewer), resumed.Version, verificationFence, contracts.PhaseResult{Provider: reviewer.Binding.Identity, Artifact: verificationRaw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: resumed.Type, AcceptanceDigest: planIdentity.Digest}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	reviewerKey := ProviderAttemptResultKey{AttemptID: reviewer.ID, Ref: resumed.Ref, Phase: domain.PhaseVerification, Attempt: reviewer.Attempt}
	intent, err := workflowprompt.CanonicalVerificationIntentBytes(verificationArtifact)
	if err != nil {
		t.Fatal(err)
	}
	proofBytes, err := workflowprompt.CanonicalVerificationProofBytes(verificationArtifact)
	if err != nil {
		t.Fatal(err)
	}
	f := strings.Repeat("1", 40)
	prebuild := completeEvidenceRepositoryCommand(t, database, ctx, RepositoryCommandPurposePrebuildVerification, resumed.Ref, resumed.Version, verificationFence, reviewerKey, sha256Digest(intent), sha256Digest(proofBytes), "", "", 1)
	prebuildResult, err := database.LoadRepositoryCommandResult(ctx, prebuild)
	if err != nil {
		t.Fatal(err)
	}
	project, err := database.Project(ctx, resumed.Ref.Channel, resumed.Ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	fCommitDigest, err := CanonicalOperatorSourceResumeCheckpointDigest(OperatorSourceResumeCheckpointDigestInput{Ref: resumed.Ref, WorktreePath: sourceProof.Worktree.Path, Branch: sourceProof.Worktree.Branch, Identity: sourceProof.Worktree.IdentityJSON, BaseSHA: sourceProof.Worktree.BaseSHA, Source: sourceProof.SourceCommit, Retained: sourceProof.Verification.Revision, Provider: reviewerKey, Command: prebuild, ResultDigest: prebuildResult.ResultDigest, Artifact: verificationArtifact})
	if err != nil {
		t.Fatal(err)
	}
	fIntent := GitMutationIntent{EffectFence: EffectFence{Ref: resumed.Ref, TicketVersion: resumed.Version, Fence: verificationFence}, RequestDigest: fCommitDigest, Repository: project.Path, Worktree: sourceProof.Worktree.Path, Branch: sourceProof.Worktree.Branch, Operation: "commit", BaseRef: project.BaseRef, ExpectedBaseOID: sourceProof.Worktree.BaseSHA, ExpectedHeadOID: source.CommitOID}
	fIntent.SemanticKey = CanonicalGitMutationSemanticKey(fIntent)
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: fIntent.SemanticKey, Ref: fIntent.Ref, Kind: "git/commit", TicketVersion: fIntent.TicketVersion, Fence: fIntent.Fence, RequestDigest: fIntent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	fClaim, err := database.IssueGitMutationClaim(ctx, fIntent)
	if err != nil {
		t.Fatal(err)
	}
	fLease, err := database.AcquireGitMutation(ctx, fClaim)
	if err != nil {
		t.Fatal(err)
	}
	fTree := strings.Repeat("2", 40)
	if err := fLease.(contracts.GitMutationRecoveryFactsLease).RecordPreparedCommit(ctx, f, fTree); err != nil {
		t.Fatal(err)
	}
	if err := fLease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ConfirmPreparedCommit(ctx, fClaim, contracts.PreparedCommitObservation{CommitOID: f, ParentOID: source.CommitOID, TreeOID: fTree}); err != nil {
		t.Fatal(err)
	}

	// Crash after F's prepared Git commit but before RecordVerification. The
	// re-open/fence path must recover exactly F and the existing prebuild
	// result; it must not re-run Reviewer, prebuild, or a commit.
	if err := database.restoreRuntimeControls(ctx); err != nil {
		t.Fatal(err)
	}
	fRecoveryLeader, err := database.AcquireLeader(ctx, domain.ChannelDev, "source-resume-prepared-f-recovery")
	if err != nil || fRecoveryLeader <= leader {
		t.Fatalf("F recovery leader=%d prior=%d err=%v", fRecoveryLeader, leader, err)
	}
	if changed, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, fRecoveryLeader); err != nil || changed != 1 {
		t.Fatalf("fence prepared F changed=%d err=%v", changed, err)
	}
	fencedVerification, err := database.Ticket(ctx, resumed.Ref)
	if err != nil || fencedVerification.State != domain.StateVerifying {
		t.Fatalf("fenced verification=%+v err=%v", fencedVerification, err)
	}
	fencedVerificationFence := domain.Fence{LeaderEpoch: fRecoveryLeader, RunnerEpoch: fencedVerification.RunnerEpoch}
	preparedF, found, err := database.OperatorSourceResumePreparedCheckpoint(ctx, fencedVerification.Ref, fencedVerification.Version, fencedVerificationFence)
	if err != nil || !found || preparedF.CommitOID != f || preparedF.ParentOID != source.CommitOID || preparedF.TreeOID != fTree {
		t.Fatalf("recovered F=%+v found=%v err=%v", preparedF, found, err)
	}
	// Startup recovery may authenticate the prepared checkpoint while sealed,
	// but no mutation may consume it until the runtime admission is explicitly
	// rearmed at the recovered ticket identity.
	openExactRuntimeAdmission(t, database, fencedVerification.Ref)
	recoveredArtifact := VerificationArtifact{Ref: fencedVerification.Ref, ExpectedVersion: fencedVerification.Version, Fence: fencedVerificationFence, Intent: intent, Proof: proofBytes, OwnedFiles: verificationArtifact.OwnedFiles, CheckpointID: f, ProviderResult: &reviewerKey, Checkpoint: preparedF, CommandResult: prebuild}
	reviewerResult, _, err := database.loadHistoricalProviderAttemptResult(ctx, database.db, reviewerKey)
	if err != nil {
		t.Fatal(err)
	}
	proofConn, err := database.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, liveProofFound, liveProofErr := database.operatorSourceResumeProofFrom(ctx, proofConn, recoveredArtifact.Ref, recoveredArtifact.ExpectedVersion, recoveredArtifact.Fence)
	suffixErr := database.operatorSourceResumeProviderResultReachesFence(ctx, proofConn, recoveredArtifact.Ref, reviewerKey, reviewerResult, recoveredArtifact.ExpectedVersion, recoveredArtifact.Fence)
	proofErr := database.authenticateOperatorSourceResumeVerificationBinding(ctx, proofConn, recoveredArtifact, reviewerResult)
	fenceErr := database.assertTicketFence(ctx, proofConn, recoveredArtifact.Ref, recoveredArtifact.ExpectedVersion, recoveredArtifact.Fence)
	newestErr := assertNewestBoundResult(ctx, proofConn, recoveredArtifact.Ref, domain.PhaseVerification, "reviewer", reviewerKey)
	_, _, commandErr := authenticateVerificationCommandEvidence(ctx, proofConn, recoveredArtifact, &verificationArtifact)
	_ = proofConn.Close()
	if liveProofErr != nil || !liveProofFound || suffixErr != nil || proofErr != nil {
		t.Fatalf("prepared F live proof found=%v err=%v suffix=%v binding=%v", liveProofFound, liveProofErr, suffixErr, proofErr)
	}
	if fenceErr != nil || newestErr != nil || commandErr != nil {
		t.Fatalf("prepared F prerequisites fence=%v newest=%v command=%v", fenceErr, newestErr, commandErr)
	}
	if _, err := database.RecordVerification(ctx, recoveredArtifact); err != nil {
		t.Fatal(err)
	}
	if proof, found, err := database.OperatorSourceResumeProof(ctx, fencedVerification.Ref, fencedVerification.Version, fencedVerificationFence); err != nil || !found || proof.Version != fencedVerification.Version {
		t.Fatalf("authenticated fresh verification projection invalidated source endpoint: proof=%+v found=%v err=%v", proof, found, err)
	}
	if _, err := database.TransitionVerification(ctx, Transition{Ref: fencedVerification.Ref, ExpectedVersion: fencedVerification.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "phase_pass", Fence: fencedVerificationFence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	building, err := database.Ticket(ctx, fencedVerification.Ref)
	if err != nil || building.State != domain.StateBuilding {
		t.Fatalf("building=%+v err=%v", building, err)
	}
	buildFence := domain.Fence{LeaderEpoch: fRecoveryLeader, RunnerEpoch: building.RunnerEpoch}
	builderArtifact := phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "build source resume candidate", ChangedFiles: []string{"src/feature.go"}, Commands: [][]string{{"go", "test", "./..."}}}
	builderRaw, err := json.Marshal(builderArtifact)
	if err != nil {
		t.Fatal(err)
	}
	builder, err := database.BeginProviderAttempt(ctx, supervisedOperatorSource(t, database, ctx, ProviderAttemptRequest{Ref: building.Ref, ExpectedVersion: building.Version, Fence: buildFence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(pair.Builder), ConfigDigest: building.ConfigDigest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RecordProviderLaunch(ctx, builder, contracts.ProviderLaunch{PID: int(builder.ID), PGID: int(builder.ID), BootIdentity: "source-resume", ProcessStartIdentity: "prepared-builder", Worktree: builder.Worktree}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteProviderAttemptSuccess(ctx, builder, proof(t, builder), building.Version, buildFence, contracts.PhaseResult{Provider: builder.Binding.Identity, Artifact: builderRaw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: building.Type}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	builderKey := ProviderAttemptResultKey{AttemptID: builder.ID, Ref: building.Ref, Phase: domain.PhaseBuild, Attempt: builder.Attempt}
	postbuild := completeEvidenceRepositoryCommand(t, database, ctx, RepositoryCommandPurposePostbuildCandidate, building.Ref, building.Version, buildFence, builderKey, sha256Digest(intent), sha256Digest(proofBytes), f, "", 0)
	postbuildResult, err := database.LoadRepositoryCommandResult(ctx, postbuild)
	if err != nil {
		t.Fatal(err)
	}
	worktree, err := database.Worktree(ctx, building.Ref)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := database.verificationEvidenceForCandidate(ctx, building.Ref)
	if err != nil {
		t.Fatal(err)
	}
	verificationIdentity, err := workflowprompt.NewVerificationIdentity(verificationArtifact, fresh.Revision.IntentDigest, fresh.Revision.ProofDigest, fresh.Revision.CheckpointID)
	if err != nil {
		t.Fatal(err)
	}
	digest := CanonicalRepositoryCommitDigest("candidate", building.Ref, building.Version, buildFence, worktree, builderKey, postbuild, postbuildResult.ResultDigest, struct {
		Plan         workflowprompt.PlanIdentity
		Verification workflowprompt.VerificationIdentity
		Builder      phaseartifact.Builder
	}{planIdentity, verificationIdentity, builderArtifact})
	gitIntent := GitMutationIntent{EffectFence: EffectFence{Ref: building.Ref, TicketVersion: building.Version, Fence: buildFence}, RequestDigest: digest, Repository: project.Path, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit", BaseRef: project.BaseRef, ExpectedBaseOID: worktree.BaseSHA, ExpectedHeadOID: f}
	gitIntent.SemanticKey = CanonicalGitMutationSemanticKey(gitIntent)
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: gitIntent.SemanticKey, Ref: gitIntent.Ref, Kind: "git/commit", TicketVersion: gitIntent.TicketVersion, Fence: gitIntent.Fence, RequestDigest: gitIntent.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := database.IssueGitMutationClaim(ctx, gitIntent)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := database.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	g, tree := strings.Repeat("3", 40), strings.Repeat("4", 40)
	if err := lease.(contracts.GitMutationRecoveryFactsLease).RecordPreparedCommit(ctx, g, tree); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := database.MarkEffectUncertain(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: claim.TicketRef, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}); err != nil {
		t.Fatalf("mark prepared G uncertain: %v", err)
	}
	liveSource, liveSourceFound, liveSourceErr := database.OperatorSourceResumeProof(ctx, building.Ref, building.Version, buildFence)
	if liveSourceErr != nil || !liveSourceFound {
		t.Fatalf("prepared candidate source proof=%+v found=%v err=%v", liveSource, liveSourceFound, liveSourceErr)
	}
	builderResult, builderParsed, builderResultErr := database.loadHistoricalProviderAttemptResult(ctx, database.db, builderKey)
	builderSuffixErr := database.operatorSourceResumeProviderResultReachesFence(ctx, database.db, building.Ref, builderKey, builderResult, building.Version, buildFence)
	preparedEvidence := CandidateEvidence{Ref: building.Ref, Snapshot: domain.CandidateSnapshot{BaseSHA: worktree.BaseSHA, VerificationIntentDigest: fresh.Revision.IntentDigest, ProofDigest: fresh.Revision.ProofDigest, CommandPolicyDigest: strings.TrimPrefix(postbuildResult.Claim.PolicyDigest, "sha256:")}, BuilderResult: builderKey, CommandResult: postbuild}
	_, _, candidateCommandErr := authenticateCandidateCommandEvidence(ctx, database.db, preparedEvidence, builderResult, fresh.Revision.IntentDigest, fresh.Revision.ProofDigest, fresh.Revision.CheckpointID)
	freshResult, freshParsed, freshResultErr := database.loadHistoricalProviderAttemptResult(ctx, database.db, fresh.ProviderResult)
	witnessVerificationIdentity, witnessVerificationErr := workflowprompt.NewVerificationIdentity(*freshParsed.Verify, fresh.Revision.IntentDigest, fresh.Revision.ProofDigest, fresh.Revision.CheckpointID)
	witnessDigest := CanonicalRepositoryCommitDigest("candidate", building.Ref, building.Version, buildFence, liveSource.Worktree, builderKey, postbuild, postbuildResult.ResultDigest, struct {
		Plan         workflowprompt.PlanIdentity
		Verification workflowprompt.VerificationIdentity
		Builder      phaseartifact.Builder
	}{planIdentity, witnessVerificationIdentity, *builderParsed.Builder})
	facts, factsErr := database.GitMutationIntentFacts(ctx, claim.SemanticKey)
	if builderResultErr != nil || builderSuffixErr != nil || candidateCommandErr != nil || freshResultErr != nil || freshResult.Claim.Role != "reviewer" || witnessVerificationErr != nil || factsErr != nil || facts.Claim.RequestDigest != witnessDigest {
		t.Fatalf("prepared candidate prerequisites builder=%v suffix=%v command=%v", builderResultErr, builderSuffixErr, candidateCommandErr)
	}
	if strict, strictFound, strictErr := database.OperatorSourceResumePreparedCandidateWitness(ctx, building.Ref, building.Version, buildFence); strictFound || !errors.Is(strictErr, ErrEvidenceConflict) || strict.Commit.CommitOID != "" {
		t.Fatalf("strict prepared witness exposed unconfirmed G=%+v found=%v err=%v", strict, strictFound, strictErr)
	}
	witness, found, err := database.OperatorSourceResumeRecoverablePreparedCandidateWitness(ctx, building.Ref, building.Version, buildFence)
	if err != nil || !found || witness.Commit.CommitOID != g || witness.Commit.ParentOID != f || witness.Builder != builderKey || witness.Command.Key != postbuild || witness.Verification.Checkpoint.CommitOID != f || !sameJSON(witness.Source.SourceCommit, source) {
		t.Fatalf("prepared witness=%+v found=%v err=%v", witness, found, err)
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(builderArtifact)
	if err != nil {
		t.Fatal(err)
	}
	uncertainEvidence := CandidateEvidence{Ref: building.Ref, ExpectedVersion: building.Version, Fence: buildFence, BuilderResult: builderKey, Commit: witness.Commit, CommandResult: witness.Command.Key, Reason: "uncertain prepared source-resume candidate", Snapshot: domain.CandidateSnapshot{BaseSHA: worktree.BaseSHA, HeadSHA: witness.Commit.CommitOID, TreeSHA: witness.Commit.TreeOID, SourceDigest: building.SourceDigest, VerificationIntentDigest: fresh.Revision.IntentDigest, ProofDigest: fresh.Revision.ProofDigest, CommandPolicyDigest: strings.TrimPrefix(witness.Command.Claim.PolicyDigest, "sha256:"), BuilderEvidenceDigest: builderDigest}}
	if _, err := database.RecordCandidate(ctx, uncertainEvidence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("RecordCandidate accepted same-fence uncertain G: %v", err)
	}
	if _, err := database.ConfirmPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: witness.Commit.CommitOID, ParentOID: witness.Commit.ParentOID, TreeOID: witness.Commit.TreeOID}); err != nil {
		t.Fatalf("confirm prepared G before restart: %v", err)
	}
	forgedEvidence := uncertainEvidence
	forgedEvidence.Commit = CommitObservation{CommitOID: strings.Repeat("a", 40), ParentOID: witness.Commit.ParentOID, TreeOID: strings.Repeat("b", 40)}
	forgedEvidence.Snapshot.HeadSHA = forgedEvidence.Commit.CommitOID
	forgedEvidence.Snapshot.TreeSHA = forgedEvidence.Commit.TreeOID
	if _, err := database.RecordCandidate(ctx, forgedEvidence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("RecordCandidate accepted forged confirmed source-resume child: %v", err)
	}

	// Model the daemon crash after G's ConfirmPreparedCommit but before
	// RecordCandidate. Fencing must carry the Builder/command path to the new
	// endpoint, where the same authenticated G can be persisted without a
	// second provider, command, or Git mutation.
	if err := database.restoreRuntimeControls(ctx); err != nil {
		t.Fatal(err)
	}
	recoveryLeader, err := database.AcquireLeader(ctx, domain.ChannelDev, "source-resume-prepared-g-recovery")
	if err != nil || recoveryLeader <= fRecoveryLeader {
		t.Fatalf("recovery leader=%d prior=%d err=%v", recoveryLeader, fRecoveryLeader, err)
	}
	if changed, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, recoveryLeader); err != nil || changed != 1 {
		t.Fatalf("fence prepared G changed=%d err=%v", changed, err)
	}
	recoveredTicket, err := database.Ticket(ctx, building.Ref)
	if err != nil || recoveredTicket.State != domain.StateBuilding {
		t.Fatalf("recovered ticket=%+v err=%v", recoveredTicket, err)
	}
	recoveredFence := domain.Fence{LeaderEpoch: recoveryLeader, RunnerEpoch: recoveredTicket.RunnerEpoch}
	recovered, found, err := database.OperatorSourceResumePreparedCandidateWitness(ctx, recoveredTicket.Ref, recoveredTicket.Version, recoveredFence)
	initialSource, recoveredSource := witness.Source, recovered.Source
	initialSource.Version, initialSource.Fence = 0, domain.Fence{}
	recoveredSource.Version, recoveredSource.Fence = 0, domain.Fence{}
	if err != nil || !found || recovered.Commit != witness.Commit || recovered.Builder != witness.Builder || recovered.Command.Key != witness.Command.Key || recovered.Source.Version != recoveredTicket.Version || recovered.Source.Fence != recoveredFence || !sameJSON(recoveredSource, initialSource) {
		t.Fatalf("recovered prepared witness=%+v found=%v err=%v", recovered, found, err)
	}
	openExactRuntimeAdmission(t, database, recoveredTicket.Ref)
	currentVerification, err := database.verificationEvidenceForCandidate(ctx, recoveredTicket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	recoveredEvidence := CandidateEvidence{Ref: recoveredTicket.Ref, ExpectedVersion: recoveredTicket.Version, Fence: recoveredFence, BuilderResult: recovered.Builder, Commit: recovered.Commit, CommandResult: recovered.Command.Key, Reason: "recovered prepared source-resume candidate", Snapshot: domain.CandidateSnapshot{BaseSHA: worktree.BaseSHA, HeadSHA: recovered.Commit.CommitOID, TreeSHA: recovered.Commit.TreeOID, SourceDigest: recoveredTicket.SourceDigest, VerificationIntentDigest: currentVerification.Revision.IntentDigest, ProofDigest: currentVerification.Revision.ProofDigest, CommandPolicyDigest: strings.TrimPrefix(recovered.Command.Claim.PolicyDigest, "sha256:"), BuilderEvidenceDigest: builderDigest}}
	if validationErr := validateCandidate(recoveredEvidence.Snapshot); validationErr != nil {
		t.Fatalf("recovered candidate snapshot=%+v err=%v", recoveredEvidence.Snapshot, validationErr)
	}
	if _, err := database.RecordCandidate(ctx, recoveredEvidence); err != nil {
		t.Fatalf("persist recovered candidate: %v", err)
	}
	candidate, err := database.ValidateCurrentCandidateForBuildTransition(ctx, recoveredTicket.Ref, recoveredTicket.Version, recoveredFence)
	if err != nil || candidate.Commit != recovered.Commit || candidate.CommandBinding.Key != recovered.Command.Key || candidate.BuilderResult != recovered.Builder {
		t.Fatalf("persisted recovered candidate=%+v err=%v", candidate, err)
	}
	if _, err := database.TransitionCandidate(ctx, Transition{
		Ref: recoveredTicket.Ref, ExpectedVersion: recoveredTicket.Version,
		From: domain.StateBuilding, To: domain.StatePublishing,
		Trigger: "phase_pass", Fence: recoveredFence, EventPayload: `{}`,
	}, candidate.Snapshot); err != nil {
		t.Fatalf("publish recovered source-resume candidate: %v", err)
	}
	publishing, err := database.Ticket(ctx, recoveredTicket.Ref)
	if err != nil || publishing.State != domain.StatePublishing || publishing.Version != recoveredTicket.Version+1 {
		t.Fatalf("publishing ticket=%+v err=%v", publishing, err)
	}
	if err := database.AuthenticatePublishingRecovery(ctx, publishing.Ref, candidate, publishing.Version, recoveredFence); err != nil {
		t.Fatalf("source-resume candidate publication authority: %v", err)
	}
	if got := openRuntimeAuthorityVersion(t, database, publishing.Ref); got != publishing.Version {
		t.Fatalf("candidate transition authority version=%d want=%d", got, publishing.Version)
	}

	// Only the exact source-resume proof may defer an old pre-publication
	// control row to candidate-only publishing recovery. A different resumed
	// target is not a source handoff and must remain fail-closed.
	rollback := errors.New("rollback tampered source-resume control")
	var sourceRecoveryErr error
	if err := database.write(ctx, func(conn *sql.Conn) error {
		// The V51 phase entry is intentionally FK-bound to this Store-owned
		// source-resume event. Model a privileged durable corruption by removing
		// its dependent immutable witnesses inside the rolled-back transaction,
		// rather than weakening the production foreign key.
		for _, trigger := range []string{"provider_phase_attempt_entries_immutable_delete", "provider_phase_entries_immutable_delete"} {
			if _, err := conn.ExecContext(ctx, `DROP TRIGGER `+trigger); err != nil {
				return err
			}
		}
		if _, err := conn.ExecContext(ctx, `DELETE FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase='verification' AND entry_ticket_version=?`, publishing.Ref.Channel, publishing.Ref.Project, publishing.Ref.Ticket, resumed.Version); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `DELETE FROM provider_phase_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase='verification' AND entry_ticket_version=?`, publishing.Ref.Channel, publishing.Ref.Project, publishing.Ref.Ticket, resumed.Version); err != nil {
			return err
		}
		if _, err := conn.ExecContext(ctx, `UPDATE events SET to_state='building' WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_resume' AND from_state='paused' AND to_state='verifying'`, publishing.Ref.Channel, publishing.Ref.Project, publishing.Ref.Ticket, resumed.Version); err != nil {
			return err
		}
		provider, _, err := database.loadHistoricalProviderAttemptResult(ctx, conn, candidate.BuilderResult)
		if err != nil {
			return err
		}
		sourceRecoveryErr = database.operatorSourceResumeCandidateOnlyPublishingRecovery(ctx, conn, publishing.Ref, candidate, provider, publishing.Version, recoveredFence)
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("rollback tampered source-resume control: %v", err)
	}
	if !errors.Is(sourceRecoveryErr, ErrEvidenceConflict) {
		t.Fatalf("tampered prepublication control recovery=%v", sourceRecoveryErr)
	}

	// Crash after the candidate transition but before publication. The open
	// admission must have moved with the ticket so the next daemon can fence
	// exactly once and rearm the authenticated candidate-only publishing state.
	if err := database.restoreRuntimeControls(ctx); err != nil {
		t.Fatal(err)
	}
	publicationLeader, err := database.AcquireLeader(ctx, domain.ChannelDev, "source-resume-candidate-publication-recovery")
	if err != nil || publicationLeader <= recoveryLeader {
		t.Fatalf("publication recovery leader=%d prior=%d err=%v", publicationLeader, recoveryLeader, err)
	}
	if changed, err := database.FenceRecoveredRunners(ctx, domain.ChannelDev, publicationLeader); err != nil || changed != 1 {
		t.Fatalf("fence recovered publishing candidate changed=%d err=%v", changed, err)
	}
	recoveredPublishing, err := database.Ticket(ctx, publishing.Ref)
	if err != nil || recoveredPublishing.State != domain.StatePublishing || recoveredPublishing.Version != publishing.Version+1 || recoveredPublishing.RunnerEpoch != publishing.RunnerEpoch+1 {
		t.Fatalf("recovered publishing ticket=%+v prior=%+v err=%v", recoveredPublishing, publishing, err)
	}
	stopped, err := database.StoppedRuntimeTicket(ctx, recoveredPublishing.Ref)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := database.PostPublicationRearmProof(ctx, recoveredPublishing.Ref, stopped)
	if err != nil || capability == nil {
		t.Fatalf("rearm recovered candidate-only publication capability=%+v err=%v", capability, err)
	}
	var admission *RuntimeAdmissionCapability
	if err := database.ActivateRearm(ctx, capability, func(value *RuntimeAdmissionCapability) error {
		_, _, _, _ = value.ConsumeRuntimeAdmission()
		admission = value
		return nil
	}); err != nil {
		t.Fatalf("activate recovered publication runtime: %v", err)
	}
	if admission == nil || admission.OpenStoreAdmission(ctx) != nil {
		t.Fatal("open recovered publication runtime admission")
	}

	// Carry the source-resumed candidate through publication and one diagnosed
	// red-CI correction. The repair Builder must use the published candidate as
	// its Git parent while retaining the original source-resume handoff only as
	// authenticated predecessor provenance.
	recoveredPublishingFence := domain.Fence{LeaderEpoch: publicationLeader, RunnerEpoch: recoveredPublishing.RunnerEpoch}
	recoveredCandidate, err := database.RecoverableCandidate(ctx, recoveredPublishing.Ref)
	if err != nil {
		t.Fatalf("load recovered source-resume candidate before publication: %v", err)
	}
	if err := database.authenticateCandidateOnlyPublishingRecoveryAt(ctx, database.db, recoveredPublishing.Ref, recoveredCandidate, recoveredPublishing.Version, recoveredPublishingFence, false); err != nil {
		t.Fatalf("authenticate recovered source-resume candidate before publication: %v", err)
	}
	recordFixturePublication(t, database, ctx, recoveredPublishing, recoveredPublishingFence)
	if _, err := database.TransitionPublishedCandidate(ctx, Transition{
		Ref: recoveredPublishing.Ref, ExpectedVersion: recoveredPublishing.Version,
		From: domain.StatePublishing, To: domain.StateWaitingCI,
		Trigger: "effects_confirmed", Fence: recoveredPublishingFence, EventPayload: `{}`,
	}); err != nil {
		t.Fatalf("source-resume publication to waiting CI: %v", err)
	}
	waiting, err := database.Ticket(ctx, recoveredPublishing.Ref)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := database.LoadPublishedCandidate(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := recordCIAuthorityPolicy(database, ctx, publication, waiting, ciAuthorityLintPolicy()); err != nil {
		t.Fatal(err)
	}
	red := ciAuthorityObservationFor(publication, waiting, recoveredPublishingFence, "red", time.Now().UTC(), []CIObservationCheck{{CanonicalName: "lint", ExternalID: "source-resume-repair-lint", NormalizedState: "failure", FailingDiagnosticText: "source-resume candidate needs one repair"}})
	if err := database.recordCIObservation(ctx, red); err != nil {
		t.Fatal(err)
	}
	red, err = database.LoadCurrentCIObservation(ctx, waiting.Ref)
	if err != nil {
		t.Fatal(err)
	}
	correction := redCICorrectionAuthority(t, waiting, red)
	if _, err := database.ConsumeCIObservation(ctx, CIObservationTransition{
		Ref: waiting.Ref, ObservationDigest: red.ObservationDigest,
		ExpectedVersion: waiting.Version, Fence: red.ObservedFence,
		CorrectionBudget: &correction,
	}); err != nil {
		t.Fatalf("enter source-resume candidate repair: %v", err)
	}
	repairBuilding, err := database.Ticket(ctx, waiting.Ref)
	if err != nil || repairBuilding.State != domain.StateBuilding {
		t.Fatalf("source-resume repair building=%+v err=%v", repairBuilding, err)
	}
	repairFence := domain.Fence{LeaderEpoch: publicationLeader, RunnerEpoch: repairBuilding.RunnerEpoch}
	repairContext, err := database.CandidateRepairBuildContext(ctx, repairBuilding.Ref, repairBuilding.Version, repairFence)
	if err != nil || repairContext.PredecessorGeneration != candidate.Snapshot.Generation || repairContext.PredecessorHeadSHA != candidate.Snapshot.HeadSHA {
		t.Fatalf("source-resume repair context=%+v err=%v", repairContext, err)
	}
	repairVerification, err := database.CurrentVerification(ctx, repairBuilding.Ref)
	if err != nil || repairVerification.TicketVersion != repairBuilding.Version || repairVerification.Fence != repairFence {
		t.Fatalf("source-resume repair verification=%+v err=%v", repairVerification, err)
	}
	repairBuilderArtifact := phaseartifact.Builder{Schema: "sf.builder/v1", Summary: "repair source-resume candidate", ChangedFiles: []string{"src/feature.go"}, Commands: [][]string{{"go", "test", "./..."}}}
	repairBuilderRaw, err := json.Marshal(repairBuilderArtifact)
	if err != nil {
		t.Fatal(err)
	}
	repairBuilder, err := database.BeginProviderAttempt(ctx, supervisedOperatorSource(t, database, ctx, ProviderAttemptRequest{Ref: repairBuilding.Ref, ExpectedVersion: repairBuilding.Version, Fence: repairFence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(pair.Builder), ConfigDigest: repairBuilding.ConfigDigest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RecordProviderLaunch(ctx, repairBuilder, contracts.ProviderLaunch{PID: int(repairBuilder.ID), PGID: int(repairBuilder.ID), BootIdentity: "source-resume", ProcessStartIdentity: "ci-repair-builder", Worktree: repairBuilder.Worktree}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteProviderAttemptSuccess(ctx, repairBuilder, proof(t, repairBuilder), repairBuilding.Version, repairFence, contracts.PhaseResult{Provider: repairBuilder.Binding.Identity, Artifact: repairBuilderRaw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: repairBuilding.Type}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	repairBuilderKey := ProviderAttemptResultKey{AttemptID: repairBuilder.ID, Ref: repairBuilding.Ref, Phase: domain.PhaseBuild, Attempt: repairBuilder.Attempt}
	repairBuilderDigest, err := phaseartifact.BuilderEvidenceDigest(repairBuilderArtifact)
	if err != nil {
		t.Fatal(err)
	}
	repairPolicyDigest := sha256Digest([]byte("source-resume-ci-repair-policy"))
	repairCommand := completeEvidenceRepositoryCommand(t, database, ctx, RepositoryCommandPurposePostbuildCandidate,
		repairBuilding.Ref, repairBuilding.Version, repairFence, repairBuilderKey,
		repairVerification.Revision.IntentDigest, repairVerification.Revision.ProofDigest,
		repairVerification.Checkpoint.CommitOID, "sha256:"+repairPolicyDigest, 0)
	repairSnapshot := domain.CandidateSnapshot{
		BaseSHA: candidate.Snapshot.BaseSHA, HeadSHA: strings.Repeat("5", 40), TreeSHA: strings.Repeat("6", 40),
		SourceDigest: repairBuilding.SourceDigest, VerificationIntentDigest: repairVerification.Revision.IntentDigest,
		ProofDigest: repairVerification.Revision.ProofDigest, CommandPolicyDigest: repairPolicyDigest,
		BuilderEvidenceDigest: repairBuilderDigest,
	}
	repairEvidence := CandidateEvidence{
		Ref: repairBuilding.Ref, ExpectedVersion: repairBuilding.Version, Fence: repairFence,
		Snapshot: repairSnapshot, BuilderResult: repairBuilderKey,
		Commit: CommitObservation{CommitOID: repairSnapshot.HeadSHA, ParentOID: candidate.Snapshot.HeadSHA, TreeOID: repairSnapshot.TreeSHA},
		Reason: "authenticated source-resume CI repair", CommandResult: repairCommand,
	}
	if _, err := database.RecordCandidate(ctx, repairEvidence); err != nil {
		t.Fatalf("record source-resume repair candidate: %v", err)
	}
	repaired, err := database.LatestCandidate(ctx, repairBuilding.Ref)
	if err != nil || repaired.Snapshot.Generation != candidate.Snapshot.Generation+1 || repaired.Commit.ParentOID != candidate.Snapshot.HeadSHA {
		t.Fatalf("load source-resume repair candidate=%+v err=%v", repaired, err)
	}

	// A later repair must not make the original source provenance optional.
	// Corrupt the immutable source-resume payload in this isolated database and
	// prove an otherwise exact lost-response replay fails closed.
	if _, err := database.db.ExecContext(ctx, `UPDATE events SET payload='{}' WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_resume' AND from_state='paused' AND to_state='verifying'`, resumed.Ref.Channel, resumed.Ref.Project, resumed.Ref.Ticket, resumed.Version); err != nil {
		t.Fatal(err)
	}
	if replay, err := database.RecordCandidate(ctx, repairEvidence); !errors.Is(err, ErrEvidenceConflict) || len(replay) != 0 {
		t.Fatalf("repair replay accepted malformed source provenance candidate=%+v err=%v", replay, err)
	}
}

func recordOperatorSourceFreshVerificationAtEndpoint(t *testing.T, database *Store, ctx context.Context, leader uint64, resumed Ticket, source contracts.OperatorSourceCommit) VerificationArtifact {
	t.Helper()
	pair, err := database.ProviderPair(ctx, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: resumed.RunnerEpoch}
	sourceProof, found, err := database.OperatorSourceResumeProof(ctx, resumed.Ref, resumed.Version, fence)
	if err != nil || !found {
		t.Fatalf("source proof=%+v found=%v err=%v", sourceProof, found, err)
	}
	plan, err := database.Plan(ctx, resumed.Ref)
	if err != nil || plan.Document.Planner == nil {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	planIdentity, err := workflowprompt.NewPlanIdentity(*plan.Document.Planner)
	if err != nil {
		t.Fatal(err)
	}
	verification := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: planIdentity.Digest, ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"verify"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: sha256Digest([]byte("fresh source-resume endpoint verification"))}
	raw, err := json.Marshal(verification)
	if err != nil {
		t.Fatal(err)
	}
	reviewer, err := database.BeginProviderAttempt(ctx, supervisedOperatorSource(t, database, ctx, ProviderAttemptRequest{Ref: resumed.Ref, ExpectedVersion: resumed.Version, Fence: fence, Phase: domain.PhaseVerification, Role: "reviewer", Binding: runtime(pair.Reviewer), ConfigDigest: resumed.ConfigDigest, Capacity: 1, At: time.Now().UTC()}))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.RecordProviderLaunch(ctx, reviewer, contracts.ProviderLaunch{PID: int(reviewer.ID), PGID: int(reviewer.ID), BootIdentity: "source-resume", ProcessStartIdentity: "fresh-endpoint-review", Worktree: reviewer.Worktree}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.CompleteProviderAttemptSuccess(ctx, reviewer, proof(t, reviewer), resumed.Version, fence, contracts.PhaseResult{Provider: reviewer.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: resumed.Type, AcceptanceDigest: planIdentity.Digest}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	reviewerKey := ProviderAttemptResultKey{AttemptID: reviewer.ID, Ref: resumed.Ref, Phase: domain.PhaseVerification, Attempt: reviewer.Attempt}
	intent, err := workflowprompt.CanonicalVerificationIntentBytes(verification)
	if err != nil {
		t.Fatal(err)
	}
	proofBytes, err := workflowprompt.CanonicalVerificationProofBytes(verification)
	if err != nil {
		t.Fatal(err)
	}
	prebuild := completeEvidenceRepositoryCommand(t, database, ctx, RepositoryCommandPurposePrebuildVerification, resumed.Ref, resumed.Version, fence, reviewerKey, sha256Digest(intent), sha256Digest(proofBytes), "", "", 1)
	prebuildResult, err := database.LoadRepositoryCommandResult(ctx, prebuild)
	if err != nil {
		t.Fatal(err)
	}
	project, err := database.Project(ctx, resumed.Ref.Channel, resumed.Ref.Project)
	if err != nil {
		t.Fatal(err)
	}
	commit := strings.Repeat("7", 40)
	tree := strings.Repeat("8", 40)
	requestDigest, err := CanonicalOperatorSourceResumeCheckpointDigest(OperatorSourceResumeCheckpointDigestInput{Ref: resumed.Ref, WorktreePath: sourceProof.Worktree.Path, Branch: sourceProof.Worktree.Branch, Identity: sourceProof.Worktree.IdentityJSON, BaseSHA: sourceProof.Worktree.BaseSHA, Source: sourceProof.SourceCommit, Retained: sourceProof.Verification.Revision, Provider: reviewerKey, Command: prebuild, ResultDigest: prebuildResult.ResultDigest, Artifact: verification})
	if err != nil {
		t.Fatal(err)
	}
	intentClaim := GitMutationIntent{EffectFence: EffectFence{Ref: resumed.Ref, TicketVersion: resumed.Version, Fence: fence}, RequestDigest: requestDigest, Repository: project.Path, Worktree: sourceProof.Worktree.Path, Branch: sourceProof.Worktree.Branch, Operation: "commit", BaseRef: project.BaseRef, ExpectedBaseOID: sourceProof.Worktree.BaseSHA, ExpectedHeadOID: source.CommitOID}
	intentClaim.SemanticKey = CanonicalGitMutationSemanticKey(intentClaim)
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: intentClaim.SemanticKey, Ref: intentClaim.Ref, Kind: "git/commit", TicketVersion: intentClaim.TicketVersion, Fence: intentClaim.Fence, RequestDigest: intentClaim.RequestDigest}); err != nil {
		t.Fatal(err)
	}
	claim, err := database.IssueGitMutationClaim(ctx, intentClaim)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := database.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.(contracts.GitMutationRecoveryFactsLease).RecordPreparedCommit(ctx, commit, tree); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ConfirmPreparedCommit(ctx, claim, contracts.PreparedCommitObservation{CommitOID: commit, ParentOID: source.CommitOID, TreeOID: tree}); err != nil {
		t.Fatal(err)
	}
	artifact := VerificationArtifact{Ref: resumed.Ref, ExpectedVersion: resumed.Version, Fence: fence, Intent: intent, Proof: proofBytes, OwnedFiles: verification.OwnedFiles, CheckpointID: commit, ProviderResult: &reviewerKey, Checkpoint: CommitObservation{CommitOID: commit, ParentOID: source.CommitOID, TreeOID: tree}, CommandResult: prebuild}
	if _, err := database.RecordVerification(ctx, artifact); err != nil {
		t.Fatal(err)
	}
	return artifact
}

func operatorSourceResumeBuildingFixture(t *testing.T) (*Store, context.Context, uint64, Ticket) {
	t.Helper()
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "operator-source-resume")
	if err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "provider", Ticket: "SF-operator-source-resume"}
	if err := db.CreateTicket(ctx, Ticket{Ref: ref, SourceDigest: sha256Digest([]byte("digest-SF-operator-source-resume")), Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	branch := testAllocatedBranch(ref, strings.Repeat("ab", 16))
	ticket, err := db.StartOrAdopt(ctx, ref, 1, branch, domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	worktree := "/tmp/provider/SF-operator-source-resume"
	identity := strings.ReplaceAll(strings.ReplaceAll(repositoryCommandIdentity(t, "/tmp/provider", worktree, branch, "main"), "git@example.test:nysa.git", "https://github.com/acme/app.git"), "/tmp/nysa-origin", "git@github.com:acme/app.git")
	branchKey := string(ref.Channel) + "\x00" + string(ref.Project) + "\x00" + string(ref.Ticket)
	if _, err := db.LoadOrStoreBranchUnderFence(ctx, branchKey, branch, ticket.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}); err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterWorktree(ctx, WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Path: worktree, Branch: branch, IdentityJSON: []byte(identity), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	builder, reviewer := setupProviderPair(t, db, ctx)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	launch := func(phase domain.Phase, role string, binding contracts.RuntimeBinding, artifact []byte, validation phaseartifact.Validation) ProviderAttemptClaim {
		claim, err := db.BeginProviderAttempt(ctx, supervisedOperatorSource(t, db, ctx, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: phase, Role: role, Binding: binding, ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
		if err != nil {
			t.Fatal(err)
		}
		if err := db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "source-resume", ProcessStartIdentity: fmt.Sprintf("source-resume-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: artifact, UsageTrusted: true, UsageUnits: 1}, validation, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		return claim
	}
	planner := phaseartifact.Planner{Schema: "sf.planner/v1", Acceptance: []string{"a"}, Proof: phaseartifact.ProofPlan{Kind: phaseartifact.ProofAcceptance, Command: []string{"go", "test"}, Details: "d"}, Paths: []string{"src"}, Commands: [][]string{{"go", "test"}}, Risks: []string{"r"}}
	plannerRaw, _ := json.Marshal(planner)
	plannerClaim := launch(domain.PhasePlanning, "planner", runtime(builder), plannerRaw, phaseartifact.Validation{TicketType: ticket.Type})
	planKey := ProviderAttemptResultKey{AttemptID: plannerClaim.ID, Ref: ticket.Ref, Phase: domain.PhasePlanning, Attempt: plannerClaim.Attempt}
	if _, err := db.RecordPlan(ctx, PlanArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Document: PlanDocument{Planner: &planner, ProviderResult: &planKey, Acceptance: planner.Acceptance, ProofKind: string(planner.Proof.Kind), Paths: planner.Paths, Commands: planner.Commands, Risks: planner.Risks}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionPlan(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "phase_pass", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	ticket, _ = db.Ticket(ctx, ticket.Ref)
	fence.RunnerEpoch = ticket.RunnerEpoch
	planIdentity, _ := workflowprompt.NewPlanIdentity(planner)
	verification := phaseartifact.Verification{Schema: "sf.verification/v1", AcceptanceDigest: planIdentity.Digest, ProofKind: phaseartifact.ProofAcceptance, OwnedFiles: []string{"verify"}, Command: []string{"go", "test", "./..."}, PrebuildOutcome: "red", EvidenceDigest: sha256Digest([]byte("source-resume-verification"))}
	verificationRaw, _ := json.Marshal(verification)
	reviewerClaim := launch(domain.PhaseVerification, "reviewer", runtime(reviewer), verificationRaw, phaseartifact.Validation{TicketType: ticket.Type, AcceptanceDigest: planIdentity.Digest})
	intent, _ := workflowprompt.CanonicalVerificationIntentBytes(verification)
	proofBytes, _ := workflowprompt.CanonicalVerificationProofBytes(verification)
	checkpoint := strings.Repeat("c", 40)
	verificationKey := ProviderAttemptResultKey{AttemptID: reviewerClaim.ID, Ref: ticket.Ref, Phase: domain.PhaseVerification, Attempt: reviewerClaim.Attempt}
	command := completeEvidenceRepositoryCommand(t, db, ctx, RepositoryCommandPurposePrebuildVerification, ticket.Ref, ticket.Version, fence, verificationKey, sha256Digest(intent), sha256Digest(proofBytes), "", "", 1)
	if _, err := db.RecordVerification(ctx, VerificationArtifact{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Intent: intent, Proof: proofBytes, OwnedFiles: verification.OwnedFiles, CheckpointID: checkpoint, ProviderResult: &verificationKey, Checkpoint: CommitObservation{CommitOID: checkpoint, ParentOID: strings.Repeat("a", 40), TreeOID: strings.Repeat("d", 40)}, CommandResult: command}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TransitionVerification(ctx, Transition{Ref: ticket.Ref, ExpectedVersion: ticket.Version, From: domain.StateVerifying, To: domain.StateBuilding, Trigger: "phase_pass", Fence: fence, EventPayload: `{}`}); err != nil {
		t.Fatal(err)
	}
	ticket, err = db.Ticket(ctx, ticket.Ref)
	if err != nil || ticket.State != domain.StateBuilding {
		t.Fatalf("building=%+v err=%v", ticket, err)
	}
	return db, ctx, leader, ticket
}
