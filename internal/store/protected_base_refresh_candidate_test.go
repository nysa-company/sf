package store

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

func TestProtectedBaseRefreshRequiresFreshBuilderAndCandidateAnchor(t *testing.T) {
	for _, publication := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-publication", true: "with-publication"}[publication], func(t *testing.T) {
			protectedBaseRefreshFreshBuilderCandidate(t, publication)
		})
	}
}

func protectedBaseRefreshFreshBuilderCandidate(t *testing.T, publication bool) {
	f := newProtectedBaseRefreshCompletionFixtureWithPublication(t, true, false, publication)
	defer f.db.Close()
	ctx := f.ctx
	if _, err := f.db.CompleteProtectedBaseRefresh(ctx, f.claim, f.identity); err != nil {
		t.Fatal(err)
	}
	building, err := f.db.Ticket(ctx, f.ref)
	if err != nil || building.State != domain.StateBuilding || building.Version != f.claim.TicketVersion+1 {
		t.Fatalf("building ticket=%+v err=%v", building, err)
	}
	worktree, err := f.db.Worktree(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	verification, err := f.db.HistoricalVerification(ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	qualification, _ := setupProviderPair(t, f.db, ctx)
	fence := domain.Fence{LeaderEpoch: f.claim.LeaderEpoch, RunnerEpoch: building.RunnerEpoch}
	if _, err := f.db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: f.ref, ExpectedVersion: building.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("fresh Builder entry must retire predecessor result: %v", err)
	}
	request := supervised(t, ProviderAttemptRequest{Ref: f.ref, ExpectedVersion: building.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder", Binding: runtime(qualification), ConfigDigest: building.ConfigDigest, Capacity: 1, At: time.Now().UTC()})
	request.WorktreeIdentity = string(worktree.IdentityJSON)
	request.Input.WorktreeIdentity = string(worktree.IdentityJSON)
	request.BaseSHA = worktree.BaseSHA
	request.Input.BaseSHA = worktree.BaseSHA
	claim, err := f.db.BeginProviderAttempt(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.db.RecordProviderLaunch(ctx, claim, contracts.ProviderLaunch{PID: int(claim.ID), PGID: int(claim.ID), BootIdentity: "refresh-candidate", ProcessStartIdentity: fmt.Sprintf("refresh-candidate-%d", claim.ID), Worktree: claim.Worktree}); err != nil {
		t.Fatal(err)
	}
	raw := []byte(`{"schema":"sf.builder/v1","summary":"refresh candidate","changed_files":["internal/refresh.go"],"commands":[["go","test"]]}`)
	if _, err := f.db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), building.Version, fence, contracts.PhaseResult{Provider: claim.Binding.Identity, Artifact: raw, UsageTrusted: true, UsageUnits: 1}, phaseartifact.Validation{TicketType: building.Type}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	builderKey := ProviderAttemptResultKey{AttemptID: claim.ID, Ref: f.ref, Phase: domain.PhaseBuild, Attempt: claim.Attempt}
	if reusable, err := f.db.LatestReusableProviderAttempt(ctx, LatestReusableProviderAttemptRequest{Ref: f.ref, ExpectedVersion: building.Version, Fence: fence, Phase: domain.PhaseBuild, Role: "builder"}); err != nil || reusable.Key != builderKey {
		t.Fatalf("fresh Builder result must remain reusable: key=%+v err=%v", reusable.Key, err)
	}
	_, parsed, err := f.db.LoadHistoricalProviderAttemptResult(ctx, builderKey)
	if err != nil || parsed.Builder == nil {
		t.Fatalf("builder=%+v err=%v", parsed, err)
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	if err != nil {
		t.Fatal(err)
	}
	policyDigest := sha256Digest([]byte("refresh-candidate-policy"))
	command := completeEvidenceRepositoryCommand(t, f.db, ctx, RepositoryCommandPurposePostbuildCandidate, f.ref, building.Version, fence, builderKey, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Revision.CheckpointID, "sha256:"+policyDigest, 0)
	successor := domain.CandidateSnapshot{Generation: f.oldCandidate.Snapshot.Generation + 1, BaseSHA: worktree.BaseSHA, HeadSHA: strings.Repeat("6", 40), TreeSHA: strings.Repeat("5", 40), SourceDigest: f.oldCandidate.Snapshot.SourceDigest, VerificationIntentDigest: f.oldCandidate.Snapshot.VerificationIntentDigest, ProofDigest: f.oldCandidate.Snapshot.ProofDigest, CommandPolicyDigest: policyDigest, BuilderEvidenceDigest: builderDigest}
	evidence := CandidateEvidence{Ref: f.ref, ExpectedVersion: building.Version, Fence: fence, Snapshot: successor, BuilderResult: builderKey, Commit: CommitObservation{CommitOID: successor.HeadSHA, ParentOID: f.commit, TreeOID: successor.TreeSHA}, Reason: "protected base refresh", CommandResult: command}
	oldEvidence := evidence
	oldEvidence.BuilderResult = f.oldCandidate.BuilderResult
	if _, err := f.db.RecordCandidate(ctx, oldEvidence); err == nil {
		t.Fatal("old Builder result reused after refresh")
	}
	if _, err := f.db.RecordCandidate(ctx, evidence); err != nil {
		t.Fatal(err)
	}
	stored, err := f.db.RecoverableCandidate(ctx, f.ref)
	if err != nil || stored.Snapshot != successor || stored.Commit.ParentOID != f.commit {
		t.Fatalf("successor=%+v err=%v", stored, err)
	}
	if _, err := f.db.TransitionCandidate(ctx, Transition{Ref: f.ref, ExpectedVersion: building.Version, From: domain.StateBuilding, To: domain.StatePublishing, Trigger: "phase_pass", Fence: fence, EventPayload: "{}"}, successor); err != nil {
		t.Fatal(err)
	}
	if after, err := f.db.Ticket(ctx, f.ref); err != nil || after.State != domain.StatePublishing || after.Version != building.Version+1 {
		t.Fatalf("candidate transition ticket=%+v err=%v", after, err)
	}
}
