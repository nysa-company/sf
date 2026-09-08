package store

import (
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestProtectedBaseRefreshReadersRebindAcrossRepeatedLeaderRecovery(t *testing.T) {
	f := newProtectedBaseRefreshCompletionFixture(t, true, false)
	defer f.db.Close()
	if _, err := f.db.CompleteProtectedBaseRefresh(f.ctx, f.claim, f.identity); err != nil {
		t.Fatal(err)
	}
	oldTicket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	oldFence := domain.Fence{LeaderEpoch: f.claim.LeaderEpoch, RunnerEpoch: oldTicket.RunnerEpoch}
	leader, err := f.db.AcquireLeader(f.ctx, f.ref.Channel, "refresh-reader-restart-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.CurrentVerification(f.ctx, f.ref); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("current verification admitted pre-fence restart: %v", err)
	}
	if _, err := f.db.ProtectedBaseRefreshBuildContext(f.ctx, f.ref, oldTicket.Version, oldFence); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("refresh context admitted pre-fence restart: %v", err)
	}
	changed, err := f.db.FenceRecoveredRunners(f.ctx, f.ref.Channel, leader)
	if err != nil || changed != 1 {
		t.Fatalf("first recovery changed=%d err=%v", changed, err)
	}
	current, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	liveFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	if _, err := f.db.ProtectedBaseRefreshBuildContext(f.ctx, f.ref, current.Version, liveFence); err != nil {
		t.Fatalf("first recovered refresh context: %v", err)
	}
	if _, err := f.db.CurrentVerification(f.ctx, f.ref); err != nil {
		t.Fatalf("first recovered current verification: %v", err)
	}
	secondLeader, err := f.db.AcquireLeader(f.ctx, f.ref.Channel, "refresh-reader-restart-3")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.CurrentVerification(f.ctx, f.ref); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("current verification admitted second pre-fence restart: %v", err)
	}
	changed, err = f.db.FenceRecoveredRunners(f.ctx, f.ref.Channel, secondLeader)
	if err != nil || changed != 1 {
		t.Fatalf("second recovery changed=%d err=%v", changed, err)
	}
	current, err = f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ProtectedBaseRefreshBuildContext(f.ctx, f.ref, current.Version, domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: current.RunnerEpoch}); err != nil {
		t.Fatalf("second recovered refresh context: %v", err)
	}
	if _, err := f.db.CurrentVerification(f.ctx, f.ref); err != nil {
		t.Fatalf("second recovered current verification: %v", err)
	}
}

func TestPendingProtectedBaseRefreshReclaimsPreparedTupleAfterRestart(t *testing.T) {
	f := newProtectedBaseRefreshCompletionFixture(t, true, false)
	defer f.db.Close()
	leader, err := f.db.AcquireLeader(f.ctx, f.ref.Channel, "refresh-pending-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.db.FenceRecoveredRunners(f.ctx, f.ref.Channel, leader); err != nil || changed != 1 {
		t.Fatalf("recovery changed=%d err=%v", changed, err)
	}
	current, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	currentFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	pending, found, err := f.db.PendingProtectedBaseRefresh(f.ctx, f.ref, current.Version, currentFence)
	if err != nil || !found || pending.Candidate.Snapshot != f.oldCandidate.Snapshot || pending.Worktree.BaseSHA != f.oldWorktree.BaseSHA || pending.NewBaseSHA != f.claim.ExpectedBaseOID || pending.Mutation.TicketVersion != current.Version || pending.Mutation.Fence != currentFence {
		t.Fatalf("pending=%+v found=%v err=%v", pending, found, err)
	}
	reclaimed, err := f.db.ReclaimProtectedBaseRefresh(f.ctx, f.ref, current.Version, currentFence)
	if err != nil || reclaimed.TicketVersion != current.Version || reclaimed.LeaderEpoch != leader || reclaimed.RunnerEpoch != current.RunnerEpoch || reclaimed.ClaimEpoch == f.claim.ClaimEpoch {
		t.Fatalf("reclaimed=%+v err=%v", reclaimed, err)
	}
	lease, err := f.db.AcquireGitMutation(f.ctx, reclaimed)
	if err != nil {
		t.Fatal(err)
	}
	prepared, ok := lease.(contracts.GitBaseRefreshPreparedReader)
	if !ok {
		t.Fatal("reclaimed lease missing prepared reader")
	}
	value, found, err := prepared.PreparedBaseRefresh(f.ctx)
	if err != nil || !found || value.CommitOID != f.commit || value.Parents != [2]string{f.claim.ExpectedHeadOID, f.claim.ExpectedBaseOID} {
		t.Fatalf("reclaimed prepared=%+v found=%v err=%v", value, found, err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ConfirmEffect(f.ctx, EffectFence{SemanticKey: reclaimed.SemanticKey, Ref: reclaimed.TicketRef, TicketVersion: reclaimed.TicketVersion, Fence: domain.Fence{LeaderEpoch: reclaimed.LeaderEpoch, RunnerEpoch: reclaimed.RunnerEpoch, ClaimEpoch: reclaimed.ClaimEpoch}}, f.commit); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.CompleteProtectedBaseRefresh(f.ctx, f.claim, f.identity); !errors.Is(err, ErrEvidenceConflict) {
		t.Fatalf("original claim completed after reclaim: %v", err)
	}
	if _, err := f.db.CompleteProtectedBaseRefresh(f.ctx, reclaimed, f.identity); err != nil {
		t.Fatal(err)
	}
	if after, err := f.db.Ticket(f.ctx, f.ref); err != nil || after.Version != current.Version+1 || after.State != domain.StateBuilding {
		t.Fatalf("completed recovered refresh ticket=%+v err=%v", after, err)
	}
	secondLeader, err := f.db.AcquireLeader(f.ctx, f.ref.Channel, "refresh-pending-post-completion-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.db.FenceRecoveredRunners(f.ctx, f.ref.Channel, secondLeader); err != nil || changed != 1 {
		t.Fatalf("post-completion recovery changed=%d err=%v", changed, err)
	}
	latest, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	latestFence := domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: latest.RunnerEpoch}
	if _, err := f.db.ProtectedBaseRefreshBuildContext(f.ctx, f.ref, latest.Version, latestFence); err != nil {
		t.Fatalf("split-ledger refresh context: %v", err)
	}
	if _, err := f.db.CurrentVerification(f.ctx, f.ref); err != nil {
		t.Fatalf("split-ledger current verification: %v", err)
	}
}
