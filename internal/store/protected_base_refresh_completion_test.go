package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
)

type protectedBaseRefreshCompletionFixture struct {
	db           *Store
	ctx          context.Context
	ref          domain.TicketRef
	claim        contracts.GitMutationClaim
	identity     []byte
	commit       string
	oldCandidate StoredCandidate
	oldWorktree  StoredWorktree
}

func newProtectedBaseRefreshCompletionFixture(t *testing.T, prepare, keepLease bool) protectedBaseRefreshCompletionFixture {
	return newProtectedBaseRefreshCompletionFixtureWithPublication(t, prepare, keepLease, false)
}

func newProtectedBaseRefreshCompletionFixtureWithPublication(t *testing.T, prepare, keepLease, publication bool) protectedBaseRefreshCompletionFixture {
	t.Helper()
	db, ctx, current, fence := publicationLifecycleFixture(t)
	if publication {
		recordFixturePublication(t, db, ctx, current, fence)
	}
	base := strings.Repeat("9", 40)
	proof, err := db.ProtectedBaseRefreshProofIntent(ctx, current.Ref, current.Version, fence, base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{SemanticKey: proof.Intent.SemanticKey, Ref: current.Ref, Kind: "git/protected-ref-fetch", TicketVersion: current.Version, Fence: fence, RequestDigest: proof.ContextDigest}); err != nil {
		t.Fatal(err)
	}
	proofClaim, err := db.IssueGitMutationClaim(ctx, proof.Intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: proofClaim.SemanticKey, Ref: current.Ref, TicketVersion: proofClaim.TicketVersion, Fence: domain.Fence{LeaderEpoch: proofClaim.LeaderEpoch, RunnerEpoch: proofClaim.RunnerEpoch, ClaimEpoch: proofClaim.ClaimEpoch}}, proof.ObservedIdentity); err != nil {
		t.Fatal(err)
	}
	reservation, err := db.ReserveProtectedBaseRefresh(ctx, current.Ref, current.Version, fence, base)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := db.IssueGitMutationClaim(ctx, reservation.Mutation)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := db.AcquireGitMutation(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	commit, tree := strings.Repeat("8", 40), strings.Repeat("7", 40)
	if prepare {
		recorder, ok := lease.(contracts.GitBaseRefreshPreparationLease)
		if !ok {
			t.Fatal("missing preparation lease")
		}
		if err := recorder.RecordBaseRefreshPreparation(ctx, commit, tree, [2]string{claim.ExpectedHeadOID, claim.ExpectedBaseOID}); err != nil {
			t.Fatal(err)
		}
	}
	if !keepLease {
		if err := lease.Release(); err != nil {
			t.Fatal(err)
		}
	}
	if prepare {
		if _, err := db.ConfirmEffect(ctx, EffectFence{SemanticKey: claim.SemanticKey, Ref: current.Ref, TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, commit); err != nil {
			t.Fatal(err)
		}
	}
	worktree, err := db.Worktree(ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	var identity gitboundary.Identity
	if err := json.Unmarshal(worktree.IdentityJSON, &identity); err != nil {
		t.Fatal(err)
	}
	identity.BaseHead = base
	identityJSON, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := db.RecoverableCandidate(ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	return protectedBaseRefreshCompletionFixture{db: db, ctx: ctx, ref: current.Ref, claim: claim, identity: identityJSON, commit: commit, oldCandidate: candidate, oldWorktree: worktree}
}

func TestCompleteProtectedBaseRefreshPublishesEffectiveWorktreeAndReplays(t *testing.T) {
	f := newProtectedBaseRefreshCompletionFixture(t, true, false)
	defer f.db.Close()
	oldVerification, err := f.db.HistoricalVerification(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := f.db.CompleteProtectedBaseRefresh(f.ctx, f.claim, f.identity)
	if err != nil {
		t.Fatal(err)
	}
	if completion.Worktree.HeadSHA != f.commit || completion.Worktree.BaseSHA != f.claim.ExpectedBaseOID || completion.Version != f.claim.TicketVersion+1 || completion.Preparation.Parents != [2]string{f.claim.ExpectedHeadOID, f.claim.ExpectedBaseOID} {
		t.Fatalf("completion=%+v", completion)
	}
	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil || ticket.State != domain.StateBuilding || ticket.Version != completion.Version {
		t.Fatalf("ticket=%+v err=%v", ticket, err)
	}
	worktree, err := f.db.Worktree(f.ctx, f.ref)
	if err != nil || worktree.BaseSHA != completion.Worktree.BaseSHA || worktree.HeadSHA != f.commit || !reflect.DeepEqual(worktree.IdentityJSON, f.identity) {
		t.Fatalf("worktree=%+v err=%v", worktree, err)
	}
	var candidateHead, candidateBase string
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT head_sha,base_sha FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, f.ref.Channel, f.ref.Project, f.ref.Ticket, f.oldCandidate.Snapshot.Generation).Scan(&candidateHead, &candidateBase); err != nil || candidateHead != f.oldCandidate.Snapshot.HeadSHA || candidateBase != f.oldCandidate.Snapshot.BaseSHA {
		t.Fatalf("immutable candidate changed: %s %s %v", candidateHead, candidateBase, err)
	}
	currentVerification, err := f.db.CurrentVerification(f.ctx, f.ref)
	if err != nil || currentVerification.Revision.Revision != oldVerification.Revision.Revision || !reflect.DeepEqual(currentVerification.Intent, oldVerification.Intent) || !reflect.DeepEqual(currentVerification.Proof, oldVerification.Proof) {
		t.Fatalf("current verification=%+v err=%v", currentVerification, err)
	}
	historicalWorktree, err := f.db.HistoricalProviderWorktree(f.ctx, oldVerification.ProviderResult)
	if err != nil || !reflect.DeepEqual(historicalWorktree, f.oldWorktree) {
		t.Fatalf("historical worktree=%+v err=%v", historicalWorktree, err)
	}
	historicalBuilder, _, err := f.db.LoadHistoricalProviderAttemptResult(f.ctx, f.oldCandidate.BuilderResult)
	if err != nil || historicalBuilder.Claim.WorktreeIdentity != string(f.oldWorktree.IdentityJSON) {
		t.Fatalf("historical builder=%+v err=%v", historicalBuilder, err)
	}
	currentTicket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	refreshContext, err := f.db.ProtectedBaseRefreshBuildContext(f.ctx, f.ref, currentTicket.Version, domain.Fence{LeaderEpoch: completion.Fence.LeaderEpoch, RunnerEpoch: currentTicket.RunnerEpoch})
	if err != nil || !reflect.DeepEqual(refreshContext.Predecessor, f.oldCandidate) || refreshContext.Verification.Revision.Revision != oldVerification.Revision.Revision {
		t.Fatalf("refresh build context=%+v err=%v", refreshContext, err)
	}
	replay, err := f.db.CompleteProtectedBaseRefresh(f.ctx, f.claim, f.identity)
	if err != nil || !reflect.DeepEqual(replay, completion) {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if f.db.mutations.revokedBy(f.ref, completion.Version, completion.Fence) {
		t.Fatal("replay revoked fresh Builder endpoint")
	}
	var count int
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM protected_base_refresh_completions WHERE refresh_id=?`, completion.RefreshID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("completion rows=%d err=%v", count, err)
	}
	if err := f.db.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND trigger='base_or_candidate_head_changed'`, f.ref.Channel, f.ref.Project, f.ref.Ticket).Scan(&count); err != nil || count != 1 {
		t.Fatalf("duplicate transition: %d %v", count, err)
	}
}

func TestCompleteProtectedBaseRefreshRejectsUnreadyOrMismatchedEvidence(t *testing.T) {
	tests := []struct {
		name                                                                           string
		prepare, keepLease, badIdentity, wrongBase, unrelated, staleLeader, staleClaim bool
		want                                                                           error
	}{
		{name: "missing preparation", want: ErrEvidenceConflict},
		{name: "active lease", prepare: true, keepLease: true, want: ErrGitMutationLease},
		{name: "altered inode", prepare: true, badIdentity: true, want: ErrEvidenceConflict},
		{name: "wrong base head", prepare: true, wrongBase: true, want: ErrEvidenceConflict},
		{name: "stale leader", prepare: true, staleLeader: true, want: ErrEvidenceConflict},
		{name: "stale claim", prepare: true, staleClaim: true, want: ErrEvidenceConflict},
		{name: "unrelated planned effect", prepare: true, unrelated: true, want: ErrControlNotDrained},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newProtectedBaseRefreshCompletionFixture(t, tc.prepare, tc.keepLease)
			defer f.db.Close()
			identity := f.identity
			if tc.badIdentity || tc.wrongBase {
				var value gitboundary.Identity
				_ = json.Unmarshal(identity, &value)
				if tc.badIdentity {
					value.WorktreeIno++
				} else {
					value.BaseHead = strings.Repeat("6", 40)
				}
				identity, _ = json.Marshal(value)
			}
			claim := f.claim
			if tc.staleLeader {
				claim.LeaderEpoch++
			}
			if tc.staleClaim {
				claim.ClaimEpoch++
			}
			if tc.unrelated {
				if _, err := f.db.PlanEffect(f.ctx, EffectPlan{SemanticKey: "completion-unrelated", Ref: f.ref, Kind: "draft", TicketVersion: claim.TicketVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch}, RequestDigest: "sha256:" + strings.Repeat("4", 64)}); err != nil {
					t.Fatal(err)
				}
			}
			before, err := f.db.Ticket(f.ctx, f.ref)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.db.CompleteProtectedBaseRefresh(f.ctx, claim, identity)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err=%v want=%v", err, tc.want)
			}
			after, err := f.db.Ticket(f.ctx, f.ref)
			if err != nil || after.Version != before.Version || after.State != before.State {
				t.Fatalf("ticket mutated before=%+v after=%+v err=%v", before, after, err)
			}
			worktree, err := f.db.Worktree(f.ctx, f.ref)
			if err != nil || !reflect.DeepEqual(worktree, f.oldWorktree) {
				t.Fatalf("worktree mutated=%+v err=%v", worktree, err)
			}
			var count int
			if err := f.db.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM protected_base_refresh_completions`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("completion rows=%d err=%v", count, err)
			}
		})
	}
}

func TestCompletedRefreshBridgesHistoricalPublicationToOriginalWorktree(t *testing.T) {
	f := newProtectedBaseRefreshCompletionFixtureWithPublication(t, true, false, true)
	defer f.db.Close()
	before, err := f.db.LoadPublishedCandidate(f.ctx, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.CompleteProtectedBaseRefresh(f.ctx, f.claim, f.identity); err != nil {
		t.Fatal(err)
	}
	historical, err := f.db.LoadHistoricalPublishedCandidate(f.ctx, f.ref)
	if err != nil || !reflect.DeepEqual(historical, before) || historical.Worktree.BaseSHA != f.oldWorktree.BaseSHA || historical.Candidate.Snapshot.BaseSHA != f.oldCandidate.Snapshot.BaseSHA {
		t.Fatalf("historical publication=%+v before=%+v err=%v", historical, before, err)
	}
}
