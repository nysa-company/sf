package daemon

import (
	"context"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// Only the classifier is a seam; claim/effect reconciliation and confirmation
// below use a real Store. Store's separate test builds a real amendment receipt
// and checks this classifier after the same ReconcileEffects fence change.
type checkpointDeferralStore struct {
	*store.Store
	claim           contracts.GitMutationClaim
	deferCheckpoint bool
	err             error
	calls           int
}

func (s *checkpointDeferralStore) DeferPostbuildAmendmentCheckpointRecovery(_ context.Context, claim contracts.GitMutationClaim) (bool, error) {
	s.calls++
	if claim != s.claim {
		return false, store.ErrEvidenceConflict
	}
	return s.deferCheckpoint, s.err
}

func TestDaemonPreparedRecoveryDefersProtectedCheckpointBeforeGenericObservation(t *testing.T) {
	for _, boundary := range []string{"before prepared", "before CAS", "after CAS unsynced", "malformed companion", "ordinary"} {
		t.Run(boundary, func(t *testing.T) {
			daemon, _, cancel := testDaemon(t)
			defer cancel()
			defer daemon.Close()
			ctx := context.Background()
			intent, claim, child, tree := seedPreparedCommit(t, daemon, domain.TicketID("SF-deferred-checkpoint"), boundary != "before prepared")
			leader, err := daemon.store.AcquireLeader(ctx, daemon.channel, "deferral-recovery-leader")
			if err != nil {
				t.Fatal(err)
			}
			effects, err := daemon.store.ReconcileEffects(ctx, daemon.channel, leader)
			if err != nil {
				t.Fatal(err)
			}
			facts, err := daemon.store.GitMutationIntentFacts(ctx, claim.SemanticKey)
			if err != nil || facts.Claim != claim || facts.Effect.LeaderEpoch != leader || facts.Effect.ClaimEpoch <= claim.ClaimEpoch {
				t.Fatalf("recovery facts err=%v", err)
			}
			source := &checkpointDeferralStore{Store: daemon.store, claim: claim, deferCheckpoint: boundary != "ordinary"}
			if boundary == "malformed companion" {
				source.err = store.ErrEvidenceConflict
			}
			observations := 0
			observer := preparedCommitObserverFunc(func(context.Context, contracts.GitMutationClaim) (contracts.PreparedCommitObservation, error) {
				observations++
				return contracts.PreparedCommitObservation{CommitOID: child, ParentOID: intent.ExpectedHeadOID, TreeOID: tree}, nil
			})
			err = reconcilePreparedCommits(ctx, effects, source, observer)
			if (err != nil) != (boundary == "malformed companion") {
				t.Fatalf("reconcile err=%v", err)
			}
			current, err := daemon.store.Effect(ctx, claim.SemanticKey)
			if err != nil {
				t.Fatal(err)
			}
			if boundary == "ordinary" {
				if observations != 1 || current.State != store.EffectConfirmed || current.ObservedIdentity != child {
					t.Fatalf("ordinary observations=%d state=%s", observations, current.State)
				}
			} else if observations != 0 || current.State != store.EffectUncertain || current.ObservedIdentity != "" {
				t.Fatalf("premature settlement observations=%d state=%s", observations, current.State)
			}
			if source.calls != 1 {
				t.Fatalf("classifier calls=%d", source.calls)
			}
		})
	}
}
