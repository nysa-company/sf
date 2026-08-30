package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func runtimeControlTicket(t *testing.T) (*Store, domain.TicketRef, uint64, Ticket) {
	t.Helper()
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-runtime-control"}
	if err := database.CreateTicket(ctx, ticket(ref, "runtime-control")); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "runtime-control")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, queued.Version, "dev/nysa/runtime-control", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	return database, ref, leader, started
}

func TestRuntimeControlSealSurvivesStoreReopen(t *testing.T) {
	database, ref, leader, started := runtimeControlTicket(t)
	if _, err := database.ControlProof(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	var sequence int
	var name, path string
	if err := database.db.QueryRowContext(t.Context(), `PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.PlanEffect(t.Context(), EffectPlan{SemanticKey: "reopen/sealed", Ref: ref, Kind: "repository_command", TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, RequestDigest: "reopen-sealed"}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("reopened store admitted sealed ticket: %v", err)
	}
}

func TestRuntimeControlOpenAdmissionSealsOnStoreReopen(t *testing.T) {
	database, ref, leader, started := runtimeControlTicket(t)
	proof, err := database.ControlProof(t.Context(), ref)
	if err != nil || !proof.Drained() {
		t.Fatalf("initial proof=%+v err=%v", proof, err)
	}
	if _, err := database.Transition(t.Context(), Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateVerifying, Trigger: "test_open_recovery", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}"}); err != nil {
		t.Fatal(err)
	}
	active, err := database.Ticket(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := database.RearmProof(t.Context(), ref, proof.Ticket)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.ActivateRearm(t.Context(), capability, func(admission *RuntimeAdmissionCapability) error {
		gotRef, version, fence, ok := admission.ConsumeRuntimeAdmission()
		if !ok || gotRef != ref || version != active.Version || fence.LeaderEpoch != leader || fence.RunnerEpoch != active.RunnerEpoch {
			t.Fatal("activation issued the wrong admission authority")
		}
		return admission.OpenStoreAdmission(t.Context())
	}); err != nil {
		t.Fatal(err)
	}
	var sequence int
	var name, path string
	if err := database.db.QueryRowContext(t.Context(), `PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, err := reopened.PlanEffect(t.Context(), EffectPlan{SemanticKey: "reopen/open", Ref: ref, Kind: "repository_command", TicketVersion: active.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: active.RunnerEpoch}, RequestDigest: "reopen-open"}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("reopened Store admitted crashed open authority: %v", err)
	}
}

func TestRuntimeAdmissionSealAcceptsOnlyImmediateControlSuccessor(t *testing.T) {
	database, ref, leader, started := runtimeControlTicket(t)
	transition, err := database.TransitionAndInvalidateRunner(t.Context(), Transition{Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateStopping, ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: `{"intent":"pause"}`})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.sealRuntimeAdmission(t.Context(), ref, started.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}); err != nil {
		t.Fatalf("old active authority did not recognize its immediate control successor: %v", err)
	}
	if latched, ok := database.mutations.control(ref); !ok || latched.version != transition.Version || latched.runner != started.RunnerEpoch+1 || latched.leader != leader {
		t.Fatalf("old close replaced successor control latch: %+v present=%v", latched, ok)
	}
	if err := database.sealRuntimeAdmission(t.Context(), ref, transition.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("mismatched current-version/old-runner close=%v", err)
	}
	if err := database.sealRuntimeAdmission(t.Context(), ref, started.Version, domain.Fence{LeaderEpoch: leader + 1, RunnerEpoch: started.RunnerEpoch}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("wrong-leader close=%v", err)
	}
	if err := database.sealRuntimeAdmission(t.Context(), ref, started.Version-1, domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch - 1}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("older authority closed successor=%v", err)
	}
}

func TestControlProofSerializesRacingEffectAndFencesItsStart(t *testing.T) {
	database, ref, leader, started := runtimeControlTicket(t)
	ctx := t.Context()
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: "race/runtime-control", Ref: ref, Kind: "repository_command", TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, RequestDigest: "runtime-control-race"}); err != nil {
		t.Fatal(err)
	}
	claim, err := database.ClaimEffect(ctx, EffectFence{SemanticKey: "race/runtime-control", Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}})
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		_, err := database.ExternalMutationGuard().RunExternalMutation(context.Background(), claim.ExternalClaim(), func(context.Context) ([]byte, error) {
			close(entered)
			<-release
			return nil, nil
		})
		runDone <- err
	}()
	<-entered
	proofDone := make(chan TicketControlProof, 1)
	errDone := make(chan error, 1)
	go func() {
		proof, err := database.ControlProof(context.Background(), ref)
		proofDone <- proof
		errDone <- err
	}()
	select {
	case err := <-errDone:
		t.Fatalf("control proof bypassed live external mutation: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-runDone; err != nil {
		t.Fatalf("original external mutation=%v", err)
	}
	proof := <-proofDone
	if err := <-errDone; err != nil || proof.UnreconciledEffects != 1 {
		t.Fatalf("proof=%+v err=%v", proof, err)
	}
	var startedAfterProof atomic.Bool
	if _, err := database.ExternalMutationGuard().RunExternalMutation(ctx, claim.ExternalClaim(), func(context.Context) ([]byte, error) {
		startedAfterProof.Store(true)
		return nil, nil
	}); !errors.Is(err, ErrStaleFence) || startedAfterProof.Load() {
		t.Fatalf("revoked effect start err=%v started=%v", err, startedAfterProof.Load())
	}
}

func TestControlProofIsLinearizedAndFailsClosedForPublication(t *testing.T) {
	database, ref, leader, started := runtimeControlTicket(t)
	ctx := t.Context()
	status, err := database.ControlProof(ctx, ref)
	if err != nil || !status.Drained() || !status.StrictlyPrePublication() {
		t.Fatalf("initial status=%+v err=%v", status, err)
	}
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: "revoked/runtime-control", Ref: ref, Kind: "repository_command", TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, RequestDigest: "runtime-control-revoked"}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("control proof did not fence old effect plan: %v", err)
	}
	database, ref, leader, started = runtimeControlTicket(t)
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: "publish/runtime-control", Ref: ref, Kind: "branch_push", TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, RequestDigest: "runtime-control-publish"}); err != nil {
		t.Fatal(err)
	}
	status, err = database.ControlProof(ctx, ref)
	if err != nil || status.Drained() || status.StrictlyPrePublication() || status.PublicationOrMergeEffects != 1 || status.OutstandingPublicationOrMergeEffects != 1 {
		t.Fatalf("publishing status=%+v err=%v", status, err)
	}
}

func TestControlProofRejectsQuarantinedWriterAndUncertainEffect(t *testing.T) {
	database, ref, leader, started := runtimeControlTicket(t)
	ctx := t.Context()
	// This is a deliberately malformed historical provider row. The status
	// reader must still retain its quarantine as a live writer rather than
	// assuming only a current coordinator could have persisted it.
	if _, err := database.db.ExecContext(ctx, `INSERT INTO provider_attempts(channel,project_id,ticket_id,phase,attempt,provider,model,family,version,outcome,state)
		VALUES(?,?,?,?,?,?,?,?,?,?, 'quarantined')`, ref.Channel, ref.Project, ref.Ticket, "planning", 1, "test", "model", "family", "v1", "undrained"); err != nil {
		t.Fatal(err)
	}
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: "uncertain/runtime-control", Ref: ref, Kind: "repository_command", TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, RequestDigest: "runtime-control-uncertain"}); err != nil {
		t.Fatal(err)
	}
	claim, err := database.ClaimEffect(ctx, EffectFence{SemanticKey: "uncertain/runtime-control", Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}})
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	if _, err := database.db.ExecContext(ctx, `UPDATE effects SET state='uncertain' WHERE semantic_key=?`, claim.Effect.SemanticKey); err != nil {
		t.Fatal(err)
	}
	status, err := database.ControlProof(ctx, ref)
	if err != nil || status.Drained() || status.ProviderWriters != 1 || status.UnreconciledEffects != 1 {
		t.Fatalf("quarantine/uncertainty status=%+v err=%v at=%s", status, err, time.Now().UTC())
	}
}
