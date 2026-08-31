package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func openExactRuntimeAdmission(t *testing.T, database *Store, ref domain.TicketRef) {
	t.Helper()
	stopped, err := database.StoppedRuntimeTicket(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := database.RearmProof(context.Background(), ref, stopped)
	if err != nil {
		t.Fatal(err)
	}
	var admission *RuntimeAdmissionCapability
	if err := database.ActivateRearm(context.Background(), capability, func(value *RuntimeAdmissionCapability) error {
		_, _, _, _ = value.ConsumeRuntimeAdmission()
		admission = value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := admission.OpenStoreAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func claimedEffectBeforeControl(t *testing.T, key string) (*Store, domain.TicketRef, uint64, Ticket, EffectFence) {
	t.Helper()
	database, ctx := openTestStore(t)
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: domain.TicketID("SF-" + key)}
	if err := database.CreateTicket(ctx, ticket(ref, "digest-"+key)); err != nil {
		t.Fatal(err)
	}
	leader, err := database.AcquireLeader(ctx, domain.ChannelDev, "effect-control")
	if err != nil {
		t.Fatal(err)
	}
	queued, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	started, err := database.StartOrAdopt(ctx, ref, queued.Version, "dev/nysa/"+string(ref.Ticket)+"/planning", domain.Fence{LeaderEpoch: leader, RunnerEpoch: queued.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	effect := EffectFence{SemanticKey: key, Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}}
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: key, Ref: ref, Kind: "branch_push", TicketVersion: started.Version, Fence: effect.Fence, RequestDigest: "request"}); err != nil {
		t.Fatal(err)
	}
	claim, err := database.ClaimEffect(ctx, effect)
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	effect.Fence.ClaimEpoch = claim.Effect.ClaimEpoch
	return database, ref, leader, started, effect
}

func TestInvalidatedEffectAbsenceCanBeSafelyRebound(t *testing.T) {
	database, ref, leader, started, prior := claimedEffectBeforeControl(t, "effect-control-absent")
	ctx := t.Context()
	control, err := database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning, To: domain.StateStopping,
		ResumeState: domain.StatePlanning, Trigger: "operator_pause_or_take", Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: "{}",
	})
	if err != nil {
		t.Fatal(err)
	}
	stopping, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	current := EffectFence{SemanticKey: prior.SemanticKey, Ref: ref, TicketVersion: control.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopping.RunnerEpoch}}
	settled, err := database.ReconcileInvalidatedEffect(ctx, InvalidatedEffectObservation{Prior: EffectObservation{EffectFence: prior, Present: false}, Current: current})
	if err != nil || settled.State != EffectFailed {
		t.Fatalf("settled=%+v err=%v", settled, err)
	}
	paused, err := database.Transition(ctx, Transition{Ref: ref, ExpectedVersion: stopping.Version, From: domain.StateStopping, To: domain.StatePaused, ResumeState: domain.StatePlanning, Trigger: "process_and_effects_drained", Fence: current.Fence, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := database.Transition(ctx, Transition{Ref: ref, ExpectedVersion: paused.Version, From: domain.StatePaused, To: domain.StatePlanning, Trigger: "operator_resume", Fence: current.Fence, EventPayload: "{}"})
	if err != nil {
		t.Fatal(err)
	}
	reboundPlan := EffectPlan{SemanticKey: prior.SemanticKey, Ref: ref, Kind: "branch_push", TicketVersion: resumed.Version, Fence: current.Fence, RequestDigest: "request"}
	rebound, err := database.PlanEffect(ctx, reboundPlan)
	if !errors.Is(err, ErrStaleFence) {
		t.Fatalf("publication-sensitive ticket escaped sealed resume: rebound=%+v err=%v", rebound, err)
	}
}

func TestInvalidatedEffectPresenceIsConfirmedAndNeverRebound(t *testing.T) {
	database, ref, _, started, prior := claimedEffectBeforeControl(t, "effect-control-present")
	ctx := t.Context()
	newLeader, err := database.AcquireLeader(ctx, domain.ChannelDev, "effect-control-present-restart")
	if err != nil {
		t.Fatal(err)
	}
	uncertain, err := database.ReconcileEffects(ctx, domain.ChannelDev, newLeader)
	if err != nil || len(uncertain) != 1 {
		t.Fatalf("reconcile=%+v err=%v", uncertain, err)
	}
	current := EffectFence{SemanticKey: prior.SemanticKey, Ref: ref, TicketVersion: started.Version, Fence: domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: uncertain[0].RunnerEpoch}}
	recoveredPrior := EffectFence{SemanticKey: uncertain[0].SemanticKey, Ref: uncertain[0].Ref, TicketVersion: uncertain[0].TicketVersion, Fence: domain.Fence{LeaderEpoch: uncertain[0].LeaderEpoch, RunnerEpoch: uncertain[0].RunnerEpoch, ClaimEpoch: uncertain[0].ClaimEpoch}}
	observation := InvalidatedEffectObservation{Prior: EffectObservation{EffectFence: recoveredPrior, Present: true, Identity: "remote@abc"}, Current: current}
	confirmed, err := database.ReconcileInvalidatedEffect(ctx, observation)
	if err != nil || confirmed.State != EffectConfirmed || confirmed.ObservedIdentity != "remote@abc" || confirmed.TicketVersion != current.TicketVersion || confirmed.LeaderEpoch != current.Fence.LeaderEpoch || confirmed.RunnerEpoch != current.Fence.RunnerEpoch || confirmed.ClaimEpoch != recoveredPrior.Fence.ClaimEpoch+1 {
		t.Fatalf("confirmed=%+v err=%v", confirmed, err)
	}
	if replay, err := database.ReconcileInvalidatedEffect(ctx, observation); err != nil || replay.State != EffectConfirmed {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if _, err := database.ConfirmEffect(ctx, prior, "late-response"); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("late response=%v", err)
	}
	if err := database.write(ctx, func(conn *sql.Conn) error {
		return checkPublicationEffect(ctx, conn, ref, current.TicketVersion, current.Fence, PublicationEffectEvidence{
			SemanticKey: prior.SemanticKey, Kind: "branch_push", RequestDigest: "request", ClaimEpoch: confirmed.ClaimEpoch, ObservedIdentity: confirmed.ObservedIdentity,
		})
	}); err != nil {
		t.Fatalf("current publication witness=%v", err)
	}
	replanned, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: prior.SemanticKey, Ref: ref, Kind: "branch_push", TicketVersion: started.Version, Fence: current.Fence, RequestDigest: "request"})
	if err != nil || replanned.State != EffectConfirmed || replanned.ClaimEpoch != confirmed.ClaimEpoch || replanned.ObservedIdentity != confirmed.ObservedIdentity {
		t.Fatalf("confirmed effect was not idempotently reused: effect=%+v err=%v", replanned, err)
	}
	reused, err := database.ClaimEffect(ctx, EffectFence{SemanticKey: prior.SemanticKey, Ref: ref, TicketVersion: started.Version, Fence: current.Fence})
	if err != nil || reused.Claimed || reused.Effect.ClaimEpoch != confirmed.ClaimEpoch {
		t.Fatalf("confirmed effect was re-executed: claim=%+v err=%v", reused, err)
	}
	wrong := observation
	wrong.Prior.Fence.ClaimEpoch++
	if _, err := database.ReconcileInvalidatedEffect(ctx, wrong); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("wrong prior claim error=%v", err)
	}
}
