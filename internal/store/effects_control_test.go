package store

import (
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

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
	if err != nil || rebound.TicketVersion != resumed.Version || rebound.RunnerEpoch != current.Fence.RunnerEpoch || rebound.State != EffectFailed {
		t.Fatalf("rebound=%+v err=%v", rebound, err)
	}
	claim, err := database.ClaimEffect(ctx, EffectFence{SemanticKey: prior.SemanticKey, Ref: ref, TicketVersion: resumed.Version, Fence: current.Fence})
	if err != nil || !claim.Claimed || claim.Effect.ClaimEpoch <= prior.Fence.ClaimEpoch {
		t.Fatalf("new claim=%+v err=%v", claim, err)
	}
	reboundPlan.RequestDigest = "changed"
	if _, err := database.PlanEffect(ctx, reboundPlan); !errors.Is(err, ErrEffectKey) {
		t.Fatalf("changed semantic request was accepted: %v", err)
	}
}

func TestInvalidatedEffectPresenceIsConfirmedAndNeverRebound(t *testing.T) {
	database, ref, leader, started, prior := claimedEffectBeforeControl(t, "effect-control-present")
	ctx := t.Context()
	_, err := database.TransitionAndInvalidateRunner(ctx, Transition{
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
	current := EffectFence{SemanticKey: prior.SemanticKey, Ref: ref, TicketVersion: stopping.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: stopping.RunnerEpoch}}
	observation := InvalidatedEffectObservation{Prior: EffectObservation{EffectFence: prior, Present: true, Identity: "remote@abc"}, Current: current}
	confirmed, err := database.ReconcileInvalidatedEffect(ctx, observation)
	if err != nil || confirmed.State != EffectConfirmed || confirmed.ObservedIdentity != "remote@abc" {
		t.Fatalf("confirmed=%+v err=%v", confirmed, err)
	}
	if replay, err := database.ReconcileInvalidatedEffect(ctx, observation); err != nil || replay.State != EffectConfirmed {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if _, err := database.PlanEffect(ctx, EffectPlan{SemanticKey: prior.SemanticKey, Ref: ref, Kind: "branch_push", TicketVersion: stopping.Version, Fence: current.Fence, RequestDigest: "request"}); !errors.Is(err, ErrEffectKey) {
		t.Fatalf("confirmed stale effect was rebound: %v", err)
	}
	wrong := observation
	wrong.Prior.Fence.ClaimEpoch++
	if _, err := database.ReconcileInvalidatedEffect(ctx, wrong); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("wrong prior claim error=%v", err)
	}
}
