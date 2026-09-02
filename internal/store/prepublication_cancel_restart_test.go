package store

import (
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

// Cancellation is still a pre-publication disposition.  In particular, a
// merge observer must not turn an operator cancellation into a proof request
// merely because the ticket is temporarily in the cancelling state or the
// daemon was restarted between the two control calls.
func TestMergeObservationPrePublicationRemainsTrueAfterOperatorCancelRestart(t *testing.T) {
	database, ref, leader, started := runtimeControlTicket(t)
	ctx := t.Context()

	pre, err := database.MergeObservationPrePublication(ctx, ref)
	if err != nil || !pre {
		t.Fatalf("initial pre-publication classification=%v err=%v", pre, err)
	}
	if _, err := database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: ref, ExpectedVersion: started.Version, From: domain.StatePlanning,
		To: domain.StateCancelling, Trigger: "operator_cancel",
		Fence:        domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch},
		EventPayload: `{"operator":"test"}`,
	}); err != nil {
		t.Fatalf("operator cancel: %v", err)
	}
	cancelling, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if cancelling.State != domain.StateCancelling || cancelling.Version != started.Version+1 || cancelling.RunnerEpoch != started.RunnerEpoch+1 {
		t.Fatalf("durable cancelling ticket=%+v started=%+v", cancelling, started)
	}
	pre, err = database.MergeObservationPrePublication(ctx, ref)
	if err != nil || !pre {
		t.Fatalf("cancelling pre-publication classification=%v err=%v", pre, err)
	}

	var sequence int
	var name, path string
	if err := database.db.QueryRowContext(ctx, `PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	newLeader, err := reopened.AcquireLeader(ctx, ref.Channel, "cancel-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(ctx, ref.Channel, newLeader); err != nil || changed != 1 {
		t.Fatalf("fence recovered cancellation changed=%d err=%v", changed, err)
	}
	recovered, err := reopened.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.State != domain.StateCancelling || recovered.Version != cancelling.Version+1 || recovered.RunnerEpoch != cancelling.RunnerEpoch+1 {
		t.Fatalf("reopened cancelling ticket=%+v before=%+v", recovered, cancelling)
	}
	pre, err = reopened.MergeObservationPrePublication(ctx, ref)
	if err != nil || !pre {
		t.Fatalf("reopened cancelling pre-publication classification=%v err=%v", pre, err)
	}
	secondLeader, err := reopened.AcquireLeader(ctx, ref.Channel, "cancel-second-restart")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reopened.FenceRecoveredRunners(ctx, ref.Channel, secondLeader); err != nil || changed != 1 {
		t.Fatalf("second cancellation fence changed=%d err=%v", changed, err)
	}
	recovered, err = reopened.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	pre, err = reopened.MergeObservationPrePublication(ctx, ref)
	if err != nil || !pre {
		t.Fatalf("second recovered cancellation classification=%v err=%v", pre, err)
	}
	if _, err := reopened.CompleteControlTransition(ctx, Transition{
		Ref: recovered.Ref, ExpectedVersion: recovered.Version, From: domain.StateCancelling,
		To: domain.StateCancelled, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: secondLeader, RunnerEpoch: recovered.RunnerEpoch}, EventPayload: `{}`,
	}); err != nil {
		t.Fatalf("complete cancellation after restart: %v", err)
	}
	cancelled, err := reopened.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.State != domain.StateCancelled {
		t.Fatalf("reopened cancellation completion=%+v", cancelled)
	}
	if changed, err := reopened.FenceRecoveredRunners(ctx, ref.Channel, secondLeader); err != nil || changed != 0 {
		t.Fatalf("terminal cancellation startup fence changed=%d err=%v", changed, err)
	}
	events, err := reopened.Events(ctx, ref.Channel, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var cancelEvents int
	for _, event := range events {
		if event.Ref == ref && event.Trigger == "operator_cancel" && event.From == domain.StatePlanning && event.To == domain.StateCancelling {
			cancelEvents++
		}
	}
	if cancelEvents != 1 {
		t.Fatalf("operator cancel event count=%d events=%+v", cancelEvents, events)
	}
}

func TestPrePublicationCancellationProofFailsClosedWithoutExactAuthority(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *Store, Ticket)
	}{
		{
			name: "missing-sealed-control",
			mutate: func(t *testing.T, database *Store, cancelling Ticket) {
				if _, err := database.db.ExecContext(t.Context(), `DELETE FROM runtime_ticket_controls WHERE channel=? AND project_id=? AND ticket_id=?`, cancelling.Ref.Channel, cancelling.Ref.Project, cancelling.Ref.Ticket); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "duplicate-state-change",
			mutate: func(t *testing.T, database *Store, cancelling Ticket) {
				if _, err := database.db.ExecContext(t.Context(), `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, cancelling.Ref.Channel, cancelling.Ref.Project, cancelling.Ref.Ticket, cancelling.Version, "typed_blocker", domain.StatePlanning, domain.StateBlocked, `{}`, "2026-09-02T00:00:00Z"); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, ref, leader, started := runtimeControlTicket(t)
			if _, err := database.TransitionAndInvalidateRunner(t.Context(), Transition{
				Ref: ref, ExpectedVersion: started.Version, From: started.State,
				To: domain.StateCancelling, Trigger: "operator_cancel",
				Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}, EventPayload: `{}`,
			}); err != nil {
				t.Fatal(err)
			}
			cancelling := databaseTicket(t, database, t.Context(), ref)
			tc.mutate(t, database, cancelling)
			if pre, err := database.MergeObservationPrePublication(t.Context(), ref); err != nil || pre {
				t.Fatalf("malformed cancellation classified pre-publication=%v err=%v", pre, err)
			}
			newLeader, err := database.AcquireLeader(t.Context(), ref.Channel, "malformed-cancel-restart")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.FenceRecoveredRunners(t.Context(), ref.Channel, newLeader); !errors.Is(err, ErrPublicationEvidence) {
				t.Fatalf("malformed cancellation startup fence err=%v, want publication evidence", err)
			}
		})
	}
}

func TestMergeObservationPrePublicationAllowlist(t *testing.T) {
	for _, target := range []domain.State{
		domain.StateQueued, domain.StatePlanning, domain.StateVerifying,
		domain.StateBuilding, domain.StateStopping, domain.StatePaused,
		domain.StateBlocked, domain.StateCancelling,
	} {
		t.Run(string(target), func(t *testing.T) {
			database, ctx := openTestStore(t)
			ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: domain.TicketID("SF-prepub-" + string(target))}
			if err := database.CreateTicket(ctx, ticket(ref, "prepub-"+string(target))); err != nil {
				t.Fatal(err)
			}
			leader, err := database.AcquireLeader(ctx, ref.Channel, "prepub-"+string(target))
			if err != nil {
				t.Fatal(err)
			}
			current, err := database.Ticket(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			if target != domain.StateQueued {
				current, err = database.StartOrAdopt(ctx, ref, current.Version, "prepub/"+string(target), domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch})
				if err != nil {
					t.Fatal(err)
				}
				switch target {
				case domain.StateVerifying, domain.StateBuilding:
					current = providerState(t, database, ctx, current, leader, target)
				case domain.StateStopping, domain.StatePaused, domain.StateBlocked, domain.StateCancelling, domain.StateCancelled:
					current = prePublicationControlState(t, database, ctx, current, leader, target)
				}
			}
			pre, err := database.MergeObservationPrePublication(ctx, ref)
			if err != nil || !pre {
				t.Fatalf("state=%s pre-publication classification=%v err=%v ticket=%+v", target, pre, err, current)
			}
			if target != domain.StateCancelling {
				if _, err := database.TransitionAndInvalidateRunner(ctx, Transition{
					Ref: current.Ref, ExpectedVersion: current.Version, From: current.State,
					To: domain.StateCancelling, Trigger: "operator_cancel",
					Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}, EventPayload: `{"prepublication":true}`,
				}); err != nil {
					t.Fatalf("cancel from %s: %v", target, err)
				}
				current = databaseTicket(t, database, ctx, ref)
				pre, err = database.MergeObservationPrePublication(ctx, ref)
				if err != nil || !pre {
					t.Fatalf("cancel origin=%s pre-publication classification=%v err=%v ticket=%+v", target, pre, err, current)
				}
			}
		})
	}
}

func prePublicationControlState(t *testing.T, database *Store, ctx context.Context, current Ticket, leader uint64, target domain.State) Ticket {
	t.Helper()
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}
	if target == domain.StateStopping || target == domain.StatePaused || target == domain.StateBlocked {
		stopping, err := database.TransitionAndInvalidateRunner(ctx, Transition{
			Ref: current.Ref, ExpectedVersion: current.Version, From: current.State,
			To: domain.StateStopping, ResumeState: current.State, Trigger: "operator_pause_or_take",
			Fence: fence, EventPayload: `{}`,
		})
		if err != nil {
			t.Fatal(err)
		}
		current = databaseTicket(t, database, ctx, current.Ref)
		if target == domain.StateStopping {
			return current
		}
		if _, err := database.CompleteControlTransition(ctx, Transition{
			Ref: current.Ref, ExpectedVersion: stopping.Version, From: domain.StateStopping,
			To: domain.StatePaused, ResumeState: current.ResumeState,
			Trigger: "process_and_effects_drained",
			Fence:   domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}, EventPayload: `{}`,
		}); err != nil {
			t.Fatal(err)
		}
		current = databaseTicket(t, database, ctx, current.Ref)
		if target == domain.StatePaused {
			return current
		}
		if _, err := database.Transition(ctx, Transition{
			Ref: current.Ref, ExpectedVersion: current.Version, From: domain.StatePaused,
			To: domain.StateBlocked, ResumeState: current.ResumeState, Trigger: "operator_resume",
			Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}, EventPayload: `{}`,
		}); err != nil {
			t.Fatal(err)
		}
		return databaseTicket(t, database, ctx, current.Ref)
	}

	stopping, err := database.TransitionAndInvalidateRunner(ctx, Transition{
		Ref: current.Ref, ExpectedVersion: current.Version, From: current.State,
		To: domain.StateCancelling, Trigger: "operator_cancel", Fence: fence, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	current = databaseTicket(t, database, ctx, current.Ref)
	if target == domain.StateCancelling {
		return current
	}
	if _, err := database.CompleteControlTransition(ctx, Transition{
		Ref: current.Ref, ExpectedVersion: stopping.Version, From: domain.StateCancelling,
		To: domain.StateCancelled, Trigger: "process_and_effects_drained",
		Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: current.RunnerEpoch}, EventPayload: `{}`,
	}); err != nil {
		t.Fatal(err)
	}
	return databaseTicket(t, database, ctx, current.Ref)
}

func databaseTicket(t *testing.T, database *Store, ctx context.Context, ref domain.TicketRef) Ticket {
	t.Helper()
	value, err := database.Ticket(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// A caller-controlled payload cannot manufacture a pre-publication proof for
// a ticket that already crossed into publication.  The classifier is
// intentionally read-only and derives its answer from durable effects and
// merge intents, not from an operator's claimed "merge not observed" fact.
func TestMergeObservationPrePublicationRejectsForgedPostPublicationProof(t *testing.T) {
	fixture, current, leader := preparePostPublicationRearmState(t, domain.StateWaitingApproval)
	defer fixture.db.Close()

	pre, err := fixture.db.MergeObservationPrePublication(fixture.ctx, current.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if pre {
		t.Fatal("post-publication ticket was classified as pre-publication")
	}
	// Also exercise the pure proof predicate with the most favorable forged
	// counts.  The ticket state itself remains an independent publication
	// boundary, so zero caller-supplied effects cannot override it.
	if (TicketControlProof{Ticket: current}).StrictlyPrePublication() {
		t.Fatal("forged zero-effect proof was accepted for a post-publication ticket")
	}
	if _, err := fixture.db.TransitionAndInvalidateRunner(fixture.ctx, Transition{
		Ref: current.Ref, ExpectedVersion: current.Version, From: current.State,
		To: domain.StateCancelling, Trigger: "operator_cancel",
		Fence:        domain.Fence{LeaderEpoch: leader.LeaderEpoch, RunnerEpoch: current.RunnerEpoch},
		EventPayload: `{"prepublication":true,"merge_not_observed":true}`,
	}); err != nil {
		t.Fatalf("post-publication cancel fixture: %v", err)
	}
	pre, err = fixture.db.MergeObservationPrePublication(fixture.ctx, current.Ref)
	if err != nil || pre {
		t.Fatalf("post-publication forged classification=%v err=%v", pre, err)
	}
}
