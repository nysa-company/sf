package daemon

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

type activityFixtureSupervisor struct {
	contracts.ProcessSupervisor
	read func(contracts.DrainRequest, uint64) contracts.ActivitySnapshot
}

func (s activityFixtureSupervisor) ActivitySnapshot(request contracts.DrainRequest, after uint64) contracts.ActivitySnapshot {
	return s.read(request, after)
}

func TestActivityCurrentnessRequiresExactDurableFenceAndActivePhase(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "app", Ticket: "SF-1"}
	ticket := store.Ticket{Ref: ref, State: domain.StateBuilding, Version: 9, RunnerEpoch: 3}
	attempt := store.ProviderAttempt{ProviderAttemptClaim: store.ProviderAttemptClaim{Ref: ref, Phase: domain.PhaseBuild, ExpectedVersion: 9, RunnerEpoch: 3, LeaderEpoch: 2}, State: "active", Outcome: "running"}
	if !activityCurrentAttempt(ticket, attempt, 2) {
		t.Fatal("exact active attempt unavailable")
	}
	for name, mutate := range map[string]func(*store.Ticket, *store.ProviderAttempt){
		"pause":    func(t *store.Ticket, _ *store.ProviderAttempt) { t.State = domain.StatePaused },
		"stopping": func(t *store.Ticket, _ *store.ProviderAttempt) { t.State = domain.StateStopping },
		"version":  func(t *store.Ticket, _ *store.ProviderAttempt) { t.Version++ },
		"runner":   func(t *store.Ticket, _ *store.ProviderAttempt) { t.RunnerEpoch++ },
		"daemon":   func(_ *store.Ticket, a *store.ProviderAttempt) { a.LeaderEpoch++ },
		"phase":    func(_ *store.Ticket, a *store.ProviderAttempt) { a.Phase = domain.PhaseReview },
		"finished": func(_ *store.Ticket, a *store.ProviderAttempt) { a.State = "finished" },
		"project":  func(_ *store.Ticket, a *store.ProviderAttempt) { a.Ref.Project = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			changed, other := ticket, attempt
			mutate(&changed, &other)
			if activityCurrentAttempt(changed, other, 2) {
				t.Fatal("stale attempt labeled current")
			}
		})
	}
}

func TestActivitySocketReadPreservesStateAndReportsUnavailableRestart(t *testing.T) {
	d, paths, _ := testDaemon(t)
	ctx := context.Background()
	path := writeTicket(t, t.TempDir(), "Observe without mutation")
	if code, output, _ := executeCLI(t, ctx, paths, "submit", path, "--project", "demo", "--json"); code != 0 {
		t.Fatal(output)
	}
	before, err := d.store.TicketByID(ctx, domain.ChannelStable, "SF-test-1")
	if err != nil {
		t.Fatal(err)
	}
	request := api.Request{Version: api.Version, RequestID: "activity", Method: "ticket.activity", Ticket: "SF-test-1", Parameters: json.RawMessage(`{"channel":"stable","after_epoch":999,"after_sequence":123}`)}
	response := d.ticketActivity(ctx, request, domain.OperatorIdentity{})
	var data struct {
		Activity struct{ Available, Current, Running, Restart bool }
	}
	if !response.OK || response.Mutation.Attempted || json.Unmarshal(response.Data, &data) != nil || data.Activity.Available || data.Activity.Current || data.Activity.Running || !data.Activity.Restart {
		t.Fatalf("response=%+v %s", response, response.Data)
	}
	after, err := d.store.TicketByID(ctx, domain.ChannelStable, "SF-test-1")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("observation changed ticket")
	}
	request.Parameters = json.RawMessage(`{"channel":"stable","after_epoch":0,"after_sequence":1}`)
	if got := d.ticketActivity(ctx, request, domain.OperatorIdentity{}); got.OK || got.Error.Code != "invalid_argument" {
		t.Fatal("invalid cursor accepted")
	}
}

func TestActivityHistoricalAttemptNeverAppearsRunning(t *testing.T) {
	d, _, _ := testDaemon(t)
	ticket := prepareDaemonPublishingStatusFixture(t, d, "SF-activity-history")
	attempts, err := d.store.ProviderAttempts(context.Background(), ticket.Ref)
	if err != nil || len(attempts) == 0 {
		t.Fatalf("attempts=%d err=%v", len(attempts), err)
	}
	latest := attempts[len(attempts)-1]
	d.providerSupervisor = activityFixtureSupervisor{read: func(request contracts.DrainRequest, after uint64) contracts.ActivitySnapshot {
		if request.ClaimID != latest.ID || request.RequestDigest != latest.RequestDigest || request.Ref != ticket.Ref {
			t.Fatal("observer claim was not taken from durable attempt")
		}
		return contracts.ActivitySnapshot{Available: true, Sequence: 7, StartedAt: time.Now(), Events: []contracts.ActivityEvent{}}
	}}
	parameters, _ := json.Marshal(map[string]any{"channel": ticket.Ref.Channel, "after_epoch": 0, "after_sequence": 0})
	response := d.ticketActivity(context.Background(), api.Request{Version: api.Version, RequestID: "activity", Method: "ticket.activity", Ticket: string(ticket.Ref.Ticket), Parameters: parameters}, domain.OperatorIdentity{})
	var data struct {
		Activity struct{ Available, Historical, Current, Running bool }
	}
	if !response.OK || json.Unmarshal(response.Data, &data) != nil || !data.Activity.Available || !data.Activity.Historical || data.Activity.Current || data.Activity.Running {
		t.Fatalf("stale running: %s", response.Data)
	}
}
