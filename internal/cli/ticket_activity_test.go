package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestLiveActivityCursorRejectsMalformedAndUnmarkedRegression(t *testing.T) {
	base := liveTicketActivity{DaemonEpoch: 2, Sequence: 10, MonitorAt: time.Now()}
	if !validActivityCursor(base, 2, 9) {
		t.Fatal("valid cursor rejected")
	}
	for name, mutate := range map[string]func(*liveTicketActivity){
		"future_event": func(a *liveTicketActivity) {
			a.Observation.Events = []contracts.ActivityEvent{{Sequence: 11, At: time.Now(), Source: "sf", Kind: "process_started"}}
		},
		"cursor_regression":     func(a *liveTicketActivity) { a.Sequence = 8 },
		"epoch_without_restart": func(a *liveTicketActivity) { a.DaemonEpoch = 3 },
		"arbitrary_label": func(a *liveTicketActivity) {
			a.Observation.Events = []contracts.ActivityEvent{{Sequence: 10, At: time.Now(), Source: "provider", Kind: "SECRET"}}
		},
		"stale_running": func(a *liveTicketActivity) { a.Running = true },
	} {
		t.Run(name, func(t *testing.T) {
			value := base
			mutate(&value)
			if validActivityCursor(value, 2, 9) {
				t.Fatal("invalid cursor accepted")
			}
		})
	}
	base.Sequence, base.Gap = 1, true
	if !validActivityCursor(base, 2, 9) {
		t.Fatal("explicit gap rejected")
	}
}

func TestLiveWatchReadsTerminalSnapshotWithCursorAndNoMutation(t *testing.T) {
	calls := 0
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		calls++
		if request.Method != "ticket.activity" || request.Ticket != "SF-1" {
			t.Fatalf("unexpected request=%+v", request)
		}
		var parameters struct {
			AfterEpoch    uint64 `json:"after_epoch"`
			AfterSequence uint64 `json:"after_sequence"`
		}
		if json.Unmarshal(request.Parameters, &parameters) != nil {
			t.Fatal("invalid parameters")
		}
		if calls == 2 && (parameters.AfterEpoch != 2 || parameters.AfterSequence != 9) {
			t.Fatalf("cursor not reused: %s", request.Parameters)
		}
		state := domain.StateBuilding
		if calls == 2 {
			state = domain.StateDone
		}
		response := responseOK()
		response.Data, _ = json.Marshal(map[string]any{"ticket": selectableTicket{ID: "SF-1", Project: "app", Channel: domain.ChannelStable, State: state}, "activity": liveTicketActivity{DaemonEpoch: 2, Sequence: 9, MonitorAt: time.Now(), Reason: "no live attempt"}})
		return response, nil
	})
	var output bytes.Buffer
	if code := Execute(context.Background(), []string{"ticket", "watch", "SF-1", "--json"}, &output, &bytes.Buffer{}, client); code != 0 || calls != 2 || bytes.Count(output.Bytes(), []byte("\n")) != 2 {
		t.Fatalf("exit=%d calls=%d output=%s", code, calls, &output)
	}
}

func TestHistoricalActivityElapsedStopsAtExitAndNeverClaimsProof(t *testing.T) {
	start := time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC)
	value := liveTicketActivity{Available: true, Historical: true, MonitorAt: start.Add(time.Minute), Observation: contracts.ActivitySnapshot{StartedAt: start, ExitedAt: start.Add(2 * time.Second), Events: []contracts.ActivityEvent{{At: start, Source: "provider", Kind: "command_completed"}}}}
	var output bytes.Buffer
	if err := renderLiveTicketActivity(&output, value, true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Elapsed 2s") || !strings.Contains(output.String(), "historical attempt") || !strings.Contains(output.String(), "not an SF proof result") {
		t.Fatal(output.String())
	}
}

func TestLiveWatchDisplaysTransientGapBeforeNextHeartbeat(t *testing.T) {
	calls := 0
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		calls++
		state := domain.StateBuilding
		if calls == 3 {
			state = domain.StateDone
		}
		activity := liveTicketActivity{DaemonEpoch: 2, Sequence: uint64(calls), MonitorAt: time.Now(), Gap: calls == 2, Reason: "no current attempt"}
		response := responseOK()
		response.Data, _ = json.Marshal(map[string]any{"ticket": selectableTicket{ID: "SF-1", Project: "app", Channel: domain.ChannelStable, State: state}, "activity": activity})
		return response, nil
	})
	var output bytes.Buffer
	if code := Execute(context.Background(), []string{"ticket", "watch", "SF-1"}, &output, &bytes.Buffer{}, client); code != 0 {
		t.Fatalf("exit=%d", code)
	}
	if strings.Count(output.String(), "Activity gap:") != 1 {
		t.Fatalf("transient gap lost or repeated: %s", &output)
	}
}

func TestLiveWatchReconnectsOnlyTheSameReadAndMarksDaemonRestart(t *testing.T) {
	calls := 0
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		calls++
		if request.Method != "ticket.activity" || request.Ticket != "SF-1" {
			t.Fatalf("reconnect changed operation: %+v", request)
		}
		if calls == 2 {
			return failure("daemon_unavailable", "offline", []string{"sf", "daemon", "run"}), nil
		}
		state, epoch := domain.StateBuilding, uint64(2)
		if calls == 3 {
			state, epoch = domain.StateDone, 3
		}
		response := responseOK()
		response.Data, _ = json.Marshal(map[string]any{"ticket": selectableTicket{ID: "SF-1", Project: "app", Channel: domain.ChannelStable, State: state}, "activity": liveTicketActivity{DaemonEpoch: epoch, Sequence: 1, MonitorAt: time.Now(), Restart: calls == 3, Reason: "no live attempt"}})
		return response, nil
	})
	var output, stderr bytes.Buffer
	if code := Execute(context.Background(), []string{"ticket", "watch", "SF-1"}, &output, &stderr, client); code != 0 || calls != 3 {
		t.Fatalf("exit=%d calls=%d", code, calls)
	}
	if !strings.Contains(stderr.String(), "Monitor disconnected") || !strings.Contains(output.String(), "new daemon epoch") {
		t.Fatalf("missing reconnect marker: %s %s", &output, &stderr)
	}
}
