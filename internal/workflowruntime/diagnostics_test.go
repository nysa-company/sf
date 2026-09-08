package workflowruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func TestRuntimeDiagnosticsAreBoundedOwnedAndSanitized(t *testing.T) {
	r := &Runtime{}
	for i := 0; i < 100; i++ {
		ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: domain.TicketID(fmt.Sprintf("SF-%d", i))}
		r.recordDiagnostic(TickResult{Ref: ref, Outcome: OutcomeReadiness, Err: errors.New("untrusted-secret-value")}, time.Unix(int64(i), 0))
	}
	values := r.RuntimeDiagnostics()
	if len(values) != 64 || values[0].Ref.Ticket != "SF-36" {
		t.Fatalf("unexpected retention: %+v", values)
	}
	last := values[len(values)-1]
	values[0].Outcome = "mutated"
	if r.RuntimeDiagnostics()[0].Outcome != string(OutcomeReadiness) {
		t.Fatal("snapshot aliased storage")
	}
	for _, outcome := range []Outcome{OutcomeIdle, OutcomeCanceled, OutcomeInProgress, Outcome("untrusted-secret-value")} {
		r.recordDiagnostic(TickResult{Ref: last.Ref, Outcome: outcome}, time.Now())
	}
	if r.RuntimeDiagnostics()[63] != last {
		t.Fatal("benign/unknown tick erased failure")
	}
	r.recordDiagnostic(TickResult{Ref: last.Ref, Outcome: OutcomeInvoked}, time.Now())
	values = r.RuntimeDiagnostics()
	if len(values) != 64 || values[63].Outcome != string(OutcomeInvoked) {
		t.Fatal("completion did not replace failure")
	}
	payload, err := json.Marshal(values)
	if err != nil || strings.Contains(string(payload), "untrusted-secret-value") {
		t.Fatalf("payload=%s err=%v", payload, err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.recordDiagnostic(TickResult{Ref: last.Ref, Outcome: OutcomeBusy}, time.Now())
				_ = r.RuntimeDiagnostics()
			}
		}()
	}
	wg.Wait()
}

func TestRuntimeLoopRecordsReadinessFailure(t *testing.T) {
	r, err := NewRuntime(NewScheduler(domain.ChannelDev, fakeTickets{err: errors.New("untrusted-secret-value")}, &fakeEnsure{}, &fakeWorker{}), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background(), domain.Fence{LeaderEpoch: 9}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		values := r.RuntimeDiagnostics()
		if len(values) == 1 {
			if values[0].Outcome != string(OutcomeReadiness) || values[0].Fence.LeaderEpoch != 9 {
				t.Fatalf("values=%+v", values)
			}
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("runtime discarded readiness failure")
		case <-poll.C:
		}
	}
}
