package workflowruntime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
)

func TestPlannerAdmissionReasonSurvivesSchedulerProjection(t *testing.T) {
	for _, code := range []providercoord.AdmissionDiagnostic{
		providercoord.DiagnosticBindingUnavailable, providercoord.DiagnosticQualificationRefused,
		providercoord.DiagnosticAdmissionRefused, providercoord.DiagnosticProviderMismatch,
	} {
		t.Run(string(code), func(t *testing.T) {
			request, evidence, coordinator := plannerFixture(t)
			coordinator.result = providercoord.Result{Code: providercoord.NeedsOperator, Diagnostic: code, NeedsOperator: true}
			_, err := (PlannerRunner{Store: evidence, Coordinator: coordinator}).RunArtifact(context.Background(), request)
			if !errors.Is(err, ErrPlannerNotReady) || diagnosticReason(err) != string(code) {
				t.Fatalf("reason=%q err=%v", diagnosticReason(err), err)
			}
			ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "demo", Ticket: "SF-diagnostic"}
			worker := &fakeWorker{err: err}
			scheduler := NewScheduler(domain.ChannelDev, fakeTickets{tickets: []store.Ticket{ticket(ref, domain.StatePlanning)}}, &fakeEnsure{}, worker)
			result := scheduler.Tick(context.Background(), domain.Fence{LeaderEpoch: 9, RunnerEpoch: 7})
			runtime := &Runtime{}
			runtime.recordDiagnostic(result, time.Unix(1, 0))
			values := runtime.RuntimeDiagnostics()
			if result.Outcome != OutcomeWorker || len(values) != 1 || values[0].Reason != string(code) {
				t.Fatalf("result=%+v values=%+v", result, values)
			}
		})
	}
}

func TestDiagnosticReasonRejectsUntrustedPayload(t *testing.T) {
	for _, err := range []error{errors.New("untrusted-secret-value"), diagnosticError{code: "untrusted-secret-value", err: errors.New("untrusted-secret-value")}} {
		_, safe := classifyWorker(err)
		runtime := &Runtime{}
		runtime.recordDiagnostic(TickResult{Outcome: OutcomeWorker, Err: safe}, time.Unix(1, 0))
		data, _ := json.Marshal(runtime.RuntimeDiagnostics())
		if strings.Contains(string(data), "untrusted-secret-value") || strings.Contains(safe.Error(), "untrusted-secret-value") {
			t.Fatal("raw error escaped")
		}
	}
}
