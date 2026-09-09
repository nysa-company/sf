package daemon

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type diagnosticRuntime struct{ values []contracts.RuntimeDiagnostic }

func (*diagnosticRuntime) Start(context.Context, domain.Fence) error           { return nil }
func (*diagnosticRuntime) Close() error                                        { return nil }
func (r *diagnosticRuntime) RuntimeDiagnostics() []contracts.RuntimeDiagnostic { return r.values }

func TestStatusRuntimeDiagnosticsAreScopedAndNotAuthority(t *testing.T) {
	d, paths, _ := testDaemon(t)
	path := writeTicket(t, t.TempDir(), "Runtime diagnostic")
	if code, output, _ := executeCLI(t, context.Background(), paths, "submit", path, "--project", "demo", "--json"); code != 0 {
		t.Fatal(output)
	}
	ref := domain.TicketRef{Channel: domain.ChannelStable, Project: "demo", Ticket: "SF-test-1"}
	other := ref
	other.Ticket = "SF-other"
	foreign := ref
	foreign.Channel = domain.ChannelDev
	d.runtimeMu.Lock()
	d.runtime = &diagnosticRuntime{values: []contracts.RuntimeDiagnostic{
		{Ref: ref, Outcome: "repository_preflight_failed", TicketVersion: 7, ObservedAt: time.Unix(100, 0)},
		{Ref: other, Outcome: "worker_failed"}, {Ref: foreign, Outcome: "busy"},
		{Ref: ref, Outcome: "untrusted-secret-value"},
	}}
	d.runtimeMu.Unlock()
	code, output, _ := executeCLI(t, context.Background(), paths, "status", string(ref.Ticket), "--json")
	var response api.Response
	var data struct {
		Activity struct {
			Available    bool `json:"available"`
			Observations []struct {
				Ticket  string `json:"ticket"`
				Outcome string `json:"outcome"`
			} `json:"observations"`
		} `json:"runtime_activity"`
	}
	if code != 0 || json.Unmarshal([]byte(output), &response) != nil || json.Unmarshal(response.Data, &data) != nil {
		t.Fatalf("status=%s", output)
	}
	if !data.Activity.Available || len(data.Activity.Observations) != 1 || data.Activity.Observations[0].Ticket != string(ref.Ticket) || data.Activity.Observations[0].Outcome != "repository_preflight_failed" || strings.Contains(output, "untrusted-secret-value") || response.Mutation.Attempted {
		t.Fatalf("status=%s", output)
	}
	value, err := d.store.Ticket(context.Background(), ref)
	if err != nil || value.State != domain.StateQueued || value.Version != 1 {
		t.Fatalf("diagnostic changed state: %+v %v", value, err)
	}
	d.runtimeMu.Lock()
	activity := d.runtimeActivity(&ref)
	d.runtimeMu.Unlock()
	if activity["available"] != false {
		t.Fatal("status blocked on runtime composition")
	}
}
