package daemon

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

func TestStatusExposesDurableEstimatedAccountingWithoutClaimingActualSpend(t *testing.T) {
	d, paths, _ := testDaemon(t)
	ctx := context.Background()
	path := writeTicket(t, t.TempDir(), "Estimated accounting")
	if code, output, _ := executeCLI(t, ctx, paths, "submit", path, "--project", "demo", "--json"); code != 0 {
		t.Fatalf("submit=%s", output)
	}
	ticket, err := d.store.TicketByID(ctx, domain.ChannelStable, "SF-test-1")
	if err != nil {
		t.Fatal(err)
	}
	before, err := d.evidenceView(ctx, ticket.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := before["provider_accounting"]; exists {
		t.Fatal("invented opt-in for ordinary ticket")
	}
	if err := d.store.ApproveProviderEstimatedAccounting(ctx, ticket.Ref, ticket.Version, domain.Fence{LeaderEpoch: d.epoch, RunnerEpoch: ticket.RunnerEpoch}); err != nil {
		t.Fatal(err)
	}
	code, output, _ := executeCLI(t, ctx, paths, "status", "SF-test-1", "--json")
	var response api.Response
	var data struct {
		Evidence struct {
			Accounting map[string]any `json:"provider_accounting"`
		} `json:"evidence"`
	}
	if code != 0 || json.Unmarshal([]byte(output), &response) != nil || json.Unmarshal(response.Data, &data) != nil {
		t.Fatalf("status=%s", output)
	}
	a := data.Evidence.Accounting
	if a["mode"] != "reported_estimate_v1" || a["actual_total_known"] != false || a["hard_dollar_cap"] != false || a["sf_launch_limit"] != float64(16) || a["request_timeout"] != "45m0s" {
		t.Fatalf("accounting=%v", a)
	}
}
