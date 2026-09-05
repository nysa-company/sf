package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestRunReadinessRefusalKeepsTicketQueuedAndSanitizesReason(t *testing.T) {
	for _, test := range []struct {
		code   string
		reason error
	}{
		{"unsupported_repository_recipe", ErrStartRecipeUnsupported},
		{"unsupported_runtime", ErrStartRuntimeUnsupported},
	} {
		t.Run(test.code, func(t *testing.T) {
			d, paths, _ := testDaemon(t)
			d.doctor = func(context.Context, store.Project) error {
				return fmt.Errorf("untrusted-secret-value: %w", test.reason)
			}
			path := writeTicket(t, t.TempDir(), "Readiness refusal")
			code, output, _ := executeCLI(t, context.Background(), paths, "run", path, "--project", "demo", "--json")
			var response api.Response
			if code == 0 || json.Unmarshal([]byte(output), &response) != nil || response.Error == nil || response.Error.Code != test.code || strings.Contains(output, "untrusted-secret-value") {
				t.Fatalf("code=%d response=%s", code, output)
			}
			value, err := d.store.TicketByID(context.Background(), domain.ChannelStable, "SF-test-1")
			if err != nil || value.State != domain.StateQueued || value.Version != 1 || value.WorkflowID != "" {
				t.Fatalf("failed readiness started ticket: %+v err=%v", value, err)
			}
		})
	}
}
