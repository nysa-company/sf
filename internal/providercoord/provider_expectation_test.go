package providercoord

import (
	"context"
	"testing"

	"github.com/nysa-company/sf/internal/testkit"
)

func TestConfiguredProviderMismatchNeverClaimsOrFallsBack(t *testing.T) {
	db, request, coordinator, ref, primary := newCoordinatorFixture(t, testkit.NewSupervisor())
	request.ExpectedProvider = "claude"
	result := coordinator.Run(context.Background(), request)
	if result.Code != NeedsOperator || len(primary.CallsSnapshot()) != 0 || len(result.Attempts) != 0 {
		t.Fatalf("mismatched provider invoked: %+v", result)
	}
	attempts, err := db.ProviderAttempts(context.Background(), ref)
	if err != nil || len(attempts) != 0 {
		t.Fatalf("mismatched provider reserved an attempt: %v", err)
	}
}
