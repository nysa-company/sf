package processsupervisor

import (
	"context"
	"github.com/nysa-company/sf/internal/contracts"
	"testing"
)

func TestRepositoryCommandDrainerFailsClosedOnUnclearIdentity(t *testing.T) {
	d := RepositoryCommandDrainer{}
	if err := d.DrainRepositoryCommand(context.Background(), contracts.RepositoryCommandLaunch{PID: 42, PGID: 42}); err == nil {
		t.Fatal("drainer accepted an identity without boot/start proofs")
	}
}
