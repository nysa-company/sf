package github_test

import (
	"context"
	"os"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/ghrunner"
	githubclient "github.com/nysa-company/sf/internal/github"
)

type integrationQuarantine struct{ writes int }

func (q *integrationQuarantine) QuarantineExternalMutations(context.Context) error {
	q.writes++
	return nil
}
func (*integrationQuarantine) ExternalMutationsQuarantined(context.Context) (bool, error) {
	return false, nil
}

type integrationGuard struct{}

func (integrationGuard) RunExternalMutation(ctx context.Context, _ domain.ExternalEffectClaim, start func(context.Context) ([]byte, error)) ([]byte, error) {
	return start(ctx)
}

type integrationVerifier struct{}

func (integrationVerifier) VerifyProtectedBranch(context.Context, contracts.RepositoryIdentity, string, string, string) (contracts.ProtectedBranchObservation, error) {
	return contracts.ProtectedBranchObservation{}, nil
}

type integrationIntents struct{}

func (integrationIntents) RecordMergeIntent(context.Context, domain.MergeIntent) error { return nil }

func TestSupervisedRunnerPrelaunchCancellationDoesNotQuarantineClient(t *testing.T) {
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner, err := ghrunner.New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runner.Close() })
	quarantine := &integrationQuarantine{}
	client, err := githubclient.NewClient(path, t.TempDir(), t.TempDir(), runner, func(context.Context, domain.ExternalEffectClaim) error { return nil }, integrationGuard{}, integrationVerifier{}, integrationIntents{}, quarantine)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.AuthStatus(ctx); err == nil {
		t.Fatal("expected prelaunch cancellation")
	}
	if quarantine.writes != 0 {
		t.Fatalf("prelaunch cancellation quarantined client: %d", quarantine.writes)
	}
}
