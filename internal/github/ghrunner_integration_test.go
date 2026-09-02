package github_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

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
func (integrationIntents) RecordGuardedMergeObservation(context.Context, domain.MergeIntent, contracts.PublishedPullRequestObservation) error {
	return nil
}

type integrationSerialRunner struct {
	mu       sync.Mutex
	running  bool
	overlaps int
	cleanups int
}

type integrationBusyRunner struct {
	mu       sync.Mutex
	cleanups int
}

func (*integrationBusyRunner) Run(context.Context, string, []string, []string) ([]byte, error) {
	return nil, githubclient.ErrRunnerBusy
}

func (r *integrationBusyRunner) Cleanup(context.Context) (githubclient.CleanupProof, error) {
	r.mu.Lock()
	r.cleanups++
	r.mu.Unlock()
	return githubclient.CleanupProof{Quarantined: true}, githubclient.ErrProcessCleanup
}

func (r *integrationSerialRunner) Run(context.Context, string, []string, []string) ([]byte, error) {
	r.mu.Lock()
	if r.running {
		r.overlaps++
		r.mu.Unlock()
		return nil, githubclient.ErrRunnerBusy
	}
	r.running = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.running = false
		r.mu.Unlock()
	}()
	time.Sleep(25 * time.Millisecond)
	return []byte(`{"hosts":{"github.com":[{"state":"success","active":true,"host":"github.com","login":"sf-test"}]}}`), nil
}

func (r *integrationSerialRunner) Cleanup(context.Context) (githubclient.CleanupProof, error) {
	r.mu.Lock()
	r.cleanups++
	r.mu.Unlock()
	return githubclient.CleanupProof{Drained: true}, nil
}

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

func TestClientSerializesRunnerOwnershipPairs(t *testing.T) {
	runner := &integrationSerialRunner{}
	quarantine := &integrationQuarantine{}
	client, err := githubclient.NewClient("/bin/echo", t.TempDir(), t.TempDir(), runner, func(context.Context, domain.ExternalEffectClaim) error { return nil }, integrationGuard{}, integrationVerifier{}, integrationIntents{}, quarantine)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { results <- client.AuthStatus(context.Background()) }()
	go func() { results <- client.AuthStatus(context.Background()) }()
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent AuthStatus: %v", err)
		}
	}
	runner.mu.Lock()
	overlaps, cleanups := runner.overlaps, runner.cleanups
	runner.mu.Unlock()
	if overlaps != 0 || cleanups != 2 {
		t.Fatalf("runner ownership overlaps=%d cleanups=%d", overlaps, cleanups)
	}
	if quarantine.writes != 0 {
		t.Fatalf("concurrent ownership caused quarantine: %d", quarantine.writes)
	}
}

func TestClientDoesNotStealCleanupForBusyRunner(t *testing.T) {
	runner := &integrationBusyRunner{}
	client, err := githubclient.NewClient("/bin/echo", t.TempDir(), t.TempDir(), runner, func(context.Context, domain.ExternalEffectClaim) error { return nil }, integrationGuard{}, integrationVerifier{}, integrationIntents{}, &integrationQuarantine{})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.AuthStatus(context.Background()); !errors.Is(err, githubclient.ErrRunnerBusy) {
		t.Fatalf("busy runner error=%v", err)
	}
	runner.mu.Lock()
	cleanups := runner.cleanups
	runner.mu.Unlock()
	if cleanups != 0 {
		t.Fatalf("busy invocation stole %d cleanup calls", cleanups)
	}
}
