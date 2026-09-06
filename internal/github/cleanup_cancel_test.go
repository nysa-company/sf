package github

import (
	"context"
	"errors"
	"testing"
	"time"
)

type canceledCommandRunner struct {
	cancel          context.CancelFunc
	cleanupErr      error
	cleanupDeadline time.Time
}

func (r *canceledCommandRunner) Run(context.Context, string, []string, []string) ([]byte, error) {
	r.cancel()
	return nil, context.Canceled
}

func (r *canceledCommandRunner) Cleanup(ctx context.Context) (CleanupProof, error) {
	r.cleanupErr = ctx.Err()
	r.cleanupDeadline, _ = ctx.Deadline()
	if r.cleanupErr != nil {
		return CleanupProof{}, r.cleanupErr
	}
	return CleanupProof{Drained: true}, nil
}

func TestCanceledGitHubCommandStillGetsBoundedCleanupProof(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &canceledCommandRunner{cancel: cancel}
	quarantined := false
	client := Client{binaryPath: "/bin/echo", home: t.TempDir(), configDir: t.TempDir(), runner: runner,
		quarantiner: cleanupQuarantinerFunc(func(context.Context) error { quarantined = true; return nil })}
	_, err := client.run(ctx, "auth", "status")
	if err == nil || errors.Is(err, ErrProcessCleanup) || errors.Is(err, ErrCleanupQuarantineFatal) {
		t.Fatalf("command cancellation must remain a command failure, not failed cleanup: %v", err)
	}
	if runner.cleanupErr != nil || runner.cleanupDeadline.IsZero() || time.Until(runner.cleanupDeadline) > 5*time.Second || quarantined {
		t.Fatalf("cleanup err=%v deadline=%v quarantined=%v", runner.cleanupErr, runner.cleanupDeadline, quarantined)
	}
}
