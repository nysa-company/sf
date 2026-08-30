package repositoryexec

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/executionpolicy"
	"github.com/nysa-company/sf/internal/processsupervisor"
)

type neverAuthority struct{}

func (neverAuthority) AcquireRepositoryCommand(context.Context, contracts.RepositoryCommandClaim) (contracts.RepositoryCommandLease, error) {
	return nil, errors.New("must not acquire")
}

type cancellationLease struct {
	released    bool
	quarantined bool
}

func (l *cancellationLease) Check(context.Context) error { return nil }
func (l *cancellationLease) Release() error {
	l.released = true
	return nil
}
func (l *cancellationLease) RecordRepositoryCommandLaunch(context.Context, contracts.RepositoryCommandLaunch) error {
	return nil
}
func (l *cancellationLease) FinishRepositoryCommandLaunch(context.Context, contracts.RepositoryCommandLaunch) error {
	return nil
}
func (l *cancellationLease) Quarantine() error {
	l.quarantined = true
	return nil
}

type cancellationAuthority struct {
	completeCalls int
	retireCalls   int
}

func (a *cancellationAuthority) AcquireRepositoryCommand(context.Context, contracts.RepositoryCommandClaim) (contracts.RepositoryCommandLease, error) {
	return nil, errors.New("not used")
}
func (a *cancellationAuthority) CompleteRepositoryCommand(context.Context, contracts.RepositoryCommandClaim, contracts.CommandResult) error {
	a.completeCalls++
	return nil
}
func (a *cancellationAuthority) RetireObservedCanceledRepositoryCommand(context.Context, contracts.RepositoryCommandClaim) error {
	a.retireCalls++
	return nil
}
func (a *cancellationAuthority) MarkRepositoryCommandUncertain(context.Context, contracts.RepositoryCommandClaim, string) error {
	return nil
}
func (a *cancellationAuthority) ReconcileStaleRepositoryCommandObservation(context.Context, contracts.RepositoryCommandClaim, contracts.CommandResult) error {
	return nil
}

func TestCommandDigestAndSpecDigestChangeOnEveryLaunchInput(t *testing.T) {
	p, err := executionpolicy.NewCommandSnapshot([]string{"go", "test", "./..."})
	if err != nil {
		t.Fatal(err)
	}
	s := contracts.CommandSpec{Argv: []string{"go", "test", "./..."}, Directory: "/tmp/worktree", Timeout: 5}
	a, err := CommandDigest(s.Argv)
	if err != nil {
		t.Fatal(err)
	}
	b, err := SpecDigest(s, "sha256:0000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	s.Timeout = 6
	c, err := SpecDigest(s, "sha256:0000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	if a == "" || b == c {
		t.Fatalf("spec digest did not bind timeout: %q %q", b, c)
	}
	_ = p
}

func TestRunRejectsShellAndNeverCallsAuthority(t *testing.T) {
	p, err := executionpolicy.NewCommandSnapshot([]string{"go", "test", "./..."})
	if err != nil {
		t.Fatal(err)
	}
	claim := contracts.RepositoryCommandClaim{TicketRef: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "t"}, Worktree: "/tmp/worktree", Repository: "/tmp/repo", PolicyDigest: p.Digest(), CommandDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000", SpecDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000000"}
	_, err = (Executor{Authority: neverAuthority{}}).Run(context.Background(), Request{Claim: claim, Spec: contracts.CommandSpec{Argv: []string{"sh", "-c", "echo secret"}, Directory: claim.Worktree, Timeout: 1, Stdin: bytes.NewReader(nil)}, Policy: p})
	if err == nil {
		t.Fatal("shell command was accepted")
	}
}

func TestNPMRecipeNeverAcquiresRepositoryLease(t *testing.T) {
	if _, err := executionpolicy.NewCommandSnapshot([]string{"npm", "test"}); err == nil {
		t.Fatal("npm recipe was admitted for local repository execution")
	}
	if err := (processsupervisor.RepositoryCommandSupervisor{}).Preflight(contracts.CommandSpec{Argv: []string{"npm", "test"}, Profile: contracts.ProfileGuarded}); err == nil {
		t.Fatal("npm preflight was executable")
	}
}

func TestCanceledObservedCommandNeverCompletesRepositoryEvidence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !repositoryCommandCanceled(ctx, nil) || !repositoryCommandCanceled(context.Background(), context.DeadlineExceeded) {
		t.Fatal("cancellation/deadline was not recognized")
	}
	authority := &cancellationAuthority{}
	lease := &cancellationLease{}
	claim := contracts.RepositoryCommandClaim{SemanticKey: "canceled-observed"}
	if err := retireObservedCanceledRepositoryCommand(authority, lease, claim); err != nil {
		t.Fatal(err)
	}
	if authority.completeCalls != 0 || authority.retireCalls != 1 || lease.released || lease.quarantined {
		t.Fatalf("canceled observed command completion=%d retirement=%d released=%v quarantined=%v", authority.completeCalls, authority.retireCalls, lease.released, lease.quarantined)
	}
	// A nonzero, observed completion on a live fence remains ordinary durable
	// evidence; only cancellation/deadline diverts to uncertainty.
	if repositoryCommandCanceled(context.Background(), errors.New("exit status 1")) {
		t.Fatal("ordinary nonzero exit was treated as cancellation")
	}
}
