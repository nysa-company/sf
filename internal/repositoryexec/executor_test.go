package repositoryexec

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

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

type lifecycleAuthority struct {
	acquireCalls int
	retireCalls  int
	acquireErr   error
	returnNil    bool
	lease        *cancellationLease
}

func (a *lifecycleAuthority) AcquireRepositoryCommand(context.Context, contracts.RepositoryCommandClaim) (contracts.RepositoryCommandLease, error) {
	a.acquireCalls++
	if a.acquireErr != nil {
		if a.returnNil {
			return nil, a.acquireErr
		}
		a.lease = &cancellationLease{}
		return a.lease, a.acquireErr
	}
	if a.returnNil {
		return nil, nil
	}
	a.lease = &cancellationLease{}
	return a.lease, nil
}
func (a *lifecycleAuthority) RetireUnleasedRepositoryCommand(ctx context.Context, _ contracts.RepositoryCommandClaim) error {
	a.retireCalls++
	return ctx.Err()
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

func TestRunRetiresEveryPreAcquireValidationFailure(t *testing.T) {
	policy, err := executionpolicy.NewCommandSnapshot([]string{"go", "test", "./..."})
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), "missing")
	spec := contracts.CommandSpec{Argv: []string{"go", "test", "./..."}, Directory: directory, Timeout: time.Second, Profile: contracts.ProfileGuarded}
	commandDigest, err := CommandDigest(spec.Argv)
	if err != nil {
		t.Fatal(err)
	}
	specDigest, err := SpecDigest(spec, "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	if err != nil {
		t.Fatal(err)
	}
	claim := contracts.RepositoryCommandClaim{TicketRef: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "t"}, Repository: directory, Worktree: directory, CommandDigest: commandDigest, SpecDigest: specDigest, PolicyDigest: policy.Digest()}
	cases := map[string]func(*contracts.CommandSpec, *contracts.RepositoryCommandClaim){
		"profile": func(s *contracts.CommandSpec, _ *contracts.RepositoryCommandClaim) {
			s.Profile = contracts.ProfileAutonomous
		},
		"command digest": func(_ *contracts.CommandSpec, c *contracts.RepositoryCommandClaim) {
			c.CommandDigest = "sha256:" + strings.Repeat("f", 64)
		},
		"stdin":       func(s *contracts.CommandSpec, _ *contracts.RepositoryCommandClaim) { s.Stdin = bytes.NewReader(nil) },
		"spec digest": func(s *contracts.CommandSpec, _ *contracts.RepositoryCommandClaim) { s.Timeout += time.Second },
		"policy": func(s *contracts.CommandSpec, _ *contracts.RepositoryCommandClaim) {
			// Keep the command digest/spec valid so this reaches the policy
			// admission rather than being mistaken for a binding failure.
			s.Argv = []string{"go", "test", "./internal/..."}
		},
		"path": func(_ *contracts.CommandSpec, c *contracts.RepositoryCommandClaim) {
			c.Worktree = filepath.Join(directory, "changed")
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			authority := &lifecycleAuthority{}
			requestSpec, requestClaim := spec, claim
			mutate(&requestSpec, &requestClaim)
			if name == "policy" {
				requestClaim.CommandDigest, _ = CommandDigest(requestSpec.Argv)
				requestClaim.SpecDigest, _ = SpecDigest(requestSpec, "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
			}
			_, runErr := (Executor{Authority: authority}).Run(context.Background(), Request{Claim: requestClaim, Spec: requestSpec, Policy: policy})
			if runErr == nil || authority.acquireCalls != 0 || authority.retireCalls != 1 {
				t.Fatalf("err=%v acquire=%d retire=%d", runErr, authority.acquireCalls, authority.retireCalls)
			}
		})
	}
}

func TestRunDoesNotRetireAfterAcquireError(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository supervisor is Darwin-only")
	}
	directory := t.TempDir()
	directory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte("module example.test\n\ngo 1.23\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, err := executionpolicy.NewCommandSnapshot([]string{"go", "test", "./..."})
	if err != nil {
		t.Fatal(err)
	}
	spec := contracts.CommandSpec{Argv: []string{"go", "test", "./..."}, Directory: directory, Timeout: time.Second, Profile: contracts.ProfileGuarded}
	commandDigest, _ := CommandDigest(spec.Argv)
	specDigest, _ := SpecDigest(spec, "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	authority := &lifecycleAuthority{}
	claim := contracts.RepositoryCommandClaim{TicketRef: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "t"}, Repository: directory, Worktree: directory, CommandDigest: commandDigest, SpecDigest: specDigest, PolicyDigest: policy.Digest()}
	_, runErr := (Executor{Authority: authority}).Run(context.Background(), Request{Claim: claim, Spec: spec, Policy: policy})
	if runErr == nil || authority.acquireCalls != 1 || authority.retireCalls != 0 {
		t.Fatalf("err=%v acquire=%d retire=%d", runErr, authority.acquireCalls, authority.retireCalls)
	}
}

func TestRunRetiresWhenAcquireReturnsBeforeLease(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository supervisor is Darwin-only")
	}
	directory := t.TempDir()
	directory, err := filepath.EvalSymlinks(directory)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte("module example.test\n\ngo 1.23\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, err := executionpolicy.NewCommandSnapshot([]string{"go", "test", "./..."})
	if err != nil {
		t.Fatal(err)
	}
	spec := contracts.CommandSpec{Argv: []string{"go", "test", "./..."}, Directory: directory, Timeout: time.Second, Profile: contracts.ProfileGuarded}
	commandDigest, _ := CommandDigest(spec.Argv)
	specDigest, _ := SpecDigest(spec, "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	authority := &lifecycleAuthority{acquireErr: errors.New("acquire rejected"), returnNil: true}
	claim := contracts.RepositoryCommandClaim{TicketRef: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "t"}, Repository: directory, Worktree: directory, CommandDigest: commandDigest, SpecDigest: specDigest, PolicyDigest: policy.Digest()}
	_, runErr := (Executor{Authority: authority}).Run(context.Background(), Request{Claim: claim, Spec: spec, Policy: policy})
	if runErr == nil || authority.acquireCalls != 1 || authority.retireCalls != 1 {
		t.Fatalf("err=%v acquire=%d retire=%d", runErr, authority.acquireCalls, authority.retireCalls)
	}
}

func TestRunTreatsNilLeaseAsAcquireAmbiguity(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository supervisor is Darwin-only")
	}
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte("module example.test\n\ngo 1.23\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, err := executionpolicy.NewCommandSnapshot([]string{"go", "test", "./..."})
	if err != nil {
		t.Fatal(err)
	}
	spec := contracts.CommandSpec{Argv: []string{"go", "test", "./..."}, Directory: directory, Timeout: time.Second, Profile: contracts.ProfileGuarded}
	commandDigest, _ := CommandDigest(spec.Argv)
	specDigest, _ := SpecDigest(spec, "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	claim := contracts.RepositoryCommandClaim{TicketRef: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "t"}, Repository: directory, Worktree: directory, CommandDigest: commandDigest, SpecDigest: specDigest, PolicyDigest: policy.Digest()}
	authority := &lifecycleAuthority{returnNil: true}
	_, runErr := (Executor{Authority: authority}).Run(context.Background(), Request{Claim: claim, Spec: spec, Policy: policy})
	if !errors.Is(runErr, ErrInvalidBinding) || authority.acquireCalls != 1 || authority.retireCalls != 0 {
		t.Fatalf("err=%v acquire=%d retire=%d", runErr, authority.acquireCalls, authority.retireCalls)
	}
}

func TestRunTreatsLeaseAndAcquireErrorAsAmbiguous(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("repository supervisor is Darwin-only")
	}
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte("module example.test\n\ngo 1.23\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, err := executionpolicy.NewCommandSnapshot([]string{"go", "test", "./..."})
	if err != nil {
		t.Fatal(err)
	}
	spec := contracts.CommandSpec{Argv: []string{"go", "test", "./..."}, Directory: directory, Timeout: time.Second, Profile: contracts.ProfileGuarded}
	commandDigest, _ := CommandDigest(spec.Argv)
	specDigest, _ := SpecDigest(spec, "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	claim := contracts.RepositoryCommandClaim{TicketRef: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "t"}, Repository: directory, Worktree: directory, CommandDigest: commandDigest, SpecDigest: specDigest, PolicyDigest: policy.Digest()}
	authority := &lifecycleAuthority{acquireErr: errors.New("response lost"), returnNil: false}
	_, runErr := (Executor{Authority: authority}).Run(context.Background(), Request{Claim: claim, Spec: spec, Policy: policy})
	if runErr == nil || authority.acquireCalls != 1 || authority.retireCalls != 0 {
		t.Fatalf("err=%v acquire=%d retire=%d", runErr, authority.acquireCalls, authority.retireCalls)
	}
	if authority.lease == nil || !authority.lease.quarantined {
		t.Fatal("ambiguous acquired lease was not quarantined")
	}
}
