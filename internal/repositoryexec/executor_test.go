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

func TestCommandDigestAndSpecDigestChangeOnEveryLaunchInput(t *testing.T) {
	p, err := executionpolicy.NewCommandSnapshot([]string{"git", "status", "--short"})
	if err != nil {
		t.Fatal(err)
	}
	s := contracts.CommandSpec{Argv: []string{"git", "status", "--short"}, Directory: "/tmp/worktree", Timeout: 5}
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
	p, err := executionpolicy.NewCommandSnapshot([]string{"git", "status", "--short"})
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
