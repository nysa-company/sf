package git

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

type commitContentionAuthority struct {
	calls          int
	first          contracts.GitMutationClaim
	err            error
	leaseWithError bool
}

func (a *commitContentionAuthority) AcquireGitMutation(_ context.Context, claim contracts.GitMutationClaim) (contracts.GitMutationLease, error) {
	a.calls++
	if a.calls == 1 {
		a.first = claim
		if a.leaseWithError {
			return testMutationLease{}, a.err
		}
		return nil, a.err
	}
	if claim != a.first {
		return nil, errors.New("retry changed immutable claim")
	}
	return testMutationLease{}, nil
}

func TestGitCommitWaitsOnlyForDefiniteContention(t *testing.T) {
	for _, tc := range []struct {
		name, operation string
		err             error
		leaseWithError  bool
		calls           int
		ok              bool
	}{
		{"commit waits", "commit", contracts.ErrGitMutationContended, false, 2, true},
		{"protected proof waits", "protected-ref-fetch", contracts.ErrGitMutationContended, false, 2, true},
		{"protected proof lost response", "protected-ref-fetch", errors.New("lost acquisition response"), false, 1, false},
		{"lost response", "commit", errors.New("lost acquisition response"), false, 1, false},
		{"lease and error", "commit", contracts.ErrGitMutationContended, true, 1, false},
		{"creation remains immediate", "create-worktree", contracts.ErrGitMutationContended, false, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claim, _ := testClaim(t.Context(), contracts.GitMutationClaim{Repository: "/tmp/repository", Worktree: "/tmp/worktree", Branch: "sf/dev/test", Operation: tc.operation, BaseRef: "main", ExpectedBaseOID: strings.Repeat("a", 40), ExpectedHeadOID: strings.Repeat("b", 40)})
			authority := &commitContentionAuthority{err: tc.err, leaseWithError: tc.leaseWithError}
			lease, err := (Runner{MutationAuthority: authority}).acquireSuppliedMutation(t.Context(), claim, claim)
			if (err == nil) != tc.ok || authority.calls != tc.calls || (tc.ok && lease == nil) {
				t.Fatalf("lease=%T err=%v calls=%d", lease, err, authority.calls)
			}
		})
	}
}

func TestGitCommitContentionHonorsCallerDeadline(t *testing.T) {
	claim, _ := testClaim(t.Context(), contracts.GitMutationClaim{Repository: "/tmp/repository", Worktree: "/tmp/worktree", Branch: "sf/dev/test", Operation: "commit", BaseRef: "main", ExpectedBaseOID: strings.Repeat("a", 40), ExpectedHeadOID: strings.Repeat("b", 40)})
	authority := &commitContentionAuthority{err: contracts.ErrGitMutationContended}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	lease, err := (Runner{MutationAuthority: authority}).acquireSuppliedMutation(ctx, claim, claim)
	if lease != nil || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, contracts.ErrGitMutationContended) || authority.calls != 1 {
		t.Fatalf("lease=%T err=%v calls=%d", lease, err, authority.calls)
	}
}
