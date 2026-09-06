package repositoryexec

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

type resourceAuthority struct {
	cancellationAuthority
	resourceCalls int
	claim         contracts.RepositoryCommandClaim
	err           error
}

func (a *resourceAuthority) RetireObservedResourceLimitedRepositoryCommand(ctx context.Context, claim contracts.RepositoryCommandClaim) error {
	a.resourceCalls++
	a.claim = claim
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Second || ctx.Err() != nil {
		return errors.New("retirement must have a live bounded persistence context")
	}
	return a.err
}

func TestResourceRetirementUsesDedicatedAuthorityAndQuarantinesFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		a := &resourceAuthority{}
		if fail {
			a.err = errors.New("injected persistence error")
		}
		lease := &cancellationLease{}
		claim := contracts.RepositoryCommandClaim{SemanticKey: "exact-resource-claim", ClaimEpoch: 7}
		err := retireObservedResourceLimitedRepositoryCommand(a, lease, claim)
		if (err != nil) != fail || (fail && !errors.Is(err, a.err)) {
			t.Fatalf("retirement error: %v", err)
		}
		if a.resourceCalls != 1 || a.completeCalls != 0 || a.retireCalls != 0 || a.claim != claim {
			t.Fatalf("wrong authority dispatch: %+v", a)
		}
		if lease.released || lease.quarantined != fail {
			t.Fatalf("wrong lease disposition: %+v", lease)
		}
	}
	lease := &cancellationLease{}
	if err := retireObservedResourceLimitedRepositoryCommand(&cancellationAuthority{}, lease, contracts.RepositoryCommandClaim{}); !errors.Is(err, ErrInvalidBinding) || !lease.quarantined || lease.released {
		t.Fatalf("missing authority did not fail closed: %v %+v", err, lease)
	}
}
