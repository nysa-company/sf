package store

import (
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestEstimatedPolicyPinsEveryRoleToTicketSnapshot(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx, "claude")
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "config-route")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-config-route", leader), leader, domain.StatePlanning)
	if err := db.ApproveProviderEstimatedAccounting(ctx, ticket.Ref, ticket.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}); err != nil {
		t.Fatal(err)
	}
	for role, expected := range map[string]string{"planner": "claude", "builder": "codex", "reviewer": "codex"} {
		for _, provider := range []string{"claude", "codex", "cursor"} {
			r := ProviderAttemptRequest{Ref: ticket.Ref, Role: role, ConfigDigest: digest, Binding: contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: provider}}}
			err := validateEstimatedProviderRoute(ctx, db.db, r)
			if provider == expected && err != nil || provider != expected && !errors.Is(err, ErrProviderPairRefused) {
				t.Fatalf("%s/%s: %v", role, provider, err)
			}
			r.ConfigDigest = "wrong"
			if err := validateEstimatedProviderRoute(ctx, db.db, r); !errors.Is(err, ErrProviderPairRefused) {
				t.Fatal("wrong snapshot digest accepted", err)
			}
		}
	}
}
