package providercoord

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

func TestEstimatedProviderCapacityRetainsOtherTicketAcrossReopenAndCancel(t *testing.T) {
	ctx := context.Background()
	db, initial, coordinator, provider, path := estimatedRetryFixture(t)
	requests := []Request{initial}
	for _, id := range []domain.TicketID{"SF-capacity-second", "SF-capacity-third"} {
		r := initial
		r.Input.Ticket.Ticket = id
		if err := db.CreateTicket(ctx, store.Ticket{Ref: r.Input.Ticket, SourceDigest: string(id), Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
			t.Fatal(err)
		}
		branch := "dev/p/" + string(id)
		ticket, err := db.StartOrAdopt(ctx, r.Input.Ticket, 1, branch, r.Fence)
		if err != nil {
			t.Fatal(err)
		}
		r.ExpectedVersion, r.Fence.RunnerEpoch = ticket.Version, ticket.RunnerEpoch
		r.Input.Worktree = filepath.Join(t.TempDir(), string(id))
		if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: r.Fence, Path: r.Input.Worktree, Branch: branch, IdentityJSON: []byte(r.Input.WorktreeIdentity), BaseSHA: r.Input.BaseSHA, HeadSHA: strings.Repeat("b", 40)}); err != nil {
			t.Fatal(err)
		}
		if err := db.ApproveProviderEstimatedAccounting(ctx, ticket.Ref, ticket.Version, r.Fence); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, r)
	}
	begin := func(r Request) (store.ProviderAttemptClaim, error) {
		r.Input.Provider, r.Input.AuthMode = provider.binding.Identity, provider.binding.AuthMode
		r.Input.LeaderEpoch, r.Input.RunnerEpoch, r.Input.ExpectedVersion = r.Fence.LeaderEpoch, r.Fence.RunnerEpoch, r.ExpectedVersion
		return db.BeginProviderAttempt(ctx, store.ProviderAttemptRequest{Ref: r.Input.Ticket, ExpectedVersion: r.ExpectedVersion, Fence: r.Fence, Phase: r.Input.Phase, Role: string(r.Role), Binding: provider.binding, ConfigDigest: r.ConfigDigest, Capacity: 2, At: time.Now().UTC(), Repository: r.Input.Repository, Worktree: r.Input.Worktree, WorktreeIdentity: r.Input.WorktreeIdentity, BaseSHA: r.Input.BaseSHA, SupervisorKey: coordinator.supervisor.PublicKey(), Input: r.Input})
	}
	first, err := begin(requests[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := begin(requests[1])
	if err != nil {
		t.Fatal(err)
	}
	// Store capacity is account-scoped even across independently qualified
	// model profiles. This does not assert live daemon runtime replacement.
	pair, err := db.ProviderPair(ctx, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	provider.binding.Identity.Model = "fixture-second-model"
	binding := provider.binding
	created, run := time.Now().UTC(), strings.Repeat("f", 32)
	process := coordinator.supervisor.(*testkit.Supervisor)
	attestation, err := process.Signer.SignQualification(contracts.QualificationAttestation{Channel: domain.ChannelDev, RunID: run, Identity: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, ProbeDigest: strings.Repeat("e", 64), Profile: contracts.ProfileGuarded, CreatedUnixNanos: created.UnixNano(), LeaderEpoch: initial.Fence.LeaderEpoch, Nonce: run})
	if err != nil {
		t.Fatal(err)
	}
	qualification, _, err := db.RecordAttestedProviderQualification(ctx, store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, ProbeDigest: attestation.ProbeDigest, Profile: store.QualificationGuarded, CreatedAt: created}, attestation)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, qualification.ID, qualification.ID, pair.Reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := begin(requests[2]); !errors.Is(err, store.ErrProviderCapacity) {
		t.Fatalf("third admission=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := begin(requests[2]); !errors.Is(err, store.ErrProviderCapacity) {
		t.Fatalf("reopen reset capacity: %v", err)
	}
	if attempts, err := db.ProviderAttempts(ctx, requests[2].Input.Ticket); err != nil || len(attempts) != 0 {
		t.Fatal("capacity refusal consumed attempt", err)
	}
	finish := func(claim store.ProviderAttemptClaim) {
		t.Helper()
		proof, err := coordinator.supervisor.Drain(ctx, drainRequest(claim))
		if err != nil {
			t.Fatal(err)
		}
		if err := db.RecordProviderCostEstimate(ctx, claim, proof, nil); err != nil {
			t.Fatal(err)
		}
		if err := db.FinishProviderAttempt(ctx, claim, proof, claim.ExpectedVersion, domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch}, "cancelled", "cancelled", 0, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	}
	finish(first)
	third, err := begin(requests[2])
	if err != nil {
		t.Fatal(err)
	}
	if third.Binding.Identity.Model == second.Binding.Identity.Model || third.Binding.AuthDigest != second.Binding.AuthDigest {
		t.Fatal("fixture must share an account across distinct model profiles")
	}
	active, err := db.ActiveProviderAttempts(ctx, domain.ChannelDev)
	if err != nil || len(active) != 2 {
		t.Fatalf("active count=%d err=%v", len(active), err)
	}
	seen := map[int64]bool{}
	for _, claim := range active {
		seen[claim.ID] = true
	}
	if !seen[second.ID] || !seen[third.ID] || seen[first.ID] {
		t.Fatal("cancellation released another ticket's slot")
	}
	finish(second)
	finish(third)
	if active, err := db.ActiveProviderAttempts(ctx, domain.ChannelDev); err != nil || len(active) != 0 {
		t.Fatal("retained active claims", err)
	}
}
