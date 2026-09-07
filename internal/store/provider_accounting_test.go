package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

func TestProviderCostEstimateIsImmutableAndNeverACharge(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown", true: "estimate"}[known], func(t *testing.T) {
			db, ctx := openTestStore(t)
			digest := setupProviderProject(t, db, ctx, "cursor")
			leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "estimate-record")
			if err != nil {
				t.Fatal(err)
			}
			ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-estimate-record", leader), leader, domain.StatePlanning)
			fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
			if err := db.ApproveProviderEstimatedAccounting(ctx, ticket.Ref, ticket.Version, fence); err != nil {
				t.Fatal(err)
			}
			// Credential-free protocol fixture; no real CLI or credentials used.
			builder, _, err := db.RecordProviderQualification(ctx, qualificationValue(strings.Repeat("a", 32), "cursor", "fixture-family-a", QualificationGuarded))
			if err != nil {
				t.Fatal(err)
			}
			reviewer, _, err := db.RecordProviderQualification(ctx, qualificationValue(strings.Repeat("b", 32), "fixture-reviewer", "fixture-family-b", QualificationGuarded))
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := db.SelectProviderPair(ctx, domain.ChannelDev, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			claim, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
			if err != nil {
				t.Fatal(err)
			}
			var estimate *int64
			if known {
				n := ticket.MaxCostMicroUSD
				estimate = &n
			}
			raw := contracts.PhaseResult{Provider: claim.Binding.Identity, ReportedCostEstimateMicroUSD: estimate, Artifact: []byte(`{"schema":"sf.planner/v1","acceptance":["a"],"proof":{"kind":"acceptance","command":["go","test"],"details":"d"},"paths":["internal"],"commands":[["go","test"]],"risks":["r"]}`)}
			if db.ProviderResultAccountingAccepted(ctx, claim, raw) {
				t.Fatal("missing observation accepted")
			}
			if err := db.RecordProviderCostEstimate(ctx, claim, contracts.DrainProof{}, estimate); !errors.Is(err, ErrProviderDrain) {
				t.Fatalf("unsigned drain=%v", err)
			}
			for i := 0; i < 2; i++ {
				if err := db.RecordProviderCostEstimate(ctx, claim, proof(t, claim), estimate); err != nil {
					t.Fatal(err)
				}
			}
			value, gotKnown, err := db.ProviderCostEstimate(ctx, claim)
			if err != nil || gotKnown != known || known && value != ticket.MaxCostMicroUSD {
				t.Fatalf("value=%d known=%v err=%v", value, gotKnown, err)
			}
			if !db.ProviderResultAccountingAccepted(ctx, claim, raw) {
				t.Fatal("recorded policy estimate refused")
			}
			bad := raw
			bad.UsageUnits = 1
			if db.ProviderResultAccountingAccepted(ctx, claim, bad) {
				t.Fatal("untrusted currency accepted")
			}
			changed := int64(8)
			bad = raw
			bad.ReportedCostEstimateMicroUSD = &changed
			if db.ProviderResultAccountingAccepted(ctx, claim, bad) {
				t.Fatal("mismatched result estimate accepted")
			}
			if err := db.RecordProviderCostEstimate(ctx, claim, proof(t, claim), &changed); !errors.Is(err, ErrEvidenceConflict) {
				t.Fatal("changed replay accepted", err)
			}
			var charge int64
			if err := db.db.QueryRowContext(ctx, `SELECT usage_units FROM provider_attempts WHERE id=?`, claim.ID).Scan(&charge); err != nil || charge != 0 {
				t.Fatal("estimate changed charges", err)
			}
			if _, err := db.db.ExecContext(ctx, `DELETE FROM provider_cost_estimates`); err == nil {
				t.Fatal("estimate deleted")
			}
			if known {
				if err := db.FinishProviderAttempt(ctx, claim, proof(t, claim), ticket.Version, fence, "failed", "failed", 0, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				_, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: ticket.Ref, ExpectedVersion: ticket.Version, Fence: fence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(builder), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()}))
				if !errors.Is(err, ErrBudgetExhausted) {
					t.Fatalf("estimate ceiling did not block next launch: %v", err)
				}
			} else {
				for i := 0; i < 2; i++ {
					if _, err := db.CompleteProviderAttemptSuccess(ctx, claim, proof(t, claim), ticket.Version, fence, raw, phaseartifact.Validation{TicketType: domain.TicketFeature}, time.Now().UTC()); err != nil {
						t.Fatal("estimated completion/replay", err)
					}
				}
				if _, _, err := db.LoadHistoricalProviderAttemptResult(ctx, ProviderAttemptResultKey{AttemptID: claim.ID, Ref: claim.Ref, Phase: claim.Phase, Attempt: claim.Attempt}); err != nil {
					t.Fatal("historical result", err)
				}
			}
		})
	}
}

func TestProviderAccountingRequiresExplicitPreAttemptChoice(t *testing.T) {
	db, ctx := openTestStore(t)
	digest := setupProviderProject(t, db, ctx)
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "accounting-policy")
	if err != nil {
		t.Fatal(err)
	}
	ticket := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-accounting", leader), leader, domain.StatePlanning)
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	if _, err := db.ProviderAccountingPolicy(ctx, ticket.Ref); !errors.Is(err, ErrNotFound) {
		t.Fatal("legacy ticket was opted in")
	}
	if err := db.ApproveProviderEstimatedAccounting(ctx, ticket.Ref, ticket.Version+1, fence); !errors.Is(err, ErrStaleFence) {
		t.Fatal("stale choice accepted")
	}
	if err := db.ApproveProviderEstimatedAccounting(ctx, ticket.Ref, ticket.Version, fence); err != nil {
		t.Fatal(err)
	}
	policy, err := db.ProviderAccountingPolicy(ctx, ticket.Ref)
	if err != nil || policy.RequestLimit != contracts.MultiCLIRequestLimit || policy.RequestTimeout != contracts.MultiCLIRequestTimeout || policy.EstimateLimitMicroUSD != ticket.MaxCostMicroUSD {
		t.Fatalf("policy=%+v err=%v", policy, err)
	}
	if err := db.ApproveProviderEstimatedAccounting(ctx, ticket.Ref, ticket.Version, fence); err != nil {
		t.Fatal("exact replay refused", err)
	}
	for _, statement := range []string{`UPDATE provider_accounting_policies SET request_limit=16`, `DELETE FROM provider_accounting_policies`} {
		if _, err := db.db.ExecContext(ctx, statement); err == nil {
			t.Fatal("immutable policy mutation accepted")
		}
	}
	other := providerState(t, db, ctx, setupProviderTicket(t, db, ctx, "SF-accounting-late", leader), leader, domain.StatePlanning)
	planner, _ := setupProviderPair(t, db, ctx)
	otherFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: other.RunnerEpoch}
	if _, err := db.BeginProviderAttempt(ctx, supervised(t, ProviderAttemptRequest{Ref: other.Ref, ExpectedVersion: other.Version, Fence: otherFence, Phase: domain.PhasePlanning, Role: "planner", Binding: runtime(planner), ConfigDigest: digest, Capacity: 1, At: time.Now().UTC()})); err != nil {
		t.Fatal(err)
	}
	if err := db.ApproveProviderEstimatedAccounting(ctx, other.Ref, other.Version, otherFence); !errors.Is(err, ErrProviderAttempt) {
		t.Fatalf("late accounting change=%v", err)
	}
}
