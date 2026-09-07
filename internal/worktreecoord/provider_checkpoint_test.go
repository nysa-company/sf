package worktreecoord

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// These tests isolate sequencing; physical Git refusal is exercised by the
// real-Git AuthenticateExistingRegisteredWorktree suite, and exact claim/phase
// authority by Store's ProviderAttemptCheckpoint suite.
func TestProviderCheckpointInspectionSandwich(t *testing.T) {
	for _, mode := range []string{"valid", "first refusal", "dirty", "different registration", "late revocation", "changed checkpoint", "cancel during inspection"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "fixture", Ticket: "SF-checkpoint"}
			claim := store.ProviderAttemptClaim{ID: 1, Ref: ref, Phase: domain.PhasePlanning, ExpectedVersion: 2, LeaderEpoch: 3, RunnerEpoch: 4, Worktree: "/private/fixture", RequestDigest: strings.Repeat("a", 64)}
			checkpoint := store.ProviderAttemptCheckpoint{AttemptID: claim.ID, ProviderRetryWorktreeProof: store.ProviderRetryWorktreeProof{Ref: ref, Phase: claim.Phase, Version: claim.ExpectedVersion, Fence: domain.Fence{LeaderEpoch: claim.LeaderEpoch, RunnerEpoch: claim.RunnerEpoch}, Worktree: store.StoredWorktree{Path: claim.Worktree}, ExpectedHead: strings.Repeat("b", 40)}}
			loads, inspections := 0, 0
			load := func(context.Context, store.ProviderAttemptClaim) (store.ProviderAttemptCheckpoint, error) {
				loads++
				if mode == "first refusal" || (loads == 2 && mode == "late revocation") {
					return store.ProviderAttemptCheckpoint{}, store.ErrStaleFence
				}
				got := checkpoint
				if loads == 2 && mode == "changed checkpoint" {
					got.ExpectedHead = strings.Repeat("c", 40)
				}
				return got, nil
			}
			inspect := func(_ context.Context, gotRef domain.TicketRef, head string) (store.StoredWorktree, error) {
				inspections++
				if gotRef != ref || head != checkpoint.ExpectedHead {
					t.Fatal("physical inspection did not receive Store head")
				}
				if mode == "dirty" {
					return store.StoredWorktree{}, ErrUnready
				}
				got := checkpoint.Worktree
				if mode == "different registration" {
					got.Path += "/other"
				}
				if mode == "cancel during inspection" {
					cancel()
				}
				return got, nil
			}
			got, err := inspectProviderAttemptCheckpoint(ctx, claim, load, inspect)
			if mode == "valid" {
				if err != nil || got.Checkpoint.AttemptID != claim.ID || len(got.Digest) != 64 || loads != 2 || inspections != 1 {
					t.Fatal("valid inspection failed")
				}
				return
			}
			if err == nil || got.Digest != "" || got.Checkpoint.AttemptID != 0 {
				t.Fatal("uncertain inspection produced evidence")
			}
			if mode == "first refusal" && inspections != 0 {
				t.Fatal("physical inspection preceded authority")
			}
			if mode == "dirty" && !errors.Is(err, ErrUnready) {
				t.Fatal("dirty refusal lost")
			}
		})
	}
}

func TestProviderCheckpointInspectionRequiresStore(t *testing.T) {
	if got, err := (Coordinator{}).InspectProviderAttemptCheckpoint(context.Background(), store.ProviderAttemptClaim{}); err == nil || got.Digest != "" {
		t.Fatal("missing Store accepted")
	}
}
