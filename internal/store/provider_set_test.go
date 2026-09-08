package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func TestProviderSetSelectionIsAtomicAndReplaysAllRoles(t *testing.T) {
	database, ctx := openTestStore(t)
	record := func(digit, provider, family string) ProviderQualification {
		t.Helper()
		value, _, err := database.RecordProviderQualification(ctx, qualificationValue(strings.Repeat(digit, 32), provider, family, QualificationGuarded))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	builder := record("1", "fixture-builder", "builder-family")
	reviewer := record("2", "fixture-reviewer", "reviewer-family")
	planner := record("3", "fixture-planner", "planner-family")
	now := time.Now().UTC()
	first, changed, err := database.SelectProviderPair(ctx, domain.ChannelDev, builder.ID, reviewer.ID, now)
	if err != nil || !changed || first.Planner.ID != builder.ID {
		t.Fatalf("initial selection: changed=%v err=%v", changed, err)
	}
	// The reversed pair is valid, but an absent planner must prevent all writes.
	if _, changed, err = database.SelectProviderSet(ctx, domain.ChannelDev, planner.ID+1000, reviewer.ID, builder.ID, now.Add(time.Second)); !errors.Is(err, ErrProviderPairRefused) || changed {
		t.Fatalf("invalid planner: changed=%v err=%v", changed, err)
	}
	loaded, err := database.ProviderPair(ctx, domain.ChannelDev)
	if err != nil || loaded.Planner.ID != builder.ID || loaded.Builder.ID != builder.ID || loaded.Reviewer.ID != reviewer.ID || !loaded.SelectedAt.Equal(now) {
		t.Fatalf("failed selection changed previous roles: %+v err=%v", loaded, err)
	}
	stamp := now.Add(2 * time.Second)
	selected, changed, err := database.SelectProviderSet(ctx, domain.ChannelDev, planner.ID, builder.ID, reviewer.ID, stamp)
	if err != nil || !changed || selected.Planner.ID != planner.ID {
		t.Fatalf("planner-only change: changed=%v err=%v", changed, err)
	}
	replay, changed, err := database.SelectProviderSet(ctx, domain.ChannelDev, planner.ID, builder.ID, reviewer.ID, stamp.Add(time.Second))
	if err != nil || changed || replay.Planner.ID != planner.ID || !replay.SelectedAt.Equal(stamp) {
		t.Fatalf("replay: changed=%v err=%v", changed, err)
	}
	loaded, err = database.ProviderPair(ctx, domain.ChannelDev)
	if err != nil || loaded.Planner.ID != planner.ID || loaded.Builder.ID != builder.ID || loaded.Reviewer.ID != reviewer.ID {
		t.Fatalf("loaded selection mismatch: %+v err=%v", loaded, err)
	}
	if _, changed, err = database.SelectProviderPair(ctx, domain.ChannelDev, builder.ID, reviewer.ID, stamp.Add(2*time.Second)); err != nil || !changed {
		t.Fatalf("pair must explicitly restore planner=builder: changed=%v err=%v", changed, err)
	}
}

func TestProviderSetInvalidPlannerDoesNotCreateSelection(t *testing.T) {
	database, ctx := openTestStore(t)
	builder, _, err := database.RecordProviderQualification(ctx, qualificationValue(strings.Repeat("1", 32), "fixture-builder", "builder-family", QualificationGuarded))
	if err != nil {
		t.Fatal(err)
	}
	reviewer, _, err := database.RecordProviderQualification(ctx, qualificationValue(strings.Repeat("2", 32), "fixture-reviewer", "reviewer-family", QualificationGuarded))
	if err != nil {
		t.Fatal(err)
	}
	if _, changed, err := database.SelectProviderSet(ctx, domain.ChannelDev, 9999, builder.ID, reviewer.ID, time.Now()); !errors.Is(err, ErrProviderPairRefused) || changed {
		t.Fatalf("invalid set: changed=%v err=%v", changed, err)
	}
	if _, err := database.ProviderPair(ctx, domain.ChannelDev); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed selection created a row: %v", err)
	}
}
