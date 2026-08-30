package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

func TestProviderQualificationIsExactSanitizedAndIdempotent(t *testing.T) {
	database, ctx := openTestStore(t)
	input := qualificationValue("11111111111111111111111111111111", "cursor", "cursor-family", QualificationGuarded)
	first, created, err := database.RecordProviderQualification(ctx, input)
	if err != nil || !created || first.ID <= 0 || first.Profile != QualificationGuarded || first.FailedProbes == nil {
		t.Fatalf("first=%+v created=%v err=%v", first, created, err)
	}
	replay, created, err := database.RecordProviderQualification(ctx, input)
	if err != nil || created || replay.ID != first.ID || !replay.CreatedAt.Equal(input.CreatedAt) {
		t.Fatalf("replay=%+v created=%v err=%v", replay, created, err)
	}
	loaded, err := database.LatestProviderQualification(ctx, domain.ChannelDev, input.Provider)
	if err != nil || loaded.ID != first.ID || loaded.BinaryDigest != input.BinaryDigest || loaded.PolicyDigest != input.PolicyDigest || loaded.FixtureDigest != input.FixtureDigest {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	conflict := input
	conflict.Provider.Model = "different-model"
	if _, _, err := database.RecordProviderQualification(ctx, conflict); !errors.Is(err, ErrQualificationConflict) {
		t.Fatalf("conflict err=%v", err)
	}
	var rows int
	if err := database.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_qualifications WHERE channel='dev'`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rows=%d err=%v", rows, err)
	}
}

func TestProviderPairRequiresCurrentPassingIndependentFamilies(t *testing.T) {
	database, ctx := openTestStore(t)
	builder, _, err := database.RecordProviderQualification(ctx, qualificationValue("11111111111111111111111111111111", "cursor", "cursor-family", QualificationGuarded))
	if err != nil {
		t.Fatal(err)
	}
	reviewer, _, err := database.RecordProviderQualification(ctx, qualificationValue("22222222222222222222222222222222", "claude", "claude-family", QualificationGuarded))
	if err != nil {
		t.Fatal(err)
	}
	selectedAt := time.Date(2026, 8, 29, 18, 0, 0, 123, time.UTC)
	pair, changed, err := database.SelectProviderPair(ctx, domain.ChannelDev, builder.ID, reviewer.ID, selectedAt)
	if err != nil || !changed || pair.Builder.ID != builder.ID || pair.Reviewer.ID != reviewer.ID || !pair.SelectedAt.Equal(selectedAt) {
		t.Fatalf("pair=%+v changed=%v err=%v", pair, changed, err)
	}
	replay, changed, err := database.SelectProviderPair(ctx, domain.ChannelDev, builder.ID, reviewer.ID, selectedAt.Add(time.Hour))
	if err != nil || changed || !replay.SelectedAt.Equal(selectedAt) {
		t.Fatalf("replay=%+v changed=%v err=%v", replay, changed, err)
	}
	loaded, err := database.ProviderPair(ctx, domain.ChannelDev)
	if err != nil || loaded.Builder.Provider != builder.Provider || loaded.Reviewer.Provider != reviewer.Provider || !loaded.SelectedAt.Equal(selectedAt) {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	if _, err := database.ProviderPair(ctx, domain.ChannelStable); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stable pair err=%v", err)
	}
}

func TestNewVerdictInvalidatesPairAndStaleOrDisabledRecordsCannotBeSelected(t *testing.T) {
	database, ctx := openTestStore(t)
	builderInput := qualificationValue("11111111111111111111111111111111", "cursor", "cursor-family", QualificationGuarded)
	builder, _, _ := database.RecordProviderQualification(ctx, builderInput)
	reviewerInput := qualificationValue("22222222222222222222222222222222", "claude", "claude-family", QualificationGuarded)
	reviewer, _, _ := database.RecordProviderQualification(ctx, reviewerInput)
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	disabledInput := builderInput
	disabledInput.RunID = "33333333333333333333333333333333"
	disabledInput.Profile = QualificationDisabled
	disabledInput.FailedProbes = []string{"network", "parent"}
	disabledInput.ReasonCode = "hostile_fixture_failed"
	disabledInput.CreatedAt = builderInput.CreatedAt.Add(time.Minute)
	disabled, created, err := database.RecordProviderQualification(ctx, disabledInput)
	if err != nil || !created || disabled.FailedProbes[0] != "network" || disabled.FailedProbes[1] != "parent" {
		t.Fatalf("disabled=%+v created=%v err=%v", disabled, created, err)
	}
	if _, err := database.ProviderPair(ctx, domain.ChannelDev); !errors.Is(err, ErrNotFound) {
		t.Fatalf("invalidated pair err=%v", err)
	}
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, builder.ID, reviewer.ID, time.Now().UTC()); !errors.Is(err, ErrProviderPairRefused) {
		t.Fatalf("stale selection err=%v", err)
	}
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, disabled.ID, reviewer.ID, time.Now().UTC()); !errors.Is(err, ErrProviderPairRefused) {
		t.Fatalf("disabled selection err=%v", err)
	}
}

func TestFailedProviderVersionDisablesOtherModelsUntilFreshPass(t *testing.T) {
	database, ctx := openTestStore(t)
	firstModel := qualificationValue("11111111111111111111111111111111", "codex", "family-one", QualificationGuarded)
	firstModel.Provider.Model = "model-one"
	first, _, _ := database.RecordProviderQualification(ctx, firstModel)
	reviewerInput := qualificationValue("22222222222222222222222222222222", "claude", "review-family", QualificationGuarded)
	reviewer, _, _ := database.RecordProviderQualification(ctx, reviewerInput)
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, first.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	failure := qualificationValue("33333333333333333333333333333333", "codex", "family-two", QualificationDisabled)
	failure.Provider.Model = "model-two"
	failure.FailedProbes = []string{"network"}
	failure.ReasonCode = "hostile_fixture_failed"
	failure.CreatedAt = firstModel.CreatedAt.Add(time.Minute)
	if _, _, err := database.RecordProviderQualification(ctx, failure); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ProviderPair(ctx, domain.ChannelDev); !errors.Is(err, ErrNotFound) {
		t.Fatalf("globally disabled pair err=%v", err)
	}
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, first.ID, reviewer.ID, time.Now().UTC()); !errors.Is(err, ErrProviderPairRefused) {
		t.Fatalf("older model survived provider/version failure: %v", err)
	}
	fresh := firstModel
	fresh.RunID = "44444444444444444444444444444444"
	fresh.CreatedAt = failure.CreatedAt.Add(time.Minute)
	freshRecord, _, err := database.RecordProviderQualification(ctx, fresh)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, freshRecord.ID, reviewer.ID, time.Now().UTC()); err != nil {
		t.Fatalf("fresh pass did not re-enable provider/version: %v", err)
	}
}

func TestSameFamilyAndCrossChannelPairsAreRefused(t *testing.T) {
	database, ctx := openTestStore(t)
	first, _, _ := database.RecordProviderQualification(ctx, qualificationValue("11111111111111111111111111111111", "cursor", "shared-family", QualificationGuarded))
	second, _, _ := database.RecordProviderQualification(ctx, qualificationValue("22222222222222222222222222222222", "claude", "shared-family", QualificationGuarded))
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, first.ID, second.ID, time.Now().UTC()); !errors.Is(err, ErrProviderPairRefused) {
		t.Fatalf("same-family err=%v", err)
	}
	stable := qualificationValue("33333333333333333333333333333333", "codex", "other-family", QualificationGuarded)
	stable.Channel = domain.ChannelStable
	stableRecord, _, err := database.RecordProviderQualification(ctx, stable)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.SelectProviderPair(ctx, domain.ChannelDev, first.ID, stableRecord.ID, time.Now().UTC()); !errors.Is(err, ErrProviderPairRefused) {
		t.Fatalf("cross-channel err=%v", err)
	}
}

func TestQualificationValidationRejectsRawOrAmbiguousData(t *testing.T) {
	database, ctx := openTestStore(t)
	base := qualificationValue("11111111111111111111111111111111", "cursor", "cursor-family", QualificationGuarded)
	tests := []struct {
		name   string
		mutate func(*ProviderQualification)
	}{
		{"preassigned id", func(value *ProviderQualification) { value.ID = 1 }},
		{"run id", func(value *ProviderQualification) { value.RunID = "not-hex" }},
		{"provider", func(value *ProviderQualification) { value.Provider.Provider = "Cursor" }},
		{"model control", func(value *ProviderQualification) { value.Provider.Model = "model\nsecret" }},
		{"digest", func(value *ProviderQualification) { value.PolicyDigest = strings.Repeat("A", 64) }},
		{"passing failures", func(value *ProviderQualification) { value.FailedProbes = []string{"network"} }},
		{"disabled no failures", func(value *ProviderQualification) { value.Profile = QualificationDisabled; value.ReasonCode = "failed" }},
		{"duplicate probes", func(value *ProviderQualification) {
			value.Profile = QualificationDisabled
			value.FailedProbes = []string{"network", "network"}
			value.ReasonCode = "failed"
		}},
		{"unsafe reason", func(value *ProviderQualification) {
			value.Profile = QualificationDisabled
			value.FailedProbes = []string{"network"}
			value.ReasonCode = "token=raw"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			test.mutate(&value)
			if _, _, err := database.RecordProviderQualification(ctx, value); err == nil {
				t.Fatalf("invalid qualification accepted: %+v", value)
			}
		})
	}
	var sensitiveColumns int
	rows, err := database.db.QueryContext(ctx, `PRAGMA table_info(provider_qualifications)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		lower := strings.ToLower(name)
		for _, forbidden := range []string{"prompt", "transcript", "credential", "token", "account", "path"} {
			if strings.Contains(lower, forbidden) {
				sensitiveColumns++
			}
		}
	}
	if sensitiveColumns != 0 {
		t.Fatalf("qualification schema contains %d sensitive columns", sensitiveColumns)
	}
}

func TestConcurrentQualificationReplayCreatesOneRecord(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/qualifications.sqlite"
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	input := qualificationValue("11111111111111111111111111111111", "cursor", "cursor-family", QualificationGuarded)
	start := make(chan struct{})
	var wait sync.WaitGroup
	errorsSeen := make(chan error, 2)
	for _, database := range []*Store{first, second} {
		wait.Add(1)
		go func(database *Store) {
			defer wait.Done()
			<-start
			_, _, err := database.RecordProviderQualification(ctx, input)
			errorsSeen <- err
		}(database)
	}
	close(start)
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("concurrent replay err=%v", err)
		}
	}
	var count int
	if err := first.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_qualifications`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func qualificationValue(runID, provider, family string, profile QualificationProfile) ProviderQualification {
	return ProviderQualification{
		Channel: domain.ChannelDev, RunID: runID,
		Provider:     domain.ProviderIdentity{Provider: provider, Model: provider + "-model", Family: family, Version: "1.0.0"},
		BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64),
		Profile: profile, CreatedAt: time.Date(2026, 8, 29, 18, 0, 0, 0, time.UTC),
	}
}
