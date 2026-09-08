//go:build sf_e2e

package main

import (
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestCompiledRefreshedHistoryRequiresFreshBoundedSameRuntimeRoles(t *testing.T) {
	var before []store.ProviderAttempt
	for index, phase := range []domain.Phase{domain.PhasePlanning, domain.PhaseVerification, domain.PhaseBuild, domain.PhaseReview} {
		a := store.ProviderAttempt{ProviderAttemptClaim: store.ProviderAttemptClaim{ID: int64(index + 1), Phase: phase, Role: string(phase), Attempt: 1, ExpectedVersion: uint64(index + 1), BaseSHA: "old"}, State: "completed", Outcome: "completed", FinishedAt: time.Now()}
		a.Binding.Identity.Model = "selected"
		before = append(before, a)
	}
	after := append([]store.ProviderAttempt(nil), before...)
	for index, source := range before[2:] {
		source.ID, source.Attempt, source.ExpectedVersion = int64(index+5), 2, uint64(index+10)
		source.BaseSHA, source.Input.BaseSHA = "new", "new"
		after = append(after, source)
	}
	if err := validateCompiledRefreshedHistory(before, after, "new"); err != nil {
		t.Fatal(err)
	}
	repaired := append([]store.ProviderAttempt(nil), after...)
	repaired[4].State, repaired[4].Outcome = "failed", "invalid_artifact"
	success := after[4]
	success.ID, success.Attempt = 7, 3
	repaired = append(repaired, success)
	if err := validateCompiledRefreshedHistory(before, repaired, "new"); err != nil {
		t.Fatal("bounded same-entry repair refused", err)
	}
	repaired[len(repaired)-1].RunnerEpoch++
	if validateCompiledRefreshedHistory(before, repaired, "new") == nil {
		t.Fatal("repair crossed runner fence")
	}
	for name, mutate := range map[string]func([]store.ProviderAttempt) []store.ProviderAttempt{
		"fallback": func(h []store.ProviderAttempt) []store.ProviderAttempt {
			h[4].Binding.Identity.Model = "other"
			return h
		},
		"old-base": func(h []store.ProviderAttempt) []store.ProviderAttempt { h[4].Input.BaseSHA = "old"; return h },
		"history":  func(h []store.ProviderAttempt) []store.ProviderAttempt { h[0].Outcome = "failed"; return h },
		"replay": func(h []store.ProviderAttempt) []store.ProviderAttempt {
			h[4].ExpectedVersion = before[2].ExpectedVersion
			return h
		},
		"reset":          func(h []store.ProviderAttempt) []store.ProviderAttempt { h[4].Attempt = 1; return h },
		"missing-review": func(h []store.ProviderAttempt) []store.ProviderAttempt { return h[:5] },
		"uncertain": func(h []store.ProviderAttempt) []store.ProviderAttempt {
			h[4].State, h[4].Outcome = "failed", "result_indeterminate"
			return h
		},
	} {
		t.Run(name, func(t *testing.T) {
			if validateCompiledRefreshedHistory(before, mutate(append([]store.ProviderAttempt(nil), after...)), "new") == nil {
				t.Fatal("invalid refreshed history accepted")
			}
		})
	}
}
