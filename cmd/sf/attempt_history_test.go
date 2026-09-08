package main

import (
	"errors"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func compiledLiveIdentityMatches(identity domain.ProviderIdentity, provider string) bool {
	model, family := "gpt-5.6-luna", "openai-gpt-5.6"
	if provider == "claude" {
		model, family = "claude-sonnet-5", "anthropic-claude"
	} else if provider == "cursor" {
		model = "gpt-5.6-luna-low"
	} else if provider != "codex" {
		return false
	}
	return identity.Provider == provider && identity.Model == model && identity.Family == family
}

func TestCompiledLiveIdentityMatchesExactSelectedProvider(t *testing.T) {
	for _, identity := range []domain.ProviderIdentity{
		{Provider: "codex", Model: "gpt-5.6-luna", Family: "openai-gpt-5.6"},
		{Provider: "claude", Model: "claude-sonnet-5", Family: "anthropic-claude"},
		{Provider: "cursor", Model: "gpt-5.6-luna-low", Family: "openai-gpt-5.6"},
	} {
		if !compiledLiveIdentityMatches(identity, identity.Provider) {
			t.Errorf("selected provider rejected: %s", identity.Provider)
		}
		for _, field := range []string{"provider", "model", "family"} {
			wrong := identity
			switch field {
			case "provider":
				wrong.Provider = "foreign"
			case "model":
				wrong.Model += "-foreign"
			case "family":
				wrong.Family = "foreign"
			}
			if compiledLiveIdentityMatches(wrong, identity.Provider) {
				t.Errorf("accepted changed %s", field)
			}
		}
	}
}

// Validate the bounded repair contract, not an idealized no-repair call count.
// Errors deliberately omit claims, prompts, schemas and raw provider output.
func validateCompiledAttemptHistory(attempts []store.ProviderAttempt, finalReview bool) error {
	roles := map[domain.Phase]string{domain.PhasePlanning: "planner", domain.PhaseVerification: "reviewer", domain.PhaseBuild: "builder"}
	if finalReview {
		roles[domain.PhaseReview] = "reviewer"
	}
	groups := map[domain.Phase][]store.ProviderAttempt{}
	for _, attempt := range attempts {
		if roles[attempt.Phase] == "" || roles[attempt.Phase] != attempt.Role || attempt.FinishedAt.IsZero() {
			return errors.New("unexpected or unfinished provider phase")
		}
		groups[attempt.Phase] = append(groups[attempt.Phase], attempt)
	}
	for phase := range roles {
		group := groups[phase]
		if len(group) < 1 || len(group) > 2 {
			return errors.New("missing phase or exceeded two-attempt role bound")
		}
		for index, attempt := range group {
			first := group[0]
			if attempt.Attempt != index+1 || attempt.Binding != first.Binding || attempt.ExpectedVersion != first.ExpectedVersion || attempt.LeaderEpoch != first.LeaderEpoch || attempt.RunnerEpoch != first.RunnerEpoch {
				return errors.New("repair changed binding, fence or attempt sequence")
			}
			if index == len(group)-1 {
				if attempt.State != "completed" || attempt.Outcome != "completed" {
					return errors.New("phase has no successful terminal result")
				}
			} else if attempt.State != "failed" || (attempt.Outcome != "invalid_artifact" && attempt.Outcome != "invocation_failed") {
				// Store records invocation_failed only before supervisor launch,
				// with no process identity. It is not an uncertain execution.
				return errors.New("unproven failure was treated as bounded retry")
			}
		}
	}
	return nil
}

func TestCompiledAttemptHistoryAllowsOnlyBoundedSameBindingRepair(t *testing.T) {
	var history []store.ProviderAttempt
	for _, entry := range []struct {
		phase domain.Phase
		role  string
	}{{domain.PhasePlanning, "planner"}, {domain.PhaseVerification, "reviewer"}, {domain.PhaseBuild, "builder"}} {
		a := store.ProviderAttempt{ProviderAttemptClaim: store.ProviderAttemptClaim{Phase: entry.phase, Role: entry.role, Attempt: 1}, State: "completed", Outcome: "completed", FinishedAt: time.Now()}
		if entry.phase == domain.PhasePlanning {
			failed := a
			failed.State, failed.Outcome = "failed", "invalid_artifact"
			history = append(history, failed)
			a.Attempt = 2
		}
		history = append(history, a)
	}
	if err := validateCompiledAttemptHistory(history, false); err != nil {
		t.Fatal(err)
	}
	prelaunch := append([]store.ProviderAttempt(nil), history...)
	prelaunch[0].Outcome = "invocation_failed"
	if err := validateCompiledAttemptHistory(prelaunch, false); err != nil {
		t.Fatal("Store-proven no-process failure rejected", err)
	}
	for _, mutate := range []func([]store.ProviderAttempt){
		func(h []store.ProviderAttempt) { h[0].Outcome = "result_indeterminate" },
		func(h []store.ProviderAttempt) { h[0].Outcome = "cancelled" },
		func(h []store.ProviderAttempt) { h[0].Outcome = "unknown" },
		func(h []store.ProviderAttempt) { h[0].State, h[0].Outcome = "quarantined", "invocation_failed" },
		func(h []store.ProviderAttempt) { h[1].Attempt = 3 },
		func(h []store.ProviderAttempt) { h[1].Binding.Identity.Model = "different" },
		func(h []store.ProviderAttempt) { h[1].RunnerEpoch++ },
		func(h []store.ProviderAttempt) { h[1].State, h[1].Outcome = "failed", "invalid_artifact" },
	} {
		changed := append([]store.ProviderAttempt(nil), history...)
		mutate(changed)
		if validateCompiledAttemptHistory(changed, false) == nil {
			t.Fatal("invalid history admitted")
		}
	}
	if validateCompiledAttemptHistory(history, true) == nil {
		t.Fatal("missing final review admitted")
	}
}
