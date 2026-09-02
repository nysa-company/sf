package domain

import (
	"strings"
	"testing"
)

func TestTerminalStates(t *testing.T) {
	for _, state := range []State{StateDone, StateExternalMerged, StateCancelled} {
		if !state.Terminal() {
			t.Fatalf("%s must be terminal", state)
		}
	}
	for _, state := range []State{StateQueued, StatePlanning, StatePaused, StateBlocked} {
		if state.Terminal() {
			t.Fatalf("%s must not be terminal", state)
		}
	}
}

func TestTicketRefValidation(t *testing.T) {
	valid := TicketRef{Channel: ChannelDev, Project: "nysa", Ticket: "SF-1"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid ticket reference: %v", err)
	}

	for name, ref := range map[string]TicketRef{
		"channel": {Channel: "other", Project: "nysa", Ticket: "SF-1"},
		"project": {Channel: ChannelDev, Ticket: "SF-1"},
		"ticket":  {Channel: ChannelDev, Project: "nysa"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := ref.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestMergeIntentProtectionWitnessAcceptsCanonicalClassicChecksDigest(t *testing.T) {
	for name, intent := range map[string]MergeIntent{
		"legacy":                       {ProtectionKind: "", ActiveRulesetCount: 0},
		"legacy or zero-check classic": {ProtectionKind: "classic", ActiveRulesetCount: 0},
		"current classic":              {ProtectionKind: "classic", ProtectionChecksDigest: strings.Repeat("a", 64), ActiveRulesetCount: 0},
		"ruleset":                      {ProtectionKind: "ruleset", ProtectionChecksDigest: strings.Repeat("b", 64), ActiveRulesetCount: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := intent.ValidateProtectionWitness(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for name, intent := range map[string]MergeIntent{
		"legacy digest":     {ProtectionKind: "", ProtectionChecksDigest: strings.Repeat("a", 64)},
		"unknown kind":      {ProtectionKind: "future", ProtectionChecksDigest: strings.Repeat("a", 64)},
		"short classic":     {ProtectionKind: "classic", ProtectionChecksDigest: strings.Repeat("a", 63)},
		"uppercase classic": {ProtectionKind: "classic", ProtectionChecksDigest: strings.Repeat("A", 64)},
		"nonhex classic":    {ProtectionKind: "classic", ProtectionChecksDigest: strings.Repeat("z", 64)},
		"classic ruleset":   {ProtectionKind: "classic", ProtectionChecksDigest: strings.Repeat("a", 64), ActiveRulesetCount: 1},
		"empty ruleset":     {ProtectionKind: "ruleset", ActiveRulesetCount: 1},
		"short ruleset":     {ProtectionKind: "ruleset", ProtectionChecksDigest: strings.Repeat("b", 63), ActiveRulesetCount: 1},
		"uppercase ruleset": {ProtectionKind: "ruleset", ProtectionChecksDigest: strings.Repeat("B", 64), ActiveRulesetCount: 1},
		"nonhex ruleset":    {ProtectionKind: "ruleset", ProtectionChecksDigest: strings.Repeat("q", 64), ActiveRulesetCount: 1},
		"zero rulesets":     {ProtectionKind: "ruleset", ProtectionChecksDigest: strings.Repeat("b", 64), ActiveRulesetCount: 0},
		"multiple rulesets": {ProtectionKind: "ruleset", ProtectionChecksDigest: strings.Repeat("b", 64), ActiveRulesetCount: 2},
	} {
		t.Run(name, func(t *testing.T) {
			if err := intent.ValidateProtectionWitness(); err == nil {
				t.Fatal("invalid protection witness accepted")
			}
		})
	}
}
