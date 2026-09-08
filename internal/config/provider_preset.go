package config

import (
	"errors"
	"fmt"
)

// ProviderPreset is a portable preference, never model independence evidence.
// Exact models/families must still be qualified and selected by the daemon.
func ProviderPreset(name string) (ProviderOrder, error) {
	builder, reviewer := "codex", "codex"
	switch name {
	case "":
		return ProviderOrder{}, nil
	case "codex-codex":
	case "claude-codex":
		builder = "claude"
	case "codex-claude":
		reviewer = "claude"
	case "cursor-codex":
		builder = "cursor"
	case "codex-cursor":
		reviewer = "cursor"
	case "cursor-claude":
		builder, reviewer = "cursor", "claude"
	case "claude-cursor":
		builder, reviewer = "claude", "cursor"
	case "cursor-cursor":
		builder, reviewer = "cursor", "cursor"
	default:
		return ProviderOrder{}, errors.New("unsupported provider preset; choose codex-codex, claude-codex, codex-claude, cursor-codex, codex-cursor, cursor-claude, claude-cursor, or cursor-cursor; independent exact models must be qualified")
	}
	return ProviderOrder{Planner: []string{builder}, Builder: []string{builder}, Reviewer: []string{reviewer}}, nil
}

func (plan *NysaPureConfigPlan) addProviderPreset(name string, providers ProviderOrder) error {
	if name == "" {
		return nil
	}
	if plan.Existing {
		var document projectDocument
		if err := decodeStrict(plan.prepared, &document); err != nil {
			return err
		}
		if document.Providers == nil || !sameStrings(document.Providers.Planner, providers.Planner) || !sameStrings(document.Providers.Builder, providers.Builder) || !sameStrings(document.Providers.Reviewer, providers.Reviewer) {
			return errors.New("existing .sf/config.toml does not match the requested provider preset; review it and use config apply; init never overwrites it")
		}
		return nil
	}
	plan.Encoded = append(plan.Encoded, []byte(fmt.Sprintf("\n[providers]\nplanner = [%q]\nbuilder = [%q]\nreviewer = [%q]\n", providers.Planner[0], providers.Builder[0], providers.Reviewer[0]))...)
	return nil
}
