// Package multiprovider composes qualified local CLIs without provider fallback.
package multiprovider

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/codexprovider"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/cursorprovider"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
)

var ErrUnavailable = errors.New("requested local provider is unavailable or unqualified")

func QualifyLocalPair(ctx context.Context, db *store.Store, channel domain.Channel, builder, reviewer string, s *processsupervisor.Supervisor) (codexprovider.QualificationResult, error) {
	return QualifyLocalModels(ctx, db, channel, builder, reviewer, "", "", s)
}

func QualifyLocalModels(ctx context.Context, db *store.Store, channel domain.Channel, builder, reviewer, builderModel, reviewerModel string, s *processsupervisor.Supervisor) (codexprovider.QualificationResult, error) {
	if builderModel == "" && reviewerModel == "" && builder == "codex" && reviewer == "codex" {
		return codexprovider.QualifyLocalPair(ctx, db, channel, builder, reviewer, s)
	}
	result := codexprovider.QualificationResult{Channel: channel}
	if db == nil || s == nil || !channel.Valid() {
		return result, ErrUnavailable
	}
	builderModel, builderFamily, resolveErr := resolveLocalModel(builder, "builder", builderModel)
	if resolveErr != nil {
		return result, resolveErr
	}
	reviewerModel, reviewerFamily, resolveErr := resolveLocalModel(reviewer, "reviewer", reviewerModel)
	if resolveErr != nil {
		return result, resolveErr
	}
	// Reject explicit aliases/unknown models and same-family selections before
	// any paid qualification. Defaults retain existing role configuration.
	if builderFamily == reviewerFamily {
		return result, ErrUnavailable
	}
	qualify := func(provider, role, model string) (store.ProviderQualification, error) {
		if provider == "codex" {
			return codexprovider.QualifyLocalRoleModel(ctx, db, channel, role, model, s)
		}
		command := "claude"
		if provider == "cursor" {
			command = "cursor-agent"
		}
		exe, err := exec.LookPath(command)
		if err != nil {
			return store.ProviderQualification{}, ErrUnavailable
		}
		leader, err := db.LeaderEpoch(ctx, channel)
		if err != nil {
			return store.ProviderQualification{}, err
		}
		result.ModelCallMade = true
		var binding contracts.RuntimeBinding
		var proof contracts.QualificationAttestation
		if provider == "cursor" {
			binding, proof, err = s.QualifyCursor(ctx, exe, model, channel, leader)
		} else {
			binding, proof, err = s.QualifyClaude(ctx, exe, model, channel, leader)
		}
		if err != nil {
			return store.ProviderQualification{}, err
		}
		value := store.ProviderQualification{Channel: channel, RunID: proof.RunID, Provider: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, ProbeDigest: proof.ProbeDigest, Profile: store.QualificationGuarded, CreatedAt: time.Unix(0, proof.CreatedUnixNanos).UTC()}
		stored, _, err := db.RecordAttestedProviderQualification(ctx, value, proof)
		return stored, err
	}
	var err error
	result.Builder, err = qualify(builder, "builder", builderModel)
	if err != nil {
		return result, err
	}
	result.Reviewer, err = qualify(reviewer, "reviewer", reviewerModel)
	if err != nil {
		return result, err
	}
	return selectQualifiedPair(ctx, db, channel, result)
}

func resolveLocalModel(provider, role, model string) (string, string, error) {
	if role != "builder" && role != "reviewer" {
		return "", "", ErrUnavailable
	}
	if model == "" {
		switch provider {
		case "codex":
			var err error
			model, err = codexprovider.DefaultRoleModel(role)
			if err != nil {
				return "", "", err
			}
		case "claude":
			model = "claude-sonnet-5"
		case "cursor":
			model = "gpt-5.6-luna-low"
			if role == "reviewer" {
				model = "claude-sonnet-5-low"
			}
		}
	}
	var family string
	var ok bool
	switch provider {
	case "codex":
		family, ok = codexprovider.ModelFamily(model)
	case "claude":
		family, ok = claudeprovider.ModelFamily(model)
	case "cursor":
		family, ok = cursorprovider.ModelFamily(model)
	}
	if !ok {
		return "", "", ErrUnavailable
	}
	return model, family, nil
}

func selectQualifiedPair(ctx context.Context, db *store.Store, channel domain.Channel, result codexprovider.QualificationResult) (codexprovider.QualificationResult, error) {
	if result.Builder.Profile != store.QualificationGuarded || result.Reviewer.Profile != store.QualificationGuarded || result.Builder.Provider.Family == result.Reviewer.Provider.Family {
		return result, ErrUnavailable
	}
	if _, _, err := db.SelectProviderPair(ctx, channel, result.Builder.ID, result.Reviewer.ID, time.Now().UTC()); err != nil {
		return result, err
	}
	result.Independent = true
	return result, nil
}

func Compose(ctx context.Context, channel domain.Channel, db *store.Store, process contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
	return composeLocal(ctx, channel, db, process, codexprovider.LocalRuntimeCandidatesForModels)
}

func composeLocal(ctx context.Context, channel domain.Channel, db *store.Store, process contracts.ProcessSupervisor, candidatesForModels func([]string) ([]providercoord.RuntimeCandidate, int, error)) (*providercoord.Coordinator, error) {
	if db == nil || process == nil || !channel.Valid() {
		return nil, ErrUnavailable
	}
	pair, err := db.ProviderPair(ctx, channel)
	if err != nil {
		return providercoord.ComposeQualified(ctx, channel, db, process, nil, 1)
	}
	// A leader takeover invalidates qualification before startup composition.
	// Do not spend startup's bounded deadline inspecting runtimes that cannot
	// be admitted. ComposeQualified rechecks authority after actual inspection.
	for _, q := range []store.ProviderQualification{pair.Planner, pair.Builder, pair.Reviewer} {
		if q.Profile != store.QualificationGuarded || q.AuthMode == "" || q.ProbeDigest == "" || len(q.AttestationSignature) != 64 || !db.QualificationCurrent(ctx, channel, q) {
			return providercoord.ComposeQualified(ctx, channel, db, process, nil, 1)
		}
	}
	var models []string
	for _, q := range []store.ProviderQualification{pair.Planner, pair.Builder, pair.Reviewer} {
		if q.Provider.Provider == "codex" {
			models = append(models, q.Provider.Model)
		}
	}
	candidates, capacity, err := candidatesForModels(models)
	if err != nil {
		return nil, err
	}
	s, ok := process.(*processsupervisor.Supervisor)
	if !ok {
		return providercoord.ComposeQualified(ctx, channel, db, process, candidates, capacity)
	}
	seen := map[domain.ProviderIdentity]bool{}
	for _, q := range []store.ProviderQualification{pair.Planner, pair.Builder, pair.Reviewer} {
		if (q.Provider.Provider != "claude" && q.Provider.Provider != "cursor") || seen[q.Provider] {
			continue
		}
		seen[q.Provider] = true
		command := "claude"
		if q.Provider.Provider == "cursor" {
			command = "cursor-agent"
		}
		exe, err := exec.LookPath(command)
		if err != nil {
			continue
		}
		exe, err = filepath.EvalSymlinks(exe)
		if err != nil {
			continue
		}
		model := q.Provider.Model
		if q.Provider.Provider == "cursor" {
			binding, display, err := s.ObserveCursorRuntime(ctx, exe, model)
			if err != nil {
				continue
			}
			observer := func(ctx context.Context) (contracts.RuntimeBinding, error) {
				current, _, err := s.ObserveCursorRuntime(ctx, exe, model)
				return current, err
			}
			adapter, err := cursorprovider.New("cursor/"+model, exe, filepath.Dir(exe), display, binding, observer)
			if err != nil {
				continue
			}
			candidates = append(candidates, providercoord.RuntimeCandidate{Provider: adapter, Executable: exe, AuthHome: filepath.Dir(exe)})
			continue
		}
		binding, err := s.ObserveClaudeRuntime(ctx, exe, model)
		if err != nil {
			continue
		}
		observer := func(ctx context.Context) (contracts.RuntimeBinding, error) {
			return s.ObserveClaudeRuntime(ctx, exe, model)
		}
		adapter, err := claudeprovider.New("claude/"+model, exe, filepath.Dir(exe), binding, observer)
		if err != nil {
			continue
		}
		candidates = append(candidates, providercoord.RuntimeCandidate{Provider: adapter, Executable: exe, AuthHome: filepath.Dir(exe)})
	}
	return providercoord.ComposeQualified(ctx, channel, db, process, candidates, capacity)
}
