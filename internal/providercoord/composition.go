package providercoord

import (
	"context"
	"errors"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// RuntimeCandidate is local composition data, never portable project config.
// Adapters do not gain the supervisor's qualification-signing capability.
type RuntimeCandidate struct {
	Provider             contracts.Provider
	Executable, AuthHome string
}

type runtimeRegistrar interface {
	RegisterRuntime(contracts.RuntimeBinding, string, string) (string, error)
}

// ComposeQualified constructs only the exact Store-selected three-role set.
// Missing/expired qualifications produce an unavailable coordinator, not a
// substituted route. Store rechecks admission at launch; composition is not
// a lease or an authority snapshot for later attempts.
func ComposeQualified(ctx context.Context, channel domain.Channel, database *store.Store, process contracts.ProcessSupervisor, candidates []RuntimeCandidate, capacity int) (*Coordinator, error) {
	if database == nil || process == nil || !channel.Valid() || capacity < 1 || capacity > 2 {
		return nil, errors.New("valid provider composition inputs required")
	}
	unavailable := func() (*Coordinator, error) { return New(NewRegistry(), map[Role]Route{}, database, nil, process) }
	pair, err := database.ProviderPair(ctx, channel)
	if err != nil || pair.Builder.Provider.Family == pair.Reviewer.Provider.Family {
		return unavailable()
	}
	registrar, ok := process.(runtimeRegistrar)
	if !ok {
		return unavailable()
	}
	selected := map[domain.ProviderIdentity]store.ProviderQualification{}
	for _, q := range []store.ProviderQualification{pair.Planner, pair.Builder, pair.Reviewer} {
		if q.Profile != store.QualificationGuarded || q.AuthMode == "" || q.ProbeDigest == "" || len(q.AttestationSignature) != 64 || !database.QualificationCurrent(ctx, channel, q) {
			return unavailable()
		}
		selected[q.Provider] = q
	}
	registry := NewRegistry()
	names := map[domain.ProviderIdentity]string{}
	used := map[string]bool{}
	for _, candidate := range candidates {
		if candidate.Provider == nil {
			continue
		}
		binding, err := candidate.Provider.Binding(ctx)
		if err != nil {
			continue
		}
		q, wanted := selected[binding.Identity]
		if !wanted {
			continue
		}
		if !validBinding(binding) || binding.BinaryDigest != q.BinaryDigest || binding.PolicyDigest != q.PolicyDigest || binding.FixtureDigest != q.FixtureDigest || binding.AuthDigest != q.AuthDigest || binding.AuthMode != q.AuthMode {
			return unavailable()
		}
		name := candidate.Provider.Name()
		if name == "" || used[name] || names[binding.Identity] != "" {
			return unavailable()
		}
		digest, err := registrar.RegisterRuntime(binding, candidate.Executable, candidate.AuthHome)
		if err != nil || digest != binding.BinaryDigest || registry.Register(ctx, candidate.Provider) != nil {
			return unavailable()
		}
		names[binding.Identity], used[name] = name, true
	}
	planner, builder, reviewer := names[pair.Planner.Provider], names[pair.Builder.Provider], names[pair.Reviewer.Provider]
	if planner == "" || builder == "" || reviewer == "" || builder == reviewer {
		return unavailable()
	}
	current, err := database.ProviderPair(ctx, channel)
	if err != nil || current.Planner.ID != pair.Planner.ID || current.Builder.ID != pair.Builder.ID || current.Reviewer.ID != pair.Reviewer.ID {
		return unavailable()
	}
	return New(registry, map[Role]Route{RolePlanner: {Primary: planner, Capacity: capacity}, RoleBuilder: {Primary: builder, Capacity: capacity}, RoleReviewer: {Primary: reviewer, Capacity: capacity}}, database, nil, process)
}
