package codexprovider

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
)

type executableRegistrar interface {
	RegisterExecutable(domain.ProviderIdentity, string) (string, error)
}

// Compose constructs no routes unless a current, exact guarded qualification
// can be re-probed in this daemon environment. This deliberately leaves the
// daemon usable for Doctor and operator repair when Codex is absent or stale.
func Compose(ctx context.Context, channel domain.Channel, database *store.Store, process contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
	return ComposeProfiles(ctx, channel, database, process, defaultProfiles())
}

// ComposeProfiles is the explicit production configuration boundary. Each
// profile names a real Codex model and its actual inference family; two
// profiles are independent only when their recorded families differ. No route
// is synthesized from an alias or a duplicate family.
func ComposeProfiles(ctx context.Context, channel domain.Channel, database *store.Store, process contracts.ProcessSupervisor, profiles []Config) (*providercoord.Coordinator, error) {
	registry := providercoord.NewRegistry()
	routes := map[providercoord.Role]providercoord.Route{}
	if !channel.Valid() || database == nil || process == nil {
		return nil, errors.New("valid channel, store, and process supervisor are required")
	}
	pair, err := database.ProviderPair(ctx, channel)
	if err != nil || pair.Builder.Provider.Provider != "codex" || pair.Reviewer.Provider.Provider != "codex" || pair.Builder.Provider.Family == pair.Reviewer.Provider.Family {
		return providercoord.New(registry, routes, database, nil, process)
	}
	registrar, ok := process.(executableRegistrar)
	if !ok {
		return providercoord.New(registry, routes, database, nil, process)
	}
	byIdentity := map[domain.ProviderIdentity]*Adapter{}
	for _, profile := range profiles {
		adapter, adapterErr := New(profile)
		if adapterErr != nil {
			continue
		}
		binding, bindingErr := adapter.Binding(ctx)
		if bindingErr != nil || !qualificationMatches(database, ctx, channel, binding) {
			continue
		}
		if _, exists := byIdentity[binding.Identity]; exists {
			continue
		}
		registeredDigest, registerErr := registrar.RegisterExecutable(binding.Identity, adapter.executable)
		if registerErr != nil || registeredDigest != binding.BinaryDigest || registry.Register(ctx, adapter) != nil {
			continue
		}
		byIdentity[binding.Identity] = adapter
	}
	builder, builderOK := byIdentity[pair.Builder.Provider]
	reviewer, reviewerOK := byIdentity[pair.Reviewer.Provider]
	if !builderOK || !reviewerOK || builder.Name() == reviewer.Name() {
		return providercoord.New(providercoord.NewRegistry(), routes, database, nil, process)
	}
	routes[providercoord.RolePlanner] = providercoord.Route{Primary: builder.Name()}
	routes[providercoord.RoleBuilder] = providercoord.Route{Primary: builder.Name()}
	routes[providercoord.RoleReviewer] = providercoord.Route{Primary: reviewer.Name()}
	return providercoord.New(registry, routes, database, nil, process)
}

func qualificationMatches(database *store.Store, ctx context.Context, channel domain.Channel, binding contracts.RuntimeBinding) bool {
	qualification, err := database.LatestProviderQualification(ctx, channel, binding.Identity)
	return err == nil && qualification.Profile == store.QualificationGuarded && qualification.BinaryDigest == binding.BinaryDigest && qualification.PolicyDigest == binding.PolicyDigest && qualification.FixtureDigest == binding.FixtureDigest
}

func defaultProfiles() []Config {
	executable, err := exec.LookPath("codex")
	if err != nil || !filepath.IsAbs(executable) {
		return nil
	}
	authHome := os.Getenv("CODEX_HOME")
	if authHome == "" {
		if home, homeErr := os.UserHomeDir(); homeErr == nil {
			authHome = filepath.Join(home, ".codex")
		}
	}
	builderModel, reviewerModel := os.Getenv("SF_CODEX_BUILDER_MODEL"), os.Getenv("SF_CODEX_REVIEWER_MODEL")
	builderFamily, builderOK := familyForModel(builderModel)
	reviewerFamily, reviewerOK := familyForModel(reviewerModel)
	if !builderOK || !reviewerOK || builderFamily == reviewerFamily {
		return nil
	}
	return []Config{
		{Route: "codex-builder", Executable: executable, AuthHome: authHome, Model: builderModel},
		{Route: "codex-reviewer", Executable: executable, AuthHome: authHome, Model: reviewerModel},
	}
}
