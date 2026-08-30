package codexprovider

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/nysa-company/sf/internal/auth"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/providercoord"
	"github.com/nysa-company/sf/internal/store"
)

// QualificationResult is the safe daemon response for the friendly CLI. It
// intentionally contains identities and verdicts only, never probe output,
// paths, credentials, or provider transcript bytes.
type QualificationResult struct {
	Channel       domain.Channel              `json:"channel"`
	Builder       store.ProviderQualification `json:"builder"`
	Reviewer      store.ProviderQualification `json:"reviewer"`
	Independent   bool                        `json:"independent"`
	ModelCallMade bool                        `json:"model_call_made"`
}

// QualifyLocalPair runs only under a daemon-owned supervisor. A direct CLI
// cannot produce an attestation and therefore cannot manufacture readiness.
func QualifyLocalPair(ctx context.Context, database *store.Store, channel domain.Channel, builderName, reviewerName string, attestor QualificationAttestor) (QualificationResult, error) {
	if database == nil || attestor == nil || !channel.Valid() || builderName != "codex" || reviewerName != "codex" {
		return QualificationResult{}, ErrUnsafeConfiguration
	}
	profiles := defaultProfiles()
	if len(profiles) != 2 {
		return QualificationResult{}, ErrUnavailable
	}
	adapters := make([]*Adapter, 0, 2)
	for _, profile := range profiles {
		runner, err := newOuterQualificationRunner(auth.OSRunner{}, profile.AuthHome)
		if err != nil {
			return QualificationResult{}, err
		}
		profile.Runner = runner
		adapter, err := New(profile)
		if err != nil {
			return QualificationResult{}, err
		}
		adapters = append(adapters, adapter)
	}
	builder, err := Qualify(ctx, database, channel, adapters[0], LocalQualificationFixture(), attestor)
	if err != nil {
		return QualificationResult{}, err
	}
	reviewer, err := Qualify(ctx, database, channel, adapters[1], LocalQualificationFixture(), attestor)
	if err != nil {
		return QualificationResult{}, err
	}
	result := QualificationResult{Channel: channel, Builder: builder, Reviewer: reviewer, ModelCallMade: false}
	if builder.Profile != store.QualificationGuarded || reviewer.Profile != store.QualificationGuarded || builder.Provider.Family == reviewer.Provider.Family {
		return result, fmt.Errorf("unsafe qualification failed: builder=%s reviewer=%s", builder.ReasonCode, reviewer.ReasonCode)
	}
	if _, _, err := database.SelectProviderPair(ctx, channel, builder.ID, reviewer.ID, time.Now().UTC()); err != nil {
		return result, err
	}
	result.Independent = true
	return result, nil
}

type executableRegistrar interface {
	RegisterRuntime(contracts.RuntimeBinding, string, string) (string, error)
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
		registeredDigest, registerErr := registrar.RegisterRuntime(binding, adapter.executable, adapter.authHome)
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
	return err == nil && qualification.Profile == store.QualificationGuarded && qualification.BinaryDigest == binding.BinaryDigest && qualification.PolicyDigest == binding.PolicyDigest && qualification.FixtureDigest == binding.FixtureDigest && qualification.AuthDigest == binding.AuthDigest && qualification.AuthMode == binding.AuthMode && qualification.ProbeDigest != "" && qualification.AttestedLeaderEpoch > 0 && len(qualification.AttestationSignature) == 64 && database.QualificationCurrent(ctx, channel, qualification)
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
	// The supported independent pair is explicit in code and can be overridden
	// only by the operator's local configuration environment. Qualification is
	// still mandatory for both identities; defaults never imply readiness.
	builderModel, reviewerModel := os.Getenv("SF_CODEX_BUILDER_MODEL"), os.Getenv("SF_CODEX_REVIEWER_MODEL")
	if builderModel == "" {
		builderModel = "gpt-5.6-luna"
	}
	if reviewerModel == "" {
		reviewerModel = "gpt-5.5"
	}
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
