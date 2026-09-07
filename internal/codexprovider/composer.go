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

// QualifyLocalRole measures one configured Codex role without changing the
// selected pair. Multi-provider composition commits selection only at the end.
func QualifyLocalRole(ctx context.Context, database *store.Store, channel domain.Channel, role string, attestor QualificationAttestor) (store.ProviderQualification, error) {
	return QualifyLocalRoleModel(ctx, database, channel, role, "", attestor)
}

// QualifyLocalRoleModel pins an explicit model without changing process-wide
// environment. An empty model preserves the existing role default.
func QualifyLocalRoleModel(ctx context.Context, database *store.Store, channel domain.Channel, role, model string, attestor QualificationAttestor) (store.ProviderQualification, error) {
	profiles := configuredProfiles()
	index := 0
	if role == "reviewer" {
		index = 1
	} else if role != "builder" {
		return store.ProviderQualification{}, ErrUnsafeConfiguration
	}
	if len(profiles) != 2 {
		return store.ProviderQualification{}, ErrUnavailable
	}
	profile := profiles[index]
	if model != "" {
		if _, ok := familyForModel(model); !ok {
			return store.ProviderQualification{}, ErrUnsafeConfiguration
		}
		profile.Model = model
	}
	runner, err := newOuterQualificationRunner(auth.OSRunner{}, profile.AuthHome)
	if err != nil {
		return store.ProviderQualification{}, err
	}
	profile.Runner = runner
	adapter, err := New(profile)
	if err != nil {
		return store.ProviderQualification{}, err
	}
	return Qualify(ctx, database, channel, adapter, LocalQualificationFixture(), attestor)
}

func LocalRuntimeCandidates() ([]providercoord.RuntimeCandidate, int, error) {
	return LocalRuntimeCandidatesForModels(nil)
}

// LocalRuntimeCandidatesForModels reconstructs exact selected identities after
// restart; selection and signed qualification remain Store authority.
func LocalRuntimeCandidatesForModels(models []string) ([]providercoord.RuntimeCandidate, int, error) {
	capacity, err := configuredProviderCapacity()
	if err != nil {
		return nil, 0, err
	}
	var candidates []providercoord.RuntimeCandidate
	seenModels := map[string]bool{}
	profiles := configuredProfiles()
	if len(models) > 0 {
		if len(models) > 3 || len(profiles) == 0 {
			return nil, capacity, ErrUnavailable
		}
		base := profiles[0]
		configured := profiles
		profiles = nil
		for _, model := range models {
			if _, ok := familyForModel(model); !ok {
				return nil, capacity, ErrUnsafeConfiguration
			}
			profile := base
			profile.Model, profile.Route = model, "codex-"+model
			for _, existing := range configured {
				if existing.Model == model {
					profile = existing
					break
				}
			}
			profiles = append(profiles, profile)
		}
	}
	for _, profile := range profiles {
		// Both role defaults may name the same installed model in a mixed
		// pair. Publish one candidate, not two aliases for one identity.
		if seenModels[profile.Model] {
			continue
		}
		seenModels[profile.Model] = true
		adapter, err := New(profile)
		if err == nil {
			candidates = append(candidates, providercoord.RuntimeCandidate{Provider: adapter, Executable: adapter.executable, AuthHome: adapter.authHome})
		}
	}
	return candidates, capacity, nil
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

// Compose constructs no routes unless a current, exact guarded qualification
// can be re-probed in this daemon environment. This deliberately leaves the
// daemon usable for Doctor and operator repair when Codex is absent or stale.
func Compose(ctx context.Context, channel domain.Channel, database *store.Store, process contracts.ProcessSupervisor) (*providercoord.Coordinator, error) {
	capacity, err := configuredProviderCapacity()
	if err != nil {
		return nil, err
	}
	return ComposeProfilesWithCapacity(ctx, channel, database, process, defaultProfiles(), capacity)
}

// ComposeProfiles is the explicit production configuration boundary. Each
// profile names a real Codex model and its actual inference family; two
// profiles are independent only when their recorded families differ. No route
// is synthesized from an alias or a duplicate family.
func ComposeProfiles(ctx context.Context, channel domain.Channel, database *store.Store, process contracts.ProcessSupervisor, profiles []Config) (*providercoord.Coordinator, error) {
	return ComposeProfilesWithCapacity(ctx, channel, database, process, profiles, 1)
}

// ComposeProfilesWithCapacity applies an explicit, tightly bounded capacity
// to the shared provider/auth lease. Store remains the authority for machine,
// project, and provider admission; this only permits the caller to opt into
// the already-supported two-worker ceiling.
func ComposeProfilesWithCapacity(ctx context.Context, channel domain.Channel, database *store.Store, process contracts.ProcessSupervisor, profiles []Config, capacity int) (*providercoord.Coordinator, error) {
	if capacity < 1 || capacity > 2 {
		return nil, errors.New("invalid Codex provider capacity")
	}
	if !channel.Valid() || database == nil || process == nil {
		return nil, errors.New("valid channel, store, and process supervisor are required")
	}
	candidates := make([]providercoord.RuntimeCandidate, 0, len(profiles))
	for _, profile := range profiles {
		adapter, adapterErr := New(profile)
		if adapterErr != nil {
			continue
		}
		candidates = append(candidates, providercoord.RuntimeCandidate{Provider: adapter, Executable: adapter.executable, AuthHome: adapter.authHome})
	}
	return providercoord.ComposeQualified(ctx, channel, database, process, candidates, capacity)
}

func composeRoutes(builder, reviewer string, capacity int) map[providercoord.Role]providercoord.Route {
	return map[providercoord.Role]providercoord.Route{
		providercoord.RolePlanner:  {Primary: builder, Capacity: capacity},
		providercoord.RoleBuilder:  {Primary: builder, Capacity: capacity},
		providercoord.RoleReviewer: {Primary: reviewer, Capacity: capacity},
	}
}

func configuredProviderCapacity() (int, error) {
	switch os.Getenv("SF_CODEX_PROVIDER_CAPACITY") {
	case "", "1":
		return 1, nil
	case "2":
		return 2, nil
	default:
		return 0, errors.New("invalid SF_CODEX_PROVIDER_CAPACITY: expected 1 or 2")
	}
}

func qualificationMatches(database *store.Store, ctx context.Context, channel domain.Channel, binding contracts.RuntimeBinding) bool {
	qualification, err := database.LatestProviderQualification(ctx, channel, binding.Identity)
	return err == nil && qualification.Profile == store.QualificationGuarded && qualification.BinaryDigest == binding.BinaryDigest && qualification.PolicyDigest == binding.PolicyDigest && qualification.FixtureDigest == binding.FixtureDigest && qualification.AuthDigest == binding.AuthDigest && qualification.AuthMode == binding.AuthMode && qualification.ProbeDigest != "" && qualification.AttestedLeaderEpoch > 0 && len(qualification.AttestationSignature) == 64 && database.QualificationCurrent(ctx, channel, qualification)
}

func defaultProfiles() []Config {
	profiles := configuredProfiles()
	if len(profiles) != 2 {
		return nil
	}
	builderFamily, _ := familyForModel(profiles[0].Model)
	reviewerFamily, _ := familyForModel(profiles[1].Model)
	if builderFamily == reviewerFamily {
		return nil
	}
	return profiles
}

// Individual candidates need not be independent of an unused Codex role.
// Independence is enforced on the selected pair, including mixed CLI pairs.
func configuredProfiles() []Config {
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
	_, builderOK := familyForModel(builderModel)
	_, reviewerOK := familyForModel(reviewerModel)
	if !builderOK || !reviewerOK {
		return nil
	}
	return []Config{
		{Route: "codex-builder", Executable: executable, AuthHome: authHome, Model: builderModel},
		{Route: "codex-reviewer", Executable: executable, AuthHome: authHome, Model: reviewerModel},
	}
}

// DefaultRoleModel reports the configured exact role model without probing or
// invoking a provider. Invalid local configuration never selects a fallback.
func DefaultRoleModel(role string) (string, error) {
	var model string
	switch role {
	case "builder":
		model = os.Getenv("SF_CODEX_BUILDER_MODEL")
		if model == "" {
			model = "gpt-5.6-luna"
		}
	case "reviewer":
		model = os.Getenv("SF_CODEX_REVIEWER_MODEL")
		if model == "" {
			model = "gpt-5.5"
		}
	default:
		return "", ErrUnsafeConfiguration
	}
	if _, ok := ModelFamily(model); !ok {
		return "", ErrUnsafeConfiguration
	}
	return model, nil
}
