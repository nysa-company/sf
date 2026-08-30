package cli

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/codexprovider"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

type providerQualificationView struct {
	Provider domain.ProviderIdentity    `json:"provider"`
	Profile  store.QualificationProfile `json:"profile"`
	Failed   []string                   `json:"failed_probes,omitempty"`
	Selected bool                       `json:"selected"`
}

type providerQualificationResult struct {
	Channel       domain.Channel            `json:"channel"`
	Builder       providerQualificationView `json:"builder"`
	Reviewer      providerQualificationView `json:"reviewer"`
	Independent   bool                      `json:"independent"`
	ModelCallMade bool                      `json:"model_call_made"`
}

// RunProviderQualification is the direct local path for `providers qualify`.
// It opens only the selected channel's database, performs bounded version,
// capability, auth, and local sandbox probes, and records the exact verdicts.
// The qualifier never invokes `codex exec`; qualification is consequently
// safe to run while the daemon is stopped and cannot spend model tokens.
func RunProviderQualification(ctx context.Context, channel domain.Channel, builderName, reviewerName string) api.Response {
	binary := binaryForChannel(channel)
	usage := []string{binary, "providers", "qualify", "--builder", builderName, "--reviewer", reviewerName}
	if !channel.Valid() || strings.TrimSpace(builderName) == "" || strings.TrimSpace(reviewerName) == "" {
		return failure("invalid_argument", "channel, builder, and reviewer are required", []string{binary, "providers", "qualify", "--help"})
	}
	if builderName != "codex" || reviewerName != "codex" {
		return failure("provider_unavailable", "only the qualified Codex adapter is available in this local build", usage)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return failure("provider_unavailable", "current-user home directory is unavailable", []string{binary, "auth", "status"})
	}
	paths, err := config.PathsFor(home, channel)
	if err != nil {
		return failure("init_failed", "channel paths could not be resolved", []string{binary, "doctor"})
	}
	if err := config.PrepareChannel(paths); err != nil {
		return failure("init_failed", "channel state could not be prepared: "+err.Error(), []string{binary, "doctor"})
	}
	database, err := store.OpenChannel(ctx, paths.Database, paths.Backups, channel)
	if err != nil {
		return failure("init_failed", "channel database could not be opened: "+err.Error(), []string{binary, "doctor"})
	}
	defer database.Close()
	executable, err := exec.LookPath("codex")
	if err != nil {
		return failure("provider_unavailable", "codex is not installed", []string{binary, "auth", "status"})
	}
	executable, err = filepath.Abs(executable)
	if err != nil {
		return failure("provider_unavailable", "codex executable path is unavailable", []string{binary, "doctor"})
	}
	authHome := os.Getenv("CODEX_HOME")
	if authHome == "" {
		authHome = filepath.Join(home, ".codex")
	}
	builderModel, reviewerModel := os.Getenv("SF_CODEX_BUILDER_MODEL"), os.Getenv("SF_CODEX_REVIEWER_MODEL")
	if builderModel == "" {
		builderModel = "gpt-5.6-luna"
	}
	if reviewerModel == "" {
		reviewerModel = "gpt-5.5"
	}
	builder, err := codexprovider.New(codexprovider.Config{Route: "codex-builder", Executable: executable, AuthHome: authHome, Model: builderModel})
	if err != nil {
		return failure("provider_unavailable", "builder Codex configuration is unsafe", []string{binary, "doctor"})
	}
	reviewer, err := codexprovider.New(codexprovider.Config{Route: "codex-reviewer", Executable: executable, AuthHome: authHome, Model: reviewerModel})
	if err != nil {
		return failure("provider_unavailable", "reviewer Codex configuration is unsafe", []string{binary, "doctor"})
	}
	fixture := codexprovider.LocalQualificationFixture()
	builderQualification, builderErr := codexprovider.Qualify(ctx, database, channel, builder, fixture)
	reviewerQualification, reviewerErr := codexprovider.Qualify(ctx, database, channel, reviewer, fixture)
	if builderErr != nil || reviewerErr != nil {
		return failure("provider_unavailable", "qualification could not be recorded", []string{binary, "doctor"})
	}
	result := providerQualificationResult{Channel: channel, Builder: qualificationView(builderQualification), Reviewer: qualificationView(reviewerQualification), ModelCallMade: false}
	if builderQualification.Profile == store.QualificationGuarded && reviewerQualification.Profile == store.QualificationGuarded && builderQualification.Provider.Family != reviewerQualification.Provider.Family {
		if _, _, err := database.SelectProviderPair(ctx, channel, builderQualification.ID, reviewerQualification.ID, time.Now().UTC()); err == nil {
			result.Independent = true
			result.Builder.Selected, result.Reviewer.Selected = true, true
		}
	}
	data, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return failure("init_failed", "qualification result could not be encoded", []string{binary, "doctor"})
	}
	if !result.Independent {
		response := failure("unqualified_provider", "the requested provider pair is not independently qualified; no model call was made", []string{binary, "providers", "qualify", "--help"})
		response.Data = data
		return response
	}
	return api.Response{Version: api.Version, RequestID: requestID(), OK: true, Mutation: api.Mutation{Attempted: true, Kind: "provider.qualify", Identity: string(channel), Observed: true}, Data: data}
}

func qualificationView(value store.ProviderQualification) providerQualificationView {
	return providerQualificationView{Provider: value.Provider, Profile: value.Profile, Failed: append([]string(nil), value.FailedProbes...)}
}
