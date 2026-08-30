package cli

import (
	"context"
	"strings"

	"github.com/nysa-company/sf/internal/api"
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

// RunProviderQualification remains a compatibility helper for embedders. The
// public CLI routes qualification through the running foreground daemon so a
// passing verdict is signed by its current supervisor.
func RunProviderQualification(ctx context.Context, channel domain.Channel, builderName, reviewerName string) api.Response {
	binary := binaryForChannel(channel)
	_ = ctx
	if !channel.Valid() || strings.TrimSpace(builderName) == "" || strings.TrimSpace(reviewerName) == "" {
		return failure("invalid_argument", "channel, builder, and reviewer are required", []string{binary, "providers", "qualify", "--help"})
	}
	return failure("daemon_unavailable", "provider qualification must be signed by the running local daemon", []string{binary, "daemon", "run"})
}

func qualificationView(value store.ProviderQualification) providerQualificationView {
	return providerQualificationView{Provider: value.Provider, Profile: value.Profile, Failed: append([]string(nil), value.FailedProbes...)}
}
