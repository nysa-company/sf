package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

func TestFailedQualificationGuidancePreservesSelectedPair(t *testing.T) {
	for _, pair := range [][2]string{{"claude", "codex"}, {"codex", "claude"}, {"cursor", "codex"}, {"claude", "cursor"}} {
		d := &Daemon{channel: domain.ChannelDev}
		d.providerQualifier = func(_ context.Context, _ *store.Store, _ domain.Channel, builder, reviewer string) (any, error) {
			if builder != pair[0] || reviewer != pair[1] {
				t.Fatal("selected pair changed")
			}
			return nil, errors.New("qualification refused")
		}
		parameters, err := json.Marshal(map[string]string{"builder": pair[0], "reviewer": pair[1]})
		if err != nil {
			t.Fatal(err)
		}
		response := d.qualifyProvider(context.Background(), api.Request{Version: api.Version, RequestID: "pair-preserved", Method: "provider.qualify", Parameters: parameters})
		want := []string{"sf-dev", "providers", "qualify", "--builder", pair[0], "--reviewer", pair[1]}
		if response.OK || response.Error == nil || response.Error.Code != "unqualified_provider" || response.NextAction == nil || !reflect.DeepEqual(response.NextAction.Argv, want) {
			t.Fatalf("qualification guidance changed selected pair: %+v", response)
		}
	}
}

func TestCursorQualificationUsesConfiguredAuthorityWithoutChangingModels(t *testing.T) {
	d := &Daemon{channel: domain.ChannelDev}
	called := 0
	d.providerModelQualifier = func(_ context.Context, _ *store.Store, _ domain.Channel, b, r, bm, rm string) (any, error) {
		called++
		if b != "cursor" || r != "claude" || bm != "gpt-5.6-luna-low" || rm != "claude-sonnet-5" {
			t.Fatal("requested route changed")
		}
		return nil, errors.New("fixture qualification refusal")
	}
	parameters := `{"builder":"cursor","reviewer":"claude","builder_model":"gpt-5.6-luna-low","reviewer_model":"claude-sonnet-5"}`
	response := d.qualifyProvider(context.Background(), api.Request{Version: api.Version, RequestID: "cursor-qualification", Method: "provider.qualify", Parameters: json.RawMessage(parameters)})
	if response.OK || response.Mutation.Attempted || response.Error == nil || response.Error.Code != "unqualified_provider" || called != 1 {
		t.Fatalf("qualification authority not preserved: %+v", response)
	}
}

func TestExactModelQualificationNeverFallsBackToLegacyCallback(t *testing.T) {
	d := &Daemon{channel: domain.ChannelDev}
	d.providerQualifier = func(context.Context, *store.Store, domain.Channel, string, string) (any, error) {
		t.Fatal("explicit model discarded")
		return nil, nil
	}
	request := api.Request{Version: api.Version, RequestID: "exact-model", Method: "provider.qualify", Parameters: json.RawMessage(`{"builder":"claude","reviewer":"codex","builder_model":"claude-sonnet-5","reviewer_model":"gpt-5.6-luna"}`)}
	if response := d.qualifyProvider(context.Background(), request); response.OK || response.Error.Code != "provider_unavailable" {
		t.Fatal("legacy model request admitted")
	}
	d.providerModelQualifier = func(_ context.Context, _ *store.Store, _ domain.Channel, b, r, bm, rm string) (any, error) {
		if b != "claude" || r != "codex" || bm != "claude-sonnet-5" || rm != "gpt-5.6-luna" {
			t.Fatal("exact selection changed")
		}
		return nil, errors.New("fixture refuses before launch")
	}
	response := d.qualifyProvider(context.Background(), request)
	want := []string{"sf-dev", "providers", "qualify", "--builder", "claude", "--reviewer", "codex", "--builder-model", "claude-sonnet-5", "--reviewer-model", "gpt-5.6-luna"}
	if response.OK || response.NextAction == nil || !reflect.DeepEqual(response.NextAction.Argv, want) {
		t.Fatal("retry changed exact model selection")
	}
}
