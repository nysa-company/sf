package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
)

func TestTicketWatchCancellationDuringReadDetachesWithoutMutation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	client := fakeClient(func(_ context.Context, request api.Request) (api.Response, error) {
		calls++
		if request.Method != "ticket.activity" {
			t.Fatalf("unexpected request: %+v", request)
		}
		cancel()
		return api.Response{}, errors.New("read cancelled")
	})
	var output, stderr bytes.Buffer
	if code := Execute(ctx, []string{"ticket", "watch", "SF-1", "--json"}, &output, &stderr, client); code != 0 || calls != 1 || output.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d calls=%d output=%s stderr=%s", code, calls, &output, &stderr)
	}
}

func TestTicketViewRejectsUnknownSectionBeforeRequest(t *testing.T) {
	calls := 0
	client := fakeClient(func(context.Context, api.Request) (api.Response, error) { calls++; return responseOK(), nil })
	if code := Execute(context.Background(), []string{"ticket", "view", "SF-1", "--section", "transcript", "--json"}, &bytes.Buffer{}, &bytes.Buffer{}, client); code != int(ExitInput) || calls != 0 {
		t.Fatalf("exit=%d calls=%d", code, calls)
	}
}

func TestCanonicalActionsOnlyRewriteKnownLocationsAndExecutables(t *testing.T) {
	response := responseOK()
	response.NextAction = &domain.NextAction{Code: "inspect", Argv: []string{"sf", "status", "SF-1"}}
	response.Data = json.RawMessage(`{"version":9007199254740993,"next_action":{"argv":["sf","start","SF-1"]},"tickets":[{"next_action":{"argv":["sf-dev","cancel","SF-2"]}}],"evidence":{"next_action":{"argv":["sf","approve","SF-untrusted"]}},"source":"sf start SF-text"}`)
	got := canonicalResponse(response)
	if strings.Join(got.NextAction.Argv, " ") != "sf ticket view SF-1" || !bytes.Contains(got.Data, []byte(`9007199254740993`)) || !bytes.Contains(got.Data, []byte(`["sf","ticket","start","SF-1"]`)) || !bytes.Contains(got.Data, []byte(`["sf","approve","SF-untrusted"]`)) || !bytes.Contains(got.Data, []byte(`sf start SF-text`)) {
		t.Fatalf("mapped=%s action=%v", got.Data, got.NextAction)
	}
	for _, argv := range [][]string{{"other", "start", "SF-1"}, {"/tmp/sf", "cancel", "SF-1"}, {"sf", "unknown", "SF-1"}, {"sf", "daemon", "restart"}, {"sf", "status", "--watch"}} {
		if mapped := canonicalActionArgv(argv); !reflect.DeepEqual(mapped, argv) {
			t.Fatalf("unexpected rewrite: %v -> %v", argv, mapped)
		}
	}
}

func TestLegacyJSONPreservesNextActionsWhileCanonicalMapsThem(t *testing.T) {
	response := responseOK()
	response.Data = json.RawMessage(`{"state":"queued","next_action":{"argv":["sf","start","SF-1"]}}`)
	for _, canonical := range []bool{false, true} {
		var output bytes.Buffer
		a := newApp(nil, &output, &bytes.Buffer{})
		a.json, a.canonical = true, canonical
		if err := a.emit(response); err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(output.Bytes(), []byte(`"ticket","start"`)) != canonical {
			t.Fatalf("canonical=%t output=%s", canonical, &output)
		}
	}
}

func TestArtifactHumanRenderingIncludesProvenanceAndUnavailable(t *testing.T) {
	var output bytes.Buffer
	value := map[string]any{"state": "queued", "ticket": "SF-1", "artifacts": map[string]any{"plan": map[string]any{"text": "Stored provider-reported plan; not execution evidence.\nRead files", "available": true}, "pr": map[string]any{"text": "Unavailable: no authenticated published pull request is recorded", "available": false}}}
	if err := renderHumanData(&output, value); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "provider-reported") || !strings.Contains(output.String(), "Unavailable:") {
		t.Fatal(output.String())
	}
}
