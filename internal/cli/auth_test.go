package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	localauth "github.com/nysa-company/sf/internal/auth"
	"github.com/nysa-company/sf/internal/domain"
)

type fakeAuthentication struct {
	statuses  []localauth.Status
	login     localauth.Status
	attempted bool
	err       error
	provider  localauth.Provider
}

func (service *fakeAuthentication) StatusAll(context.Context) []localauth.Status {
	return append([]localauth.Status(nil), service.statuses...)
}

func (service *fakeAuthentication) Login(_ context.Context, provider localauth.Provider, _ localauth.Terminal) (localauth.Status, bool, error) {
	service.provider = provider
	return service.login, service.attempted, service.err
}

func TestAuthStatusIsSanitizedAndChannelCorrect(t *testing.T) {
	service := &fakeAuthentication{statuses: []localauth.Status{
		{Provider: localauth.GitHub, Executable: "gh", Installed: true, Authenticated: true, State: localauth.StateAuthenticated, Version: "gh version 2.98.0"},
		{Provider: localauth.Cursor, Executable: "cursor-agent", Installed: true, State: localauth.StateUnauthenticated, Version: "cursor-agent 1", Reason: "interactive login is required"},
		{Provider: localauth.Claude, Executable: "claude", State: localauth.StateUnavailable, Reason: "executable is not installed"},
	}}
	response := RunAuthStatus(context.Background(), domain.ChannelDev, service)
	if !response.OK || response.Mutation.Attempted {
		t.Fatalf("response=%+v", response)
	}
	var report authReport
	if err := json.Unmarshal(response.Data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Schema != authSchema || report.Channel != domain.ChannelDev || report.CredentialsStoredBySF || len(report.Providers) != 3 {
		t.Fatalf("report=%+v", report)
	}
	if action := report.Providers[1].NextAction; action == nil || len(action.Argv) != 4 || action.Argv[0] != "sf-dev" || action.Argv[3] != "cursor" {
		t.Fatalf("cursor action=%+v", action)
	}
	if action := report.Providers[2].NextAction; action == nil || action.Argv[0] != "sf-dev" {
		t.Fatalf("claude action=%+v", action)
	}
	encoded, _ := json.Marshal(response)
	for _, forbidden := range []string{"token=", "Authorization", "/Users/"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatalf("unsafe auth response contains %q: %s", forbidden, encoded)
		}
	}
}

func TestAuthLoginReportsOnlyConfirmedOfficialState(t *testing.T) {
	service := &fakeAuthentication{
		login:     localauth.Status{Provider: localauth.Codex, Executable: "codex", Installed: true, Authenticated: true, State: localauth.StateAuthenticated, Version: "codex-cli 0.151.0"},
		attempted: true,
	}
	response := RunAuthLogin(context.Background(), domain.ChannelStable, "CODEX", localauth.Terminal{}, service)
	if !response.OK || !response.Mutation.Attempted || !response.Mutation.Observed || response.Mutation.Kind != "credential.login" || response.Mutation.Identity != "codex" || service.provider != localauth.Codex {
		t.Fatalf("response=%+v provider=%s", response, service.provider)
	}
	if err := validateCLIResponse(response); err != nil {
		t.Fatal(err)
	}
}

func TestAuthLoginFailuresHaveOneExecutableActionAndTruthfulMutation(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		attempted bool
		code      string
		exit      ExitCode
	}{
		{name: "missing", err: localauth.ErrNotInstalled, code: "provider_unavailable", exit: ExitAction},
		{name: "unsafe", err: localauth.ErrBinaryChanged, code: "safety_blocked", exit: ExitPolicy},
		{name: "failed after launch", err: localauth.ErrLoginFailed, attempted: true, code: "provider_auth_missing", exit: ExitAction},
		{name: "canceled", err: errors.Join(localauth.ErrLoginFailed, context.Canceled), attempted: true, code: "provider_waiting", exit: ExitWait},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &fakeAuthentication{attempted: test.attempted, err: test.err}
			response := RunAuthLogin(context.Background(), domain.ChannelDev, "claude", localauth.Terminal{}, service)
			if response.OK || response.Error == nil || response.Error.Code != test.code || response.Mutation.Attempted != test.attempted || response.NextAction == nil || response.NextAction.Argv[0] != "sf-dev" || exitCode(response) != test.exit {
				t.Fatalf("response=%+v", response)
			}
			if err := validateCLIResponse(response); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUnknownAuthProviderIsInputError(t *testing.T) {
	response := RunAuthLogin(context.Background(), domain.ChannelStable, "not-real", localauth.Terminal{}, &fakeAuthentication{})
	if response.OK || response.Error == nil || response.Error.Code != "invalid_argument" || exitCode(response) != ExitInput || response.NextAction == nil || response.NextAction.Argv[0] != "sf" {
		t.Fatalf("response=%+v", response)
	}
	var rendered bytes.Buffer
	if err := Render(&rendered, response, false); err != nil {
		t.Fatal(err)
	}
	if bytes.Count(rendered.Bytes(), []byte("Next:")) != 1 {
		t.Fatalf("rendered=%q", rendered.String())
	}
}

func TestAuthStatusResponseRoundTripsWireEnvelope(t *testing.T) {
	response := RunAuthStatus(context.Background(), domain.ChannelStable, &fakeAuthentication{})
	var output bytes.Buffer
	if err := Render(&output, response, true); err != nil {
		t.Fatal(err)
	}
	var decoded api.Response
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil || !decoded.OK {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
}
