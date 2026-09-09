package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
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

type fakeConfigurableAuthentication struct {
	fakeAuthentication
	options localauth.LoginOptions
}

func (service *fakeConfigurableAuthentication) LoginWithOptions(ctx context.Context, provider localauth.Provider, terminal localauth.Terminal, options localauth.LoginOptions) (localauth.Status, bool, error) {
	service.options = options
	return service.Login(ctx, provider, terminal)
}

func TestAuthLoginExplicitProtocolDispatchAndValidation(t *testing.T) {
	for _, protocol := range []string{"ssh", "https"} {
		service := &fakeConfigurableAuthentication{fakeAuthentication: fakeAuthentication{login: localauth.Status{Provider: localauth.GitHub, Authenticated: true, State: localauth.StateAuthenticated}, attempted: true}}
		response := RunAuthLoginWithOptions(context.Background(), domain.ChannelDev, "github", localauth.Terminal{}, service, localauth.LoginOptions{GitProtocol: protocol})
		if !response.OK || service.options.GitProtocol != protocol || service.provider != localauth.GitHub || !response.Mutation.Attempted {
			t.Fatalf("response=%+v options=%+v", response, service.options)
		}
	}
	for _, test := range []struct{ provider, protocol string }{{"codex", "ssh"}, {"claude", "https"}, {"cursor", "ssh"}, {"github", "SSH"}, {"github", "git"}} {
		service := &fakeConfigurableAuthentication{}
		response := RunAuthLoginWithOptions(context.Background(), domain.ChannelStable, test.provider, localauth.Terminal{}, service, localauth.LoginOptions{GitProtocol: test.protocol})
		if response.OK || response.Error == nil || response.Error.Code != "invalid_argument" || response.Mutation.Attempted || service.provider != "" {
			t.Fatalf("test=%+v response=%+v", test, response)
		}
	}
}

func TestAuthLoginExplicitProtocolRequiresSupportingService(t *testing.T) {
	service := &fakeAuthentication{}
	response := RunAuthLoginWithOptions(context.Background(), domain.ChannelStable, "github", localauth.Terminal{}, service, localauth.LoginOptions{GitProtocol: "ssh"})
	if response.OK || response.Error == nil || response.Error.Code != "invalid_argument" || service.provider != "" {
		t.Fatalf("response=%+v", response)
	}
}

func TestAuthLoginExplicitProtocolFailurePreservesRetrySelection(t *testing.T) {
	service := &fakeConfigurableAuthentication{fakeAuthentication: fakeAuthentication{attempted: true, err: localauth.ErrLoginFailed}}
	response := RunAuthLoginWithOptions(context.Background(), domain.ChannelDev, "github", localauth.Terminal{}, service, localauth.LoginOptions{GitProtocol: "ssh"})
	if response.OK || !response.Mutation.Attempted || response.NextAction == nil || !reflect.DeepEqual(response.NextAction.Argv, []string{"sf-dev", "auth", "login", "github", "--git-protocol", "ssh"}) {
		t.Fatalf("response=%+v", response)
	}
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

func TestAuthenticatedCLIsDoNotImplyExecutionReadiness(t *testing.T) {
	for _, channel := range []domain.Channel{domain.ChannelStable, domain.ChannelDev} {
		service := &fakeAuthentication{}
		for _, provider := range []localauth.Provider{localauth.Claude, localauth.Cursor, localauth.Codex} {
			service.statuses = append(service.statuses, localauth.Status{Provider: provider, Installed: true, Authenticated: true, State: localauth.StateAuthenticated})
		}
		response := RunAuthStatus(context.Background(), channel, service)
		var report authReport
		if err := json.Unmarshal(response.Data, &report); err != nil {
			t.Fatal(err)
		}
		if report.Scope != authScope {
			t.Fatal("missing qualification boundary")
		}
		for _, provider := range report.Providers {
			if provider.NextAction == nil || len(provider.NextAction.Argv) != 2 || provider.NextAction.Argv[0] != binaryForChannel(channel) || provider.NextAction.Argv[1] != "doctor" {
				t.Fatal("wrong readiness action")
			}
		}
		var output bytes.Buffer
		if err := Render(&output, response, false); err != nil {
			t.Fatal(err)
		}
		for _, expected := range []string{authScope, "claude: authenticated", "cursor: authenticated", "codex: authenticated", binaryForChannel(channel) + " doctor"} {
			if !bytes.Contains(output.Bytes(), []byte(expected)) {
				t.Fatalf("missing %q in %s", expected, output.String())
			}
		}
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
