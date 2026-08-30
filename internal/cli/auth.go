package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nysa-company/sf/internal/api"
	localauth "github.com/nysa-company/sf/internal/auth"
	"github.com/nysa-company/sf/internal/domain"
)

const authSchema = "sf.auth/v1"

type authenticationService interface {
	StatusAll(context.Context) []localauth.Status
	Login(context.Context, localauth.Provider, localauth.Terminal) (localauth.Status, bool, error)
}

type authStatusView struct {
	Provider      localauth.Provider `json:"provider"`
	Executable    string             `json:"executable"`
	Installed     bool               `json:"installed"`
	Authenticated bool               `json:"authenticated"`
	State         localauth.State    `json:"state"`
	Version       string             `json:"version,omitempty"`
	Reason        string             `json:"reason,omitempty"`
	NextAction    *domain.NextAction `json:"next_action,omitempty"`
}

type authReport struct {
	Schema                string           `json:"schema"`
	Channel               domain.Channel   `json:"channel"`
	CredentialsStoredBySF bool             `json:"credentials_stored_by_sf"`
	Providers             []authStatusView `json:"providers"`
}

func RunAuthStatus(ctx context.Context, channel domain.Channel, service authenticationService) api.Response {
	if !channel.Valid() || service == nil {
		return failure("invalid_argument", "authentication status requires a valid channel", []string{binaryForChannel(channel), "auth", "status"})
	}
	report := authReport{Schema: authSchema, Channel: channel, CredentialsStoredBySF: false}
	for _, status := range service.StatusAll(ctx) {
		report.Providers = append(report.Providers, authView(channel, status))
	}
	data, err := json.Marshal(report)
	if err != nil {
		return failure("internal_error", "authentication status could not be encoded", []string{binaryForChannel(channel), "doctor"})
	}
	return api.Response{Version: api.Version, RequestID: requestID(), OK: true, Mutation: api.Mutation{}, Data: data}
}

func RunAuthLogin(ctx context.Context, channel domain.Channel, providerName string, terminal localauth.Terminal, service authenticationService) api.Response {
	binary := binaryForChannel(channel)
	if !channel.Valid() || service == nil {
		return failure("invalid_argument", "authentication login requires a valid channel", []string{binary, "auth", "login", "--help"})
	}
	provider, err := localauth.ParseProvider(providerName)
	if err != nil {
		return failure("invalid_argument", "provider must be one of github, cursor, claude, or codex", []string{binary, "auth", "login", "--help"})
	}
	status, attempted, err := service.Login(ctx, provider, terminal)
	if err != nil {
		response := authLoginFailure(binary, provider, err)
		response.Mutation = api.Mutation{Attempted: attempted, Kind: "credential.login", Identity: string(provider)}
		return response
	}
	report := authReport{Schema: authSchema, Channel: channel, CredentialsStoredBySF: false, Providers: []authStatusView{authView(channel, status)}}
	data, marshalErr := json.Marshal(report)
	if marshalErr != nil {
		response := failure("internal_error", "authentication succeeded but its response could not be encoded", []string{binary, "auth", "status"})
		response.Mutation = api.Mutation{Attempted: attempted, Kind: "credential.login", Identity: string(provider), Observed: status.Authenticated}
		return response
	}
	return api.Response{
		Version: api.Version, RequestID: requestID(), OK: true,
		Mutation: api.Mutation{Attempted: attempted, Kind: "credential.login", Identity: string(provider), Observed: status.Authenticated},
		Data:     data,
	}
}

func authLoginFailure(binary string, provider localauth.Provider, err error) api.Response {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return failure("provider_waiting", "interactive authentication was interrupted", []string{binary, "auth", "login", string(provider)})
	case errors.Is(err, localauth.ErrNotInstalled):
		return failure("provider_unavailable", fmt.Sprintf("%s executable is not installed; install the official CLI before login", provider), []string{binary, "auth", "status"})
	case errors.Is(err, localauth.ErrProbeFailed), errors.Is(err, localauth.ErrBinaryChanged):
		return failure("safety_blocked", fmt.Sprintf("%s executable identity or version could not be verified", provider), []string{binary, "doctor"})
	default:
		return failure("provider_auth_missing", fmt.Sprintf("%s authentication was not confirmed", provider), []string{binary, "auth", "login", string(provider)})
	}
}

func authView(channel domain.Channel, status localauth.Status) authStatusView {
	view := authStatusView{
		Provider: status.Provider, Executable: status.Executable, Installed: status.Installed,
		Authenticated: status.Authenticated, State: status.State, Version: status.Version, Reason: status.Reason,
	}
	binary := binaryForChannel(channel)
	switch status.State {
	case localauth.StateUnauthenticated:
		view.NextAction = &domain.NextAction{Code: "provider_auth_missing", Argv: []string{binary, "auth", "login", string(status.Provider)}}
	case localauth.StateUnavailable:
		view.NextAction = &domain.NextAction{Code: "provider_unavailable", Argv: []string{binary, "auth", "status"}}
	case localauth.StateProbeFailed:
		view.NextAction = &domain.NextAction{Code: "safety_blocked", Argv: []string{binary, "doctor"}}
	}
	return view
}
