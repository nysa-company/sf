package cli

import (
	"context"
	"encoding/json"
	"os"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

type ConfigProvidersRequest struct {
	ConfigApplyRequest
	Preset string
}

func RunConfigProviders(ctx context.Context, request ConfigProvidersRequest) api.Response {
	binary := binaryForChannel(request.Channel)
	help := []string{binary, "config", "providers", "--help"}
	preferences, err := config.ProviderPreset(request.Preset)
	if err != nil || request.Preset == "" || !request.Channel.Valid() || request.Project == "" {
		return failure("invalid_argument", "project and explicit supported provider preset are required", help)
	}
	paths := request.Paths
	if paths.Root == "" {
		home := request.Home
		if home == "" {
			home, err = os.UserHomeDir()
			if err != nil {
				return failure("not_configured", "current-user home is unavailable", help)
			}
		}
		paths, err = config.PathsFor(home, request.Channel)
		if err != nil {
			return failure("not_configured", "channel paths are unavailable", help)
		}
	}
	db, err := store.OpenReadOnly(ctx, paths.Database)
	if err != nil {
		return failure("not_configured", "registered project authority is unavailable", []string{binary, "init", "--help"})
	}
	defer db.Close()
	project, err := db.Project(ctx, request.Channel, domain.ProjectID(request.Project))
	if err != nil {
		return failure("unknown_project", "project is not registered in this channel", help)
	}
	repository, err := canonicalGitRepository(ctx, project.Path)
	if err != nil || repository != project.Path {
		return failure("invalid_repository", "registered repository is unavailable or changed", []string{binary, "doctor", "--repo", project.Path})
	}
	plan, err := config.PrepareProjectConfigContext(ctx, repository)
	if err != nil {
		return failure("invalid_configuration", "could not lock project configuration: "+err.Error(), help)
	}
	defer plan.Close()
	machine, err := config.LoadMachine(paths.Machine)
	if err != nil {
		return failure("invalid_configuration", err.Error(), help)
	}
	edit, err := plan.EditProviderPreset(ctx, request.Project, machine, request.Preset)
	data, _ := json.Marshal(map[string]any{"provider_configuration": map[string]any{"project": request.Project, "preset": request.Preset, "preferences": preferences, "changed": edit.Changed, "backup_path": edit.BackupPath, "applied": false, "note": "Preferences are not applied yet. Review the file and run config apply; active tickets retain their frozen settings."}})
	mutation := api.Mutation{Attempted: edit.Changed || edit.BackupPath != "", Kind: "project.config_edit", Identity: string(request.Channel) + "/" + request.Project, Observed: !edit.Changed}
	if err != nil {
		response := failure("invalid_configuration", "provider edit could not be completed: "+err.Error(), help)
		response.Data, response.Mutation = data, mutation
		return response
	}
	return api.Response{Version: api.Version, RequestID: requestID(), OK: true, Mutation: mutation, Data: data, NextAction: &domain.NextAction{Code: "config_apply_required", Argv: []string{binary, "config", "apply", "--project", request.Project}}}
}
