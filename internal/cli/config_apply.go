package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
)

// ConfigApplyRequest names one existing channel-local project. Configuration
// apply is a direct local Store operation rather than a daemon request, so an
// operator can stage the next immutable generation while a daemon is stopped.
type ConfigApplyRequest struct {
	Channel domain.Channel
	Project string
	Home    string
	Paths   config.ChannelPaths
}

type configApplyResult struct {
	Channel          domain.Channel `json:"channel"`
	Project          string         `json:"project"`
	Repository       string         `json:"repository"`
	BaseBranch       string         `json:"base_branch"`
	ConfigGeneration uint64         `json:"config_generation"`
	ConfigDigest     string         `json:"config_digest"`
	Observed         bool           `json:"observed"`
}

// afterConfigApplyBaseVerification is test-only synchronization for the path
// replacement window between Git base inspection and the final descriptor
// identity recheck. Production leaves it nil.
var afterConfigApplyBaseVerification func()

// afterConfigApplyBeforeStore is test-only coordination after the final
// repository/config and machine-policy samples have been authenticated but
// before the immutable generation transaction begins.
var afterConfigApplyBeforeStore func()

// RunConfigApply resolves the current optional .sf/config.toml under the
// canonical repository configuration lock and appends a new project snapshot.
// It never creates or edits repository configuration. Only a queued ticket
// that starts after this returns obtains the new generation; active tickets
// retain their already-frozen authority.
func RunConfigApply(ctx context.Context, request ConfigApplyRequest) api.Response {
	binary := binaryForChannel(request.Channel)
	help := []string{binary, "config", "apply", "--help"}
	if !request.Channel.Valid() || request.Project == "" {
		return failure("invalid_argument", "channel and project are required", help)
	}
	paths := request.Paths
	var err error
	if paths.Root == "" {
		home := request.Home
		if home == "" {
			home, err = os.UserHomeDir()
			if err != nil {
				return failure("config_apply_failed", "current-user home directory is unavailable", help)
			}
		}
		paths, err = config.PathsFor(home, request.Channel)
		if err != nil {
			return failure("config_apply_failed", "channel paths could not be resolved", help)
		}
	}
	info, err := os.Lstat(paths.Database)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return failure("not_configured", "the channel has no registered local authority", []string{binary, "init", "--help"})
		}
		return failure("config_apply_failed", "local authority cannot be inspected: "+err.Error(), []string{binary, "doctor"})
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() == 0 {
		return failure("not_configured", "the channel has no registered local authority", []string{binary, "init", "--help"})
	}
	database, err := store.OpenChannel(ctx, paths.Database, paths.Backups, request.Channel)
	if err != nil {
		return configApplyStoreOpenFailure(err, binary, help)
	}
	defer database.Close()
	project, err := database.Project(ctx, request.Channel, domain.ProjectID(request.Project))
	if errors.Is(err, store.ErrNotFound) {
		return failure("unknown_project", "the project is not registered in this channel", []string{binary, "init", "--help"})
	}
	if err != nil {
		return configApplyStoreFailure(err, binary, help)
	}
	repository, err := canonicalGitRepository(ctx, project.Path)
	if err != nil || repository != project.Path {
		message := "registered repository is unavailable or no longer canonical"
		if err != nil {
			message += ": " + err.Error()
		}
		return failure("invalid_repository", message, []string{binary, "doctor", "--repo", project.Path})
	}
	identity, err := config.CaptureRepositoryIdentity(repository)
	if err != nil {
		return failure("invalid_repository", "registered repository identity could not be authenticated: "+err.Error(), []string{binary, "doctor", "--repo", repository})
	}
	plan, err := config.PrepareProjectConfigWithIdentityContext(ctx, repository, identity)
	if err != nil {
		return failure("invalid_configuration", "project configuration could not be locked: "+err.Error(), help)
	}
	defer plan.Close()
	machine, err := config.LoadMachine(paths.Machine)
	if err != nil {
		return failure("invalid_configuration", err.Error(), help)
	}
	effective, snapshot, digest, err := plan.LoadLockedProject(request.Project, machine)
	if err != nil {
		return configApplyConfigFailure(err, binary, help)
	}
	if effective.Repository != project.Path || effective.BaseBranch != project.BaseRef {
		return failure("project_conflict", "configuration would change the immutable repository path or base branch", []string{binary, "init", "--help"})
	}
	if err := verifyBaseRef(ctx, repository, effective.BaseBranch); err != nil {
		return failure("invalid_repository", "configured base branch is unavailable in the local repository", []string{binary, "doctor", "--repo", repository})
	}
	if afterConfigApplyBaseVerification != nil {
		afterConfigApplyBaseVerification()
	}
	if err := plan.ValidateUnchanged(); err != nil {
		return failure("invalid_configuration", "project configuration changed during apply: "+err.Error(), help)
	}
	if currentMachine, err := config.LoadMachine(paths.Machine); err != nil {
		return failure("invalid_configuration", err.Error(), help)
	} else if currentMachine != machine {
		return failure("invalid_configuration", "machine configuration changed during apply", help)
	}
	if afterConfigApplyBeforeStore != nil {
		afterConfigApplyBeforeStore()
	}
	applied, observed, err := database.ApplyProjectConfiguration(ctx, store.ProjectConfiguration{
		Channel: request.Channel, Project: domain.ProjectID(request.Project), Path: project.Path, BaseRef: project.BaseRef,
		Digest: digest, Snapshot: snapshot,
	})
	if err != nil {
		return configApplyStoreFailure(err, binary, help)
	}
	data, err := json.Marshal(configApplyResult{
		Channel: request.Channel, Project: request.Project, Repository: applied.Path, BaseBranch: applied.BaseRef,
		ConfigGeneration: applied.ConfigGeneration, ConfigDigest: applied.ConfigDigest, Observed: observed,
	})
	if err != nil {
		return failure("config_apply_failed", "configuration generation was applied but its response could not be encoded", []string{binary, "doctor"})
	}
	return api.Response{
		Version: api.Version, RequestID: requestID(), OK: true,
		Mutation: api.Mutation{Attempted: true, Kind: "project.config_apply", Identity: string(request.Channel) + "/" + request.Project, Observed: observed},
		Data:     data,
	}
}

func configApplyConfigFailure(err error, binary string, help []string) api.Response {
	next := help
	if errors.Is(err, config.ErrCommandDetection) {
		next = []string{binary, "config", "--help"}
	}
	return failure("invalid_configuration", err.Error(), next)
}

func configApplyStoreOpenFailure(err error, binary string, help []string) api.Response {
	message := err.Error()
	if strings.Contains(message, "schema") || strings.Contains(message, "migration") || strings.Contains(message, "foreign key") {
		return failure("schema_mismatch", "local authority schema is incompatible: "+message, []string{binary, "doctor"})
	}
	return failure("config_apply_failed", "local authority could not be opened: "+message, []string{binary, "doctor"})
}

func configApplyStoreFailure(err error, binary string, help []string) api.Response {
	switch {
	case errors.Is(err, store.ErrBusy):
		return failure("store_busy", "local authority is busy", help)
	case errors.Is(err, store.ErrProjectConflict):
		return failure("project_conflict", "configuration conflicts with immutable project registration or an earlier generation", []string{binary, "doctor"})
	case errors.Is(err, store.ErrNotFound):
		return failure("unknown_project", "the project is not registered in this channel", []string{binary, "init", "--help"})
	default:
		return failure("config_apply_failed", fmt.Sprintf("could not apply configuration generation: %v", err), help)
	}
}
