package cli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/executionpolicy"
	"github.com/nysa-company/sf/internal/goclosure"
	"github.com/nysa-company/sf/internal/nodeclosure"
	"github.com/nysa-company/sf/internal/nysapure"
)

// Resolve user conveniences at the CLI edge. RunInit continues to require
// the same canonical, authenticated root and immutable Store registration.
func resolveInitRequest(ctx context.Context, request InitRequest) (InitRequest, error) {
	if request.Repo == "" {
		request.Repo = "."
	}
	if strings.ContainsAny(request.Repo, "\x00\r\n\t") {
		return request, errors.New("repository path contains control characters")
	}
	absolute, err := filepath.Abs(request.Repo)
	if err != nil {
		return request, errors.New("current repository path is unavailable")
	}
	root, err := canonicalGitRepository(ctx, absolute)
	if err != nil {
		return request, err
	}
	request.Repo = root
	if request.Project == "" {
		request.Project = inferredProjectName(filepath.Base(root))
	}
	return request, nil
}

func inferredProjectName(value string) string {
	var result strings.Builder
	separator := false
	for _, r := range strings.ToLower(value) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			if separator && result.Len() > 0 {
				result.WriteByte('-')
			}
			result.WriteRune(r)
			separator = false
		} else {
			separator = true
		}
	}
	name := result.String()
	if name == "" {
		name = "project"
	}
	if name[0] < 'a' || name[0] > 'z' {
		name = "project-" + name
	}
	if len(name) > 48 {
		name = strings.TrimRight(name[:48], "-")
	}
	return name
}

type initPreview struct {
	Project       string `json:"project"`
	Repository    string `json:"repository"`
	Configuration string `json:"configuration"`
	LocalRecipe   string `json:"local_recipe"`
	Reason        string `json:"reason"`
	Runtime       string `json:"runtime"`
	Providers     string `json:"providers"`
	Publication   string `json:"publication"`
}

// RunInitCheck is a capability preview, not persisted authority or a launch
// proof. It never opens a writer database, creates config/locks, executes
// repository code, invokes a provider, or contacts GitHub. Init revalidates.
func RunInitCheck(ctx context.Context, request InitRequest) api.Response {
	next := []string{binaryForChannel(request.Channel), "init", "--help"}
	if !request.Channel.Valid() {
		return failure("invalid_argument", "invalid channel", next)
	}
	resolved, err := resolveInitRequest(ctx, request)
	if err != nil {
		return failure("invalid_repository", err.Error(), next)
	}
	request = resolved
	if request.Profile != "" || request.TestPath != "" {
		return failure("invalid_argument", "--check currently previews existing configuration; omit --profile and --test", next)
	}
	preview := initPreview{Project: request.Project, Repository: request.Repo, Configuration: "not_checked", LocalRecipe: "not_checked", Runtime: "not_checked", Providers: "not_checked", Publication: "not_checked"}
	response := api.Response{Version: api.Version, RequestID: requestID(), OK: true, Mutation: api.Mutation{}}
	finish := func() api.Response {
		response.Data, _ = json.Marshal(map[string]any{"setup": preview})
		return response
	}
	refuse := func(reason string) api.Response {
		preview.Reason = reason
		response = failure("invalid_configuration", reason, next)
		return finish()
	}
	paths := request.Paths
	if paths.Root == "" {
		home := request.Home
		if home == "" {
			home, err = os.UserHomeDir()
			if err != nil {
				return refuse("user-local machine policy path is unavailable")
			}
		}
		paths, err = config.PathsFor(home, request.Channel)
		if err != nil {
			return refuse("channel machine policy path is unavailable")
		}
	}
	machine, err := config.LoadMachine(paths.Machine)
	if err != nil {
		preview.Configuration = "unsupported"
		return refuse("machine configuration is invalid; inspect the channel machine policy")
	}
	effective, _, _, err := config.LoadProject(request.Repo, request.Project, machine)
	if err != nil {
		preview.Configuration = "unsupported"
		if errors.Is(err, config.ErrCommandDetection) {
			preview.LocalRecipe = "unsupported"
			// Detection errors contain code-owned explanations, never repository
			// contents or dependency-manager output.
			return refuse(err.Error())
		}
		return refuse("project configuration or dependency closure is unsupported; consult docs/configuration.md for supported recipes")
	}
	preview.Configuration = "accepted"
	if err := verifyBaseRef(ctx, request.Repo, effective.BaseBranch); err != nil {
		return refuse("configured base branch is not available locally")
	}
	for _, command := range []config.Command{effective.Commands.Verify, effective.Commands.Review} {
		decision := executionpolicy.EvaluateRepositoryCommand(command.Argv)
		if !decision.Allowed {
			preview.LocalRecipe = "unsupported"
			return refuse(decision.Reason)
		}
		var closureErr error
		switch filepath.Base(command.Argv[0]) {
		case "go":
			_, closureErr = goclosure.Validate(request.Repo)
		case "node":
			if len(command.Argv) == 3 && command.Argv[1] == nysapure.RecipeFlag {
				closureErr = nysapure.Validate(request.Repo, command.Argv[2])
			} else {
				closureErr = nodeclosure.Validate(request.Repo)
			}
		case "python3":
			closureErr = checkPythonRecipeReady(ctx, paths, request.Repo, command.Argv)
			if closureErr != nil {
				preview.Runtime = "unavailable"
				next = []string{binaryForChannel(request.Channel), "runtimes", "prepare", "python"}
				return refuse("Python requires the pinned prepared runtime and a real selected test path; additional dependencies are not installed by this profile")
			}
			preview.Runtime = "Python prepared; other runtime checks remain separate"
		default:
			closureErr = errors.New("unsupported recipe")
		}
		if closureErr != nil {
			preview.LocalRecipe = "unsupported"
			return refuse("selected command has no supported local dependency/test closure")
		}
	}
	preview.LocalRecipe = "supported"
	if runtime.GOOS != "darwin" {
		preview.Runtime = "unsupported"
		return refuse("local execution requires macOS")
	}
	preview.Reason = "Local recipe accepted. Executable versions, provider qualification and GitHub readiness still require doctor and runtime checks. No registration or execution occurred."
	return finish()
}
