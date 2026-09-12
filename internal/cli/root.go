package cli

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"github.com/nysa-company/sf/internal/api"
	localauth "github.com/nysa-company/sf/internal/auth"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/ticket"
	"github.com/nysa-company/sf/internal/version"
)

type app struct {
	client              Client
	out                 io.Writer
	errOut              io.Writer
	json                bool
	channel             domain.Channel
	last                *api.Response
	ctx                 context.Context
	runDaemon           func(context.Context) error
	input               io.Reader
	interactive         func() bool
	fetchIssue          func(context.Context, string) ([]byte, error)
	expectedDraftDigest string
	canonical           bool
}

// NewCommand returns the public CLI. The client is injected so command tests
// never need a daemon and production remains a thin socket client.
func NewCommand(client Client, out, errOut io.Writer) *cobra.Command {
	return newApp(client, out, errOut).command()
}

func newApp(client Client, out, errOut io.Writer) *app {
	if out == nil {
		out = io.Discard
	}
	if errOut == nil {
		errOut = io.Discard
	}
	channel := domain.Channel(version.Channel)
	if !channel.Valid() {
		channel = domain.ChannelStable
	}
	return &app{client: client, out: out, errOut: errOut, channel: channel, ctx: context.Background()}
}

func (a *app) command() *cobra.Command {
	use := "sf"
	if a.channel == domain.ChannelDev {
		use = "sf-dev"
	}
	root := &cobra.Command{
		Use:           use,
		Short:         "safe local software factory",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
	}
	root.SetOut(a.out)
	root.SetErr(a.errOut)
	root.PersistentPreRun = func(cmd *cobra.Command, _ []string) {
		a.ctx = cmd.Context()
		a.canonical = false
		for parent := cmd; parent != nil; parent = parent.Parent() {
			if parent.Name() == "ticket" || parent.Name() == "factory" {
				a.canonical = true
			}
		}
	}
	root.PersistentFlags().BoolVar(&a.json, "json", false, "render the versioned JSON response")
	root.AddCommand(a.submitCommand(), a.startCommand(), a.statusCommand(), a.showCommand(), a.logsCommand(), a.controlCommand("pause"), a.controlCommand("resume"), a.recoverCommand(), a.controlCommand("cancel"), a.retryCommand(), a.controlCommand("take"), a.approveCommand(), a.rejectCommand(), a.doctorCommand(), a.authCommand(), a.initCommand(), a.providersCommand(), a.daemonCommand(), a.configCommand(), a.simpleSetupCommand("update"), a.simpleSetupCommand("rollback"), a.versionCommand())
	configureCommandHelp(root)
	root.AddCommand(a.ticketsCommand())
	root.AddCommand(a.ticketCommand())
	factory := a.daemonCommand()
	factory.Use = "factory"
	factory.Short = "Run or inspect this channel's foreground factory"
	root.AddCommand(factory)
	root.AddCommand(a.runCommand())
	root.AddCommand(a.bundleCommand())
	root.AddCommand(a.runtimesCommand())
	root.AddCommand(a.homeCommand())
	a.configureTicketSelection(root)
	a.configureDecisionSelection(root)
	configureCanonicalHelp(root)
	return root
}

// Execute runs a command and renders its one authoritative response.
func Execute(ctx context.Context, args []string, out, errOut io.Writer, client Client) int {
	return ExecuteWithDaemon(ctx, args, out, errOut, client, nil)
}

// ExecuteWithDaemon adds the local foreground lifecycle at the composition
// root. cli itself remains independent of the daemon implementation.
func ExecuteWithDaemon(ctx context.Context, args []string, out, errOut io.Writer, client Client, runDaemon func(context.Context) error) int {
	a := newApp(client, out, errOut)
	a.runDaemon = runDaemon
	command := a.command()
	command.SetArgs(args)
	// unknownCommand runs before Cobra parses flags so an unknown token can be
	// rendered as a typed response. Preserve the caller's requested format in
	// that early path as well.
	a.json = jsonFlagRequested(args)
	if unknownCommand(command, args) {
		response := failure("invalid_command", "unknown command", []string{binaryName(), "--help"})
		_ = Render(errOut, response, a.json)
		return int(exitCode(response))
	}
	if executed, err := command.ExecuteContextC(ctx); err != nil {
		response := failure("invalid_command", err.Error(), commandHelpAction(executed))
		_ = Render(errOut, response, a.json)
		return int(exitCode(response))
	}
	if a.last == nil {
		return int(ExitOK)
	}
	return int(exitCode(*a.last))
}

func jsonFlagRequested(args []string) bool {
	for _, arg := range args {
		if arg == "--json" || arg == "--json=true" {
			return true
		}
	}
	return false
}

// unknownCommand compensates for Cobra's helpful-but-successful root help
// behavior when an otherwise unknown token is supplied to a non-runnable root.
// It deliberately stops after a runnable command so ticket paths and flag
// values are never mistaken for subcommands.
func unknownCommand(root *cobra.Command, args []string) bool {
	current := root
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") {
			continue
		}
		if arg == "help" || arg == "completion" {
			return false
		}
		var next *cobra.Command
		for _, candidate := range current.Commands() {
			if candidate.Name() == arg || candidate.HasAlias(arg) {
				next = candidate
				break
			}
		}
		if next == nil {
			return true
		}
		current = next
		if current.Runnable() {
			return false
		}
	}
	return false
}

func (a *app) request(method, ticket string, params any) api.Response {
	data, err := json.Marshal(params)
	if err != nil {
		return failure("invalid_argument", "could not encode command parameters", []string{binaryName(), "--help"})
	}
	if a.client == nil {
		return failure("daemon_unavailable", "the local daemon client is not configured", []string{binaryName(), "daemon", "run"})
	}
	operator := ""
	if values, ok := params.(map[string]any); ok {
		operator, _ = values["operator"].(string)
	}
	response, err := a.client.Call(a.context(), api.Request{Version: api.Version, RequestID: requestID(), Method: method, Ticket: ticket, OperatorLabel: operator, Parameters: data})
	if err != nil {
		return failure("daemon_unavailable", "the local daemon is unavailable", []string{binaryName(), "daemon", "run"})
	}
	if err := validateCLIResponse(response); err != nil {
		code := "internal_error"
		if response.Version != "" && response.Version != api.Version {
			code = "protocol_incompatible"
		}
		return failure(code, "the daemon returned an invalid response: "+err.Error(), []string{binaryName(), "--help"})
	}
	return response
}

func (a *app) context() context.Context {
	if a.ctx != nil {
		return a.ctx
	}
	return context.Background()
}

func (a *app) emit(response api.Response) error {
	if a.canonical {
		response = canonicalResponse(response)
	}
	copy := response
	a.last = &copy
	if err := Render(a.out, response, a.json); err == nil {
		return nil
	} else {
		code := "internal_error"
		if response.Version != "" && response.Version != api.Version {
			code = "protocol_incompatible"
		}
		fallback := failure(code, "could not render the daemon response: "+err.Error(), []string{binaryName(), "--help"})
		a.last = &fallback
		// The response has already been classified as an internal/compatibility
		// failure. A second writer failure must not be reclassified as a Cobra
		// input error (or trigger a second response envelope).
		_ = Render(a.out, fallback, a.json)
		return nil
	}
}

func failure(code, message string, argv []string) api.Response {
	return api.Response{Version: api.Version, RequestID: requestID(), OK: false, Mutation: api.Mutation{Attempted: false}, Error: &api.Error{Code: code, Message: message}, NextAction: &domain.NextAction{Code: code, Argv: argv}}
}

func notConfigured(command string) api.Response {
	return failure("not_configured", command+" is not configured in this build", []string{binaryName(), "--help"})
}

var fallbackRequestSequence uint64

func requestID() string {
	var random [16]byte
	if _, err := cryptorand.Read(random[:]); err == nil {
		return fmt.Sprintf("cli-%x", random)
	}
	// Keep the fallback unique within a process and include process/time
	// entropy for callers running without a functioning system CSPRNG.
	sequence := atomic.AddUint64(&fallbackRequestSequence, 1)
	return "cli-fallback-" + strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatInt(int64(os.Getpid()), 36) + "-" + strconv.FormatUint(sequence, 36)
}

func binaryName() string {
	name := filepath.Base(os.Args[0])
	if name == "" || name == "." || name == "/" {
		return "sf"
	}
	return name
}

func params(values map[string]any, channel domain.Channel) map[string]any {
	values["channel"] = channel
	return values
}

func (a *app) submitCommand() *cobra.Command {
	var project string
	var allowNew bool
	command := &cobra.Command{Use: "submit <ticket.md> --project <name>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		file, err := os.Open(args[0])
		if err != nil {
			return a.emit(failure("invalid_ticket", "ticket file could not be opened", []string{binaryName(), "submit", args[0], "--project", project}))
		}
		parsed, parseErr := ticket.Parse(file)
		closeErr := file.Close()
		if parseErr != nil || closeErr != nil {
			return a.emit(failure("invalid_ticket", "ticket file does not meet the local Markdown contract", []string{binaryName(), "submit", args[0], "--project", project}))
		}
		return a.emit(a.request("ticket.submit", "", params(map[string]any{"source": string(parsed.Source), "project": project, "new": allowNew}, a.channel)))
	}}
	command.Flags().StringVar(&project, "project", "", "registered project name")
	command.Flags().BoolVar(&allowNew, "new", false, "create a new identity when the same ticket already finished")
	_ = command.MarkFlagRequired("project")
	return command
}

func (a *app) startCommand() *cobra.Command {
	var estimates bool
	command := &cobra.Command{Use: "start <ticket>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		values := map[string]any{}
		if estimates {
			values["accept_cost_estimates"] = true
		}
		return a.emit(a.request("ticket.start", args[0], params(values, a.channel)))
	}}
	command.Flags().BoolVar(&estimates, "accept-cost-estimates", false, "accept estimated (not verified) costs with time/request limits; not a hard dollar cap")
	return command
}

func (a *app) statusCommand() *cobra.Command {
	var watch bool
	command := &cobra.Command{Use: "status [ticket]", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		ticket := ""
		if len(args) == 1 {
			ticket = args[0]
		}
		if !watch {
			return a.emit(a.request("ticket.status", ticket, params(map[string]any{"watch": false}, a.channel)))
		}
		return a.watchStatus(cmd.Context(), ticket)
	}}
	command.Flags().BoolVar(&watch, "watch", false, "follow status changes")
	return command
}

func (a *app) ticketsCommand() *cobra.Command {
	var project string
	command := &cobra.Command{
		Use: "tickets", Short: "List tickets by title and state, optionally within a project",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.emit(a.request("ticket.status", "", params(map[string]any{"project": project, "watch": false}, a.channel)))
		},
	}
	command.Flags().StringVar(&project, "project", "", "filter by registered project name")
	return command
}

const statusWatchInterval = 500 * time.Millisecond

func (a *app) watchStatus(ctx context.Context, ticket string) error {
	return a.watchScopedStatus(ctx, ticket, "")
}

func (a *app) watchProjectStatus(ctx context.Context, project string) error {
	return a.watchScopedStatus(ctx, "", project)
}

func (a *app) watchScopedStatus(ctx context.Context, ticket, project string) error {
	for {
		values := map[string]any{"watch": false}
		if project != "" {
			values["project"] = project
		}
		response := a.request("ticket.status", ticket, params(values, a.channel))
		if err := a.emit(response); err != nil {
			return err
		}
		if !response.OK || a.last != nil && !a.last.OK {
			return nil
		}
		if ticket != "" && terminalStatusResponse(response.Data) {
			return nil
		}
		timer := time.NewTimer(statusWatchInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func terminalStatusResponse(data json.RawMessage) bool {
	var value struct {
		State  domain.State `json:"state"`
		Ticket *struct {
			State domain.State `json:"state"`
		} `json:"ticket"`
	}
	if len(data) == 0 || json.Unmarshal(data, &value) != nil {
		return false
	}
	state := value.State
	if value.Ticket != nil {
		state = value.Ticket.State
	}
	return state.Terminal()
}

func (a *app) showCommand() *cobra.Command {
	return &cobra.Command{Use: "show <ticket>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.show", args[0], params(map[string]any{}, a.channel)))
	}}
}

func (a *app) logsCommand() *cobra.Command {
	var follow bool
	var phase string
	command := &cobra.Command{Use: "logs <ticket>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if !follow {
			return a.emit(a.request("ticket.logs", args[0], params(map[string]any{"follow": false, "phase": phase, "after": uint64(0)}, a.channel)))
		}
		return a.followLogs(cmd.Context(), args[0], phase)
	}}
	command.Flags().BoolVar(&follow, "follow", false, "follow logs")
	command.Flags().StringVar(&phase, "phase", "", "filter by phase")
	return command
}

func (a *app) followLogs(ctx context.Context, ticket, phase string) error {
	var after uint64
	first := true
	for {
		response := a.request("ticket.logs", ticket, params(map[string]any{"follow": true, "phase": phase, "after": after}, a.channel))
		if !response.OK {
			return a.emit(response)
		}
		var page struct {
			NextAfter uint64            `json:"next_after"`
			Events    []json.RawMessage `json:"events"`
		}
		if json.Unmarshal(response.Data, &page) != nil || page.NextAfter < after {
			return a.emit(failure("invalid_response", "the daemon returned an invalid log cursor", []string{binaryName(), "logs", ticket}))
		}
		if first || len(page.Events) > 0 {
			if err := a.emit(response); err != nil {
				return err
			}
		}
		first = false
		if a.last != nil && !a.last.OK {
			return nil
		}
		after = page.NextAfter
		timer := time.NewTimer(statusWatchInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (a *app) controlCommand(name string) *cobra.Command {
	operator := defaultOperatorLabel()
	command := &cobra.Command{Use: name + " <ticket> --operator <identity>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket."+name, args[0], params(map[string]any{"operator": operator}, a.channel)))
	}}
	command.Flags().StringVar(&operator, "operator", operator, "authenticated operator identity")
	return command
}

func (a *app) recoverCommand() *cobra.Command {
	var mode string
	operator := defaultOperatorLabel()
	command := &cobra.Command{Use: "recover <ticket>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.recover", args[0], params(map[string]any{"mode": mode, "operator": operator}, a.channel)))
	}}
	command.Flags().StringVar(&mode, "mode", "", "optional recovery mode (guarded)")
	command.Flags().StringVar(&operator, "operator", operator, "authenticated operator identity")
	return command
}

func (a *app) retryCommand() *cobra.Command {
	operator := defaultOperatorLabel()
	command := &cobra.Command{Use: "retry <ticket>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.retry", args[0], params(map[string]any{"operator": operator}, a.channel)))
	}}
	command.Flags().StringVar(&operator, "operator", operator, "authenticated operator identity")
	return command
}

func (a *app) approveCommand() *cobra.Command {
	operator := defaultOperatorLabel()
	var head string
	command := &cobra.Command{Use: "approve <ticket> --operator <identity>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		values := map[string]any{"operator": operator}
		if cmd.Flags().Changed("head") {
			if !api.ValidReviewedHead(head) {
				return a.emit(failure("invalid_argument", "--head requires a full lowercase 40- or 64-character Git object ID", commandHelpAction(cmd)))
			}
			values["reviewed_head"] = head
		}
		return a.emit(a.request("ticket.approve", args[0], params(values, a.channel)))
	}}
	command.Flags().StringVar(&operator, "operator", operator, "authenticated operator identity")
	command.Flags().StringVar(&head, "head", "", "refuse unless this exact inspected commit is still the reviewed candidate")
	return command
}

func (a *app) rejectCommand() *cobra.Command {
	var reason string
	var head string
	operator := defaultOperatorLabel()
	command := &cobra.Command{Use: "reject <ticket> --operator <identity> --reason <text>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if len(reason) > 4096 {
			return a.emit(failure("invalid_argument", "rejection reason exceeds 4096 bytes", []string{binaryName(), "reject", "--help"}))
		}
		values := map[string]any{"operator": operator, "reason": reason}
		if cmd.Flags().Changed("head") {
			if !api.ValidReviewedHead(head) {
				return a.emit(failure("invalid_argument", "--head requires a full lowercase 40- or 64-character Git object ID", commandHelpAction(cmd)))
			}
			values["reviewed_head"] = head
		}
		return a.emit(a.request("ticket.reject", args[0], params(values, a.channel)))
	}}
	command.Flags().StringVar(&operator, "operator", operator, "authenticated operator identity")
	command.Flags().StringVar(&reason, "reason", "", "bounded rejection reason")
	command.Flags().StringVar(&head, "head", "", "refuse unless this exact inspected commit is still the reviewed candidate")
	_ = command.MarkFlagRequired("reason")
	return command
}

func defaultOperatorLabel() string {
	if current, err := user.Current(); err == nil && current != nil && current.Username != "" {
		return current.Username
	}
	return ""
}

func (a *app) doctorCommand() *cobra.Command {
	var repo string
	command := &cobra.Command{Use: "doctor", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if repo != "" {
			absolute, err := filepath.Abs(repo)
			if err != nil {
				return a.emit(failure("invalid_repository", "repository path could not be resolved", []string{binaryName(), "doctor", "--help"}))
			}
			repo = absolute
		}
		report := RunDoctor(cmd.Context(), productionDoctorDeps(a.channel, repo))
		return a.emit(reportResponse(report))
	}}
	command.Flags().StringVar(&repo, "repo", "", "trusted repository path")
	return command
}

func (a *app) authCommand() *cobra.Command {
	var gitProtocol string
	root := &cobra.Command{Use: "auth", Args: cobra.NoArgs}
	root.AddCommand(&cobra.Command{Use: "status", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		manager := localauth.NewManager()
		return a.emit(RunAuthStatus(cmd.Context(), a.channel, manager))
	}})
	login := &cobra.Command{Use: "login <provider>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if cmd.Flags().Changed("git-protocol") && gitProtocol == "" {
			return a.emit(failure("invalid_argument", "git protocol must be ssh or https", []string{binaryForChannel(a.channel), "auth", "login", "--help"}))
		}
		manager := localauth.NewManager()
		// Provider interaction is deliberately on stderr so --json retains one
		// machine-readable response on stdout. The exchange is never captured.
		terminal := localauth.Terminal{In: os.Stdin, Out: a.errOut, Err: a.errOut}
		return a.emit(RunAuthLoginWithOptions(cmd.Context(), a.channel, args[0], terminal, manager, localauth.LoginOptions{GitProtocol: gitProtocol}))
	}}
	login.Flags().StringVar(&gitProtocol, "git-protocol", "", "GitHub only: ssh or https; changes gh's github.com preference for all accounts, without generating or uploading SSH keys")
	root.AddCommand(login)
	return root
}

func (a *app) initCommand() *cobra.Command {
	var project, repo, profile, testPath, providerPreset string
	var check bool
	command := &cobra.Command{Use: "init [--project <name>] [--repo <path>] [--check]", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		request, err := resolveInitRequest(cmd.Context(), InitRequest{Channel: a.channel, Project: project, Repo: repo, Profile: profile, TestPath: testPath, ProviderPreset: providerPreset})
		if err != nil {
			return a.emit(failure("invalid_repository", err.Error(), []string{binaryName(), "init", "--help"}))
		}
		if check {
			return a.emit(RunInitCheck(cmd.Context(), request))
		}
		if providerPreset == "select" {
			request.ProviderPreset, err = a.selectInitialProviderPreset(cmd.Context())
			if err != nil {
				return a.emit(failure("invalid_argument", err.Error(), []string{binaryName(), "init", "--help"}))
			}
		}
		return a.emit(RunInit(cmd.Context(), request))
	}}
	command.Flags().StringVar(&project, "project", "", "project name (default: repository directory name)")
	command.Flags().StringVar(&repo, "repo", ".", "trusted repository path (default: current directory)")
	command.Flags().BoolVar(&check, "check", false, "preview local compatibility without registering or changing files")
	command.Flags().StringVar(&profile, "profile", "", "explicit project profile (for example nysa-api-pure-v1)")
	command.Flags().StringVar(&testPath, "test", "", "repository-relative entrypoint: .test.ts for TypeScript, .py or tests directory for Python")
	command.Flags().StringVar(&providerPreset, "providers", "", "initial preset: select (terminal picker) or builder-reviewer pair of codex, claude, cursor; never overwrites config")
	return command
}

func (a *app) configCommand() *cobra.Command {
	root := &cobra.Command{Use: "config", Args: cobra.NoArgs}
	var project string
	apply := &cobra.Command{Use: "apply --project <name>", Short: "freeze the current project configuration for future ticket starts", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(RunConfigApply(cmd.Context(), ConfigApplyRequest{Channel: a.channel, Project: project}))
	}}
	apply.Flags().StringVar(&project, "project", "", "registered project name")
	_ = apply.MarkFlagRequired("project")
	root.AddCommand(apply)
	var editProject, preset string
	providers := &cobra.Command{Use: "providers --project <name> --preset <name>", Short: "edit provider preferences with a backup; apply separately for future tickets", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if preset == "select" {
			selected, err := a.selectInitialProviderPreset(cmd.Context())
			if err != nil {
				return err
			}
			preset = selected
		}
		return a.emit(RunConfigProviders(cmd.Context(), ConfigProvidersRequest{ConfigApplyRequest: ConfigApplyRequest{Channel: a.channel, Project: editProject}, Preset: preset}))
	}}
	providers.Flags().StringVar(&editProject, "project", "", "registered project name")
	providers.Flags().StringVar(&preset, "preset", "", "select or builder-reviewer pair of codex, claude, cursor; independent models required")
	_ = providers.MarkFlagRequired("project")
	_ = providers.MarkFlagRequired("preset")
	root.AddCommand(providers)
	return root
}

func (a *app) providersCommand() *cobra.Command {
	root := &cobra.Command{Use: "providers", Args: cobra.NoArgs}
	var builder, reviewer, preset string
	var builderModel, reviewerModel string
	var modelSelection string
	qualify := &cobra.Command{Use: "qualify (--preset <name> | --builder <provider> --reviewer <provider>)", Short: "qualify a selected independent pair; may invoke paid models", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if cmd.Flags().Changed("models") && (modelSelection != "select" || cmd.Flags().Changed("builder-model") || cmd.Flags().Changed("reviewer-model")) {
			return fmt.Errorf("use --models select or explicit model flags, not both")
		}
		if cmd.Flags().Changed("preset") {
			if preset == "" || cmd.Flags().Changed("builder") || cmd.Flags().Changed("reviewer") {
				return fmt.Errorf("use either --preset or both --builder and --reviewer")
			}
			if preset == "select" {
				selected, err := a.selectInitialProviderPreset(cmd.Context())
				if err != nil {
					return err
				}
				preset = selected
			}
			providers, err := config.ProviderPreset(preset)
			if err != nil {
				return err
			}
			builder, reviewer = providers.Builder[0], providers.Reviewer[0]
		}
		if builder == "" || reviewer == "" {
			return fmt.Errorf("choose --preset select, an explicit preset, or both --builder and --reviewer")
		}
		values := map[string]any{"builder": builder, "reviewer": reviewer}
		if modelSelection == "select" {
			var err error
			builderModel, reviewerModel, err = a.selectProviderModels(cmd.Context(), builder, reviewer)
			if err != nil {
				return err
			}
			values["builder_model"], values["reviewer_model"] = builderModel, reviewerModel
		}
		if cmd.Flags().Changed("builder-model") {
			if strings.TrimSpace(builderModel) != builderModel || builderModel == "" {
				return fmt.Errorf("--builder-model requires an exact model ID")
			}
			if err := validateExplicitModel(builder, builderModel); err != nil {
				return err
			}
			values["builder_model"] = builderModel
		}
		if cmd.Flags().Changed("reviewer-model") {
			if strings.TrimSpace(reviewerModel) != reviewerModel || reviewerModel == "" {
				return fmt.Errorf("--reviewer-model requires an exact model ID")
			}
			if err := validateExplicitModel(reviewer, reviewerModel); err != nil {
				return err
			}
			values["reviewer_model"] = reviewerModel
		}
		return a.emit(a.request("provider.qualify", "", params(values, a.channel)))
	}}
	qualify.Flags().StringVar(&preset, "preset", "", "select (terminal picker) or builder-reviewer pair of codex, claude, cursor; independent models required")
	qualify.Flags().StringVar(&builder, "builder", "", "builder provider")
	qualify.Flags().StringVar(&reviewer, "reviewer", "", "independent reviewer provider")
	qualify.Flags().StringVar(&builderModel, "builder-model", "", "exact builder/planner model ID; no alias or fallback")
	qualify.Flags().StringVar(&modelSelection, "models", "", "select exact models by number in a terminal; qualification may invoke paid models")
	qualify.Flags().StringVar(&reviewerModel, "reviewer-model", "", "exact reviewer model ID; independently qualified family required")
	root.AddCommand(qualify)
	return root
}

func (a *app) daemonCommand() *cobra.Command {
	root := &cobra.Command{Use: "daemon", Args: cobra.NoArgs}
	root.AddCommand(&cobra.Command{Use: "run", Short: "run the channel daemon in the foreground", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if a.runDaemon == nil {
			return a.emit(notConfigured("daemon run"))
		}
		if err := a.runDaemon(cmd.Context()); err != nil {
			return a.emit(failure("daemon_start_failed", "could not run the local daemon: "+err.Error(), []string{binaryName(), "doctor"}))
		}
		return a.emit(api.Response{Version: api.Version, RequestID: requestID(), OK: true, Mutation: api.Mutation{}, Data: json.RawMessage(`{"daemon":"stopped"}`)})
	}})
	root.AddCommand(&cobra.Command{Use: "status", Short: "read local daemon status", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("daemon.status", "", params(map[string]any{}, a.channel)))
	}})
	cleanup := &cobra.Command{Use: "cleanup", Short: "checkpoint and recover quarantined GitHub processes after a host reboot", Args: cobra.NoArgs}
	for _, operation := range []struct{ name, description string }{
		{"prepare", "record a same-host recovery checkpoint without clearing quarantine"},
		{"recover", "retire checkpointed quarantine only after a verified host reboot"},
	} {
		operation := operation
		cleanup.AddCommand(&cobra.Command{Use: operation.name, Short: operation.description, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
			return a.emit(a.request("daemon.cleanup."+operation.name, "", params(map[string]any{}, a.channel)))
		}})
	}
	root.AddCommand(cleanup)
	return root
}

func (a *app) simpleSetupCommand(name string) *cobra.Command {
	return &cobra.Command{Use: name, Args: cobra.ArbitraryArgs, RunE: func(cmd *cobra.Command, args []string) error { return a.emit(notConfigured(name)) }}
}

func (a *app) versionCommand() *cobra.Command {
	return &cobra.Command{Use: "version", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(api.Response{Version: api.Version, RequestID: requestID(), OK: true, Mutation: api.Mutation{}, Data: json.RawMessage(fmt.Sprintf(`{"version":%q,"commit":%q,"channel":%q,"protocol":%q}`, version.Version, version.Commit, a.channel, api.Version))})
	}}
}
