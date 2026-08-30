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
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"github.com/nysa-company/sf/internal/api"
	localauth "github.com/nysa-company/sf/internal/auth"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/ticket"
	"github.com/nysa-company/sf/internal/version"
)

type app struct {
	client    Client
	out       io.Writer
	errOut    io.Writer
	json      bool
	channel   domain.Channel
	last      *api.Response
	ctx       context.Context
	runDaemon func(context.Context) error
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
	root := &cobra.Command{
		Use:           "sf",
		Short:         "safe local software factory",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
	}
	root.PersistentPreRun = func(cmd *cobra.Command, _ []string) { a.ctx = cmd.Context() }
	root.PersistentFlags().BoolVar(&a.json, "json", false, "render the versioned JSON response")
	root.AddCommand(a.submitCommand(), a.startCommand(), a.statusCommand(), a.showCommand(), a.logsCommand(), a.controlCommand("pause"), a.controlCommand("resume"), a.recoverCommand(), a.controlCommand("cancel"), a.retryCommand(), a.controlCommand("take"), a.approveCommand(), a.rejectCommand(), a.doctorCommand(), a.authCommand(), a.initCommand(), a.providersCommand(), a.daemonCommand(), a.simpleSetupCommand("config"), a.simpleSetupCommand("update"), a.simpleSetupCommand("rollback"), a.versionCommand())
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
	if err := command.ExecuteContext(ctx); err != nil {
		response := failure("invalid_command", err.Error(), []string{binaryName(), "--help"})
		_ = Render(errOut, response, a.json)
		return int(exitCode(response))
	}
	if a.last == nil {
		return int(ExitOK)
	}
	return int(exitCode(*a.last))
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
	return &cobra.Command{Use: "start <ticket>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.start", args[0], params(map[string]any{}, a.channel)))
	}}
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

const statusWatchInterval = 500 * time.Millisecond

func (a *app) watchStatus(ctx context.Context, ticket string) error {
	for {
		response := a.request("ticket.status", ticket, params(map[string]any{"watch": false}, a.channel))
		if err := a.emit(response); err != nil {
			return err
		}
		if !response.OK {
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

func (a *app) showCommand() *cobra.Command {
	return &cobra.Command{Use: "show <ticket>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.show", args[0], params(map[string]any{}, a.channel)))
	}}
}

func (a *app) logsCommand() *cobra.Command {
	var follow bool
	var phase string
	command := &cobra.Command{Use: "logs <ticket>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.logs", args[0], params(map[string]any{"follow": follow, "phase": phase}, a.channel)))
	}}
	command.Flags().BoolVar(&follow, "follow", false, "follow logs")
	command.Flags().StringVar(&phase, "phase", "", "filter by phase")
	return command
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
	return &cobra.Command{Use: "retry <ticket>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.retry", args[0], params(map[string]any{}, a.channel)))
	}}
}

func (a *app) approveCommand() *cobra.Command {
	operator := defaultOperatorLabel()
	command := &cobra.Command{Use: "approve <ticket> --operator <identity>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.approve", args[0], params(map[string]any{"operator": operator}, a.channel)))
	}}
	command.Flags().StringVar(&operator, "operator", operator, "authenticated operator identity")
	return command
}

func (a *app) rejectCommand() *cobra.Command {
	var reason string
	operator := defaultOperatorLabel()
	command := &cobra.Command{Use: "reject <ticket> --operator <identity> --reason <text>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if len(reason) > 4096 {
			return a.emit(failure("invalid_argument", "rejection reason exceeds 4096 bytes", []string{binaryName(), "reject", args[0], "--reason", "<short-reason>"}))
		}
		return a.emit(a.request("ticket.reject", args[0], params(map[string]any{"operator": operator, "reason": reason}, a.channel)))
	}}
	command.Flags().StringVar(&operator, "operator", operator, "authenticated operator identity")
	command.Flags().StringVar(&reason, "reason", "", "bounded rejection reason")
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
		report := RunDoctor(cmd.Context(), DoctorDeps{Channel: a.channel, Repo: repo})
		return a.emit(reportResponse(report))
	}}
	command.Flags().StringVar(&repo, "repo", "", "trusted repository path")
	return command
}

func (a *app) authCommand() *cobra.Command {
	root := &cobra.Command{Use: "auth", Args: cobra.NoArgs}
	root.AddCommand(&cobra.Command{Use: "status", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		manager := localauth.NewManager()
		return a.emit(RunAuthStatus(cmd.Context(), a.channel, manager))
	}})
	root.AddCommand(&cobra.Command{Use: "login <provider>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		manager := localauth.NewManager()
		// Provider interaction is deliberately on stderr so --json retains one
		// machine-readable response on stdout. The exchange is never captured.
		terminal := localauth.Terminal{In: os.Stdin, Out: a.errOut, Err: a.errOut}
		return a.emit(RunAuthLogin(cmd.Context(), a.channel, args[0], terminal, manager))
	}})
	return root
}

func (a *app) initCommand() *cobra.Command {
	var project, repo string
	command := &cobra.Command{Use: "init --project <name> --repo <path>", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(RunInit(cmd.Context(), InitRequest{Channel: a.channel, Project: project, Repo: repo}))
	}}
	command.Flags().StringVar(&project, "project", "", "project name")
	command.Flags().StringVar(&repo, "repo", "", "trusted repository path")
	_ = command.MarkFlagRequired("project")
	_ = command.MarkFlagRequired("repo")
	return command
}

func (a *app) providersCommand() *cobra.Command {
	root := &cobra.Command{Use: "providers", Args: cobra.NoArgs}
	var builder, reviewer string
	qualify := &cobra.Command{Use: "qualify --builder <provider> --reviewer <provider>", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error { return a.emit(notConfigured("providers qualify")) }}
	qualify.Flags().StringVar(&builder, "builder", "", "builder provider")
	qualify.Flags().StringVar(&reviewer, "reviewer", "", "independent reviewer provider")
	_ = qualify.MarkFlagRequired("builder")
	_ = qualify.MarkFlagRequired("reviewer")
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
	return root
}

func (a *app) simpleSetupCommand(name string) *cobra.Command {
	return &cobra.Command{Use: name, Args: cobra.ArbitraryArgs, RunE: func(cmd *cobra.Command, args []string) error { return a.emit(notConfigured(name)) }}
}

func (a *app) versionCommand() *cobra.Command {
	return &cobra.Command{Use: "version", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(api.Response{Version: api.Version, RequestID: requestID(), OK: true, Mutation: api.Mutation{}, Data: json.RawMessage(fmt.Sprintf(`{"version":%q,"channel":%q}`, version.Version, a.channel))})
	}}
}
