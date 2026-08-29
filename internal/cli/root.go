package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/version"
)

type app struct {
	client  Client
	out     io.Writer
	errOut  io.Writer
	json    bool
	channel domain.Channel
	last    *api.Response
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
	return &app{client: client, out: out, errOut: errOut, channel: channel}
}

func (a *app) command() *cobra.Command {
	root := &cobra.Command{
		Use:           "sf",
		Short:         "safe local software factory",
		SilenceErrors: true,
		SilenceUsage:  true,
		Args:          cobra.NoArgs,
	}
	root.PersistentFlags().BoolVar(&a.json, "json", false, "render the versioned JSON response")
	root.AddCommand(a.submitCommand(), a.startCommand(), a.statusCommand(), a.showCommand(), a.logsCommand(), a.controlCommand("pause"), a.controlCommand("resume"), a.recoverCommand(), a.controlCommand("cancel"), a.retryCommand(), a.controlCommand("take"), a.approveCommand(), a.rejectCommand(), a.doctorCommand(), a.authCommand(), a.initCommand(), a.providersCommand(), a.daemonCommand(), a.simpleSetupCommand("config"), a.simpleSetupCommand("update"), a.simpleSetupCommand("rollback"), a.versionCommand())
	return root
}

// Execute runs a command and renders its one authoritative response.
func Execute(ctx context.Context, args []string, out, errOut io.Writer, client Client) int {
	a := newApp(client, out, errOut)
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
	response, err := a.client.Call(context.Background(), api.Request{Version: api.Version, RequestID: requestID(), Method: method, Ticket: ticket, OperatorLabel: operator, Parameters: data})
	if err != nil {
		return failure("daemon_unavailable", "the local daemon is unavailable", []string{binaryName(), "daemon", "run"})
	}
	return response
}

func (a *app) emit(response api.Response) error {
	copy := response
	a.last = &copy
	return Render(a.out, response, a.json)
}

func failure(code, message string, argv []string) api.Response {
	return api.Response{Version: api.Version, RequestID: requestID(), OK: false, Mutation: api.Mutation{Attempted: false}, Error: &api.Error{Code: code, Message: message}, NextAction: &domain.NextAction{Code: code, Argv: argv}}
}

func notConfigured(command string) api.Response {
	return failure("not_configured", command+" is not configured in this build", []string{binaryName(), "--help"})
}

func requestID() string { return fmt.Sprintf("cli-%d", time.Now().UnixNano()) }

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
	command := &cobra.Command{Use: "submit <ticket.md> --project <name>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.submit", "", params(map[string]any{"path": args[0], "project": project}, a.channel)))
	}}
	command.Flags().StringVar(&project, "project", "", "registered project name")
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
		return a.emit(a.request("ticket.status", ticket, params(map[string]any{"watch": watch}, a.channel)))
	}}
	command.Flags().BoolVar(&watch, "watch", false, "follow status changes")
	return command
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
	var operator string
	command := &cobra.Command{Use: name + " <ticket> --operator <identity>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket."+name, args[0], params(map[string]any{"operator": operator}, a.channel)))
	}}
	command.Flags().StringVar(&operator, "operator", "", "authenticated operator identity")
	return command
}

func (a *app) recoverCommand() *cobra.Command {
	var mode, operator string
	command := &cobra.Command{Use: "recover <ticket>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.recover", args[0], params(map[string]any{"mode": mode, "operator": operator}, a.channel)))
	}}
	command.Flags().StringVar(&mode, "mode", "", "optional recovery mode (guarded)")
	command.Flags().StringVar(&operator, "operator", "", "authenticated operator identity")
	return command
}

func (a *app) retryCommand() *cobra.Command {
	return &cobra.Command{Use: "retry <ticket>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.retry", args[0], params(map[string]any{}, a.channel)))
	}}
}

func (a *app) approveCommand() *cobra.Command {
	var operator string
	command := &cobra.Command{Use: "approve <ticket> --operator <identity>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.approve", args[0], params(map[string]any{"operator": operator}, a.channel)))
	}}
	command.Flags().StringVar(&operator, "operator", "", "authenticated operator identity")
	return command
}

func (a *app) rejectCommand() *cobra.Command {
	var operator, reason string
	command := &cobra.Command{Use: "reject <ticket> --operator <identity> --reason <text>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return a.emit(a.request("ticket.reject", args[0], params(map[string]any{"operator": operator, "reason": reason}, a.channel)))
	}}
	command.Flags().StringVar(&operator, "operator", "", "authenticated operator identity")
	command.Flags().StringVar(&reason, "reason", "", "bounded rejection reason")
	_ = command.MarkFlagRequired("reason")
	return command
}

func (a *app) doctorCommand() *cobra.Command {
	var repo string
	command := &cobra.Command{Use: "doctor", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		response := notConfigured("doctor")
		response.Error.Code = "doctor_not_configured"
		response.Error.Message = "doctor is not configured in this build; no checks or mutations were run"
		return a.emit(response)
	}}
	command.Flags().StringVar(&repo, "repo", "", "trusted repository path")
	return command
}

func (a *app) authCommand() *cobra.Command {
	root := &cobra.Command{Use: "auth", Args: cobra.NoArgs}
	root.AddCommand(&cobra.Command{Use: "status", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error { return a.emit(notConfigured("auth status")) }})
	root.AddCommand(&cobra.Command{Use: "login <provider>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error { return a.emit(notConfigured("auth login " + args[0])) }})
	return root
}

func (a *app) initCommand() *cobra.Command {
	var project, repo string
	command := &cobra.Command{Use: "init --project <name> --repo <path>", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error { return a.emit(notConfigured("init")) }}
	command.Flags().StringVar(&project, "project", "", "project name")
	command.Flags().StringVar(&repo, "repo", "", "trusted repository path")
	return command
}

func (a *app) providersCommand() *cobra.Command {
	root := &cobra.Command{Use: "providers", Args: cobra.NoArgs}
	var builder, reviewer string
	qualify := &cobra.Command{Use: "qualify --builder <provider> --reviewer <provider>", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error { return a.emit(notConfigured("providers qualify")) }}
	qualify.Flags().StringVar(&builder, "builder", "", "builder provider")
	qualify.Flags().StringVar(&reviewer, "reviewer", "", "independent reviewer provider")
	root.AddCommand(qualify)
	return root
}

func (a *app) daemonCommand() *cobra.Command {
	root := &cobra.Command{Use: "daemon", Args: cobra.NoArgs}
	root.AddCommand(&cobra.Command{Use: "run", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error { return a.emit(notConfigured("daemon run")) }})
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
