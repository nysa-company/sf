package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/transport"
)

func TestCompiledDevTicketPickerDisambiguatesAndCancels(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS PTY acceptance")
	}
	binary := buildDevRuntimeBundle(t)
	// Keep the owner-only socket below Darwin's Unix path-length limit.
	home, err := os.MkdirTemp("/tmp", "sf-pick-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	paths, err := config.PathsFor(home, domain.ChannelDev)
	if err != nil {
		t.Fatal(err)
	}
	first, second := "SF-11111111111111111111111111111111", "SF-22222222222222222222222222222222"
	calls := make(chan api.Request, 10)
	server, err := transport.Listen(paths.Socket, uint32(os.Getuid()), transport.HandlerFunc(func(_ context.Context, _ transport.Peer, request api.Request) api.Response {
		calls <- request
		var data any
		if request.Method == "ticket.status" && request.Ticket != "" {
			data = map[string]any{"ticket": map[string]any{"channel": "dev", "project": "app", "ticket": request.Ticket, "title": "Same title", "state": "waiting_approval"}, "evidence": map[string]any{"candidate": map[string]any{"head_sha": strings.Repeat("a", 40)}}}
		} else if request.Method == "ticket.status" {
			data = map[string]any{"channel": "dev", "tickets": []any{
				map[string]any{"channel": "dev", "project": "app", "ticket": first, "title": "Same title", "state": "queued"},
				map[string]any{"channel": "dev", "project": "app", "ticket": second, "title": "Same title", "state": "queued"},
			}}
		} else {
			data = map[string]any{"ticket": map[string]any{"ticket": request.Ticket, "title": "Selected detail", "state": "queued"}}
		}
		encoded, _ := json.Marshal(data)
		return api.Response{Version: api.Version, RequestID: request.RequestID, OK: true, Data: encoded}
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() { cancel(); _ = server.Close(); <-done })
	environment := []string{"HOME=" + home, "PATH=/usr/bin:/bin:/usr/sbin:/sbin", "TMPDIR=" + home, "LANG=C", "CODEX_HOME=" + filepath.Join(home, "codex"), "GH_CONFIG_DIR=" + filepath.Join(home, "gh")}
	run := func(verb, answer string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "/usr/bin/script", "-q", "/dev/null", binary, verb, "--project", "app")
		command.Env = environment
		input, err := command.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		command.Stdout, command.Stderr = &output, &output
		if err := command.Start(); err != nil {
			input.Close()
			t.Fatal(err)
		}
		_, writeErr := input.Write([]byte(answer + "\n"))
		err = command.Wait()
		input.Close()
		if writeErr != nil || ctx.Err() != nil || (answer == "2" || answer == "2\napprove") && err != nil {
			t.Fatalf("PTY write=%v run=%v context=%v output=%s", writeErr, err, ctx.Err(), output.String())
		}
		return output.Bytes()
	}
	output := run("show", "2")
	if !bytes.Contains(output, []byte(first)) || !bytes.Contains(output, []byte(second)) || bytes.Count(output, []byte("Same title")) != 2 || !bytes.Contains(output, []byte("Selected detail")) {
		t.Fatalf("picker output=%s", output)
	}
	inventory, selected := <-calls, <-calls
	if inventory.Method != "ticket.status" || selected.Method != "ticket.show" || selected.Ticket != second {
		t.Fatalf("inventory=%+v selected=%+v", inventory, selected)
	}
	var scope struct {
		Project string `json:"project"`
		Channel string `json:"channel"`
	}
	if json.Unmarshal(inventory.Parameters, &scope) != nil || scope.Project != "app" || scope.Channel != "dev" {
		t.Fatalf("scope=%+v", scope)
	}
	output = run("start", "q")
	if !strings.Contains(string(output), "cancelled; no action was taken") {
		t.Fatalf("cancel=%s", output)
	}
	if request := <-calls; request.Method != "ticket.status" {
		t.Fatalf("cancel dispatched %+v", request)
	}
	select {
	case request := <-calls:
		t.Fatalf("unexpected dispatch after cancel: %+v", request)
	default:
	}
	output = run("approve", "2\napprove")
	if !bytes.Contains(output, []byte("Reviewed head: "+strings.Repeat("a", 40))) {
		t.Fatalf("missing exact head: %s", output)
	}
	list, detail, decision := <-calls, <-calls, <-calls
	var parameters struct {
		Head string `json:"reviewed_head"`
	}
	if list.Method != "ticket.status" || detail.Method != "ticket.status" || detail.Ticket != second || decision.Method != "ticket.approve" || decision.Ticket != second || json.Unmarshal(decision.Parameters, &parameters) != nil || parameters.Head != strings.Repeat("a", 40) {
		t.Fatalf("unbound confirmation: %+v %+v %+v", list, detail, decision)
	}
	output = run("approve", "2\nq")
	if !bytes.Contains(output, []byte("decision cancelled")) {
		t.Fatalf("confirmation cancel: %s", output)
	}
	for i := 0; i < 2; i++ {
		if request := <-calls; request.Method != "ticket.status" {
			t.Fatalf("cancel mutated: %+v", request)
		}
	}
	select {
	case request := <-calls:
		t.Fatalf("unexpected decision: %+v", request)
	default:
	}
}
