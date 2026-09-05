package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/ticket"
)

func TestNewTicketRequiresPreviewConfirmationAndNeverSubmits(t *testing.T) {
	for _, confirmation := range []string{"yes", "no", ""} {
		t.Run("confirm-"+confirmation, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ticket.md")
			var output, prompts bytes.Buffer
			a := newApp(fakeClient(func(context.Context, api.Request) (api.Response, error) {
				t.Fatal("draft contacted daemon")
				return api.Response{}, nil
			}), &output, &prompts)
			a.interactive = func() bool { return true }
			a.input = strings.NewReader("Count jobs\nImplement a read-only count in src/jobs.js.\nEmpty list returns zero.\n\n" + confirmation + "\n")
			cmd := a.command()
			cmd.SetArgs([]string{"ticket", "new", path})
			if err := cmd.ExecuteContext(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(prompts.String(), "Review the complete draft:") || !strings.Contains(prompts.String(), "max_cost_usd: 10") {
				t.Fatal("full preview missing")
			}
			if a.last == nil || a.last.OK != (confirmation == "yes") {
				t.Fatalf("response=%+v", a.last)
			}
			data, err := os.ReadFile(path)
			if confirmation != "yes" {
				if !os.IsNotExist(err) {
					t.Fatal("cancel created file")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := ticket.Parse(bytes.NewReader(data))
			if err != nil || parsed.Title != "Count jobs" || len(parsed.Acceptance) != 1 {
				t.Fatalf("parsed=%+v err=%v", parsed, err)
			}
			info, err := os.Stat(path)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("mode=%v err=%v", info, err)
			}
		})
	}
}

func TestNewTicketRefusesExistingFilesAndNoninteractiveInput(t *testing.T) {
	for _, kind := range []string{"existing", "json", "piped"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ticket.md")
			if kind == "existing" {
				if err := os.WriteFile(path, []byte("keep me"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var output, prompts bytes.Buffer
			a := newApp(nil, &output, &prompts)
			a.interactive = func() bool { return kind != "piped" }
			a.input = strings.NewReader("should never read\n")
			cmd := a.command()
			args := []string{"ticket", "new", path}
			if kind == "json" {
				args = append(args, "--json")
			}
			cmd.SetArgs(args)
			if err := cmd.ExecuteContext(context.Background()); err != nil {
				t.Fatal(err)
			}
			if a.last == nil || a.last.OK || a.last.Mutation.Attempted || prompts.Len() != 0 {
				t.Fatalf("response=%+v prompts=%s", a.last, prompts.String())
			}
			data, err := os.ReadFile(path)
			if kind == "existing" {
				if err != nil || string(data) != "keep me" {
					t.Fatal("existing file modified")
				}
			} else if !os.IsNotExist(err) {
				t.Fatal("noninteractive command created file")
			}
		})
	}
}

func TestTicketDraftRejectsControlCharactersAndOversizedInput(t *testing.T) {
	for _, input := range []string{"Title\x1b[2J\n", strings.Repeat("a", 17000) + "\n", "Title\nProblem\n\n"} {
		if _, err := collectTicketDraft(strings.NewReader(input), &bytes.Buffer{}); err == nil {
			t.Fatal("invalid draft accepted")
		}
	}
}

type draftRaceWriter struct {
	bytes.Buffer
	path string
	err  error
}

func (w *draftRaceWriter) Write(data []byte) (int, error) {
	if strings.Contains(string(data), "Save this draft?") {
		w.err = os.WriteFile(w.path, []byte("created while reviewing"), 0600)
	}
	return w.Buffer.Write(data)
}

func TestNewTicketDoesNotOverwriteFileCreatedDuringPreview(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ticket.md")
	var output bytes.Buffer
	prompts := &draftRaceWriter{path: path}
	a := newApp(nil, &output, prompts)
	a.interactive = func() bool { return true }
	a.input = strings.NewReader("Title\nProblem\nSuccess\n\nyes\n")
	cmd := a.command()
	cmd.SetArgs([]string{"ticket", "new", path})
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if prompts.err != nil {
		t.Fatal(prompts.err)
	}
	if a.last == nil || a.last.OK || a.last.Mutation.Attempted {
		t.Fatalf("response=%+v", a.last)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "created while reviewing" {
		t.Fatalf("data=%q err=%v", data, err)
	}
}
