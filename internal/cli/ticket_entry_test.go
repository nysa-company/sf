package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/ticket"
)

func entryApp(t *testing.T, input string) (*app, *bytes.Buffer) {
	t.Helper()
	var out, prompts bytes.Buffer
	a := newApp(fakeClient(func(context.Context, api.Request) (api.Response, error) {
		t.Fatal("unexpected daemon request")
		return api.Response{}, nil
	}), &out, &prompts)
	a.interactive = func() bool { return true }
	a.input = strings.NewReader(input)
	return a, &prompts
}

func executeEntry(t *testing.T, a *app, args ...string) {
	t.Helper()
	cmd := a.command()
	cmd.SetArgs(args)
	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.last == nil {
		t.Fatal("missing response")
	}
}

func TestTicketEntryMultilineSuggestedFilenameJourney(t *testing.T) {
	t.Chdir(t.TempDir())
	began := time.Now()
	a, prompts := entryApp(t, "Count jobs\nCurrent count is missing.\n\nAdd a read-only count.\n.\nEmpty returns zero.\nUnknown status returns an error.\n\nyes\n")
	executeEntry(t, a, "ticket", "new", "--multiline")
	if !a.last.OK {
		t.Fatalf("%+v", a.last)
	}
	parsed, err := readLocalTicket("count-jobs.md")
	if err != nil || len(parsed.Acceptance) != 2 || !strings.Contains(parsed.Problem, "\n\nAdd a read-only count.") {
		t.Fatalf("%+v %v", parsed, err)
	}
	if !strings.Contains(prompts.String(), "count-jobs.md") {
		t.Fatal("filename not previewed")
	}
	executeEntry(t, a, "ticket", "validate", "count-jobs.md")
	if !a.last.OK {
		t.Fatalf("%+v", a.last)
	}
	elapsed := time.Since(began)
	t.Logf("Scripted offline draft/validate duration: %s (not human usability timing)", elapsed)
	if elapsed > 2*time.Minute {
		t.Fatal("scripted drafting target exceeded")
	}
}

func TestTicketEntryFilenameAndPasteBounds(t *testing.T) {
	for _, title := range []string{"../../escape", "日本語", strings.Repeat("A", 200), ".hidden"} {
		name := draftFilename(title)
		if filepath.Base(name) != name || strings.HasPrefix(name, ".") || len(name) > 63 {
			t.Fatalf("unsafe name %q", name)
		}
	}
	for _, body := range []string{".\n", strings.Repeat("line\n", 257) + ".\n", "\x1b[2J\n.\n"} {
		if _, err := collectTicketDraftWithMode(strings.NewReader("Title\n"+body+"Success\n\nyes\n"), &bytes.Buffer{}, true, filepath.Join(t.TempDir(), "draft.md")); err == nil {
			t.Fatal("bad multiline accepted")
		}
	}
}

func TestTicketEntryHomeCancellationAndJSON(t *testing.T) {
	for _, input := range []string{"q\n", "", "0\n", "2\nq\n", "2\napp\nq\n"} {
		a, _ := entryApp(t, input)
		executeEntry(t, a, "home")
		if a.last.OK || a.last.Mutation.Attempted {
			t.Fatalf("%+v", a.last)
		}
	}
	a, prompts := entryApp(t, "1\n")
	executeEntry(t, a, "home", "--json")
	if a.last.OK || prompts.Len() != 0 {
		t.Fatal("JSON prompted")
	}
}

func TestTicketEntryHomeDraftAndScopedView(t *testing.T) {
	t.Chdir(t.TempDir())
	a, _ := entryApp(t, "1\nTitle\nProblem\n.\nSuccess\n\nyes\n")
	executeEntry(t, a, "home")
	if !a.last.OK {
		t.Fatalf("%+v", a.last)
	}
	if _, err := os.Stat("title.md"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	a, _ = entryApp(t, "3\n")
	a.client = fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
		calls++
		var values map[string]any
		if json.Unmarshal(r.Parameters, &values) != nil || r.Method != "ticket.status" || values["project"] != "app" {
			t.Fatalf("%+v", r)
		}
		return api.Response{Version: api.Version, RequestID: "response", OK: true, Data: json.RawMessage(`{"channel":"stable","tickets":[]}`)}, nil
	})
	executeEntry(t, a, "home", "--project", "app")
	if calls != 1 || !a.last.OK {
		t.Fatalf("calls=%d response=%+v", calls, a.last)
	}
}

func TestTicketEntryGitHubAdapterIsReadOnlyAndBounded(t *testing.T) {
	directory := t.TempDir()
	executable := filepath.Join(directory, "gh")
	script := "#!/bin/sh\n[ \"$*\" = 'api --hostname github.com --method GET repos/example/app/issues/42' ] || exit 9\n[ \"$GH_PROMPT_DISABLED\" = 1 ] || exit 10\nprintf '%s' '" + string(issueFixture()) + "'\n"
	if err := os.WriteFile(executable, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+":/usr/bin:/bin")
	data, err := fetchGitHubIssue(context.Background(), "repos/example/app/issues/42")
	if err != nil || !bytes.Equal(data, issueFixture()) {
		t.Fatalf("adapter output mismatch: %v", err)
	}
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexec /bin/sleep 5\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	began := time.Now()
	if _, err := fetchGitHubIssue(ctx, "repos/example/app/issues/42"); err == nil {
		t.Fatal("deadline ignored")
	}
	if time.Since(began) > 3*time.Second {
		t.Fatal("adapter deadline unbounded")
	}
}

const entryIssueURL = "https://github.com/example/app/issues/42"

func issueFixture() []byte {
	data, _ := json.Marshal(importedIssue{URL: entryIssueURL, Number: 42, Title: "Count jobs", Body: "# Untrusted heading\n\n## Acceptance\n- imported suggestion", State: "open"})
	return data
}

func TestTicketEntryIssueImportPreviewSaveAndDuplicate(t *testing.T) {
	t.Chdir(t.TempDir())
	a, _ := entryApp(t, "Empty returns zero.\n\nyes\n")
	calls := 0
	a.fetchIssue = func(_ context.Context, endpoint string) ([]byte, error) {
		calls++
		if endpoint != "repos/example/app/issues/42" {
			t.Fatal(endpoint)
		}
		return issueFixture(), nil
	}
	executeEntry(t, a, "ticket", "import", entryIssueURL)
	if !a.last.OK {
		t.Fatalf("%+v", a.last)
	}
	parsed, err := readLocalTicket("github-example-app-42.md")
	if err != nil || len(parsed.Acceptance) != 1 || parsed.Acceptance[0] != "Empty returns zero." || !strings.Contains(parsed.Problem, entryIssueURL) || !strings.Contains(parsed.Problem, "> ## Acceptance") {
		t.Fatalf("%+v %v", parsed, err)
	}
	executeEntry(t, a, "ticket", "import", entryIssueURL)
	if a.last.OK || calls != 1 {
		t.Fatal("duplicate performed network read or overwrite")
	}
	executeEntry(t, a, "ticket", "import", entryIssueURL, "--json")
	if !a.last.OK || a.last.Mutation.Attempted || calls != 2 {
		t.Fatalf("%+v", a.last)
	}
}

func TestTicketEntryIssueURLRefusals(t *testing.T) {
	for _, value := range []string{"http://github.com/a/b/issues/1", "https://evil.example/a/b/issues/1", "https://github.com/a/b/pull/1", "https://github.com/a/b/issues/01", "https://github.com/a/b/issues/1?q=x", "https://github.com/a/b/issues/1#x", "https://u@github.com/a/b/issues/1", "https://github.com/a/%2e%2e/issues/1"} {
		a, _ := entryApp(t, "")
		a.fetchIssue = func(context.Context, string) ([]byte, error) { t.Fatal("invalid URL fetched"); return nil, nil }
		executeEntry(t, a, "ticket", "import", value, "--json")
		if a.last.OK {
			t.Fatal(value)
		}
	}
}

func TestTicketEntryIssueResponseRefusals(t *testing.T) {
	for _, kind := range []string{"number", "url", "pr", "control", "large", "error", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			a, _ := entryApp(t, "")
			a.fetchIssue = func(ctx context.Context, _ string) ([]byte, error) {
				var issue importedIssue
				_ = json.Unmarshal(issueFixture(), &issue)
				switch kind {
				case "number":
					issue.Number = 43
				case "url":
					issue.URL = "https://github.com/other/app/issues/42"
				case "pr":
					issue.PullRequest = json.RawMessage(`{}`)
				case "control":
					issue.Body = "\x1b[2J"
				case "large":
					return bytes.Repeat([]byte("x"), issueReadLimit+1), nil
				case "error":
					return nil, errors.New("credential must not be shown")
				case "timeout":
					return nil, context.DeadlineExceeded
				}
				return json.Marshal(issue)
			}
			executeEntry(t, a, "ticket", "import", entryIssueURL, "--json")
			if a.last.OK || a.last.Mutation.Attempted {
				t.Fatalf("%+v", a.last)
			}
		})
	}
}

func TestTicketEntryImportCancelledNeverWrites(t *testing.T) {
	t.Chdir(t.TempDir())
	a, _ := entryApp(t, "Success\n\nno\n")
	a.fetchIssue = func(context.Context, string) ([]byte, error) { return issueFixture(), nil }
	executeEntry(t, a, "ticket", "import", entryIssueURL)
	if a.last.OK || a.last.Mutation.Attempted {
		t.Fatalf("%+v", a.last)
	}
	if _, err := os.Stat("github-example-app-42.md"); !os.IsNotExist(err) {
		t.Fatal("cancel wrote draft")
	}
}

func TestTicketEntryIssueOutputBufferBound(t *testing.T) {
	var b issueBuffer
	p := bytes.Repeat([]byte("x"), issueReadLimit+100)
	n, err := b.Write(p)
	if err != nil || n != len(p) || !b.overflow || b.buffer.Len() != issueReadLimit {
		t.Fatal("output unbounded")
	}
	if _, err := ticket.Parse(strings.NewReader(ticketTemplate)); err != nil {
		t.Fatal(err)
	}
}

func TestTicketEntryHomeRunRequiresExactPreviewAndDoesNotRetry(t *testing.T) {
	for _, mode := range []string{"cancel", "run", "estimates", "uncertain", "changed"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "draft.md")
			if err := os.WriteFile(path, []byte(ticketTemplate), 0600); err != nil {
				t.Fatal(err)
			}
			answer := "run"
			if mode == "estimates" {
				answer = "run estimates"
			}
			if mode == "cancel" {
				answer = "no"
			}
			a, _ := entryApp(t, "2\n"+path+"\n"+answer+"\n")
			if mode == "changed" {
				a.errOut = &entryChangeWriter{path: path}
			}
			calls := 0
			a.client = fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
				calls++
				if calls == 1 {
					if r.Method != "ticket.submit" {
						t.Fatal(r.Method)
					}
					if mode == "uncertain" {
						return api.Response{}, errors.New("lost response")
					}
					return runTestResponse(domain.StateQueued, "ticket_submit", false), nil
				}
				if calls != 2 || r.Method != "ticket.start" {
					t.Fatal("unexpected retry")
				}
				var parameters map[string]any
				if json.Unmarshal(r.Parameters, &parameters) != nil {
					t.Fatal("bad parameters")
				}
				consent, present := parameters["accept_cost_estimates"]
				if present != (mode == "estimates") || present && consent != true {
					t.Fatal("cost consent was implied or lost")
				}
				return runTestResponse(domain.StatePlanning, "ticket_start", false), nil
			})
			executeEntry(t, a, "home", "--project", "app")
			want := 0
			if mode == "run" || mode == "estimates" {
				want = 2
			}
			if mode == "uncertain" {
				want = 1
			}
			if calls != want || a.last.OK != (mode == "run" || mode == "estimates") {
				t.Fatalf("mode=%s calls=%d response=%+v", mode, calls, a.last)
			}
		})
	}
}

type entryChangeWriter struct{ path string }

func (w *entryChangeWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "Submit and start this draft?") {
		if err := os.WriteFile(w.path, []byte(strings.Replace(ticketTemplate, "Describe one small", "Changed one small", 1)), 0600); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func TestTicketEntryHomeApprovalStillBindsHead(t *testing.T) {
	for _, answer := range []string{"approve", "yes", "q"} {
		a, _ := entryApp(t, "4\n1\n"+answer+"\n")
		calls := 0
		head := strings.Repeat("a", 40)
		a.client = fakeClient(func(_ context.Context, r api.Request) (api.Response, error) {
			calls++
			if calls == 1 {
				return selectionInventory(selectionItem(selectedID, "app")), nil
			}
			if calls == 2 {
				item := selectionItem(selectedID, "app")
				item.State = domain.StateWaitingApproval
				data, _ := json.Marshal(map[string]any{"ticket": item, "evidence": map[string]any{"candidate": map[string]any{"head_sha": head}}})
				response := responseOK()
				response.Data = data
				return response, nil
			}
			var values map[string]any
			if calls != 3 || r.Method != "ticket.approve" || r.Ticket != selectedID || json.Unmarshal(r.Parameters, &values) != nil || values["reviewed_head"] != head {
				t.Fatalf("unbound approval %+v", r)
			}
			return responseOK(), nil
		})
		executeEntry(t, a, "home", "--project", "app")
		want := 2
		if answer == "approve" {
			want = 3
		}
		if calls != want || a.last.OK != (answer == "approve") {
			t.Fatalf("calls=%d response=%+v", calls, a.last)
		}
	}
}
