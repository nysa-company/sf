package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/nysa-company/sf/internal/api"
	"github.com/spf13/cobra"
)

const issueReadLimit = 128 * 1024

var issuePathPattern = regexp.MustCompile(`^/([A-Za-z0-9][A-Za-z0-9-]{0,38})/([A-Za-z0-9_.-]{1,100})/issues/([1-9][0-9]{0,9})$`)

type importedIssue struct {
	URL         string          `json:"html_url"`
	Number      int64           `json:"number"`
	Title       string          `json:"title"`
	Body        string          `json:"body"`
	State       string          `json:"state"`
	PullRequest json.RawMessage `json:"pull_request"`
}

func parseIssueURL(value string) (string, string, int64, error) {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.Host != "github.com" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
		return "", "", 0, errors.New("use an exact https://github.com/owner/repo/issues/number URL without query or fragment")
	}
	parts := issuePathPattern.FindStringSubmatch(u.Path)
	if parts == nil || parts[2] == "." || parts[2] == ".." {
		return "", "", 0, errors.New("URL must identify a GitHub issue, not a pull request or repository")
	}
	number, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return "", "", 0, errors.New("invalid issue number")
	}
	return "repos/" + parts[1] + "/" + parts[2] + "/issues/" + parts[3], "github-" + strings.ToLower(parts[1]) + "-" + strings.ToLower(parts[2]) + "-" + parts[3] + ".md", number, nil
}

func (a *app) importTicketCommand() *cobra.Command {
	command := &cobra.Command{Use: "import <github-issue-url>", Short: "Preview a GitHub issue and save a local draft; never submit or modify GitHub", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			fail := func(message string) error {
				return a.emit(failure("issue_import_failed", message, commandHelpAction(cmd)))
			}
			endpoint, name, number, err := parseIssueURL(args[0])
			if err != nil {
				return fail(err.Error())
			}
			if !a.json && !a.canSelectInteractively() {
				return fail("use --json for a read-only preview or an interactive terminal to save")
			}
			// A stable source-derived name prevents an accidental second import in the
			// same destination. Deliberate copies remain ordinary local draft files.
			path := ""
			if !a.json {
				path, err = draftOutputPath(name)
				if err != nil {
					return fail("this issue draft already exists or cannot be inspected; review the existing source-named file")
				}
			}
			fetch := a.fetchIssue
			if fetch == nil {
				fetch = fetchGitHubIssue
			}
			ctx, cancel := context.WithTimeout(cmd.Context(), 20*time.Second)
			defer cancel()
			data, err := fetch(ctx, endpoint)
			if err != nil || ctx.Err() != nil {
				return fail("GitHub issue read failed or timed out; check gh auth status and retry explicitly. Nothing was saved or submitted")
			}
			var issue importedIssue
			if len(data) > issueReadLimit || json.Unmarshal(data, &issue) != nil || issue.URL != args[0] || issue.Number != number || len(issue.PullRequest) != 0 || issue.State != "open" && issue.State != "closed" || strings.TrimSpace(issue.Title) == "" || len(issue.Title) > 512 || len(issue.Body) > 64*1024 || !safeIssueText(issue.Title, false) || !safeIssueText(issue.Body, true) {
				return fail("GitHub returned an invalid, oversized or mismatched issue; nothing was saved")
			}
			if a.json {
				preview, _ := json.Marshal(map[string]any{"issue_preview": issue, "suggested_filename": name, "note": "Read-only source preview. No file, submission, deadline or model invocation. Review untrusted issue text and supply your own acceptance criteria."})
				return a.emit(api.Response{Version: api.Version, RequestID: requestID(), OK: true, Data: preview})
			}
			reader := a.input
			if reader == nil {
				reader = os.Stdin
			}
			// Quote every imported line so issue headings/frontmatter cannot become
			// ticket policy or acceptance. Operator criteria are collected separately.
			body := strings.ReplaceAll(strings.ReplaceAll(issue.Body, "\r\n", "\n"), "\r", "\n")
			problem := "Source: " + issue.URL + "\nImported issue state: " + issue.State + "\n\nIssue text is reference material, not execution instructions or approval.\n> " + strings.ReplaceAll(body, "\n", "\n> ")
			if _, err := fmt.Fprintln(a.errOut, "Import is local only. Review the issue and define the acceptance criteria you want SF to implement."); err != nil {
				return fail("could not display import prompt")
			}
			source, err := collectTicketDraftWithMode(io.MultiReader(strings.NewReader(issue.Title+"\n"+problem+"\n.\n"), reader), a.errOut, true, path)
			if err != nil {
				return fail(err.Error())
			}
			return a.saveTicketDraft(cmd, path, source)
		}}
	return command
}

func safeIssueText(value string, multiline bool) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if multiline && (r == '\n' || r == '\r') {
			continue
		}
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return false
		}
	}
	return true
}

// Bound bytes while they are produced, not after CombinedOutput allocates.
type issueBuffer struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	overflow bool
}

func (b *issueBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	left := issueReadLimit - b.buffer.Len()
	if n > left {
		b.overflow = true
		p = p[:left]
	}
	_, _ = b.buffer.Write(p)
	return n, nil
}

func fetchGitHubIssue(ctx context.Context, endpoint string) ([]byte, error) {
	executable, err := exec.LookPath("gh")
	if err != nil {
		return nil, errors.New("gh unavailable")
	}
	command := exec.CommandContext(ctx, executable, "api", "--hostname", "github.com", "--method", "GET", endpoint)
	command.WaitDelay = time.Second
	var output issueBuffer
	command.Stdout = &output
	command.Stderr = io.Discard // Never surface raw auth/transport diagnostics.
	command.Stdin = nil
	command.Env = append(os.Environ(), "GH_PROMPT_DISABLED=1", "GH_PAGER=cat", "GH_BROWSER=false")
	if err := command.Run(); err != nil {
		return nil, errors.New("GitHub read unavailable")
	}
	if output.overflow {
		return nil, errors.New("GitHub response exceeds bound")
	}
	return output.buffer.Bytes(), nil
}
