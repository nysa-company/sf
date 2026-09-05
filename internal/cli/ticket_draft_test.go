package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/ticket"
)

func TestTicketTemplateUsesSubmissionParserAndNeverCallsDaemon(t *testing.T) {
	var output, errorOutput bytes.Buffer
	client := fakeClient(func(context.Context, api.Request) (api.Response, error) {
		t.Fatal("local template contacted daemon")
		return api.Response{}, nil
	})
	if code := Execute(context.Background(), []string{"ticket", "template"}, &output, &errorOutput, client); code != 0 {
		t.Fatalf("code=%d error=%s", code, errorOutput.String())
	}
	parsed, err := ticket.Parse(strings.NewReader(output.String()))
	if err != nil || len(parsed.Acceptance) != 3 || parsed.MaxDuration == 0 || parsed.MaxCostMicroUSD == 0 {
		t.Fatalf("parsed=%+v err=%v", parsed, err)
	}
}

func TestTicketValidationIsLocalBoundedAndDoesNotEchoInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name, source string
		valid        bool
	}{
		{"template", ticketTemplate, true},
		{"missing problem", "# Title\n", false},
		{"missing acceptance warning", "# Title\n\nProblem\n", true},
		{"invalid secret value", "---\npriority: credential-value-must-not-appear\n---\n# Title\nProblem\n", false},
		{"oversize", strings.Repeat("x", ticket.MaxSourceBytes+1), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ticket.md")
			if err := os.WriteFile(path, []byte(test.source), 0600); err != nil {
				t.Fatal(err)
			}
			var output, errorOutput bytes.Buffer
			client := fakeClient(func(context.Context, api.Request) (api.Response, error) {
				t.Fatal("validation contacted daemon")
				return api.Response{}, nil
			})
			code := Execute(context.Background(), []string{"ticket", "validate", path, "--json"}, &output, &errorOutput, client)
			combined := output.String() + errorOutput.String()
			var response api.Response
			if err := json.Unmarshal([]byte(combined), &response); err != nil {
				t.Fatal(err)
			}
			if response.OK != test.valid || response.Mutation.Attempted || (code == 0) != test.valid || strings.Contains(combined, "credential-value-must-not-appear") {
				t.Fatalf("code=%d response=%s", code, combined)
			}
			if test.name == "missing acceptance warning" && !strings.Contains(combined, "Add observable acceptance criteria") {
				t.Fatal("missing warning")
			}
		})
	}
}

func TestReadLocalTicketRefusesSymlinkAndDirectory(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "ticket.md")
	if err := os.WriteFile(path, []byte(ticketTemplate), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.md")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{root, link, filepath.Join(root, "missing")} {
		if _, err := readLocalTicket(invalid); err == nil {
			t.Fatalf("accepted %s", invalid)
		}
	}
}
