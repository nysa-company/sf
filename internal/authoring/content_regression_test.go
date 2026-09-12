package authoring

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/redact"
)

func validAuthoringDraft() contracts.AuthoringResult {
	return contracts.AuthoringResult{
		Kind: "draft", Title: "Bounded title", Problem: "A bounded problem.",
		Scope: []string{"one file"}, Acceptance: []string{"it is reviewed"}, Assumptions: []string{"the repository is local"},
	}
}

func TestParseRejectsStrictSchemaViolations(t *testing.T) {
	valid := `{"kind":"draft","question":"","title":"T","problem":"P","scope":["S"],"acceptance":["A"],"assumptions":["X"]}`
	for _, tc := range []struct {
		name, raw string
	}{
		{"duplicate field", strings.Replace(valid, `"title":"T"`, `"title":"T","title":"T2"`, 1)},
		{"missing field", strings.Replace(valid, `,"assumptions":["X"]`, "", 1)},
		{"unknown field", strings.Replace(valid, `,"assumptions":["X"]`, `,"assumptions":["X"],"extra":true`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse([]byte(tc.raw)); err == nil {
				t.Fatalf("Parse accepted %s", tc.name)
			}
		})
	}
}

func TestControlsRejectedBeforeRenderOrDigest(t *testing.T) {
	for _, field := range []string{"Title", "Problem", "Question"} {
		result := validAuthoringDraft()
		switch field {
		case "Title":
			result.Title = "bad\x00title"
		case "Problem":
			result.Problem = "bad\u202eproblem"
		case "Question":
			result.Question = "bad\nquestion"
		}
		if err := Validate(result); err == nil {
			t.Fatalf("Validate accepted control in %s", field)
		}
		if _, err := Markdown(result); err == nil {
			t.Fatalf("Markdown rendered invalid %s", field)
		}
	}
}

func TestSanitizeRedactsAndRevalidates(t *testing.T) {
	result := validAuthoringDraft()
	result.Problem = "token: abc123"
	clean, err := Sanitize(result, redact.NewPolicy("", nil))
	if err != nil || strings.Contains(clean.Problem, "abc123") {
		t.Fatalf("Sanitize result=%+v err=%v", clean, err)
	}
}

func TestSnapshotRejectsUnsafeContextPathsAndBounds(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "ok.txt"), []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	tooMany := make([]string, 9)
	for i := range tooMany {
		tooMany[i] = "ok.txt"
	}
	for _, tc := range []struct {
		name  string
		paths []string
	}{
		{"hidden", []string{".hidden"}},
		{"credential", []string{"credentials.txt"}},
		{"token", []string{"token.txt"}},
		{"encoded traversal", []string{"%2e%2e/ok.txt"}},
		{"absolute", []string{filepath.Join(root, "ok.txt")}},
		{"duplicate", []string{"ok.txt", "ok.txt"}},
		{"path count bound", tooMany},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := Snapshot(root, tc.paths); err == nil {
				t.Fatalf("Snapshot accepted unsafe %s path", tc.name)
			}
		})
	}
}

func TestSnapshotRejectsSymlinkAndContentBounds(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(root, "link.txt")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Snapshot(root, []string{"link.txt"}); err == nil {
		t.Fatal("Snapshot followed a symlink")
	}
	if err := os.WriteFile(filepath.Join(root, "large.txt"), []byte(strings.Repeat("x", 16<<10+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Snapshot(root, []string{"large.txt"}); err == nil {
		t.Fatal("Snapshot accepted oversized content")
	}
}
