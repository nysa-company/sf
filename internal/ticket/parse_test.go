package ticket

import (
	"bytes"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestParseMinimalTicket(t *testing.T) {
	source := "# Fix reminders\n\nUsers receive duplicates.\n\n## Acceptance\n\n- One delivery per occurrence.\n"
	parsed, err := Parse(strings.NewReader(source))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Title != "Fix reminders" || parsed.Problem != "Users receive duplicates." {
		t.Fatalf("unexpected ticket: %+v", parsed)
	}
	if parsed.Type != domain.TicketFeature || parsed.MergeMode != domain.MergeGuarded {
		t.Fatalf("unexpected defaults: %+v", parsed)
	}
	if string(parsed.Source) != source || len(parsed.Digest) != 64 {
		t.Fatal("source or digest not retained")
	}
}

func TestParseStrictFrontMatter(t *testing.T) {
	source := "---\r\ntype: bug\r\nmerge: manual\r\npriority: high\r\n---\r\n# Fix\r\n\r\nBroken.\r\n## Acceptance\r\n- Regression fails before the fix.\r\n"
	parsed, err := Parse(strings.NewReader(source))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Type != domain.TicketBug || parsed.MergeMode != domain.MergeManual || parsed.Priority != "high" {
		t.Fatalf("unexpected front matter: %+v", parsed)
	}
}

func TestParseRejectsUnsafeOrAmbiguousInput(t *testing.T) {
	tests := map[string]string{
		"unknown key":   "---\nmodel: arbitrary\n---\n# T\nP\n## Acceptance\n- A\n",
		"duplicate key": "---\ntype: bug\ntype: feature\n---\n# T\nP\n## Acceptance\n- A\n",
		"yaml alias":    "---\ntype: *bug\n---\n# T\nP\n## Acceptance\n- A\n",
		"second title":  "# T\nP\n# Inject\n## Acceptance\n- A\n",
		"missing proof": "# T\nP\n## Acceptance\n",
		"spike auto":    "---\ntype: spike\nmerge: autonomous\n---\n# T\nP\n## Acceptance\n- Report\n",
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(strings.NewReader(source)); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestParseEnforcesSizeBound(t *testing.T) {
	data := bytes.Repeat([]byte("x"), MaxSourceBytes+1)
	if _, err := Parse(bytes.NewReader(data)); err == nil {
		t.Fatal("expected size rejection")
	}
}

func TestReturnedSourceDoesNotAliasReaderBytes(t *testing.T) {
	data := []byte("# T\nProblem\n## Acceptance\n- A\n")
	parsed, err := Parse(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 'X'
	if parsed.Source[0] != '#' {
		t.Fatal("parsed source aliases caller bytes")
	}
}
