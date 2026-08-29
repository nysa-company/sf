package events

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/redact"
)

func fixtureEvents() SliceSource {
	created := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	return SliceSource{
		{Schema: Schema, ID: 1, Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-1", TicketVersion: 2, Trigger: "start", From: domain.StateQueued, To: domain.StatePlanning, Payload: []byte(`{"token":"do-not-persist","path":"/Users/alice/worktree"}`), CreatedAt: created},
		{Schema: Schema, ID: 2, Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-1", TicketVersion: 3, Trigger: "phase_pass", From: domain.StatePlanning, To: domain.StateVerifying, Payload: []byte(`{"message":"ok"}`), CreatedAt: created.Add(time.Second)},
	}
}

func TestProjectionRedactsAndRebuildsByteIdentically(t *testing.T) {
	policy := redact.NewPolicy("/Users/alice", map[string]string{"/Users/alice/worktree": "$WORKTREE"})
	projector := Projector{Policy: policy}
	var first bytes.Buffer
	if err := projector.Write(context.Background(), fixtureEvents(), &first); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(first.String(), "do-not-persist") || strings.Contains(first.String(), "/Users/alice") {
		t.Fatalf("secret/path leaked: %q", first.String())
	}
	path := filepath.Join(t.TempDir(), "events.ndjson")
	if err := projector.Rebuild(context.Background(), fixtureEvents(), path); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), rebuilt) {
		t.Fatalf("rebuild differs\nfirst=%s\nrebuilt=%s", first.Bytes(), rebuilt)
	}
}

func TestProjectionRejectsOutOfOrderEvents(t *testing.T) {
	events := fixtureEvents()
	events[1].ID = 1
	var output bytes.Buffer
	if err := (Projector{}).Write(context.Background(), events, &output); err == nil {
		t.Fatal("expected ordering error")
	}
}

func TestRebuildRejectsSymlinkedParentBeforeWriting(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(link, "events.ndjson")
	if err := (Projector{}).Rebuild(context.Background(), fixtureEvents(), path); err == nil {
		t.Fatal("expected symlinked parent rejection")
	}
	if _, err := os.Stat(filepath.Join(target, "events.ndjson")); !os.IsNotExist(err) {
		t.Fatalf("symlink target was written: %v", err)
	}
}
