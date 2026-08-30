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
	"github.com/nysa-company/sf/internal/store"
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

func TestProjectionAcceptsOnlyNormativeSubmitFromNone(t *testing.T) {
	record := Record{Schema: Schema, ID: 1, Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-1", TicketVersion: 1, Trigger: "submit_valid", From: domain.State("none"), To: domain.StateQueued, Payload: []byte(`{}`), CreatedAt: time.Now().UTC()}
	if err := record.Validate(); err != nil {
		t.Fatalf("normative submit rejected: %v", err)
	}
	record.Trigger = "phase_pass"
	if err := record.Validate(); err == nil {
		t.Fatal("non-submit transition accepted from none")
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

type pagedStore struct {
	events []store.Event
	calls  int
}

func (source *pagedStore) Events(_ context.Context, channel domain.Channel, after uint64, limit int) ([]store.Event, error) {
	source.calls++
	var result []store.Event
	for _, item := range source.events {
		if item.Ref.Channel == channel && item.ID > after {
			result = append(result, item)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

func TestStoreSourcePagesAndMapsDurableEvents(t *testing.T) {
	created := time.Date(2026, 8, 29, 13, 0, 0, 0, time.UTC)
	source := &pagedStore{events: []store.Event{
		{ID: 1, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-1"}, TicketVersion: 1, Trigger: "submit_valid", From: "none", To: domain.StateQueued, Payload: `{}`, CreatedAt: created},
		{ID: 2, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-1"}, TicketVersion: 2, Trigger: "operator_start", From: domain.StateQueued, To: domain.StatePlanning, Payload: `{"token":"must-redact"}`, CreatedAt: created.Add(time.Second)},
		{ID: 3, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-1"}, TicketVersion: 3, Trigger: "phase_pass", From: domain.StatePlanning, To: domain.StateVerifying, Payload: `{}`, CreatedAt: created.Add(2 * time.Second)},
	}}
	records, err := (StoreSource{Store: source, Channel: domain.ChannelDev, BatchSize: 2}).Events(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 || source.calls != 2 || records[2].ID != 3 || records[1].Trigger != "operator_start" {
		t.Fatalf("records=%+v calls=%d", records, source.calls)
	}
	var projected bytes.Buffer
	if err := (Projector{}).Write(context.Background(), SliceSource(records), &projected); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(projected.String(), "must-redact") {
		t.Fatalf("projection leaked secret: %s", projected.String())
	}
}

func TestStoreSourceRejectsOutOfOrderAuthorityPage(t *testing.T) {
	created := time.Now().UTC()
	source := &pagedStore{events: []store.Event{
		{ID: 2, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-1"}, TicketVersion: 1, Trigger: "submit_valid", From: "none", To: domain.StateQueued, Payload: `{}`, CreatedAt: created},
		{ID: 1, Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-1"}, TicketVersion: 2, Trigger: "operator_start", From: domain.StateQueued, To: domain.StatePlanning, Payload: `{}`, CreatedAt: created},
	}}
	if _, err := (StoreSource{Store: source, Channel: domain.ChannelDev, BatchSize: 2}).Events(context.Background()); err == nil {
		t.Fatal("out-of-order authority page was accepted")
	}
}
