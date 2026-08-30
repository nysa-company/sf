// Package events defines the append-only typed event projection boundary.
// Events are read from an authority supplied by the daemon; this package never
// treats an NDJSON file as workflow state.
package events

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/redact"
	"github.com/nysa-company/sf/internal/store"
)

const Schema = "sf.event/v1"

type Record struct {
	Schema        string           `json:"schema"`
	ID            uint64           `json:"id"`
	Channel       domain.Channel   `json:"channel"`
	Project       domain.ProjectID `json:"project"`
	Ticket        domain.TicketID  `json:"ticket"`
	TicketVersion uint64           `json:"ticket_version"`
	Trigger       string           `json:"trigger"`
	From          domain.State     `json:"from"`
	To            domain.State     `json:"to"`
	Payload       json.RawMessage  `json:"payload"`
	CreatedAt     time.Time        `json:"created_at"`
}

func (record Record) Validate() error {
	if record.Schema == "" {
		record.Schema = Schema
	}
	fromValid := record.From.Valid() || (record.Trigger == "submit_valid" && string(record.From) == "none")
	if record.Schema != Schema || record.ID == 0 || !record.Channel.Valid() || record.Project == "" || record.Ticket == "" || record.TicketVersion == 0 || record.Trigger == "" || !fromValid || !record.To.Valid() || record.CreatedAt.IsZero() {
		return errors.New("invalid typed event record")
	}
	if len(record.Payload) == 0 {
		return errors.New("event payload is required")
	}
	var value any
	if err := json.Unmarshal(record.Payload, &value); err != nil {
		return fmt.Errorf("event payload is not JSON: %w", err)
	}
	return nil
}

// Source is implemented by the SQLite event reader. Keeping it small makes
// projection tests fully hermetic.
type Source interface {
	Events(context.Context) ([]Record, error)
}

type SliceSource []Record

func (source SliceSource) Events(context.Context) ([]Record, error) {
	return append([]Record(nil), source...), nil
}

type StoreReader interface {
	Events(context.Context, domain.Channel, uint64, int) ([]store.Event, error)
}

// StoreSource adapts the SQLite authority to the disposable NDJSON
// projection. It pages by the durable event id so a large history never
// depends on a single SQLite query or an offset that can drift.
type StoreSource struct {
	Store     StoreReader
	Channel   domain.Channel
	BatchSize int
}

func (source StoreSource) Events(ctx context.Context) ([]Record, error) {
	if source.Store == nil || !source.Channel.Valid() {
		return nil, errors.New("event store and valid channel are required")
	}
	batchSize := source.BatchSize
	if batchSize == 0 {
		batchSize = 4096
	}
	if batchSize < 1 || batchSize > 100_000 {
		return nil, errors.New("event batch size must be between 1 and 100000")
	}
	var records []Record
	var after uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := source.Store.Events(ctx, source.Channel, after, batchSize)
		if err != nil {
			return nil, err
		}
		if len(batch) > batchSize {
			return nil, errors.New("SQLite event page exceeded the requested bound")
		}
		for _, item := range batch {
			if item.ID <= after || item.Ref.Channel != source.Channel {
				return nil, errors.New("SQLite returned an out-of-order or cross-channel event")
			}
			after = item.ID
			records = append(records, Record{
				Schema: Schema, ID: item.ID, Channel: item.Ref.Channel, Project: item.Ref.Project,
				Ticket: item.Ref.Ticket, TicketVersion: item.TicketVersion, Trigger: item.Trigger,
				From: item.From, To: item.To, Payload: json.RawMessage(item.Payload), CreatedAt: item.CreatedAt,
			})
		}
		if len(batch) < batchSize {
			return records, nil
		}
	}
}

type Projector struct {
	Policy redact.Policy
}

func (p Projector) Write(ctx context.Context, source Source, writer io.Writer) error {
	records, err := source.Events(ctx)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	var previous uint64
	for index, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := record.Validate(); err != nil {
			return fmt.Errorf("event %d: %w", index, err)
		}
		if index > 0 && record.ID <= previous {
			return fmt.Errorf("event %d is out of order", index)
		}
		previous = record.ID
		record.Schema = Schema
		record.Payload = p.Policy.JSON(record.Payload)
		if err := encoder.Encode(record); err != nil {
			return fmt.Errorf("encode event %d: %w", index, err)
		}
	}
	return nil
}

// Rebuild writes a projection atomically. The source is consulted once, and a
// failed rebuild leaves the previous projection untouched.
func (p Projector) Rebuild(ctx context.Context, source Source, path string) error {
	if path == "" || filepath.IsAbs(path) == false {
		return errors.New("event projection path must be absolute")
	}
	if err := validateProjectionPath(path); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".events-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	buffered := bufio.NewWriter(temporary)
	if err := p.Write(ctx, source, buffered); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := buffered.Flush(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Chmod(temporaryPath, 0o600); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

// validateProjectionPath refuses symlinked path components before creating a
// temporary file. The containing directory must be owned by this user; system
// ancestors (for example /Users or /tmp) are allowed to be root-owned while
// still being checked for symlink traversal.
func validateProjectionPath(path string) error {
	parent := filepath.Dir(filepath.Clean(path))
	for current := parent; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("event projection parent is unavailable: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if !allowedSystemSymlink(current) {
				return errors.New("event projection parent contains a symlink")
			}
		} else if !info.IsDir() {
			return errors.New("event projection parent is not a directory")
		}
		if current == filepath.Dir(current) {
			break
		}
	}
	parentInfo, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	if owner, ok := projectionOwner(parentInfo); !ok || owner != uint32(os.Geteuid()) {
		return errors.New("event projection parent is not owned by the current user")
	}
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return errors.New("event projection target is a symlink")
	}
	return nil
}

func allowedSystemSymlink(path string) bool {
	return runtime.GOOS == "darwin" && (path == "/tmp" || path == "/var")
}

func projectionOwner(info os.FileInfo) (uint32, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint32(stat.Uid), true
}
