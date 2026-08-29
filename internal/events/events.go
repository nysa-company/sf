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
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/redact"
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
	if record.Schema != Schema || record.ID == 0 || !record.Channel.Valid() || record.Project == "" || record.Ticket == "" || record.TicketVersion == 0 || record.Trigger == "" || !record.From.Valid() || !record.To.Valid() || record.CreatedAt.IsZero() {
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
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
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
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Chmod(temporaryPath, 0o600); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
