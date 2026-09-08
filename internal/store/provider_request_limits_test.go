package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// This isolates the admission count query, not qualification or complete
// provider lifecycle. Rows deliberately have no phase/outcome columns: neither
// may filter the lifetime request count. Full Store authority is unchanged.
func TestProviderRequestLimitSurvivesReopenAndCountsEveryAttempt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "requests.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE provider_attempts(channel TEXT, project_id TEXT, ticket_id TEXT)`); err != nil {
		t.Fatal(err)
	}
	request := ProviderAttemptRequest{
		Ref:     domain.TicketRef{Channel: domain.ChannelDev, Project: "project", Ticket: "ticket"},
		Binding: contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: "claude"}},
		Input:   contracts.PhaseInput{Timeout: time.Minute},
	}
	check := func(want error) {
		t.Helper()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err := admitProviderRequestLimit(ctx, conn, request); !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
	}
	for i := 0; i < contracts.MultiCLIRequestLimit; i++ {
		check(nil)
		if _, err := db.Exec(`INSERT INTO provider_attempts VALUES('dev','project','ticket')`); err != nil {
			t.Fatal(err)
		}
	}
	check(ErrBudgetExhausted)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	check(ErrBudgetExhausted)
	request.Binding.Identity.Provider = "cursor"
	check(ErrBudgetExhausted)
	request.Ref.Ticket = "other"
	check(nil)
	request.Ref.Ticket, request.Ref.Project = "ticket", "other"
	check(nil)
	request.Ref.Project, request.Ref.Channel = "project", domain.ChannelStable
	check(nil)
	request.Input.Timeout = contracts.MultiCLIRequestTimeout + 1
	check(ErrBudgetExhausted)
	request.Input.Timeout = 0
	check(ErrBudgetExhausted)
	request.Input.Timeout = contracts.MultiCLIRequestTimeout
	check(nil)
	request.Ref.Channel = domain.ChannelDev
	request.Binding.Identity.Provider = "codex"
	check(nil) // unchanged Codex accounting policy
}
