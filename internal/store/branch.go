package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// LoadOrStoreBranch implements the Git allocator's narrow SQLite authority.
// The key grammar is channel NUL project NUL ticket; parsing it here binds the
// opaque interface value back to a real durable ticket before insertion.
func (s *Store) LoadOrStoreBranch(ctx context.Context, key, proposed string) (string, error) {
	ref, err := branchKey(key)
	if err != nil {
		return "", err
	}
	if !validAllocatedBranch(ref.Channel, proposed) {
		return "", errors.New("invalid proposed sf branch")
	}
	var stored string
	err = s.write(ctx, func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(ctx, `INSERT INTO branch_allocations(authority_key, channel, project_id, ticket_id, branch_ref, created_at)
			VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(authority_key) DO NOTHING`, key, ref.Channel, ref.Project, ref.Ticket, proposed, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		var channel domain.Channel
		var project domain.ProjectID
		var ticket domain.TicketID
		if err := conn.QueryRowContext(ctx, `SELECT channel, project_id, ticket_id, branch_ref FROM branch_allocations WHERE authority_key=?`, key).Scan(&channel, &project, &ticket, &stored); err != nil {
			return err
		}
		if channel != ref.Channel || project != ref.Project || ticket != ref.Ticket || !validAllocatedBranch(channel, stored) {
			return ErrBranchConflict
		}
		return nil
	})
	return stored, err
}

func branchKey(key string) (domain.TicketRef, error) {
	parts := strings.Split(key, "\x00")
	if len(parts) != 3 {
		return domain.TicketRef{}, fmt.Errorf("invalid branch authority key")
	}
	ref := domain.TicketRef{Channel: domain.Channel(parts[0]), Project: domain.ProjectID(parts[1]), Ticket: domain.TicketID(parts[2])}
	if err := ref.Validate(); err != nil {
		return domain.TicketRef{}, fmt.Errorf("invalid branch authority key: %w", err)
	}
	return ref, nil
}

func validAllocatedBranch(channel domain.Channel, branch string) bool {
	if !channel.Valid() || len(branch) < len("sf/dev/a") || len(branch) > 255 || !strings.HasPrefix(branch, "sf/"+string(channel)+"/") {
		return false
	}
	if strings.Contains(branch, "..") || strings.Contains(branch, "@{") || strings.Contains(branch, "//") || strings.ContainsAny(branch, " ~^:?*[\\\x00\r\n\t") || strings.HasSuffix(branch, "/") || strings.HasSuffix(branch, ".lock") {
		return false
	}
	for _, component := range strings.Split(branch, "/") {
		if component == "" || component == "." || component == ".." || strings.HasSuffix(component, ".lock") {
			return false
		}
	}
	return true
}
