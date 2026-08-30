package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

// LoadBranch returns an already allocated branch without consuming allocator
// randomness. Absence is represented by an empty string so Git can propose a
// fresh suffix; any row whose opaque key, ticket identity, or exact branch
// grammar disagrees is durable corruption and fails closed.
func (s *Store) LoadBranch(ctx context.Context, key string) (string, error) {
	ref, err := branchKey(key)
	if err != nil {
		return "", err
	}
	var storedKey, stored string
	var channel domain.Channel
	var project domain.ProjectID
	var ticket domain.TicketID
	err = s.db.QueryRowContext(ctx, `SELECT authority_key, channel, project_id, ticket_id, branch_ref
		FROM branch_allocations WHERE authority_key=? OR (channel=? AND project_id=? AND ticket_id=?)
		LIMIT 1`, key, ref.Channel, ref.Project, ref.Ticket).Scan(&storedKey, &channel, &project, &ticket, &stored)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", normalizeBusy(ctx, err)
	}
	if storedKey != key || channel != ref.Channel || project != ref.Project || ticket != ref.Ticket || !validAllocatedBranch(ref, stored) {
		return "", ErrBranchConflict
	}
	return stored, nil
}

// LoadOrStoreBranch implements the Git allocator's narrow SQLite authority.
// The key grammar is channel NUL project NUL ticket; parsing it here binds the
// opaque interface value back to a real durable ticket before insertion.
func (s *Store) LoadOrStoreBranch(ctx context.Context, key, proposed string) (string, error) {
	ref, err := branchKey(key)
	if err != nil {
		return "", err
	}
	if !validAllocatedBranch(ref, proposed) {
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
		if channel != ref.Channel || project != ref.Project || ticket != ref.Ticket || !validAllocatedBranch(ref, stored) {
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

func validAllocatedBranch(ref domain.TicketRef, branch string) bool {
	if err := ref.Validate(); err != nil {
		return false
	}
	parts := strings.Split(branch, "/")
	if len(parts) != 4 || parts[0] != "sf" || parts[1] != string(ref.Channel) || parts[2] != branchDigestPart(string(ref.Project)) {
		return false
	}
	name := strings.Split(parts[3], "-")
	if len(name) != 2 || name[0] != branchDigestPart(string(ref.Ticket)) || !lowerHexBytes(name[1], 16) {
		return false
	}
	return true
}

func branchDigestPart(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:8])
}

func lowerHexBytes(value string, bytes int) bool {
	if len(value) != bytes*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytes
}
