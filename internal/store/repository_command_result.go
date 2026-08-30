package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/nysa-company/sf/internal/contracts"
)

// CompleteRepositoryCommand durably records the bounded command result before
// the lease is released. A zero exit is never inferred from process absence;
// the exact command claim and observed output identity are persisted together.
func (s *Store) CompleteRepositoryCommand(ctx context.Context, claim contracts.RepositoryCommandClaim, result contracts.CommandResult) error {
	if !validRepositoryCommandClaim(claim) || !result.Observed {
		return ErrRepositoryCommandIntent
	}
	sum := sha256.New()
	_, _ = sum.Write(result.Stdout)
	_, _ = sum.Write([]byte{0})
	_, _ = sum.Write(result.Stderr)
	identity := fmt.Sprintf("%s:exit=%d:output=sha256:%s", claim.SpecDigest, result.ExitCode, hex.EncodeToString(sum.Sum(nil)))
	state := EffectFailed
	if result.ExitCode == 0 {
		state = EffectConfirmed
	}
	return s.write(ctx, func(c *sql.Conn) error {
		if err := s.assertRepositoryCommandCurrent(ctx, c, claim); err != nil {
			return err
		}
		r, err := c.ExecContext(ctx, `UPDATE effects SET state=?,observed_identity=? WHERE semantic_key=? AND state='executing' AND claim_epoch=?`, state, identity, claim.SemanticKey, claim.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return ErrRepositoryCommandIntent
		}
		return nil
	})
}

var _ contracts.RepositoryCommandResultRecorder = (*Store)(nil)

func (s *Store) MarkRepositoryCommandUncertain(ctx context.Context, claim contracts.RepositoryCommandClaim, reason string) error {
	if !validRepositoryCommandClaim(claim) || reason == "" {
		return ErrRepositoryCommandIntent
	}
	return s.write(ctx, func(c *sql.Conn) error {
		if err := s.assertRepositoryCommandCurrent(ctx, c, claim); err != nil {
			return err
		}
		result, err := c.ExecContext(ctx, `UPDATE effects SET state='uncertain',observed_identity=? WHERE semantic_key=? AND state='executing' AND claim_epoch=?`, "uncertain:"+reason, claim.SemanticKey, claim.ClaimEpoch)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n != 1 {
			return ErrRepositoryCommandIntent
		}
		return nil
	})
}
