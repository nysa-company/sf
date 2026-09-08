package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

// ProviderAttemptCheckpoint identifies the semantic clean checkout for one
// still-active attempt. It is NOT a filesystem observation or retry permit.
// The trusted inspector must authenticate the registered identity, HEAD and
// cleanliness while this exact attempt still excludes writers. Any later
// terminal transaction must repeat this authority check on its own connection.
type ProviderAttemptCheckpoint struct {
	AttemptID int64
	ProviderRetryWorktreeProof
}

// ProviderAttemptCheckpointDigest is canonical metadata encoding, not proof
// authentication. The writer and physical inspector share this exact versioned
// format so Store can independently verify the supervisor's signed digest.
func ProviderAttemptCheckpointDigest(checkpoint ProviderAttemptCheckpoint, requestDigest string) (string, error) {
	payload, err := json.Marshal(struct {
		Checkpoint    ProviderAttemptCheckpoint
		RequestDigest string
	}{checkpoint, requestDigest})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("sf-provider-checkpoint/v1\x00"), payload...))
	return hex.EncodeToString(digest[:]), nil
}

// ProviderAttemptCheckpointForRequest is the supervisor-facing form. It loads
// the full persisted input/qualification itself and compares every request
// field before deriving the checkpoint; a caller cannot project a different
// claim or provide the expected filesystem head.
func (s *Store) ProviderAttemptCheckpointForRequest(ctx context.Context, request contracts.DrainRequest) (ProviderAttemptCheckpoint, error) {
	if s == nil || request.ClaimID <= 0 || request.Ref.Validate() != nil {
		return ProviderAttemptCheckpoint{}, ErrProviderAttempt
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ProviderAttemptCheckpoint{}, normalizeBusy(ctx, err)
	}
	defer tx.Rollback()
	claim, err := loadAuthenticatedProviderAttemptClaim(ctx, tx, request.ClaimID)
	if err != nil || drainRequestForClaim(claim) != request {
		return ProviderAttemptCheckpoint{}, ErrProviderAttempt
	}
	return s.providerAttemptCheckpointFrom(ctx, tx, claim)
}

func (s *Store) ProviderAttemptCheckpoint(ctx context.Context, claim ProviderAttemptClaim) (ProviderAttemptCheckpoint, error) {
	if s == nil || claim.ID <= 0 || claim.Ref.Validate() != nil {
		return ProviderAttemptCheckpoint{}, ErrProviderAttempt
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ProviderAttemptCheckpoint{}, normalizeBusy(ctx, err)
	}
	defer tx.Rollback()
	return s.providerAttemptCheckpointFrom(ctx, tx, claim)
}

func (s *Store) providerAttemptCheckpointFrom(ctx context.Context, q rowQueryer, claim ProviderAttemptClaim) (ProviderAttemptCheckpoint, error) {
	source, err := loadAuthenticatedProviderAttemptClaim(ctx, q, claim.ID)
	if err != nil || !sameImmutableProviderAttemptClaim(claim, source) || source.BindingDigest != bindingDigest(source.Binding) {
		return ProviderAttemptCheckpoint{}, ErrProviderAttempt
	}
	version, runner, leader, err := loadProviderAttemptResultCurrentFence(ctx, q, claim.Ref)
	if err != nil || version != claim.ExpectedVersion || runner != claim.RunnerEpoch || leader != claim.LeaderEpoch {
		return ProviderAttemptCheckpoint{}, ErrStaleFence
	}
	var state domain.State
	var resume sql.NullString
	if err := q.QueryRowContext(ctx, `SELECT state,resume_state FROM tickets WHERE channel=? AND project_id=? AND id=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket).Scan(&state, &resume); err != nil || resume.Valid || !providerAdmissionState(state, claim.Phase, claim.Role) {
		return ProviderAttemptCheckpoint{}, ErrStaleFence
	}
	var active int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts a
		JOIN phase_runs p ON p.channel=a.channel AND p.project_id=a.project_id AND p.ticket_id=a.ticket_id AND p.phase=a.phase AND p.attempt=a.attempt
		JOIN leases l ON l.channel=a.channel AND l.project_id=a.project_id AND l.ticket_id=a.ticket_id AND l.scope='provider' AND l.scope_key=a.provider_lease_key AND l.runner_epoch=a.runner_epoch
		WHERE a.id=? AND a.state='active' AND p.state='active'
		AND p.leader_epoch=a.leader_epoch AND p.runner_epoch=a.runner_epoch AND p.expected_ticket_version=a.expected_ticket_version
		AND p.provider=a.provider AND p.model=a.model AND p.family=a.family AND p.provider_version=a.version
		AND p.worktree_identity=a.worktree_identity AND p.base_sha=a.base_sha`, claim.ID).Scan(&active); err != nil || active != 1 {
		return ProviderAttemptCheckpoint{}, ErrProviderAttempt
	}
	entry, err := loadCurrentProviderPhaseEntry(ctx, q, claim.Ref, claim.Phase, version, runner, leader)
	if err != nil {
		return ProviderAttemptCheckpoint{}, ErrEvidenceConflict
	}
	var bindings int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_attempt_entries WHERE provider_attempt_id=? AND channel=? AND project_id=? AND ticket_id=? AND phase=? AND attempt=? AND entry_ticket_version=?`, claim.ID, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket, claim.Phase, claim.Attempt, entry.Version).Scan(&bindings); err != nil || bindings != 1 {
		return ProviderAttemptCheckpoint{}, ErrEvidenceConflict
	}
	worktree, project, err := providerRetryWorktreeFrom(ctx, q, claim.Ref)
	if err != nil || project.Path != claim.Repository || worktree.Path != claim.Worktree || string(worktree.IdentityJSON) != claim.WorktreeIdentity || worktree.BaseSHA != claim.BaseSHA {
		return ProviderAttemptCheckpoint{}, ErrEvidenceConflict
	}
	head, err := s.providerRetryPhaseExpectedHead(ctx, q, claim.Ref, claim.Phase, entry, project, worktree)
	if err != nil {
		return ProviderAttemptCheckpoint{}, err
	}
	return ProviderAttemptCheckpoint{AttemptID: claim.ID, ProviderRetryWorktreeProof: ProviderRetryWorktreeProof{
		Ref: claim.Ref, Phase: claim.Phase, Version: version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: runner}, Worktree: worktree, ExpectedHead: head,
	}}, nil
}
