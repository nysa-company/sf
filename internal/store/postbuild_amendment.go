package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

// PostbuildAmendmentSnapshot names observations made at the finished Builder
// boundary. Digests alone do not attest physical admission: the coordinator
// must reobserve them under the authenticated worktree/writer boundary.
type PostbuildAmendmentSnapshot struct {
	FullSnapshotDigest   string
	ImplementationDigest string
	ProtectedPathsDigest string
}

type PostbuildAmendmentBinding struct {
	Ref                        domain.TicketRef
	AmendmentTransitionVersion uint64
	RepairEntryVersion         uint64
	ConsumedVersion            uint64
	ConsumedFence              domain.Fence
	BuilderResult              ProviderAttemptResultKey
	BuilderTypedDigest         string
	VerificationRevision       uint64
	OriginalCheckpointOID      string
	Snapshot                   PostbuildAmendmentSnapshot
	BindingDigest              string
	CreatedAt                  string
}

// PostbuildVerificationAmendmentContext is logical Store evidence. Verification
// always means the original frozen proof. CurrentVerification is populated only
// after the authenticated decision, and can differ after acceptance.
type PostbuildVerificationAmendmentContext struct {
	Amendment           VerificationAmendment
	Repair              PostbuildRepair
	Snapshot            PostbuildAmendmentSnapshot
	Binding             PostbuildAmendmentBinding
	Worktree            StoredWorktree
	Plan                StoredPlan
	Verification        StoredVerification
	Builder             ProviderAttemptResult
	BuilderArtifact     phaseartifact.Builder
	Decision            VerificationAmendmentDecision
	Reviewer            ProviderAttemptResultKey
	CurrentVerification StoredVerification
}

func PostbuildAmendmentProtectedPathsDigest(paths []string) (string, error) {
	if validOwnedFiles(paths) != nil {
		return "", ErrEvidenceConflict
	}
	canonical := append([]string(nil), paths...)
	sort.Strings(canonical)
	raw, err := json.Marshal(struct {
		Schema string   `json:"schema"`
		Paths  []string `json:"paths"`
	}{"sf.postbuild-amendment-protected/v1", canonical})
	if err != nil {
		return "", err
	}
	return repositoryResultDigest(raw), nil
}

func validPostbuildAmendmentSnapshot(snapshot PostbuildAmendmentSnapshot) bool {
	return validClaimDigest(snapshot.FullSnapshotDigest) && validClaimDigest(snapshot.ImplementationDigest) && validClaimDigest(snapshot.ProtectedPathsDigest)
}

func postbuildAmendmentBindingDigest(binding PostbuildAmendmentBinding) (string, error) {
	binding.BindingDigest = ""
	raw, err := json.Marshal(binding)
	if err != nil {
		return "", err
	}
	return repositoryResultDigest(raw), nil
}

func (s *Store) TransitionPostbuildVerificationAmendmentRequest(ctx context.Context, transition Transition, key ProviderAttemptResultKey, snapshot PostbuildAmendmentSnapshot) (TransitionResult, error) {
	if !validPostbuildAmendmentSnapshot(snapshot) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	return s.transitionVerificationAmendmentRequest(ctx, transition, key, &snapshot)
}

// A nil companion is allowed only for the existing non-postbuild amendment
// lane. The active postbuild entry cannot use that API to bypass its snapshot.
func (s *Store) preparePostbuildAmendmentBinding(ctx context.Context, conn *sql.Conn, transition Transition, builder ProviderAttemptResult, prior VerificationRevision, snapshot *PostbuildAmendmentSnapshot) (*PostbuildAmendmentBinding, error) {
	repair, err := latestPostbuildRepairAt(ctx, conn, transition.Ref, transition.ExpectedVersion)
	if errors.Is(err, ErrNotFound) {
		if snapshot != nil {
			return nil, ErrEvidenceConflict
		}
		return nil, nil
	}
	if err != nil {
		return nil, ErrEvidenceConflict
	}
	superseded, err := postbuildRepairSupersededAt(ctx, conn, transition.Ref, transition.ExpectedVersion, transition.Fence, true)
	if err != nil {
		return nil, err
	}
	if superseded {
		if snapshot != nil {
			return nil, ErrEvidenceConflict
		}
		return nil, nil
	}
	if snapshot == nil || !validPostbuildAmendmentSnapshot(*snapshot) || postbuildRepairSignedSourcePrefix(ctx, conn, transition.Ref, repair.EntryVersion, repair.Fence, transition.ExpectedVersion, transition.Fence) != nil {
		return nil, ErrEvidenceConflict
	}
	if prior.Revision != repair.VerificationRevision || prior.CheckpointID != repair.OriginalCheckpointOID || prior.IntentDigest != repair.Verification.Revision.IntentDigest || prior.ProofDigest != repair.Verification.Revision.ProofDigest {
		return nil, ErrEvidenceConflict
	}
	protected, err := PostbuildAmendmentProtectedPathsDigest(repair.Verification.Revision.OwnedFiles)
	if err != nil || snapshot.ProtectedPathsDigest != protected {
		return nil, ErrEvidenceConflict
	}
	key := ProviderAttemptResultKey{Ref: transition.Ref, Phase: domain.PhaseBuild, AttemptID: builder.Claim.ID, Attempt: builder.Claim.Attempt}
	if authenticatePostbuildAmendmentBuilderEntry(ctx, conn, repair, key, builder) != nil {
		return nil, ErrEvidenceConflict
	}
	if repositoryHasProviderWriter(ctx, conn, builder.Claim.Repository) != nil || repositoryHasCommandWriter(ctx, conn, builder.Claim.Repository) != nil {
		return nil, ErrControlNotDrained
	}
	var unresolved int
	if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM git_mutation_leases WHERE repository_path=?`, builder.Claim.Repository).Scan(&unresolved) != nil || unresolved != 0 {
		return nil, ErrControlNotDrained
	}
	if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND state IN ('planned','executing','uncertain')`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket).Scan(&unresolved) != nil || unresolved != 0 {
		return nil, ErrControlNotDrained
	}
	if conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_attempts WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND (attempt>? OR id>?)`, transition.Ref.Channel, transition.Ref.Project, transition.Ref.Ticket, key.Attempt, key.AttemptID).Scan(&unresolved) != nil || unresolved != 0 {
		return nil, ErrEvidenceConflict
	}
	value := PostbuildAmendmentBinding{Ref: transition.Ref, AmendmentTransitionVersion: transition.ExpectedVersion + 1, RepairEntryVersion: repair.EntryVersion, ConsumedVersion: transition.ExpectedVersion, ConsumedFence: transition.Fence, BuilderResult: key, BuilderTypedDigest: builder.TypedSHA256, VerificationRevision: prior.Revision, OriginalCheckpointOID: prior.CheckpointID, Snapshot: *snapshot, CreatedAt: now()}
	value.BindingDigest, err = postbuildAmendmentBindingDigest(value)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func authenticatePostbuildAmendmentBuilderEntry(ctx context.Context, q candidateEvidenceQuerier, repair PostbuildRepair, key ProviderAttemptResultKey, builder ProviderAttemptResult) error {
	predecessor, _, err := (&Store{}).loadHistoricalProviderAttemptResult(ctx, q, repair.BuilderResult)
	if err != nil || key.Ref != repair.Ref || key.Phase != domain.PhaseBuild || key.AttemptID <= repair.BuilderResult.AttemptID || key.Attempt <= repair.BuilderResult.Attempt || builder.Claim.ExpectedVersion < repair.EntryVersion || builder.Claim.Repository != predecessor.Claim.Repository || builder.Claim.Worktree != predecessor.Claim.Worktree || builder.Claim.WorktreeIdentity != predecessor.Claim.WorktreeIdentity || builder.Claim.BaseSHA != predecessor.Claim.BaseSHA {
		// The full physical identity is checked by the hydrated provider result
		// and context below; no worktree path grants launch permission.
		return ErrEvidenceConflict
	}
	var count int
	if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND role='builder' AND provider_attempt_id=? AND attempt=? AND entry_ticket_version=?`, key.Ref.Channel, key.Ref.Project, key.Ref.Ticket, key.AttemptID, key.Attempt, repair.EntryVersion).Scan(&count) != nil || count != 1 {
		return ErrEvidenceConflict
	}
	return nil
}

func insertPostbuildAmendmentBinding(ctx context.Context, conn *sql.Conn, value PostbuildAmendmentBinding) error {
	_, err := conn.ExecContext(ctx, `INSERT INTO postbuild_amendment_snapshots(channel,project_id,ticket_id,amendment_transition_version,repair_entry_version,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,builder_attempt_id,builder_attempt,builder_phase,builder_role,builder_typed_digest,verification_revision,original_checkpoint_oid,full_snapshot_digest,implementation_digest,protected_paths_digest,binding_digest,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,'build','builder',?,?,?,?,?,?,?,?)`, value.Ref.Channel, value.Ref.Project, value.Ref.Ticket, value.AmendmentTransitionVersion, value.RepairEntryVersion, value.ConsumedVersion, value.ConsumedFence.LeaderEpoch, value.ConsumedFence.RunnerEpoch, value.BuilderResult.AttemptID, value.BuilderResult.Attempt, value.BuilderTypedDigest, value.VerificationRevision, value.OriginalCheckpointOID, value.Snapshot.FullSnapshotDigest, value.Snapshot.ImplementationDigest, value.Snapshot.ProtectedPathsDigest, value.BindingDigest, value.CreatedAt)
	return err
}

// Authenticate against the already-hydrated amendment to avoid recursively
// calling the amendment reader from its own integrity check.
func loadPostbuildAmendmentBinding(ctx context.Context, q candidateEvidenceQuerier, amendment VerificationAmendment) (PostbuildAmendmentBinding, PostbuildRepair, error) {
	ref := amendment.Ref
	var value PostbuildAmendmentBinding
	value.Ref, value.AmendmentTransitionVersion = ref, amendment.TransitionTicketVersion
	value.BuilderResult.Ref, value.BuilderResult.Phase = ref, domain.PhaseBuild
	err := q.QueryRowContext(ctx, `SELECT repair_entry_version,consumed_ticket_version,consumed_leader_epoch,consumed_runner_epoch,builder_attempt_id,builder_attempt,builder_typed_digest,verification_revision,original_checkpoint_oid,full_snapshot_digest,implementation_digest,protected_paths_digest,binding_digest,created_at FROM postbuild_amendment_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND amendment_transition_version=? AND builder_phase='build' AND builder_role='builder'`, ref.Channel, ref.Project, ref.Ticket, amendment.TransitionTicketVersion).Scan(&value.RepairEntryVersion, &value.ConsumedVersion, &value.ConsumedFence.LeaderEpoch, &value.ConsumedFence.RunnerEpoch, &value.BuilderResult.AttemptID, &value.BuilderResult.Attempt, &value.BuilderTypedDigest, &value.VerificationRevision, &value.OriginalCheckpointOID, &value.Snapshot.FullSnapshotDigest, &value.Snapshot.ImplementationDigest, &value.Snapshot.ProtectedPathsDigest, &value.BindingDigest, &value.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		if _, repairErr := latestPostbuildRepairAt(ctx, q, ref, amendment.ConsumedVersion); errors.Is(repairErr, ErrNotFound) {
			return value, PostbuildRepair{}, ErrNotFound
		} else if repairErr != nil {
			return value, PostbuildRepair{}, ErrEvidenceConflict
		}
		entry, entryErr := loadProviderPhaseEntryAt(ctx, q, ref, domain.PhaseBuild, amendment.ConsumedVersion)
		if entryErr != nil || entry.Trigger == "postbuild_repair" {
			return value, PostbuildRepair{}, ErrEvidenceConflict
		}
		return value, PostbuildRepair{}, ErrNotFound
	}
	if err != nil || value.ConsumedVersion != amendment.ConsumedVersion || value.AmendmentTransitionVersion != value.ConsumedVersion+1 || value.ConsumedFence != amendment.Fence || value.BuilderResult != amendment.BuilderResult || value.BuilderTypedDigest != amendment.BuilderTypedSHA256 || value.VerificationRevision != amendment.Prior.Revision || value.OriginalCheckpointOID != amendment.Prior.CheckpointID || !validPostbuildAmendmentSnapshot(value.Snapshot) {
		return value, PostbuildRepair{}, ErrEvidenceConflict
	}
	digest, err := postbuildAmendmentBindingDigest(value)
	created, timeErr := time.Parse(time.RFC3339Nano, value.CreatedAt)
	if err != nil || digest != value.BindingDigest || timeErr != nil || created.Location() != time.UTC || created.Format(time.RFC3339Nano) != value.CreatedAt {
		return value, PostbuildRepair{}, ErrEvidenceConflict
	}
	repair, err := loadPostbuildRepairEntry(ctx, q, ref, value.RepairEntryVersion)
	if err != nil || repair.VerificationRevision != value.VerificationRevision || repair.OriginalCheckpointOID != value.OriginalCheckpointOID || repair.Verification.Revision.IntentDigest != amendment.Prior.IntentDigest || repair.Verification.Revision.ProofDigest != amendment.Prior.ProofDigest || postbuildRepairSignedSourcePrefix(ctx, q, ref, repair.EntryVersion, repair.Fence, amendment.ConsumedVersion, amendment.Fence) != nil {
		return value, PostbuildRepair{}, ErrEvidenceConflict
	}
	protected, err := PostbuildAmendmentProtectedPathsDigest(repair.Verification.Revision.OwnedFiles)
	if err != nil || protected != value.Snapshot.ProtectedPathsDigest {
		return value, PostbuildRepair{}, ErrEvidenceConflict
	}
	builder, parsed, err := (&Store{}).loadHistoricalProviderAttemptResult(ctx, q, value.BuilderResult)
	if err != nil || parsed.Builder == nil || parsed.Builder.AmendmentRequest == nil || authenticatePostbuildAmendmentBuilderEntry(ctx, q, repair, value.BuilderResult, builder) != nil {
		return value, PostbuildRepair{}, ErrEvidenceConflict
	}
	return value, repair, nil
}
