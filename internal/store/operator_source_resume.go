package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

// OperatorSourceResume is the only typed transition that may turn an
// authenticated source-only handoff into a fresh Builder cycle. Store derives
// its event payload from immutable evidence rather than trusting attributes.
type OperatorSourceResume struct {
	Ref             domain.TicketRef
	ExpectedVersion uint64
	Fence           domain.Fence
	Operator        string
	SourceCommit    contracts.OperatorSourceCommit
	Remote          TakeoverRemoteBaseline
}

// OperatorSourceResumeProof is rechecked by worktreecoord immediately before
// Scheduler invokes Builder. It never waives physical worktree authentication.
type OperatorSourceResumeProof struct {
	Ref          domain.TicketRef
	Version      uint64
	Fence        domain.Fence
	Worktree     StoredWorktree
	Verification StoredVerification
	Plan         StoredPlan
	Operator     string
	SourceCommit contracts.OperatorSourceCommit
	Remote       TakeoverRemoteBaseline
}

// OperatorSourceResumePreparedCandidateWitness is the complete immutable
// authority for the narrow crash window after the resumed Builder has prepared
// candidate child G but before RecordCandidate has made G the durable current
// candidate.  It intentionally carries the historical source-resume proof,
// fresh verification F, Builder result, post-build command, and prepared Git
// observation together: callers must not reconstruct this bridge from mutable
// ticket state or caller supplied artifacts.
type OperatorSourceResumePreparedCandidateWitness struct {
	Ref          domain.TicketRef
	Version      uint64
	Fence        domain.Fence
	Project      Project
	Source       OperatorSourceResumeProof
	Verification StoredVerification
	Builder      ProviderAttemptResultKey
	Command      RepositoryCommandResult
	Commit       CommitObservation
	// Claim and EffectState are carried so the narrowly scoped materializer
	// recovery API can settle an uncertain prepared G without reconstructing its
	// immutable Git intent from caller input. The claim is also the only
	// authority accepted by ConfirmPreparedCommit.
	Claim       contracts.GitMutationClaim
	EffectState EffectState
}

// OperatorSourceResumePreparedCheckpoint exposes the one prepared Git child
// that may temporarily replace the source commit while the fresh Reviewer
// checkpoint is between update-ref and RecordVerification.  It is deliberately
// narrower than generic Git intent lookup: the current typed source-resume
// proof must match, the child must have the source commit as its exact parent,
// and ambiguity is refused rather than selecting an arbitrary repository
// mutation.
func (s *Store) OperatorSourceResumePreparedCheckpoint(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (CommitObservation, bool, error) {
	if s == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return CommitObservation{}, false, ErrEvidenceConflict
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return CommitObservation{}, false, normalizeBusy(ctx, err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return CommitObservation{}, false, normalizeBusy(ctx, err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK") }()
	return s.operatorSourceResumePreparedCheckpointFrom(ctx, conn, ref, version, fence)
}

func (s *Store) operatorSourceResumePreparedCheckpointFrom(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence) (CommitObservation, bool, error) {
	proof, found, err := s.operatorSourceResumeProofFrom(ctx, conn, ref, version, fence)
	if err != nil || !found {
		return CommitObservation{}, found, err
	}
	if proof.SourceCommit.CommitOID == "" || proof.SourceCommit.ParentOID != proof.Verification.Checkpoint.CommitOID {
		return CommitObservation{}, false, ErrEvidenceConflict
	}
	project := Project{Channel: ref.Channel, ID: ref.Project}
	if err := conn.QueryRowContext(ctx, `SELECT canonical_path,base_ref,current_config_generation FROM projects WHERE channel=? AND id=?`, ref.Channel, ref.Project).Scan(&project.Path, &project.BaseRef, &project.ConfigGeneration); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return CommitObservation{}, false, ErrNotFound
		}
		return CommitObservation{}, false, normalizeBusy(ctx, err)
	}
	rows, err := conn.QueryContext(ctx, `SELECT semantic_key FROM git_mutation_intents
		WHERE channel=? AND project_id=? AND ticket_id=? AND operation='commit'
			AND repository_path=? AND worktree_path=? AND branch_ref=?
			AND base_ref=? AND expected_base_oid=? AND expected_head_oid=?
			AND prepared_commit_oid<>'' AND prepared_tree_oid<>''
		ORDER BY semantic_key`, ref.Channel, ref.Project, ref.Ticket, project.Path, proof.Worktree.Path, proof.Worktree.Branch, project.BaseRef, proof.Worktree.BaseSHA, proof.SourceCommit.CommitOID)
	if err != nil {
		return CommitObservation{}, false, normalizeBusy(ctx, err)
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return CommitObservation{}, false, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return CommitObservation{}, false, err
	}
	if err := rows.Close(); err != nil {
		return CommitObservation{}, false, err
	}
	if len(keys) == 0 {
		return CommitObservation{}, false, nil
	}
	if len(keys) != 1 {
		return CommitObservation{}, false, ErrEvidenceConflict
	}
	facts, err := gitMutationIntentFactsFrom(ctx, conn, keys[0])
	if err != nil || facts.Claim.Operation != "commit" || facts.Claim.TicketRef != ref || facts.Claim.Repository != project.Path || facts.Claim.Worktree != proof.Worktree.Path || facts.Claim.Branch != proof.Worktree.Branch || facts.Claim.BaseRef != project.BaseRef || facts.Claim.ExpectedBaseOID != proof.Worktree.BaseSHA || facts.Claim.ExpectedHeadOID != proof.SourceCommit.CommitOID || facts.PreparedCommitOID == "" || facts.PreparedTreeOID == "" || facts.Effect.State != EffectConfirmed {
		return CommitObservation{}, false, ErrEvidenceConflict
	}
	if err := s.authenticatePreparedSourceCheckpoint(ctx, conn, ref, version, fence, proof, project, facts); err != nil {
		return CommitObservation{}, false, err
	}
	return CommitObservation{CommitOID: facts.PreparedCommitOID, ParentOID: proof.SourceCommit.CommitOID, TreeOID: facts.PreparedTreeOID}, true, nil
}

// CanonicalRepositoryCommitDigest is the shared digest contract for a
// materialized commit. Store uses it while authenticating a prepared child;
// runtime uses the same function when first issuing the Git intent.
func CanonicalRepositoryCommitDigest(kind string, ref domain.TicketRef, version uint64, fence domain.Fence, worktree StoredWorktree, provider ProviderAttemptResultKey, command contracts.RepositoryCommandResultKey, resultDigest string, value any) string {
	data, _ := json.Marshal(struct {
		Kind         string
		Ref          any
		Version      uint64
		Fence        domain.Fence
		Worktree     StoredWorktree
		Provider     ProviderAttemptResultKey
		Command      contracts.RepositoryCommandResultKey
		ResultDigest string
		Value        any
	}{kind, ref, version, fence, worktree, provider, command, resultDigest, value})
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

type OperatorSourceResumeCheckpointDigestInput struct {
	Ref          domain.TicketRef
	WorktreePath string
	Branch       string
	Identity     []byte
	BaseSHA      string
	Source       contracts.OperatorSourceCommit
	Retained     VerificationRevision
	Provider     ProviderAttemptResultKey
	Command      contracts.RepositoryCommandResultKey
	ResultDigest string
	Artifact     phaseartifact.Verification
}

// CanonicalOperatorSourceResumeCheckpointDigest freezes the source-resume
// checkpoint tuple. It is intentionally independent of the later live fence.
func CanonicalOperatorSourceResumeCheckpointDigest(value OperatorSourceResumeCheckpointDigestInput) (string, error) {
	data, err := json.Marshal(struct {
		Kind         string
		Ref          domain.TicketRef
		WorktreePath string
		Branch       string
		Identity     []byte
		BaseSHA      string
		Source       contracts.OperatorSourceCommit
		Retained     VerificationRevision
		Provider     ProviderAttemptResultKey
		Command      contracts.RepositoryCommandResultKey
		ResultDigest string
		Artifact     phaseartifact.Verification
	}{"source-resume-verification-checkpoint/v1", value.Ref, value.WorktreePath, value.Branch, value.Identity, value.BaseSHA, value.Source, value.Retained, value.Provider, value.Command, value.ResultDigest, value.Artifact})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (s *Store) authenticatePreparedSourceCheckpoint(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence, proof OperatorSourceResumeProof, project Project, facts GitMutationIntentFacts) error {
	if conn == nil {
		return ErrEvidenceConflict
	}
	if facts.Claim.RequestDigest == "" || facts.Claim.Repository != project.Path || facts.Claim.BaseRef != project.BaseRef || facts.Claim.ExpectedBaseOID != proof.Worktree.BaseSHA || facts.Claim.ExpectedHeadOID != proof.SourceCommit.CommitOID {
		return ErrEvidenceConflict
	}
	intentRows, err := conn.QueryContext(ctx, `SELECT a.id,a.attempt FROM provider_attempts a
		WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase=? AND a.role='reviewer'
		AND a.state='completed' AND a.outcome='completed' AND a.attempt>0
		ORDER BY a.id,a.attempt`, ref.Channel, ref.Project, ref.Ticket, domain.PhaseVerification)
	if err != nil {
		return normalizeBusy(ctx, err)
	}
	var reviewers []ProviderAttemptResultKey
	for intentRows.Next() {
		var key ProviderAttemptResultKey
		if err := intentRows.Scan(&key.AttemptID, &key.Attempt); err != nil {
			intentRows.Close()
			return err
		}
		key.Ref, key.Phase = ref, domain.PhaseVerification
		reviewers = append(reviewers, key)
	}
	if err := intentRows.Err(); err != nil {
		intentRows.Close()
		return err
	}
	if err := intentRows.Close(); err != nil {
		return err
	}
	commandRows, err := conn.QueryContext(ctx, `SELECT semantic_key,claim_epoch FROM repository_command_results
		WHERE channel=? AND project_id=? AND ticket_id=? AND semantic_key LIKE ?
		ORDER BY semantic_key,claim_epoch`, ref.Channel, ref.Project, ref.Ticket, "repository-command-evidence/"+RepositoryCommandPurposePrebuildVerification+"/%")
	if err != nil {
		return normalizeBusy(ctx, err)
	}
	var commandKeys []contracts.RepositoryCommandResultKey
	for commandRows.Next() {
		var key contracts.RepositoryCommandResultKey
		if err := commandRows.Scan(&key.SemanticKey, &key.ClaimEpoch); err != nil {
			commandRows.Close()
			return err
		}
		commandKeys = append(commandKeys, key)
	}
	if err := commandRows.Err(); err != nil {
		commandRows.Close()
		return err
	}
	if err := commandRows.Close(); err != nil {
		return err
	}
	commands := make([]RepositoryCommandResult, 0, len(commandKeys))
	for _, key := range commandKeys {
		result, found, err := loadRepositoryCommandResult(ctx, conn, key, true)
		if err != nil {
			return ErrEvidenceConflict
		}
		if found {
			commands = append(commands, result)
		}
	}
	for _, key := range reviewers {
		if key == proof.Verification.ProviderResult {
			continue
		}
		provider, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, key)
		if err != nil || provider.Claim.Role != "reviewer" || parsed.Verify == nil || provider.Claim.Repository != project.Path || provider.Claim.Worktree != proof.Worktree.Path || provider.Claim.WorktreeIdentity != string(proof.Worktree.IdentityJSON) || provider.Claim.BaseSHA != proof.Worktree.BaseSHA || s.operatorSourceResumeProviderResultReachesFence(ctx, conn, ref, key, provider, version, fence) != nil {
			continue
		}
		intent, err := workflowprompt.CanonicalVerificationIntentBytes(*parsed.Verify)
		if err != nil {
			continue
		}
		proofBytes, err := workflowprompt.CanonicalVerificationProofBytes(*parsed.Verify)
		if err != nil {
			continue
		}
		for _, command := range commands {
			if command.Claim.TicketRef != ref || command.Claim.TicketVersion != provider.Claim.ExpectedVersion || command.Claim.LeaderEpoch != provider.Claim.LeaderEpoch || command.Claim.RunnerEpoch != provider.Claim.RunnerEpoch || command.Claim.Repository != project.Path || command.Claim.Worktree != proof.Worktree.Path || command.Claim.WorktreeIdentity != string(proof.Worktree.IdentityJSON) || command.Claim.BaseSHA != proof.Worktree.BaseSHA {
				continue
			}
			artifact := VerificationArtifact{Ref: ref, ExpectedVersion: provider.Claim.ExpectedVersion, Fence: domain.Fence{LeaderEpoch: provider.Claim.LeaderEpoch, RunnerEpoch: provider.Claim.RunnerEpoch}, Intent: intent, Proof: proofBytes, OwnedFiles: append([]string(nil), parsed.Verify.OwnedFiles...), ProviderResult: &key, CommandResult: command.Key}
			if _, _, err := authenticateVerificationCommandEvidence(ctx, conn, artifact, parsed.Verify); err != nil {
				continue
			}
			digest, err := CanonicalOperatorSourceResumeCheckpointDigest(OperatorSourceResumeCheckpointDigestInput{Ref: ref, WorktreePath: proof.Worktree.Path, Branch: proof.Worktree.Branch, Identity: proof.Worktree.IdentityJSON, BaseSHA: proof.Worktree.BaseSHA, Source: proof.SourceCommit, Retained: proof.Verification.Revision, Provider: key, Command: command.Key, ResultDigest: command.ResultDigest, Artifact: *parsed.Verify})
			if err == nil && facts.Claim.RequestDigest == digest {
				return nil
			}
		}
	}
	return ErrEvidenceConflict
}

// authenticateOperatorSourceResumeVerificationBinding is the narrowly scoped
// RecordVerification fallback for a fresh Reviewer whose prepared checkpoint
// survived a source-resume takeover.  The ordinary provider authority validator
// intentionally cannot treat the source endpoint's first recovery row as an
// initial lifecycle.  Require the complete prepared-checkpoint proof here so the
// exception cannot be used for an arbitrary historical Reviewer result.
func (s *Store) authenticateOperatorSourceResumeVerificationBinding(ctx context.Context, conn *sql.Conn, artifact VerificationArtifact, provider ProviderAttemptResult) error {
	if artifact.ProviderResult == nil {
		return ErrEvidenceConflict
	}
	proof, found, err := s.operatorSourceResumeProofFrom(ctx, conn, artifact.Ref, artifact.ExpectedVersion, artifact.Fence)
	if err != nil || !found || proof.SourceCommit.CommitOID == "" || proof.SourceCommit.ParentOID != proof.Verification.Checkpoint.CommitOID {
		return ErrEvidenceConflict
	}
	_, project, err := operatorSourceWorktreeFrom(ctx, conn, artifact.Ref)
	if err != nil {
		return ErrEvidenceConflict
	}
	rows, err := conn.QueryContext(ctx, `SELECT semantic_key FROM git_mutation_intents
		WHERE channel=? AND project_id=? AND ticket_id=? AND operation='commit'
		AND repository_path=? AND worktree_path=? AND branch_ref=? AND base_ref=?
		AND expected_base_oid=? AND expected_head_oid=? AND prepared_commit_oid<>'' AND prepared_tree_oid<>''
		ORDER BY semantic_key`, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket, project.Path, proof.Worktree.Path, proof.Worktree.Branch, project.BaseRef, proof.Worktree.BaseSHA, proof.SourceCommit.CommitOID)
	if err != nil {
		return ErrEvidenceConflict
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return ErrEvidenceConflict
		}
		keys = append(keys, key)
	}
	if err := rows.Close(); err != nil || rows.Err() != nil || len(keys) != 1 {
		return ErrEvidenceConflict
	}
	facts, err := gitMutationIntentFactsFrom(ctx, conn, keys[0])
	if err != nil || facts.Claim.Operation != "commit" || facts.Claim.TicketRef != artifact.Ref || facts.Claim.Repository != project.Path || facts.Claim.Worktree != proof.Worktree.Path || facts.Claim.Branch != proof.Worktree.Branch || facts.Claim.BaseRef != project.BaseRef || facts.Claim.ExpectedBaseOID != proof.Worktree.BaseSHA || facts.Claim.ExpectedHeadOID != proof.SourceCommit.CommitOID || facts.PreparedCommitOID == "" || facts.PreparedTreeOID == "" || facts.Effect.State != EffectConfirmed {
		return ErrEvidenceConflict
	}
	if artifact.CommandResult.SemanticKey == "" || artifact.CommandResult.ClaimEpoch == 0 {
		return ErrEvidenceConflict
	}
	command, found, err := loadRepositoryCommandResult(ctx, conn, artifact.CommandResult, true)
	if err != nil || !found || command.Key != artifact.CommandResult || command.Claim.TicketRef != artifact.Ref || command.Claim.TicketVersion != provider.Claim.ExpectedVersion || command.Claim.LeaderEpoch != provider.Claim.LeaderEpoch || command.Claim.RunnerEpoch != provider.Claim.RunnerEpoch || command.Claim.Repository != project.Path || command.Claim.Worktree != proof.Worktree.Path || command.Claim.Branch != proof.Worktree.Branch || command.Claim.BaseRef != project.BaseRef || command.Claim.BaseSHA != proof.Worktree.BaseSHA {
		return ErrEvidenceConflict
	}
	providerStored, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, *artifact.ProviderResult)
	if err != nil || parsed.Verify == nil || providerStored.Claim.ID != provider.Claim.ID || providerStored.Claim.Attempt != provider.Claim.Attempt || providerStored.Claim.Ref != provider.Claim.Ref || providerStored.Claim.Phase != provider.Claim.Phase || providerStored.Claim.Role != provider.Claim.Role {
		return ErrEvidenceConflict
	}
	checkpointDigest, err := CanonicalOperatorSourceResumeCheckpointDigest(OperatorSourceResumeCheckpointDigestInput{
		Ref: artifact.Ref, WorktreePath: proof.Worktree.Path, Branch: proof.Worktree.Branch,
		Identity: proof.Worktree.IdentityJSON, BaseSHA: proof.Worktree.BaseSHA, Source: proof.SourceCommit,
		Retained: proof.Verification.Revision, Provider: *artifact.ProviderResult, Command: artifact.CommandResult,
		ResultDigest: command.ResultDigest, Artifact: *parsed.Verify,
	})
	if err != nil || facts.Claim.RequestDigest != checkpointDigest {
		return ErrEvidenceConflict
	}
	if artifact.Checkpoint.CommitOID != facts.PreparedCommitOID || artifact.Checkpoint.ParentOID != proof.SourceCommit.CommitOID || artifact.Checkpoint.TreeOID != facts.PreparedTreeOID {
		return ErrEvidenceConflict
	}
	if s.operatorSourceResumeProviderResultReachesFence(ctx, conn, artifact.Ref, *artifact.ProviderResult, provider, artifact.ExpectedVersion, artifact.Fence) != nil {
		return ErrEvidenceConflict
	}
	return nil
}

// OperatorSourceResumePreparedCandidate authenticates the crash window after
// Builder's Git child G is prepared but before RecordCandidate can append its
// snapshot. The child is accepted only when its intent digest recomputes from
// the exact durable Builder artifact and post-build command bound to fresh F.
func (s *Store) OperatorSourceResumePreparedCandidateWitness(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (OperatorSourceResumePreparedCandidateWitness, bool, error) {
	return s.operatorSourceResumePreparedCandidateWitness(ctx, ref, version, fence, false)
}

// OperatorSourceResumeRecoverablePreparedCandidateWitness is the one
// materializer-only recovery reader that may expose an uncertain prepared G.
// All other Store authority readers use the strict witness above and therefore
// reject an unconfirmed Git mutation.
func (s *Store) OperatorSourceResumeRecoverablePreparedCandidateWitness(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (OperatorSourceResumePreparedCandidateWitness, bool, error) {
	return s.operatorSourceResumePreparedCandidateWitness(ctx, ref, version, fence, true)
}

func (s *Store) operatorSourceResumePreparedCandidateWitness(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, allowUncertain bool) (OperatorSourceResumePreparedCandidateWitness, bool, error) {
	if s == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return OperatorSourceResumePreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK") }()
	return s.operatorSourceResumePreparedCandidateWitnessFrom(ctx, conn, ref, version, fence, allowUncertain)
}

// operatorSourceResumePreparedCandidateWitnessFrom authenticates the complete
// prepared-candidate tuple on the caller's connection. Recovery and current
// readers can therefore use it inside their existing transaction without
// opening s.db again and self-contending on SQLite.
func (s *Store) operatorSourceResumePreparedCandidateWitnessFrom(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence, allowUncertain bool) (OperatorSourceResumePreparedCandidateWitness, bool, error) {
	if s == nil || conn == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return OperatorSourceResumePreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	proof, found, err := s.operatorSourceResumeProofFrom(ctx, conn, ref, version, fence)
	if err != nil || !found {
		if err != nil {
			return OperatorSourceResumePreparedCandidateWitness{}, found, fmt.Errorf("prepared candidate source proof: %w", err)
		}
		return OperatorSourceResumePreparedCandidateWitness{}, false, nil
	}
	if proof.SourceCommit.CommitOID == "" || proof.SourceCommit.ParentOID != proof.Verification.Checkpoint.CommitOID {
		return OperatorSourceResumePreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	fresh, err := s.verificationEvidenceForCandidateFrom(ctx, conn, ref)
	if err != nil || fresh.Revision.Revision <= proof.Verification.Revision.Revision || fresh.Checkpoint.ParentOID != proof.SourceCommit.CommitOID || fresh.Checkpoint.CommitOID == proof.SourceCommit.CommitOID {
		return OperatorSourceResumePreparedCandidateWitness{}, false, fmt.Errorf("prepared candidate fresh verification: %w", ErrEvidenceConflict)
	}
	if proof.Plan.Document.ProviderResult == nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	plannerResult, plannerParsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, *proof.Plan.Document.ProviderResult)
	if err != nil || plannerResult.Claim.Role != "planner" || plannerParsed.Planner == nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	planIdentity, err := workflowprompt.NewPlanIdentity(*plannerParsed.Planner)
	if err != nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, fmt.Errorf("prepared candidate plan identity: %w", ErrEvidenceConflict)
	}
	freshResult, freshParsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, fresh.ProviderResult)
	if err != nil || freshResult.Claim.Role != "reviewer" || freshParsed.Verify == nil || freshParsed.Verify.AcceptanceDigest != planIdentity.Digest {
		return OperatorSourceResumePreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	verificationIdentity, err := workflowprompt.NewVerificationIdentity(*freshParsed.Verify, fresh.Revision.IntentDigest, fresh.Revision.ProofDigest, fresh.Revision.CheckpointID)
	if err != nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	var project Project
	project.Channel, project.ID = ref.Channel, ref.Project
	err = conn.QueryRowContext(ctx, `SELECT p.canonical_path, p.base_ref, p.current_config_generation,
		COALESCE(c.digest, ''), COALESCE(c.snapshot_bytes, X'')
		FROM projects p LEFT JOIN project_configurations c
		ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation
		WHERE p.channel=? AND p.id=?`, ref.Channel, ref.Project).Scan(&project.Path, &project.BaseRef, &project.ConfigGeneration, &project.ConfigDigest, &project.ConfigSnapshot)
	if errors.Is(err, sql.ErrNoRows) {
		return OperatorSourceResumePreparedCandidateWitness{}, false, ErrNotFound
	}
	if err != nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	rows, err := conn.QueryContext(ctx, `SELECT semantic_key FROM git_mutation_intents
		WHERE channel=? AND project_id=? AND ticket_id=? AND operation='commit'
		AND repository_path=? AND worktree_path=? AND branch_ref=? AND base_ref=?
		AND expected_base_oid=? AND expected_head_oid=? AND prepared_commit_oid<>'' AND prepared_tree_oid<>''
		ORDER BY semantic_key`, ref.Channel, ref.Project, ref.Ticket, project.Path, proof.Worktree.Path, proof.Worktree.Branch, project.BaseRef, proof.Worktree.BaseSHA, fresh.Checkpoint.CommitOID)
	if err != nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return OperatorSourceResumePreparedCandidateWitness{}, false, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, err
	}
	if len(keys) == 0 {
		return OperatorSourceResumePreparedCandidateWitness{}, false, nil
	}
	if len(keys) != 1 {
		return OperatorSourceResumePreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	facts, err := gitMutationIntentFactsFrom(ctx, conn, keys[0])
	stateValid := facts.Effect.State == EffectConfirmed || allowUncertain && facts.Effect.State == EffectUncertain
	if err != nil || facts.Claim.Operation != "commit" || facts.Claim.TicketRef != ref || facts.Claim.Repository != project.Path || facts.Claim.Worktree != proof.Worktree.Path || facts.Claim.Branch != proof.Worktree.Branch || facts.Claim.BaseRef != project.BaseRef || facts.Claim.ExpectedBaseOID != proof.Worktree.BaseSHA || facts.Claim.ExpectedHeadOID != fresh.Checkpoint.CommitOID || facts.PreparedCommitOID == "" || facts.PreparedTreeOID == "" || !stateValid {
		return OperatorSourceResumePreparedCandidateWitness{}, false, fmt.Errorf("prepared candidate Git intent: %w", ErrEvidenceConflict)
	}
	builderRows, err := conn.QueryContext(ctx, `SELECT a.id,a.attempt FROM provider_attempts a
		WHERE a.channel=? AND a.project_id=? AND a.ticket_id=? AND a.phase=? AND a.role='builder'
		AND a.state='completed' AND a.outcome='completed' AND a.attempt>0 ORDER BY a.id,a.attempt`, ref.Channel, ref.Project, ref.Ticket, domain.PhaseBuild)
	if err != nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	defer builderRows.Close()
	commandRows, err := conn.QueryContext(ctx, `SELECT semantic_key,claim_epoch FROM repository_command_results
		WHERE channel=? AND project_id=? AND ticket_id=? AND semantic_key LIKE ? ORDER BY semantic_key,claim_epoch`, ref.Channel, ref.Project, ref.Ticket, "repository-command-evidence/"+RepositoryCommandPurposePostbuildCandidate+"/%")
	if err != nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	defer commandRows.Close()
	var commands []RepositoryCommandResult
	for commandRows.Next() {
		var key contracts.RepositoryCommandResultKey
		if err := commandRows.Scan(&key.SemanticKey, &key.ClaimEpoch); err != nil {
			return OperatorSourceResumePreparedCandidateWitness{}, false, err
		}
		result, found, err := loadRepositoryCommandResult(ctx, conn, key, true)
		if err != nil {
			return OperatorSourceResumePreparedCandidateWitness{}, false, ErrEvidenceConflict
		}
		if found {
			commands = append(commands, result)
		}
	}
	if err := commandRows.Err(); err != nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, err
	}
	for builderRows.Next() {
		var attemptID int64
		var attempt int
		if err := builderRows.Scan(&attemptID, &attempt); err != nil {
			return OperatorSourceResumePreparedCandidateWitness{}, false, err
		}
		key := ProviderAttemptResultKey{AttemptID: attemptID, Ref: ref, Phase: domain.PhaseBuild, Attempt: attempt}
		builder, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, key)
		if err != nil || builder.Claim.Role != "builder" || parsed.Builder == nil || builder.Claim.Repository != project.Path || builder.Claim.Worktree != proof.Worktree.Path || builder.Claim.WorktreeIdentity != string(proof.Worktree.IdentityJSON) || builder.Claim.BaseSHA != proof.Worktree.BaseSHA || builder.Claim.ExpectedVersion != facts.Claim.TicketVersion || builder.Claim.LeaderEpoch != facts.Claim.LeaderEpoch || builder.Claim.RunnerEpoch != facts.Claim.RunnerEpoch || s.operatorSourceResumeProviderResultReachesFence(ctx, conn, ref, key, builder, version, fence) != nil {
			continue
		}
		for _, command := range commands {
			if command.Claim.TicketRef != ref || command.Claim.TicketVersion != builder.Claim.ExpectedVersion || command.Claim.LeaderEpoch != builder.Claim.LeaderEpoch || command.Claim.RunnerEpoch != builder.Claim.RunnerEpoch || command.Claim.Repository != project.Path || command.Claim.Worktree != proof.Worktree.Path || command.Claim.WorktreeIdentity != string(proof.Worktree.IdentityJSON) || command.Claim.BaseSHA != proof.Worktree.BaseSHA {
				continue
			}
			snapshot := domain.CandidateSnapshot{BaseSHA: proof.Worktree.BaseSHA, VerificationIntentDigest: fresh.Revision.IntentDigest, ProofDigest: fresh.Revision.ProofDigest, CommandPolicyDigest: strings.TrimPrefix(command.Claim.PolicyDigest, "sha256:")}
			evidence := CandidateEvidence{Ref: ref, Snapshot: snapshot, BuilderResult: key, CommandResult: command.Key}
			if _, _, err := authenticateCandidateCommandEvidence(ctx, conn, evidence, builder, fresh.Revision.IntentDigest, fresh.Revision.ProofDigest, fresh.Revision.CheckpointID); err != nil {
				continue
			}
			digest := CanonicalRepositoryCommitDigest("candidate", ref, facts.Claim.TicketVersion, domain.Fence{LeaderEpoch: facts.Claim.LeaderEpoch, RunnerEpoch: facts.Claim.RunnerEpoch}, proof.Worktree, key, command.Key, command.ResultDigest, struct {
				Plan         workflowprompt.PlanIdentity
				Verification workflowprompt.VerificationIdentity
				Builder      phaseartifact.Builder
			}{planIdentity, verificationIdentity, *parsed.Builder})
			if facts.Claim.RequestDigest == digest {
				return OperatorSourceResumePreparedCandidateWitness{
					Ref:          ref,
					Version:      version,
					Fence:        fence,
					Project:      project,
					Source:       proof,
					Verification: fresh,
					Builder:      key,
					Command:      command,
					Commit:       CommitObservation{CommitOID: facts.PreparedCommitOID, ParentOID: fresh.Checkpoint.CommitOID, TreeOID: facts.PreparedTreeOID},
					Claim:        facts.Claim,
					EffectState:  facts.Effect.State,
				}, true, nil
			}
		}
	}
	if err := builderRows.Err(); err != nil {
		return OperatorSourceResumePreparedCandidateWitness{}, false, err
	}
	return OperatorSourceResumePreparedCandidateWitness{}, false, fmt.Errorf("prepared candidate Builder or command binding: %w", ErrEvidenceConflict)
}

// OperatorSourceResumePreparedCandidate is retained for narrow read-only
// callers. Materialization must use the richer witness above so that G cannot
// be replayed without its authenticated Builder and post-build command.
func (s *Store) OperatorSourceResumePreparedCandidate(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (CommitObservation, bool, error) {
	witness, found, err := s.OperatorSourceResumePreparedCandidateWitness(ctx, ref, version, fence)
	return witness.Commit, found, err
}

// TakeoverRemoteBaseline is the authenticated remote state captured after all
// writers drained and before the ticket entered paused. Resume must observe
// this exact tuple again; an absent candidate branch is distinct from a branch
// whose OID is unknown.
type TakeoverRemoteBaseline struct {
	Registered       bool   `json:"registered"`
	WorktreePath     string `json:"worktree_path,omitempty"`
	WorktreeBranch   string `json:"worktree_branch,omitempty"`
	WorktreeIdentity string `json:"worktree_identity_digest,omitempty"`
	CandidatePresent bool   `json:"candidate_present"`
	CandidateOID     string `json:"candidate_oid,omitempty"`
	BaseOID          string `json:"base_oid,omitempty"`
}

type operatorTakeDrainEvent struct {
	Drained bool                   `json:"drained"`
	Intent  string                 `json:"intent"`
	Remote  TakeoverRemoteBaseline `json:"remote"`
}

type operatorSourceResumeEvent struct {
	Intent                   string                         `json:"intent"`
	Operator                 string                         `json:"operator"`
	ChangeKind               string                         `json:"change_kind"`
	SourceCommit             contracts.OperatorSourceCommit `json:"source_commit"`
	Remote                   TakeoverRemoteBaseline         `json:"remote"`
	CheckpointHead           string                         `json:"checkpoint_head"`
	VerificationRevision     uint64                         `json:"verification_revision"`
	VerificationIntentDigest string                         `json:"verification_intent_digest"`
	VerificationProofDigest  string                         `json:"verification_proof_digest"`
	PlanDigest               string                         `json:"plan_digest"`
	PlanPaths                []string                       `json:"plan_paths"`
	WorktreePath             string                         `json:"worktree_path"`
	WorktreeBranch           string                         `json:"worktree_branch"`
	WorktreeBaseSHA          string                         `json:"worktree_base_sha"`
	WorktreeIdentityDigest   string                         `json:"worktree_identity_digest"`
}

func (s *Store) TransitionOperatorSourceResume(ctx context.Context, request OperatorSourceResume) (TransitionResult, error) {
	if s == nil || s.mutations == nil || request.Ref.Validate() != nil || request.ExpectedVersion == 0 || request.Fence.LeaderEpoch == 0 || request.Fence.RunnerEpoch == 0 || request.Fence.ClaimEpoch != 0 || !boundedText(request.Operator, 300) || !validOperatorSourceCommit(request.SourceCommit) || !validTakeoverRemoteBaseline(request.Remote) {
		return TransitionResult{}, ErrEvidenceConflict
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.mutations.lock(ctx); err != nil {
		return TransitionResult{}, err
	}
	defer s.mutations.unlock()
	var result TransitionResult
	err := s.write(ctx, func(conn *sql.Conn) error {
		proof, err := s.operatorSourceResumePredecessor(ctx, conn, request)
		if err != nil {
			return err
		}
		if request.SourceCommit.ParentOID != proof.Verification.Checkpoint.CommitOID || !sameTakeoverRemoteBaseline(request.Remote, proof.Remote) || !validChangedPathsForSourceProof(operatorSourcePaths(request.SourceCommit), proof.Plan.Document.Paths, proof.Verification.Revision.OwnedFiles) {
			return ErrEvidenceConflict
		}
		payload, err := canonicalOperatorSourceResumeEvent(proof, request.Operator, request.SourceCommit, request.Remote)
		if err != nil {
			return ErrEvidenceConflict
		}
		updated, err := conn.ExecContext(ctx, `UPDATE tickets SET state='verifying',resume_state=NULL,version=version+1
			WHERE channel=? AND project_id=? AND id=? AND state='paused' AND version=? AND runner_epoch=?`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket, request.ExpectedVersion, request.Fence.RunnerEpoch)
		if err != nil {
			return err
		}
		if count, _ := updated.RowsAffected(); count != 1 {
			return ErrStaleFence
		}
		created, err := conn.ExecContext(ctx, `INSERT INTO events(channel,project_id,ticket_id,ticket_version,trigger,from_state,to_state,payload,created_at)
			VALUES(?,?,?,?, 'operator_resume','paused','verifying',?,?)`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket, request.ExpectedVersion+1, string(payload), time.Now().UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		result.Version = request.ExpectedVersion + 1
		result.EventID, _ = created.LastInsertId()
		return nil
	})
	return result, err
}

// OperatorSourceResumeProof returns false when the ticket was not resumed by
// this authority. A malformed durable source-resume event returns an error,
// so Scheduler cannot silently fall through to a direct worker invocation.
func (s *Store) OperatorSourceResumeProof(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence) (OperatorSourceResumeProof, bool, error) {
	if s == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 {
		return OperatorSourceResumeProof{}, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return OperatorSourceResumeProof{}, false, normalizeBusy(ctx, err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return OperatorSourceResumeProof{}, false, normalizeBusy(ctx, err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK") }()
	return s.operatorSourceResumeProofFrom(ctx, conn, ref, version, fence)
}

// OperatorSourceResumeRequiresFreshVerification identifies the first durable
// state after a source-only takeover.  It is intentionally a small optional
// capability: workers use it to suppress historical verification replay while
// the ticket is in the dedicated verifying state.
func (s *Store) OperatorSourceResumeRequiresFreshVerification(ctx context.Context, ref domain.TicketRef, version uint64) (bool, error) {
	if s == nil || ref.Validate() != nil || version == 0 {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, normalizeBusy(ctx, err)
	}
	defer conn.Close()
	var state domain.State
	var runner, leader uint64
	if err := conn.QueryRowContext(ctx, `SELECT t.state,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=? AND t.version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&state, &runner, &leader); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, normalizeBusy(ctx, err)
	}
	if state != domain.StateVerifying {
		return false, nil
	}
	endpoint, found, err := s.operatorSourceResumeEndpointFrom(ctx, conn, ref)
	if err != nil || !found {
		return false, err
	}
	if err := operatorSourceResumeStateLineage(ctx, conn, ref, endpoint, state, version, runner, leader); err != nil {
		return false, ErrEvidenceConflict
	}
	return true, nil
}

// operatorSourceResumeEndpoint is the immutable endpoint created by the
// source-only handoff.  It deliberately remains distinct from the live ticket
// fence: every daemon takeover after this endpoint must be represented by the
// runner-recovery ledger before it can be consumed again.
type operatorSourceResumeEndpoint struct {
	proof   OperatorSourceResumeProof
	event   operatorSourceResumeEvent
	version uint64
	runner  uint64
	leader  uint64
}

// operatorSourceResumeEndpointFrom authenticates the sealed control record
// and the exact building -> stopping -> paused -> verifying source-resume
// lineage.  A normal pause/resume does not have this shape: it resumes to its
// original state, whereas this authority is the sole Building -> Verifying
// handoff.  Once that shape exists, malformed or ambiguous durable evidence is
// fatal rather than an invitation to fall back to ordinary worktree handling.
func (s *Store) operatorSourceResumeEndpointFrom(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) (operatorSourceResumeEndpoint, bool, error) {
	control, err := runtimeControlFrom(ctx, conn, ref)
	if errors.Is(err, ErrStaleFence) {
		return operatorSourceResumeEndpoint{}, false, nil
	}
	if err != nil {
		return operatorSourceResumeEndpoint{}, false, err
	}
	if control.stop.version == 0 || control.stop.runner == 0 || control.stop.leader == 0 || control.stop.version > ^uint64(0)-2 {
		return operatorSourceResumeEndpoint{}, false, nil
	}
	resumeVersion := control.stop.version + 2
	var count, stateChanges int
	var raw string
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0),COALESCE(MAX(payload),'')
		FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?
		AND trigger='operator_resume' AND from_state='paused' AND to_state='verifying'`,
		ref.Channel, ref.Project, ref.Ticket, resumeVersion).Scan(&count, &stateChanges, &raw)
	if err != nil {
		return operatorSourceResumeEndpoint{}, false, err
	}
	if count == 0 {
		return operatorSourceResumeEndpoint{}, false, nil
	}
	if count != 1 || stateChanges != 1 || !s.exactSourceResumeEventSetAt(ctx, conn, ref, resumeVersion) || !exactOperatorTake(ctx, conn, ref, control.stop.version, domain.StateBuilding) || !exactTakeDrain(ctx, conn, ref, control.stop.version+1) {
		return operatorSourceResumeEndpoint{}, false, ErrEvidenceConflict
	}
	event, ok := parseOperatorSourceResumeEvent(raw)
	if !ok {
		return operatorSourceResumeEndpoint{}, false, ErrEvidenceConflict
	}
	// The source endpoint is authenticated by the stop authority.  The
	// runtime-control row may subsequently be carried over a phase transition or
	// recovery, but only through the same bounded ledger used by every other
	// historical result reader.
	if control.authority.leader == 0 || control.state != "sealed" && control.state != "armed" && control.state != "open" {
		return operatorSourceResumeEndpoint{}, false, ErrEvidenceConflict
	}
	// TransitionOperatorSourceResume itself is deliberately Store-only.  Until
	// Controller re-arms the runtime, authority still names the sealed stopping
	// endpoint; the fresh-verification marker must remain visible so Scheduler
	// can select the special admission path.  Once authority moves beyond that
	// stop endpoint it must be the source endpoint or a fully authenticated
	// successor, never an arbitrary counter jump.
	if control.authority != control.stop {
		if control.authority.version < resumeVersion || control.authority.runner < control.stop.runner {
			return operatorSourceResumeEndpoint{}, false, ErrEvidenceConflict
		}
		// A recovery row advances the ticket version while preserving Verifying;
		// version arithmetic alone therefore cannot infer the live lifecycle
		// endpoint. Authenticate the only source-resume suffixes that can carry
		// the original sealed control: verifying, building, or candidate-only
		// publishing before an external publication witness exists.
		source := operatorSourceResumeEndpoint{version: resumeVersion, runner: control.stop.runner, leader: control.stop.leader}
		if verifyingErr := validateOperatorSourceResumeFenceSuffix(ctx, conn, ref, source, domain.StateVerifying, control.authority.version, control.authority.runner, control.authority.leader); verifyingErr != nil {
			if buildingErr := validateOperatorSourceResumeFenceSuffix(ctx, conn, ref, source, domain.StateBuilding, control.authority.version, control.authority.runner, control.authority.leader); buildingErr != nil {
				if publishingErr := validateOperatorSourceResumeFenceSuffix(ctx, conn, ref, source, domain.StatePublishing, control.authority.version, control.authority.runner, control.authority.leader); publishingErr != nil {
					return operatorSourceResumeEndpoint{}, false, ErrEvidenceConflict
				}
			}
		}
	}
	proof, err := s.operatorSourceEvidenceAtRevisionFrom(ctx, conn, ref, event.VerificationRevision)
	if err != nil {
		return operatorSourceResumeEndpoint{}, false, err
	}
	baseline, err := operatorTakeRemoteBaselineFrom(ctx, conn, ref, control.stop.version+1)
	if err != nil {
		return operatorSourceResumeEndpoint{}, false, err
	}
	if !baseline.Registered || baseline.WorktreePath != proof.Worktree.Path || baseline.WorktreeBranch != proof.Worktree.Branch || baseline.WorktreeIdentity != sha256Digest(proof.Worktree.IdentityJSON) || baseline.BaseOID != proof.Worktree.BaseSHA || baseline.CandidatePresent || baseline.CandidateOID != "" {
		return operatorSourceResumeEndpoint{}, false, ErrEvidenceConflict
	}
	proof.Remote = baseline
	if !operatorSourceEventMatchesProof(event, proof) {
		return operatorSourceResumeEndpoint{}, false, ErrEvidenceConflict
	}
	return operatorSourceResumeEndpoint{proof: proof, event: event, version: resumeVersion, runner: control.stop.runner, leader: control.stop.leader}, true, nil
}

// operatorSourceResumeStateLineage proves that the immutable source endpoint
// reaches the requested live ticket only through authenticated phase work and
// contiguous recovery rows.  It is shared by Scheduler's proof and the small
// "fresh verification required" capability so neither can silently forget the
// source-only mode after a daemon restart.
func operatorSourceResumeStateLineage(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, endpoint operatorSourceResumeEndpoint, state domain.State, version, runner, leader uint64) error {
	if version < endpoint.version || runner < endpoint.runner || leader == 0 {
		return ErrEvidenceConflict
	}
	if err := validateRunnerRecoveryLedger(ctx, conn, ref, endpoint.version, endpoint.runner, endpoint.leader, version, runner, leader); err != nil {
		return ErrEvidenceConflict
	}
	rows, err := conn.QueryContext(ctx, `SELECT ticket_version,trigger,from_state,to_state FROM events
		WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND from_state<>to_state
		ORDER BY ticket_version,id`, ref.Channel, ref.Project, ref.Ticket, endpoint.version)
	if err != nil {
		return err
	}
	defer rows.Close()
	type stateChange struct {
		version uint64
		trigger string
		from    domain.State
		to      domain.State
	}
	var changes []stateChange
	for rows.Next() {
		var change stateChange
		if err := rows.Scan(&change.version, &change.trigger, &change.from, &change.to); err != nil {
			return err
		}
		changes = append(changes, change)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	switch state {
	case domain.StateVerifying:
		if len(changes) != 0 {
			return ErrEvidenceConflict
		}
	case domain.StateBuilding:
		// One or more signed recovery rows may preserve Verifying before the
		// fresh review completes. The ledger validator above authenticates those
		// counters and forbids state changes at recovery versions; require the one
		// canonical phase edge anywhere in that bounded suffix.
		if endpoint.version == ^uint64(0) || version < endpoint.version+1 || len(changes) != 1 || changes[0].version <= endpoint.version || changes[0].version > version || changes[0].trigger != "phase_pass" || changes[0].from != domain.StateVerifying || changes[0].to != domain.StateBuilding {
			return ErrEvidenceConflict
		}
	default:
		return ErrEvidenceConflict
	}
	return nil
}

// operatorSourceResumeProviderResultReachesFence is the source-resume-specific
// counterpart to providerResultReachesFence.  The first recovery row after a
// source handoff starts at the sealed source endpoint, not at the ordinary
// ticket bootstrap (runner 1 / the initial lifecycle).  The generic authority
// validator quite deliberately rejects that shape. Do not relax that global
// validator: authenticate this narrow suffix from the immutable source endpoint
// and allow only verifying->building, then candidate-only
// building->publishing phase edges plus signed +1/+1 rows.
func (s *Store) operatorSourceResumeProviderResultReachesFence(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, key ProviderAttemptResultKey, result ProviderAttemptResult, expected uint64, fence domain.Fence) error {
	if key.Ref != result.Claim.Ref || key.Phase != result.Claim.Phase || key.AttemptID != result.Claim.ID || key.Attempt != result.Claim.Attempt || !providerRoleMatchesPhase(result.Claim.Phase, result.Claim.Role) || expected == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 {
		return ErrEvidenceConflict
	}
	var endpoint operatorSourceResumeEndpoint
	var found bool
	var err error
	if conn, ok := q.(*sql.Conn); ok {
		endpoint, found, err = s.operatorSourceResumeEndpointFrom(ctx, conn, ref)
	} else {
		conn, connErr := s.db.Conn(ctx)
		if connErr != nil {
			return normalizeBusy(ctx, connErr)
		}
		defer conn.Close()
		endpoint, found, err = s.operatorSourceResumeEndpointFrom(ctx, conn, ref)
	}
	if err != nil || !found {
		return ErrEvidenceConflict
	}
	if result.Claim.ExpectedVersion < endpoint.version || result.Claim.RunnerEpoch < endpoint.runner || result.Claim.LeaderEpoch < endpoint.leader {
		return ErrEvidenceConflict
	}
	var future int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>?`, ref.Channel, ref.Project, ref.Ticket, expected).Scan(&future); err != nil || future != 0 {
		return ErrEvidenceConflict
	}
	targetState, targetVersion, targetRunner := domain.State(""), uint64(0), uint64(0)
	if err := q.QueryRowContext(ctx, `SELECT state,version,runner_epoch FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&targetState, &targetVersion, &targetRunner); err != nil || targetVersion != expected || targetRunner != fence.RunnerEpoch {
		return ErrEvidenceConflict
	}
	if targetState != domain.StateVerifying && targetState != domain.StateBuilding {
		return ErrEvidenceConflict
	}
	claimState := domain.StateVerifying
	if result.Claim.Phase == domain.PhaseBuild {
		claimState = domain.StateBuilding
	}
	if err := validateOperatorSourceResumeFenceSuffix(ctx, q, ref, endpoint, claimState, result.Claim.ExpectedVersion, result.Claim.RunnerEpoch, result.Claim.LeaderEpoch); err != nil {
		return err
	}
	return validateOperatorSourceResumeFenceSuffix(ctx, q, ref, endpoint, targetState, expected, fence.RunnerEpoch, fence.LeaderEpoch)
}

// operatorSourceResumeCandidateOnlyPublishingRecovery authenticates the one
// source-resume crash window after a recovered Candidate G is recorded at a
// newer Builder binding, then atomically consumed by building -> publishing
// before any external publication witness exists. The Builder claim is older
// than the recovered candidate binding by design; accept that difference only
// through the immutable source endpoint, its exact suffix, and the single
// candidate-only phase edge.
func (s *Store) operatorSourceResumeCandidateOnlyPublishingRecovery(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, candidate StoredCandidate, provider ProviderAttemptResult, expected uint64, fence domain.Fence) error {
	if ref.Validate() != nil || candidate.TicketVersion == 0 || candidate.Fence.LeaderEpoch == 0 || candidate.Fence.RunnerEpoch == 0 || candidate.BuilderResult.Ref != ref || candidate.BuilderResult.Phase != domain.PhaseBuild || candidate.BuilderResult.AttemptID == 0 || candidate.BuilderResult.Attempt == 0 || provider.Claim.Ref != ref || provider.Claim.Phase != domain.PhaseBuild || provider.Claim.Role != "builder" || provider.Claim.ID != candidate.BuilderResult.AttemptID || provider.Claim.Attempt != candidate.BuilderResult.Attempt || provider.Claim.ExpectedVersion == 0 || provider.Claim.LeaderEpoch == 0 || provider.Claim.RunnerEpoch == 0 || expected != candidate.TicketVersion+1 || fence.LeaderEpoch != candidate.Fence.LeaderEpoch || fence.RunnerEpoch != candidate.Fence.RunnerEpoch {
		return ErrEvidenceConflict
	}
	endpoint, found, err := s.operatorSourceResumeEndpointFrom(ctx, conn, ref)
	if err != nil || !found {
		return ErrEvidenceConflict
	}
	if err := validateOperatorSourceResumeFenceSuffix(ctx, conn, ref, endpoint, domain.StateBuilding, provider.Claim.ExpectedVersion, provider.Claim.RunnerEpoch, provider.Claim.LeaderEpoch); err != nil {
		return ErrEvidenceConflict
	}
	if err := validateOperatorSourceResumeFenceSuffix(ctx, conn, ref, endpoint, domain.StateBuilding, candidate.TicketVersion, candidate.Fence.RunnerEpoch, candidate.Fence.LeaderEpoch); err != nil {
		return ErrEvidenceConflict
	}
	if err := validateOperatorSourceResumeFenceSuffix(ctx, conn, ref, endpoint, domain.StatePublishing, expected, fence.RunnerEpoch, fence.LeaderEpoch); err != nil {
		return ErrEvidenceConflict
	}
	return nil
}

// operatorSourceResumeCandidateOnlyPublishingRearm authenticates the narrow
// post-crash rearm shape for a source-resumed, candidate-only publishing
// ticket. It is deliberately not a generic historical-candidate reader: the
// caller must separately prove the exact final recovery from control.authority
// to the live ticket. This helper proves the immutable source endpoint, the
// latest candidate's Builder and command, and the one allowed source suffix
// through Publishing while refusing any external-publication ambiguity.
func (s *Store) operatorSourceResumeCandidateOnlyPublishingRearm(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, control durableRuntimeControl, current Ticket, currentLeader uint64) error {
	if conn == nil || ref.Validate() != nil || control.state != "sealed" || current.Ref != ref || current.State != domain.StatePublishing || currentLeader == 0 ||
		control.authority.version == 0 || control.authority.runner == 0 || control.authority.leader == 0 ||
		control.authority.version == ^uint64(0) || control.authority.runner == ^uint64(0) ||
		current.Version != control.authority.version+1 || current.RunnerEpoch != control.authority.runner+1 || currentLeader <= control.authority.leader {
		return ErrEvidenceConflict
	}
	publication, publicationFound, err := loadPublicationEvidenceRow(ctx, conn, ref)
	if err != nil || publicationFound || publication.Ref != (domain.TicketRef{}) {
		return ErrEvidenceConflict
	}
	var publicationEffects, mergeIntents int
	if err := conn.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND effect_kind NOT IN ('git/create-worktree','git/commit','repository_command')),
		(SELECT COUNT(*) FROM merge_intents WHERE channel=? AND project_id=? AND ticket_id=?)`,
		ref.Channel, ref.Project, ref.Ticket, ref.Channel, ref.Project, ref.Ticket).Scan(&publicationEffects, &mergeIntents); err != nil || publicationEffects != 0 || mergeIntents != 0 {
		return ErrEvidenceConflict
	}
	endpoint, endpointFound, err := s.operatorSourceResumeEndpointFrom(ctx, conn, ref)
	if err != nil || !endpointFound {
		return ErrEvidenceConflict
	}
	candidate, err := s.latestCandidateFrom(ctx, conn, ref, false)
	if err != nil || candidate.TicketVersion == 0 || candidate.TicketVersion+1 != control.authority.version || candidate.Fence.LeaderEpoch != control.authority.leader || candidate.Fence.RunnerEpoch != control.authority.runner || candidate.Snapshot.BaseSHA != endpoint.proof.Worktree.BaseSHA || candidate.Commit.CommitOID != candidate.Snapshot.HeadSHA || candidate.Commit.TreeOID != candidate.Snapshot.TreeSHA {
		return ErrEvidenceConflict
	}
	var ticketSource string
	if err := conn.QueryRowContext(ctx, `SELECT source_digest FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&ticketSource); err != nil || candidate.Snapshot.SourceDigest != ticketSource {
		return ErrEvidenceConflict
	}
	builder, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, candidate.BuilderResult)
	if err != nil || parsed.Builder == nil || builder.Claim.Ref != ref || builder.Claim.Phase != domain.PhaseBuild || builder.Claim.Role != "builder" {
		return ErrEvidenceConflict
	}
	builderDigest, err := phaseartifact.BuilderEvidenceDigest(*parsed.Builder)
	if err != nil || builderDigest != candidate.Snapshot.BuilderEvidenceDigest {
		return ErrEvidenceConflict
	}
	if err := s.operatorSourceResumeCandidateOnlyPublishingRecovery(ctx, conn, ref, candidate, builder, control.authority.version, domain.Fence{LeaderEpoch: control.authority.leader, RunnerEpoch: control.authority.runner}); err != nil {
		return ErrEvidenceConflict
	}
	fresh, err := s.verificationEvidenceForCandidateFrom(ctx, conn, ref)
	if err != nil || fresh.Revision.Revision <= endpoint.proof.Verification.Revision.Revision || fresh.Checkpoint.ParentOID != endpoint.event.SourceCommit.CommitOID || fresh.Checkpoint.CommitOID == endpoint.event.SourceCommit.CommitOID || candidate.Snapshot.VerificationIntentDigest != fresh.Revision.IntentDigest || candidate.Snapshot.ProofDigest != fresh.Revision.ProofDigest || candidate.Commit.ParentOID != fresh.Checkpoint.CommitOID {
		return ErrEvidenceConflict
	}
	_, commandBinding, err := authenticateCandidateCommandEvidence(ctx, conn, CandidateEvidence{
		Ref:             ref,
		ExpectedVersion: candidate.TicketVersion,
		Fence:           candidate.Fence,
		Snapshot:        candidate.Snapshot,
		BuilderResult:   candidate.BuilderResult,
		Commit:          candidate.Commit,
		CommandResult:   candidate.CommandBinding.Key,
	}, builder, fresh.Revision.IntentDigest, fresh.Revision.ProofDigest, fresh.Revision.CheckpointID)
	if err != nil || commandBinding != candidate.CommandBinding {
		return ErrEvidenceConflict
	}
	return nil
}

// validateOperatorSourceResumeFenceSuffix authenticates the bounded source
// handoff suffix. It accepts the source endpoint itself, the ordinary
// verifying->building transition, and then candidate-only
// building->publishing before publication evidence exists. Any number of
// contiguous recovery rows may separate those edges; no other state/control
// transition is admitted here.
func validateOperatorSourceResumeFenceSuffix(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, endpoint operatorSourceResumeEndpoint, state domain.State, version, runner, leader uint64) error {
	if endpoint.version == 0 || endpoint.runner == 0 || endpoint.leader == 0 || version < endpoint.version || runner < endpoint.runner || leader < endpoint.leader {
		return ErrEvidenceConflict
	}
	if state != domain.StateVerifying && state != domain.StateBuilding && state != domain.StatePublishing {
		return ErrEvidenceConflict
	}
	type recovery struct {
		priorVersion, priorRunner, priorLeader uint64
		version, runner, leader                uint64
		digest, created                        string
	}
	rows, err := q.QueryContext(ctx, `SELECT prior_ticket_version,prior_runner_epoch,prior_leader_epoch,ticket_version,runner_epoch,leader_epoch,recovery_digest,created_at FROM runner_recovery_ledger WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? ORDER BY ticket_version`, ref.Channel, ref.Project, ref.Ticket, endpoint.version, version)
	if err != nil {
		return err
	}
	defer rows.Close()
	recoveries := make([]recovery, 0, 8)
	for rows.Next() {
		var value recovery
		if err := rows.Scan(&value.priorVersion, &value.priorRunner, &value.priorLeader, &value.version, &value.runner, &value.leader, &value.digest, &value.created); err != nil {
			return err
		}
		recoveries = append(recoveries, value)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(recoveries) > 64 {
		return ErrEvidenceConflict
	}

	// State-changing events are intentionally read independently from recovery
	// rows so that a phase edge can sit between two signed recovery endpoints.
	type transition struct {
		version  uint64
		trigger  string
		from, to domain.State
	}
	eventRows, err := q.QueryContext(ctx, `SELECT ticket_version,trigger,from_state,to_state FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>? AND ticket_version<=? AND from_state<>to_state ORDER BY ticket_version,id`, ref.Channel, ref.Project, ref.Ticket, endpoint.version, version)
	if err != nil {
		return err
	}
	defer eventRows.Close()
	events := make([]transition, 0, 2)
	for eventRows.Next() {
		var value transition
		if err := eventRows.Scan(&value.version, &value.trigger, &value.from, &value.to); err != nil {
			return err
		}
		events = append(events, value)
	}
	if err := eventRows.Err(); err != nil {
		return err
	}

	// Every recovery row is exact +1/+1 from the previously authenticated
	// endpoint.  A row version itself must contain no state-changing event.
	expectedVersion, expectedRunner, expectedLeader := endpoint.version, endpoint.runner, endpoint.leader
	eventIndex, currentState := 0, domain.StateVerifying
	consumeEventsThrough := func(until uint64) error {
		for eventIndex < len(events) && events[eventIndex].version <= until {
			event := events[eventIndex]
			if event.version != expectedVersion+1 || event.version > version || event.from != currentState || event.trigger != "phase_pass" {
				return ErrEvidenceConflict
			}
			switch {
			case event.from == domain.StateVerifying && event.to == domain.StateBuilding:
			case event.from == domain.StateBuilding && event.to == domain.StatePublishing:
			default:
				return ErrEvidenceConflict
			}
			currentState = event.to
			expectedVersion = event.version
			eventIndex++
		}
		return nil
	}
	for _, value := range recoveries {
		if value.version != value.priorVersion+1 || value.runner != value.priorRunner+1 || value.leader <= value.priorLeader || value.version > version {
			return ErrEvidenceConflict
		}
		if err := consumeEventsThrough(value.priorVersion); err != nil {
			return err
		}
		if value.priorVersion != expectedVersion || value.priorRunner != expectedRunner || value.priorLeader != expectedLeader {
			return ErrEvidenceConflict
		}
		step := RunnerRecoveryLedger{Ref: ref, PriorTicketVersion: value.priorVersion, PriorRunnerEpoch: value.priorRunner, PriorLeaderEpoch: value.priorLeader, TicketVersion: value.version, RunnerEpoch: value.runner, LeaderEpoch: value.leader, RecoveryDigest: value.digest}
		createdAt, parseErr := parseRunnerRecoveryTime(value.created)
		step.CreatedAt = createdAt
		if parseErr != nil || !validRunnerRecovery(step) {
			return ErrEvidenceConflict
		}
		var stateChanges int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND from_state<>to_state`, ref.Channel, ref.Project, ref.Ticket, value.version).Scan(&stateChanges); err != nil || stateChanges != 0 {
			return ErrEvidenceConflict
		}
		expectedVersion, expectedRunner, expectedLeader = value.version, value.runner, value.leader
	}
	if err := consumeEventsThrough(version); err != nil {
		return err
	}
	if eventIndex != len(events) || expectedVersion != version || expectedRunner != runner || expectedLeader != leader || currentState != state {
		return ErrEvidenceConflict
	}
	return nil
}

// operatorSourceResumeRecoveryPredecessor supplies FenceRecoveredRunners with
// the pre-fence leader for the one crash window in which a source-resumed
// ticket is Verifying but no new Reviewer has started yet.  Generic phase and
// worktree fallbacks are intentionally not authority for this mode: only the
// sealed source endpoint and its exact recovery ledger may advance it.
func (s *Store) operatorSourceResumeRecoveryPredecessor(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, state domain.State, version, runner uint64) (uint64, bool, error) {
	if state != domain.StateVerifying || version == 0 || runner == 0 {
		return 0, false, nil
	}
	endpoint, found, err := s.operatorSourceResumeEndpointFrom(ctx, conn, ref)
	if err != nil || !found {
		return 0, found, err
	}
	control, err := runtimeControlFrom(ctx, conn, ref)
	if err != nil || control.state != "sealed" {
		return 0, true, ErrEvidenceConflict
	}
	// A crash immediately after the typed source-resume transition leaves the
	// sealed runtime authority at the stopping endpoint.  The ticket must still
	// be exactly the source-resume endpoint; only that one shape may bootstrap the
	// first +1/+1 recovery row.  Later ticket versions require an explicitly
	// advanced authority or a signed recovery predecessor below.
	if control.authority == control.stop && (version != endpoint.version || runner != endpoint.runner) {
		return 0, true, ErrEvidenceConflict
	}
	if control.authority != control.stop && (control.authority.version < endpoint.version || control.authority.runner < endpoint.runner) {
		return 0, true, ErrEvidenceConflict
	}
	priorLeader := endpoint.leader
	// A rearmed runtime control may already name the exact pre-fence endpoint.
	// Otherwise the latest recovery row is the durable current leader.  There
	// is no inference from daemon_instances here: AcquireLeader has already
	// moved that value to the new leader before this method runs.
	if control.authority.version == version && control.authority.runner == runner {
		priorLeader = control.authority.leader
	} else {
		latest, latestFound, latestErr := loadLatestRunnerRecovery(ctx, conn, ref)
		if latestErr != nil {
			return 0, true, latestErr
		}
		if latestFound {
			if !validRunnerRecovery(latest) || latest.TicketVersion > version || latest.RunnerEpoch > runner {
				return 0, true, ErrEvidenceConflict
			}
			if latest.TicketVersion > endpoint.version {
				priorLeader = latest.LeaderEpoch
			}
		}
	}
	if priorLeader == 0 || priorLeader < endpoint.leader || operatorSourceResumeStateLineage(ctx, conn, ref, endpoint, state, version, runner, priorLeader) != nil {
		return 0, true, ErrEvidenceConflict
	}
	return priorLeader, true, nil
}

func (s *Store) operatorSourceResumePredecessor(ctx context.Context, conn *sql.Conn, request OperatorSourceResume) (OperatorSourceResumeProof, error) {
	var state, resume domain.State
	var version, runner, leader uint64
	if err := conn.QueryRowContext(ctx, `SELECT t.state,COALESCE(t.resume_state,''),t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, request.Ref.Channel, request.Ref.Project, request.Ref.Ticket).Scan(&state, &resume, &version, &runner, &leader); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return OperatorSourceResumeProof{}, ErrNotFound
		}
		return OperatorSourceResumeProof{}, err
	}
	if state != domain.StatePaused || resume != domain.StateBuilding || version != request.ExpectedVersion || runner != request.Fence.RunnerEpoch || leader != request.Fence.LeaderEpoch || version < 2 {
		return OperatorSourceResumeProof{}, ErrStaleFence
	}
	control, err := runtimeControlFrom(ctx, conn, request.Ref)
	if err != nil || control.state != "sealed" || control.stop.version+1 != version || control.stop.runner != runner || control.stop.leader != leader || control.authority != control.stop {
		return OperatorSourceResumeProof{}, ErrControlNotDrained
	}
	if !exactOperatorTake(ctx, conn, request.Ref, version-1, domain.StateBuilding) || !exactTakeDrain(ctx, conn, request.Ref, version) {
		return OperatorSourceResumeProof{}, ErrEvidenceConflict
	}
	return s.operatorSourceEvidenceFrom(ctx, conn, request.Ref)
}

func (s *Store) operatorSourceResumeProofFrom(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence) (OperatorSourceResumeProof, bool, error) {
	var state domain.State
	var currentVersion, currentRunner, currentLeader uint64
	if err := conn.QueryRowContext(ctx, `SELECT t.state,t.version,t.runner_epoch,d.leader_epoch FROM tickets t JOIN daemon_instances d ON d.channel=t.channel WHERE t.channel=? AND t.project_id=? AND t.id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state, &currentVersion, &currentRunner, &currentLeader); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return OperatorSourceResumeProof{}, false, ErrNotFound
		}
		return OperatorSourceResumeProof{}, false, err
	}
	if (state != domain.StateVerifying && state != domain.StateBuilding) || currentVersion != version || currentRunner != fence.RunnerEpoch || currentLeader != fence.LeaderEpoch || version < 3 {
		return OperatorSourceResumeProof{}, false, nil
	}
	endpoint, found, err := s.operatorSourceResumeEndpointFrom(ctx, conn, ref)
	if err != nil || !found {
		return OperatorSourceResumeProof{}, found, err
	}
	control, controlErr := runtimeControlFrom(ctx, conn, ref)
	if controlErr != nil || control.authority == control.stop || control.authority.version < endpoint.version || control.authority.runner < endpoint.runner {
		return OperatorSourceResumeProof{}, false, nil
	}
	if err := operatorSourceResumeStateLineage(ctx, conn, ref, endpoint, state, version, fence.RunnerEpoch, fence.LeaderEpoch); err != nil {
		return OperatorSourceResumeProof{}, false, ErrEvidenceConflict
	}
	if state == domain.StateBuilding {
		// CurrentVerification intentionally refuses historical rows after a
		// runner recovery.  The source-resume reader needs that immutable fresh
		// reviewer binding, then proves its path to the live fence explicitly.
		fresh, freshErr := s.verificationEvidenceAtRevisionFrom(ctx, conn, ref, 0)
		if freshErr != nil || fresh.Revision.Revision <= endpoint.proof.Verification.Revision.Revision || fresh.TicketVersion < endpoint.version || fresh.Fence.RunnerEpoch < endpoint.runner || fresh.Fence.LeaderEpoch == 0 || fresh.Checkpoint.ParentOID != endpoint.event.SourceCommit.CommitOID || fresh.Checkpoint.CommitOID == endpoint.event.SourceCommit.CommitOID {
			return OperatorSourceResumeProof{}, false, ErrEvidenceConflict
		}
		if err := validateRunnerRecoveryLedgerPrefix(ctx, conn, ref, endpoint.version, endpoint.runner, endpoint.leader, fresh.TicketVersion, fresh.Fence.RunnerEpoch, fresh.Fence.LeaderEpoch); err != nil {
			return OperatorSourceResumeProof{}, false, ErrEvidenceConflict
		}
		if err := validateRunnerRecoveryLedger(ctx, conn, ref, fresh.TicketVersion, fresh.Fence.RunnerEpoch, fresh.Fence.LeaderEpoch, version, fence.RunnerEpoch, fence.LeaderEpoch); err != nil {
			return OperatorSourceResumeProof{}, false, ErrEvidenceConflict
		}
	}
	proof := endpoint.proof
	proof.Version, proof.Fence, proof.Operator, proof.SourceCommit, proof.Remote = version, fence, endpoint.event.Operator, cloneOperatorSourceCommit(endpoint.event.SourceCommit), endpoint.event.Remote
	return proof, true, nil
}

func (s *Store) operatorSourceEvidenceFrom(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) (OperatorSourceResumeProof, error) {
	proof, err := s.operatorSourceEvidenceAtRevisionFrom(ctx, conn, ref, 0)
	if err != nil {
		return OperatorSourceResumeProof{}, err
	}
	var version uint64
	if err := conn.QueryRowContext(ctx, `SELECT version FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&version); err != nil || version == 0 {
		return OperatorSourceResumeProof{}, ErrEvidenceConflict
	}
	baseline, err := operatorTakeRemoteBaselineFrom(ctx, conn, ref, version)
	if err != nil {
		return OperatorSourceResumeProof{}, err
	}
	if !baseline.Registered || baseline.WorktreePath != proof.Worktree.Path || baseline.WorktreeBranch != proof.Worktree.Branch || baseline.WorktreeIdentity != sha256Digest(proof.Worktree.IdentityJSON) || baseline.BaseOID != proof.Worktree.BaseSHA || baseline.CandidatePresent || baseline.CandidateOID != "" {
		return OperatorSourceResumeProof{}, ErrEvidenceConflict
	}
	proof.Remote = baseline
	return proof, nil
}

func (s *Store) operatorSourceEvidenceAtRevisionFrom(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, revision uint64) (OperatorSourceResumeProof, error) {
	worktree, project, err := operatorSourceWorktreeFrom(ctx, conn, ref)
	if err != nil {
		return OperatorSourceResumeProof{}, err
	}
	verification, err := s.verificationEvidenceAtRevisionFrom(ctx, conn, ref, revision)
	if err != nil || verification.Checkpoint.CommitOID == "" || verification.Checkpoint.CommitOID != verification.Revision.CheckpointID {
		return OperatorSourceResumeProof{}, ErrEvidenceConflict
	}
	plan, err := operatorSourcePlanFrom(ctx, conn, ref)
	if err != nil || !validRepositoryWorktreeIdentity(string(worktree.IdentityJSON), project.Path, worktree.Path, worktree.Branch, project.BaseRef, worktree.BaseSHA) {
		return OperatorSourceResumeProof{}, ErrEvidenceConflict
	}
	return OperatorSourceResumeProof{Ref: ref, Worktree: worktree, Verification: verification, Plan: plan}, nil
}

// verificationEvidenceAtRevisionFrom is the historical counterpart of the
// current verification reader. Source takeover needs the old checkpoint to
// authenticate the dirty checkout even after the fresh reviewer has replaced
// the current verification projection.
func (s *Store) verificationEvidenceAtRevisionFrom(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, revision uint64) (StoredVerification, error) {
	return s.verificationEvidenceAtRevisionBindingFrom(ctx, q, ref, revision, 0)
}

// verificationEvidenceAtRevisionBindingFrom authenticates the exact historical
// result binding which emitted an evidence projection. A later recovery may
// append a newer binding for the same immutable revision; that must not either
// invalidate the old event or let the old event borrow the newer fence.
func (s *Store) verificationEvidenceAtRevisionBindingFrom(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, revision, bindingVersion uint64) (StoredVerification, error) {
	var stored StoredVerification
	var owned string
	query := `SELECT r.revision,r.intent_digest,r.intent_bytes,r.proof_digest,r.proof_bytes,r.owned_files_json,r.checkpoint_id FROM verification_revisions r WHERE r.channel=? AND r.project_id=? AND r.ticket_id=?`
	args := []any{ref.Channel, ref.Project, ref.Ticket}
	if revision != 0 {
		query += ` AND r.revision=?`
		args = append(args, revision)
	} else {
		query = `SELECT r.revision,r.intent_digest,r.intent_bytes,r.proof_digest,r.proof_bytes,r.owned_files_json,r.checkpoint_id FROM verifications v JOIN verification_revisions r ON r.channel=v.channel AND r.project_id=v.project_id AND r.ticket_id=v.ticket_id AND r.revision=v.current_revision WHERE v.channel=? AND v.project_id=? AND v.ticket_id=? AND v.intent_digest=r.intent_digest AND v.proof_digest=r.proof_digest`
	}
	if err := q.QueryRowContext(ctx, query, args...).Scan(&stored.Revision.Revision, &stored.Revision.IntentDigest, &stored.Intent, &stored.Revision.ProofDigest, &stored.Proof, &owned, &stored.Revision.CheckpointID); err != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	if stored.Revision.Revision == 0 || sha256Digest(stored.Intent) != stored.Revision.IntentDigest || sha256Digest(stored.Proof) != stored.Revision.ProofDigest || json.Unmarshal([]byte(owned), &stored.Revision.OwnedFiles) != nil || validOwnedFiles(stored.Revision.OwnedFiles) != nil || !validOID(stored.Revision.CheckpointID) {
		return StoredVerification{}, ErrEvidenceConflict
	}
	bindingQuery := `SELECT binding_ticket_version,leader_epoch,runner_epoch,provider_attempt_id,provider_attempt,checkpoint_commit_oid,checkpoint_parent_oid,checkpoint_tree_oid FROM verification_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND revision=?`
	bindingArgs := []any{ref.Channel, ref.Project, ref.Ticket, stored.Revision.Revision}
	if bindingVersion != 0 {
		var count int
		if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM verification_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND revision=? AND binding_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, stored.Revision.Revision, bindingVersion).Scan(&count); err != nil || count != 1 {
			return StoredVerification{}, ErrEvidenceConflict
		}
		bindingQuery += ` AND binding_ticket_version=?`
		bindingArgs = append(bindingArgs, bindingVersion)
	} else {
		bindingQuery += ` ORDER BY binding_ticket_version DESC,leader_epoch DESC,runner_epoch DESC LIMIT 1`
	}
	if err := q.QueryRowContext(ctx, bindingQuery, bindingArgs...).Scan(&stored.TicketVersion, &stored.Fence.LeaderEpoch, &stored.Fence.RunnerEpoch, &stored.ProviderResult.AttemptID, &stored.ProviderResult.Attempt, &stored.Checkpoint.CommitOID, &stored.Checkpoint.ParentOID, &stored.Checkpoint.TreeOID); err != nil || stored.TicketVersion == 0 || stored.Fence.LeaderEpoch == 0 || stored.Fence.RunnerEpoch == 0 || stored.ProviderResult.AttemptID == 0 || stored.ProviderResult.Attempt <= 0 || stored.Checkpoint.CommitOID != stored.Revision.CheckpointID || !validOID(stored.Checkpoint.ParentOID) || !validOID(stored.Checkpoint.TreeOID) {
		return StoredVerification{}, ErrEvidenceConflict
	}
	stored.ProviderResult.Ref, stored.ProviderResult.Phase = ref, domain.PhaseVerification
	binding, err := loadVerificationCommandBinding(ctx, q, ref, stored.Revision.Revision)
	if err != nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	stored.CommandBinding = binding
	if bindingVersion != 0 && (stored.TicketVersion != bindingVersion || binding.TicketVersion != bindingVersion || stored.Fence != (domain.Fence{LeaderEpoch: binding.LeaderEpoch, RunnerEpoch: binding.RunnerEpoch})) {
		return StoredVerification{}, ErrEvidenceConflict
	}
	provider, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, q, stored.ProviderResult)
	if err != nil || provider.Claim.Ref != ref || provider.Claim.Phase != domain.PhaseVerification || provider.Claim.Role != "reviewer" || parsed.Verify == nil {
		return StoredVerification{}, ErrEvidenceConflict
	}
	intent, intentErr := workflowprompt.CanonicalVerificationIntentBytes(*parsed.Verify)
	proof, proofErr := workflowprompt.CanonicalVerificationProofBytes(*parsed.Verify)
	if intentErr != nil || proofErr != nil || sha256Digest(intent) != stored.Revision.IntentDigest || sha256Digest(proof) != stored.Revision.ProofDigest || binding.ExpectedOutcome != parsed.Verify.PrebuildOutcome {
		return StoredVerification{}, ErrEvidenceConflict
	}
	return stored, nil
}

func operatorSourceWorktreeFrom(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) (StoredWorktree, Project, error) {
	var worktree StoredWorktree
	var project Project
	err := conn.QueryRowContext(ctx, `SELECT w.path,w.branch_ref,w.state,w.identity_json,w.base_sha,w.head_sha,w.ticket_version,w.leader_epoch,w.runner_epoch,p.canonical_path,p.base_ref FROM worktrees w JOIN projects p ON p.channel=w.channel AND p.id=w.project_id WHERE w.channel=? AND w.project_id=? AND w.ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&worktree.Path, &worktree.Branch, &worktree.State, &worktree.IdentityJSON, &worktree.BaseSHA, &worktree.HeadSHA, &worktree.TicketVersion, &worktree.Fence.LeaderEpoch, &worktree.Fence.RunnerEpoch, &project.Path, &project.BaseRef)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredWorktree{}, Project{}, ErrNotFound
	}
	if err != nil || worktree.State != "registered" || !validStorePath(worktree.Path) || !boundedText(worktree.Branch, 300) || !validOID(worktree.BaseSHA) || !validOID(worktree.HeadSHA) || worktree.TicketVersion == 0 || worktree.Fence.LeaderEpoch == 0 || worktree.Fence.RunnerEpoch == 0 || !validStorePath(project.Path) || !boundedText(project.BaseRef, 300) {
		return StoredWorktree{}, Project{}, ErrEvidenceConflict
	}
	return worktree, project, nil
}

func operatorSourcePlanFrom(ctx context.Context, conn *sql.Conn, ref domain.TicketRef) (StoredPlan, error) {
	var result StoredPlan
	var body, created string
	var artifact []byte
	err := conn.QueryRowContext(ctx, `SELECT p.digest,p.body,p.artifact_bytes,COALESCE(b.binding_ticket_version,p.ticket_version),COALESCE(b.leader_epoch,p.leader_epoch),COALESCE(b.runner_epoch,p.runner_epoch),p.created_at FROM plans p LEFT JOIN plan_result_bindings b ON b.rowid=(SELECT latest.rowid FROM plan_result_bindings latest WHERE latest.channel=p.channel AND latest.project_id=p.project_id AND latest.ticket_id=p.ticket_id AND latest.plan_digest=p.digest ORDER BY latest.binding_ticket_version DESC,latest.leader_epoch DESC,latest.runner_epoch DESC LIMIT 1) WHERE p.channel=? AND p.project_id=? AND p.ticket_id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&result.Digest, &body, &artifact, &result.TicketVersion, &result.Fence.LeaderEpoch, &result.Fence.RunnerEpoch, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredPlan{}, ErrNotFound
	}
	if err != nil || len(artifact) == 0 || !bytes.Equal(artifact, []byte(body)) || sha256Digest(artifact) != result.Digest || json.Unmarshal(artifact, &result.Document) != nil {
		return StoredPlan{}, ErrEvidenceConflict
	}
	canonical, err := validatePlanDocument(result.Document)
	if err != nil || !bytes.Equal(canonical, artifact) || result.TicketVersion == 0 || result.Fence.LeaderEpoch == 0 || result.Fence.RunnerEpoch == 0 {
		return StoredPlan{}, ErrEvidenceConflict
	}
	result.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return StoredPlan{}, ErrEvidenceConflict
	}
	return result, nil
}

func canonicalOperatorSourceResumeEvent(proof OperatorSourceResumeProof, operator string, source contracts.OperatorSourceCommit, remote TakeoverRemoteBaseline) ([]byte, error) {
	value := operatorSourceResumeEvent{Intent: "resume", Operator: operator, ChangeKind: "source_commit", SourceCommit: cloneOperatorSourceCommit(source), Remote: remote, CheckpointHead: proof.Verification.Checkpoint.CommitOID, VerificationRevision: proof.Verification.Revision.Revision, VerificationIntentDigest: proof.Verification.Revision.IntentDigest, VerificationProofDigest: proof.Verification.Revision.ProofDigest, PlanDigest: proof.Plan.Digest, PlanPaths: append([]string(nil), proof.Plan.Document.Paths...), WorktreePath: proof.Worktree.Path, WorktreeBranch: proof.Worktree.Branch, WorktreeBaseSHA: proof.Worktree.BaseSHA, WorktreeIdentityDigest: sha256Digest(proof.Worktree.IdentityJSON)}
	if !validOperatorSourceEvent(value) || !validChangedPathsForSourceProof(operatorSourcePaths(source), proof.Plan.Document.Paths, proof.Verification.Revision.OwnedFiles) || !sameTakeoverRemoteBaseline(remote, proof.Remote) {
		return nil, ErrEvidenceConflict
	}
	return json.Marshal(value)
}

func parseOperatorSourceResumeEvent(raw string) (operatorSourceResumeEvent, bool) {
	var value operatorSourceResumeEvent
	if !validJSON([]byte(raw)) || json.Unmarshal([]byte(raw), &value) != nil || !validOperatorSourceEvent(value) {
		return operatorSourceResumeEvent{}, false
	}
	canonical, err := json.Marshal(value)
	return value, err == nil && string(canonical) == raw
}

func validOperatorSourceEvent(value operatorSourceResumeEvent) bool {
	return value.Intent == "resume" && boundedText(value.Operator, 300) && value.ChangeKind == "source_commit" && validOperatorSourceCommit(value.SourceCommit) && value.SourceCommit.ParentOID == value.CheckpointHead && validTakeoverRemoteBaseline(value.Remote) && validOID(value.CheckpointHead) && value.VerificationRevision != 0 && validDigest(value.VerificationIntentDigest) && validDigest(value.VerificationProofDigest) && validDigest(value.PlanDigest) && validPlanPaths(value.PlanPaths) && validStorePath(value.WorktreePath) && boundedText(value.WorktreeBranch, 300) && validOID(value.WorktreeBaseSHA) && validDigest(value.WorktreeIdentityDigest)
}

func operatorSourceEventMatchesProof(event operatorSourceResumeEvent, proof OperatorSourceResumeProof) bool {
	return event.CheckpointHead == proof.Verification.Checkpoint.CommitOID && event.VerificationRevision == proof.Verification.Revision.Revision && event.VerificationIntentDigest == proof.Verification.Revision.IntentDigest && event.VerificationProofDigest == proof.Verification.Revision.ProofDigest && event.PlanDigest == proof.Plan.Digest && equalStringSlices(event.PlanPaths, proof.Plan.Document.Paths) && event.WorktreePath == proof.Worktree.Path && event.WorktreeBranch == proof.Worktree.Branch && event.WorktreeBaseSHA == proof.Worktree.BaseSHA && event.WorktreeIdentityDigest == sha256Digest(proof.Worktree.IdentityJSON) && validChangedPathsForSourceProof(operatorSourcePaths(event.SourceCommit), proof.Plan.Document.Paths, proof.Verification.Revision.OwnedFiles) && sameTakeoverRemoteBaseline(event.Remote, proof.Remote)
}

func validOperatorSourceCommit(value contracts.OperatorSourceCommit) bool {
	if !validOID(value.CommitOID) || !validOID(value.ParentOID) || !validOID(value.TreeOID) || value.CommitOID == value.ParentOID || len(value.Changes) == 0 || len(value.Changes) > 256 {
		return false
	}
	previous := ""
	for _, change := range value.Changes {
		if change.Status != "A" && change.Status != "M" && change.Status != "D" {
			return false
		}
		if !validOperatorSourceChangedFiles([]string{change.Path}) || (previous != "" && previous >= change.Path) {
			return false
		}
		previous = change.Path
	}
	return true
}

func operatorSourcePaths(value contracts.OperatorSourceCommit) []string {
	paths := make([]string, 0, len(value.Changes))
	for _, change := range value.Changes {
		paths = append(paths, change.Path)
	}
	return paths
}

func cloneOperatorSourceCommit(value contracts.OperatorSourceCommit) contracts.OperatorSourceCommit {
	value.Changes = append([]contracts.OperatorSourceChange(nil), value.Changes...)
	return value
}

func validTakeoverRemoteBaseline(value TakeoverRemoteBaseline) bool {
	if !value.Registered {
		return value.WorktreePath == "" && value.WorktreeBranch == "" && value.WorktreeIdentity == "" && !value.CandidatePresent && value.CandidateOID == "" && value.BaseOID == ""
	}
	if !validStorePath(value.WorktreePath) || !boundedText(value.WorktreeBranch, 300) || !validDigest(value.WorktreeIdentity) || !validOID(value.BaseOID) {
		return false
	}
	return (value.CandidatePresent && validOID(value.CandidateOID)) || (!value.CandidatePresent && value.CandidateOID == "")
}

func sameTakeoverRemoteBaseline(left, right TakeoverRemoteBaseline) bool { return left == right }

func validOperatorSourceChangedFiles(paths []string) bool {
	if len(paths) == 0 || len(paths) > 256 || !sort.StringsAreSorted(paths) {
		return false
	}
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if !boundedText(path, 4096) || path == "." || filepath.IsAbs(path) || filepath.Clean(path) != path || !filepath.IsLocal(path) {
			return false
		}
		if _, duplicate := seen[path]; duplicate {
			return false
		}
		seen[path] = struct{}{}
	}
	return true
}

func validPlanPaths(paths []string) bool {
	if len(paths) == 0 || len(paths) > 256 {
		return false
	}
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		// Plan paths are repository-relative lexical prefixes. Reject traversal
		// components and noncanonical spellings, but do not reject a legitimate
		// filename such as foo..bar merely for containing two dots.
		if !boundedText(path, 2_000) || filepath.IsAbs(path) || filepath.Clean(path) != path || !filepath.IsLocal(path) {
			return false
		}
		if _, duplicate := seen[path]; duplicate {
			return false
		}
		seen[path] = struct{}{}
	}
	return true
}

func validChangedPathsForSourceProof(changed, allowed, owned []string) bool {
	if !validOperatorSourceChangedFiles(changed) || !validPlanPaths(allowed) || validOwnedFiles(owned) != nil {
		return false
	}
	for _, path := range changed {
		inside := false
		for _, prefix := range allowed {
			if sourceResumePathMatches(path, prefix) {
				inside = true
				break
			}
		}
		if !inside {
			return false
		}
		for _, protected := range owned {
			if sourceResumePathMatches(path, protected) || sourceResumePathMatches(protected, path) {
				return false
			}
		}
	}
	return true
}

func sourceResumePathMatches(path, prefix string) bool {
	prefix = strings.Trim(prefix, "/")
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func exactStateChangeAt(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64) bool {
	var total, stateChanges int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0) FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&total, &stateChanges); err != nil {
		return false
	}
	return total == 1 && stateChanges == 1
}

// A source-resume endpoint remains the same control authority after its fresh
// verification is durably recorded at that version. The projection is a
// same-state evidence event, not a second control transition. Admit at most
// that one Store-owned projection while rejecting every other extra event.
type sourceResumeVerificationProjection struct {
	CheckpointCommit              string       `json:"checkpoint_commit"`
	CheckpointParent              string       `json:"checkpoint_parent"`
	CheckpointTree                string       `json:"checkpoint_tree"`
	IntentDigest                  string       `json:"intent_digest"`
	PrebuildOutcome               string       `json:"prebuild_outcome"`
	ProofDigest                   string       `json:"proof_digest"`
	ProviderAttempt               int          `json:"provider_attempt"`
	ProviderAttemptID             int64        `json:"provider_attempt_id"`
	ProviderPhase                 domain.Phase `json:"provider_phase"`
	RepositoryCommandClaimEpoch   uint64       `json:"repository_command_claim_epoch"`
	RepositoryCommandPolicyDigest string       `json:"repository_command_policy_digest"`
	RepositoryCommandSemanticKey  string       `json:"repository_command_semantic_key"`
	Revision                      uint64       `json:"revision"`
}

func (s *Store) exactSourceResumeEventSetAt(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64) bool {
	var total, stateChanges, verificationProjections int
	var raw string
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN from_state<>to_state THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN trigger='verification_recorded' AND from_state='verifying' AND to_state='verifying' THEN 1 ELSE 0 END),0),
		COALESCE(MAX(CASE WHEN trigger='verification_recorded' AND from_state='verifying' AND to_state='verifying' THEN payload ELSE '' END),'')
		FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&total, &stateChanges, &verificationProjections, &raw); err != nil {
		return false
	}
	if stateChanges != 1 || verificationProjections > 1 || total != stateChanges+verificationProjections {
		return false
	}
	if verificationProjections == 0 {
		return true
	}
	var projection sourceResumeVerificationProjection
	if !validJSON([]byte(raw)) || json.Unmarshal([]byte(raw), &projection) != nil {
		return false
	}
	canonical, err := json.Marshal(projection)
	if err != nil || !bytes.Equal(canonical, []byte(raw)) {
		return false
	}
	verification, err := s.verificationEvidenceAtRevisionBindingFrom(ctx, conn, ref, projection.Revision, version)
	if err != nil {
		return false
	}
	return projection.Revision == verification.Revision.Revision &&
		projection.IntentDigest == verification.Revision.IntentDigest &&
		projection.ProofDigest == verification.Revision.ProofDigest &&
		projection.ProviderAttemptID == verification.ProviderResult.AttemptID &&
		projection.ProviderAttempt == verification.ProviderResult.Attempt &&
		projection.ProviderPhase == verification.ProviderResult.Phase &&
		projection.CheckpointCommit == verification.Checkpoint.CommitOID &&
		projection.CheckpointParent == verification.Checkpoint.ParentOID &&
		projection.CheckpointTree == verification.Checkpoint.TreeOID &&
		projection.RepositoryCommandSemanticKey == verification.CommandBinding.Key.SemanticKey &&
		projection.RepositoryCommandClaimEpoch == verification.CommandBinding.Key.ClaimEpoch &&
		projection.RepositoryCommandPolicyDigest == verification.CommandBinding.PolicyDigest &&
		projection.PrebuildOutcome == verification.CommandBinding.ExpectedOutcome
}

func exactOperatorTake(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, from domain.State) bool {
	var payload string
	var matches int
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(payload),'') FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='operator_pause_or_take' AND from_state=? AND to_state='stopping'`, ref.Channel, ref.Project, ref.Ticket, version, from).Scan(&matches, &payload)
	return err == nil && matches == 1 && exactStateChangeAt(ctx, conn, ref, version) && validOperatorTakePayload(payload)
}

func exactTakeDrain(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64) bool {
	var payload string
	var matches int
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(payload),'') FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='process_and_effects_drained' AND from_state='stopping' AND to_state='paused'`, ref.Channel, ref.Project, ref.Ticket, version).Scan(&matches, &payload)
	return err == nil && matches == 1 && exactStateChangeAt(ctx, conn, ref, version) && validTakeDrainPayload(payload)
}

// OperatorTakeRemoteBaseline returns the exact remote observation sealed into
// the take drain event. It is read-only recovery evidence, not an observation
// of current Git state.
func (s *Store) OperatorTakeRemoteBaseline(ctx context.Context, ref domain.TicketRef, pausedVersion uint64) (TakeoverRemoteBaseline, error) {
	if s == nil || ref.Validate() != nil || pausedVersion == 0 {
		return TakeoverRemoteBaseline{}, ErrEvidenceConflict
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return TakeoverRemoteBaseline{}, normalizeBusy(ctx, err)
	}
	defer conn.Close()
	return operatorTakeRemoteBaselineFrom(ctx, conn, ref, pausedVersion)
}

func operatorTakeRemoteBaselineFrom(ctx context.Context, q candidateEvidenceQuerier, ref domain.TicketRef, pausedVersion uint64) (TakeoverRemoteBaseline, error) {
	var raw string
	var count int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(payload),'') FROM events WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version=? AND trigger='process_and_effects_drained' AND from_state='stopping' AND to_state='paused'`, ref.Channel, ref.Project, ref.Ticket, pausedVersion).Scan(&count, &raw); err != nil || count != 1 {
		return TakeoverRemoteBaseline{}, ErrEvidenceConflict
	}
	var event operatorTakeDrainEvent
	if !canonicalOperatorJSON(raw, &event) || !event.Drained || (event.Intent != "take" && event.Intent != "pause") || !validTakeoverRemoteBaseline(event.Remote) {
		return TakeoverRemoteBaseline{}, ErrEvidenceConflict
	}
	return event.Remote, nil
}

func validOperatorTakePayload(raw string) bool {
	var value struct {
		Intent      string `json:"intent"`
		Operator    string `json:"operator"`
		OperatorUID uint32 `json:"operator_uid"`
	}
	return canonicalOperatorJSON(raw, &value) && value.Intent == "take" && boundedText(value.Operator, 300) && value.OperatorUID != 0
}

func validTakeDrainPayload(raw string) bool {
	var value operatorTakeDrainEvent
	return canonicalOperatorJSON(raw, &value) && value.Drained && value.Intent == "take" && validTakeoverRemoteBaseline(value.Remote)
}

func canonicalOperatorJSON(raw string, target any) bool {
	if !validJSON([]byte(raw)) || json.Unmarshal([]byte(raw), target) != nil {
		return false
	}
	canonical, err := json.Marshal(target)
	return err == nil && string(canonical) == raw
}
