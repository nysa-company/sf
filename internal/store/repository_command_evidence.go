package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
)

const (
	RepositoryCommandPurposePrebuildVerification = "prebuild-verification"
	RepositoryCommandPurposePostbuildCandidate   = "postbuild-candidate"
)

// RepositoryCommandEvidenceRequest is the Store-owned canonical purpose
// binding a later runtime must use when it plans a guarded repository command.
// A result can serve only the exact provider attempt and artifact named here.
type RepositoryCommandEvidenceRequest struct {
	Purpose                                                    string
	Ref                                                        domain.TicketRef
	TicketVersion, LeaderEpoch, RunnerEpoch                    uint64
	ProviderResult                                             ProviderAttemptResultKey
	VerificationIntentDigest, ProofDigest, CheckpointID        string
	ConfigCommandDigest                                        string
	Worktree, WorktreeIdentity, BaseSHA                        string
	PolicyDigest, SpecDigest, ExecutablePath, ExecutableDigest string
}

// CanonicalRepositoryCommandEvidenceRequest returns the exact digest used as
// RepositoryCommandClaim.RequestDigest. Later runtime composition must use
// this helper; Store independently recomputes it at each consuming write.
func CanonicalRepositoryCommandEvidenceRequest(value RepositoryCommandEvidenceRequest) ([]byte, string, error) {
	if (value.Purpose != RepositoryCommandPurposePrebuildVerification && value.Purpose != RepositoryCommandPurposePostbuildCandidate) || value.Ref.Validate() != nil || value.TicketVersion == 0 || value.LeaderEpoch == 0 || value.RunnerEpoch == 0 || value.ProviderResult.AttemptID <= 0 || value.ProviderResult.Ref != value.Ref || value.ProviderResult.Attempt <= 0 || (value.Purpose == RepositoryCommandPurposePrebuildVerification && (value.ProviderResult.Phase != domain.PhaseVerification || value.CheckpointID != "")) || (value.Purpose == RepositoryCommandPurposePostbuildCandidate && (value.ProviderResult.Phase != domain.PhaseBuild || !validOID(value.CheckpointID))) || !validDigest(value.VerificationIntentDigest) || !validDigest(value.ProofDigest) || !validClaimDigest(value.ConfigCommandDigest) || !validStorePath(value.Worktree) || value.WorktreeIdentity == "" || !validOID(value.BaseSHA) || !validClaimDigest(value.PolicyDigest) || !validClaimDigest(value.SpecDigest) || !validStorePath(value.ExecutablePath) || !validRepositoryExecutableDigest(value.ExecutablePath, value.ExecutableDigest) {
		return nil, "", ErrEvidenceConflict
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, "", err
	}
	return payload, repositoryResultDigest(payload), nil
}

// RepositoryCommandEvidenceSemanticKey derives the semantic idempotency key
// from the same canonical request. It prevents cross-attempt result reuse.
func RepositoryCommandEvidenceSemanticKey(value RepositoryCommandEvidenceRequest) (string, error) {
	_, digest, err := CanonicalRepositoryCommandEvidenceRequest(value)
	if err != nil {
		return "", err
	}
	return "repository-command-evidence/" + value.Purpose + "/" + digest[len("sha256:"):], nil
}

func commandEvidenceRequest(purpose string, ref domain.TicketRef, expected uint64, fence domain.Fence, provider ProviderAttemptResultKey, intent, proof, checkpoint, configCommand string, result RepositoryCommandResult) RepositoryCommandEvidenceRequest {
	return RepositoryCommandEvidenceRequest{
		Purpose: purpose, Ref: ref, TicketVersion: expected, LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ProviderResult: provider,
		VerificationIntentDigest: intent, ProofDigest: proof, CheckpointID: checkpoint, ConfigCommandDigest: configCommand,
		Worktree: result.Claim.Worktree, WorktreeIdentity: result.Claim.WorktreeIdentity, BaseSHA: result.Claim.BaseSHA,
		PolicyDigest: result.Claim.PolicyDigest, SpecDigest: result.Claim.SpecDigest, ExecutablePath: result.Claim.ExecutablePath, ExecutableDigest: result.Claim.ExecutableDigest,
	}
}

func assertCommandEvidenceRequest(value RepositoryCommandEvidenceRequest, result RepositoryCommandResult) error {
	_, digest, err := CanonicalRepositoryCommandEvidenceRequest(value)
	if err != nil || result.Claim.RequestDigest != digest {
		return ErrEvidenceConflict
	}
	semanticKey, err := RepositoryCommandEvidenceSemanticKey(value)
	if err != nil || result.Claim.SemanticKey != semanticKey {
		return ErrEvidenceConflict
	}
	return nil
}

func frozenVerifyArgv(snapshot []byte, digest string) ([]string, error) {
	effective, err := config.DecodeSnapshot(snapshot, digest)
	if err != nil || effective.Commands.Verify.Validate("verify") != nil {
		return nil, ErrEvidenceConflict
	}
	return append([]string(nil), effective.Commands.Verify.Argv...), nil
}

func exactRepositoryCommandDigest(argv []string) (string, error) {
	if len(argv) == 0 {
		return "", ErrEvidenceConflict
	}
	b, err := json.Marshal(argv)
	if err != nil {
		return "", err
	}
	return repositoryResultDigest(b), nil
}

func expectedVerificationExit(outcome string, exit int) error {
	switch outcome {
	case "red", "missing", "check_failed":
		if exit != 0 {
			return nil
		}
	case "baseline", "dry_run", "report_ready":
		if exit == 0 {
			return nil
		}
	}
	return ErrEvidenceConflict
}

// CandidateSnapshot predates Store-issued command claims and stores the raw
// 64-hex SHA-256 form. Repository claims deliberately use the typed
// "sha256:" form. This is still an exact policy identity: conversion is a
// fixed injective representation change, never a recomputation or fallback.
func candidatePolicyMatches(snapshotDigest, claimDigest string) bool {
	return validDigest(snapshotDigest) && claimDigest == "sha256:"+snapshotDigest
}

func matchingBinding(binding RepositoryCommandResultBinding, result RepositoryCommandResult) bool {
	return binding.Key == result.Key && binding.TicketVersion == result.Claim.TicketVersion && binding.LeaderEpoch == result.Claim.LeaderEpoch && binding.RunnerEpoch == result.Claim.RunnerEpoch && binding.CommandDigest == result.Claim.CommandDigest && binding.SpecDigest == result.Claim.SpecDigest && binding.PolicyDigest == result.Claim.PolicyDigest && binding.ExecutablePath == result.Claim.ExecutablePath && binding.ExecutableDigest == result.Claim.ExecutableDigest
}

func resultBinding(result RepositoryCommandResult, expectedOutcome string) RepositoryCommandResultBinding {
	return RepositoryCommandResultBinding{
		Key: result.Key, TicketVersion: result.Claim.TicketVersion, LeaderEpoch: result.Claim.LeaderEpoch, RunnerEpoch: result.Claim.RunnerEpoch,
		CommandDigest: result.Claim.CommandDigest, SpecDigest: result.Claim.SpecDigest, PolicyDigest: result.Claim.PolicyDigest, ExecutablePath: result.Claim.ExecutablePath, ExecutableDigest: result.Claim.ExecutableDigest,
		ExpectedOutcome: expectedOutcome,
	}
}

// authenticateVerificationCommandEvidence re-reads all mutable ticket and
// worktree context on the caller's transaction, then authenticates the
// immutable result against the command the ticket froze at submission. The
// provider's command is compared only as an untrusted declaration.
func authenticateVerificationCommandEvidence(ctx context.Context, conn *sql.Conn, artifact VerificationArtifact, verify *phaseartifact.Verification) (RepositoryCommandResult, RepositoryCommandResultBinding, error) {
	if verify == nil || artifact.CommandResult.SemanticKey == "" || artifact.CommandResult.ClaimEpoch == 0 {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	var configDigest, providerCreated string
	var snapshot []byte
	var worktree, identity, base string
	err := conn.QueryRowContext(ctx, `SELECT t.config_digest,t.config_snapshot_bytes,w.path,w.identity_json,w.base_sha,r.created_at FROM tickets t JOIN worktrees w ON w.channel=t.channel AND w.project_id=t.project_id AND w.ticket_id=t.id JOIN provider_attempt_results r ON r.provider_attempt_id=? WHERE t.channel=? AND t.project_id=? AND t.id=?`, artifact.ProviderResult.AttemptID, artifact.Ref.Channel, artifact.Ref.Project, artifact.Ref.Ticket).Scan(&configDigest, &snapshot, &worktree, &identity, &base, &providerCreated)
	if err != nil {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	argv, err := frozenVerifyArgv(snapshot, configDigest)
	if err != nil || !equalStringSlices(argv, verify.Command) {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	commandDigest, err := exactRepositoryCommandDigest(argv)
	if err != nil {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	result, found, err := loadRepositoryCommandResult(ctx, conn, artifact.CommandResult, true)
	if err != nil || !found || result.Claim.TicketRef != artifact.Ref || result.Claim.TicketVersion != artifact.ExpectedVersion || result.Claim.LeaderEpoch != artifact.Fence.LeaderEpoch || result.Claim.RunnerEpoch != artifact.Fence.RunnerEpoch || result.Claim.Worktree != worktree || result.Claim.WorktreeIdentity != identity || result.Claim.BaseSHA != base || result.Claim.CommandDigest != commandDigest {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	if err := expectedVerificationExit(verify.PrebuildOutcome, result.Result.ExitCode); err != nil {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, err
	}
	providerObserved, err := time.Parse(time.RFC3339Nano, providerCreated)
	if err != nil || result.Result.ObservedAt.Before(providerObserved) {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	// The pre-build command is issued before the checkpoint commit exists. Its
	// identity therefore binds the provider attempt and proof projections but
	// deliberately has no checkpoint component. RecordVerification separately
	// binds the later checkpoint to that same immutable provider artifact.
	request := commandEvidenceRequest(RepositoryCommandPurposePrebuildVerification, artifact.Ref, artifact.ExpectedVersion, artifact.Fence, *artifact.ProviderResult, sha256Digest(artifact.Intent), sha256Digest(artifact.Proof), "", commandDigest, result)
	if err := assertCommandEvidenceRequest(request, result); err != nil {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, err
	}
	return result, resultBinding(result, verify.PrebuildOutcome), nil
}

func ensureVerificationCommandBinding(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, revision uint64, binding RepositoryCommandResultBinding) error {
	var stored RepositoryCommandResultBinding
	err := conn.QueryRowContext(ctx, `SELECT binding_ticket_version,leader_epoch,runner_epoch,semantic_key,claim_epoch,command_digest,spec_digest,policy_digest,executable_path,executable_digest,expected_outcome FROM verification_command_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND revision=?`, ref.Channel, ref.Project, ref.Ticket, revision).Scan(&stored.TicketVersion, &stored.LeaderEpoch, &stored.RunnerEpoch, &stored.Key.SemanticKey, &stored.Key.ClaimEpoch, &stored.CommandDigest, &stored.SpecDigest, &stored.PolicyDigest, &stored.ExecutablePath, &stored.ExecutableDigest, &stored.ExpectedOutcome)
	if err == nil {
		if stored == binding {
			return nil
		}
		return ErrEvidenceConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO verification_command_result_bindings(channel,project_id,ticket_id,revision,binding_ticket_version,leader_epoch,runner_epoch,semantic_key,claim_epoch,command_digest,spec_digest,policy_digest,executable_path,executable_digest,expected_outcome) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, revision, binding.TicketVersion, binding.LeaderEpoch, binding.RunnerEpoch, binding.Key.SemanticKey, binding.Key.ClaimEpoch, binding.CommandDigest, binding.SpecDigest, binding.PolicyDigest, binding.ExecutablePath, binding.ExecutableDigest, binding.ExpectedOutcome)
	return err
}

func loadVerificationCommandBinding(ctx context.Context, q repositoryResultQuerier, ref domain.TicketRef, revision uint64) (RepositoryCommandResultBinding, error) {
	var binding RepositoryCommandResultBinding
	err := q.QueryRowContext(ctx, `SELECT binding_ticket_version,leader_epoch,runner_epoch,semantic_key,claim_epoch,command_digest,spec_digest,policy_digest,executable_path,executable_digest,expected_outcome FROM verification_command_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND revision=?`, ref.Channel, ref.Project, ref.Ticket, revision).Scan(&binding.TicketVersion, &binding.LeaderEpoch, &binding.RunnerEpoch, &binding.Key.SemanticKey, &binding.Key.ClaimEpoch, &binding.CommandDigest, &binding.SpecDigest, &binding.PolicyDigest, &binding.ExecutablePath, &binding.ExecutableDigest, &binding.ExpectedOutcome)
	if err != nil {
		return RepositoryCommandResultBinding{}, err
	}
	result, found, err := loadRepositoryCommandResult(ctx, q, binding.Key, true)
	if err != nil || !found || result.Claim.TicketRef != ref || !matchingBinding(binding, result) || expectedVerificationExit(binding.ExpectedOutcome, result.Result.ExitCode) != nil {
		return RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	return binding, nil
}

func (s *Store) reauthenticateStoredVerificationCommand(ctx context.Context, ref domain.TicketRef, stored StoredVerification) error {
	result, err := s.LoadRepositoryCommandResult(ctx, stored.CommandBinding.Key)
	if err != nil || result.Claim.TicketRef != ref || !matchingBinding(stored.CommandBinding, result) || stored.CommandBinding.TicketVersion != stored.TicketVersion || stored.CommandBinding.LeaderEpoch != stored.Fence.LeaderEpoch || stored.CommandBinding.RunnerEpoch != stored.Fence.RunnerEpoch {
		return ErrEvidenceConflict
	}
	provider, parsed, err := s.LoadHistoricalProviderAttemptResult(ctx, stored.ProviderResult)
	if err != nil || parsed.Verify == nil || provider.Claim.Ref != ref || provider.Claim.ExpectedVersion != stored.TicketVersion || provider.Claim.LeaderEpoch != stored.Fence.LeaderEpoch || provider.Claim.RunnerEpoch != stored.Fence.RunnerEpoch {
		return ErrEvidenceConflict
	}
	var providerCreated string
	if err := s.db.QueryRowContext(ctx, `SELECT created_at FROM provider_attempt_results WHERE provider_attempt_id=?`, stored.ProviderResult.AttemptID).Scan(&providerCreated); err != nil {
		return ErrEvidenceConflict
	}
	created, err := time.Parse(time.RFC3339Nano, providerCreated)
	if err != nil || result.Result.ObservedAt.Before(created) {
		return ErrEvidenceConflict
	}
	ticket, err := s.Ticket(ctx, ref)
	if err != nil {
		return ErrEvidenceConflict
	}
	argv, err := frozenVerifyArgv(ticket.ConfigSnapshot, ticket.ConfigDigest)
	if err != nil || !equalStringSlices(argv, parsed.Verify.Command) {
		return ErrEvidenceConflict
	}
	commandDigest, err := exactRepositoryCommandDigest(argv)
	if err != nil || commandDigest != result.Claim.CommandDigest || expectedVerificationExit(parsed.Verify.PrebuildOutcome, result.Result.ExitCode) != nil || stored.CommandBinding.ExpectedOutcome != parsed.Verify.PrebuildOutcome {
		return ErrEvidenceConflict
	}
	worktree, err := s.Worktree(ctx, ref)
	if err != nil || result.Claim.Worktree != worktree.Path || result.Claim.WorktreeIdentity != string(worktree.IdentityJSON) || result.Claim.BaseSHA != worktree.BaseSHA || result.Claim.BaseSHA != provider.Claim.BaseSHA || result.Claim.Worktree != provider.Claim.Worktree || result.Claim.WorktreeIdentity != provider.Claim.WorktreeIdentity {
		return ErrEvidenceConflict
	}
	request := commandEvidenceRequest(RepositoryCommandPurposePrebuildVerification, ref, stored.TicketVersion, stored.Fence, stored.ProviderResult, stored.Revision.IntentDigest, stored.Revision.ProofDigest, "", commandDigest, result)
	return assertCommandEvidenceRequest(request, result)
}

func authenticateCandidateCommandEvidence(ctx context.Context, conn *sql.Conn, evidence CandidateEvidence, builderResult ProviderAttemptResult, intent, proof, checkpoint string) (RepositoryCommandResult, RepositoryCommandResultBinding, error) {
	if evidence.CommandResult.SemanticKey == "" || evidence.CommandResult.ClaimEpoch == 0 {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	var configDigest, worktree, identity, base, providerFinished string
	var snapshot []byte
	err := conn.QueryRowContext(ctx, `SELECT t.config_digest,t.config_snapshot_bytes,w.path,w.identity_json,w.base_sha,r.created_at FROM tickets t JOIN worktrees w ON w.channel=t.channel AND w.project_id=t.project_id AND w.ticket_id=t.id JOIN provider_attempt_results r ON r.provider_attempt_id=? WHERE t.channel=? AND t.project_id=? AND t.id=?`, evidence.BuilderResult.AttemptID, evidence.Ref.Channel, evidence.Ref.Project, evidence.Ref.Ticket).Scan(&configDigest, &snapshot, &worktree, &identity, &base, &providerFinished)
	if err != nil || builderResult.Claim.Ref != evidence.Ref {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	argv, err := frozenVerifyArgv(snapshot, configDigest)
	if err != nil {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	commandDigest, err := exactRepositoryCommandDigest(argv)
	if err != nil {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	result, found, err := loadRepositoryCommandResult(ctx, conn, evidence.CommandResult, true)
	if err != nil || !found || result.Result.ExitCode != 0 || result.Claim.TicketRef != evidence.Ref || result.Claim.TicketVersion != evidence.ExpectedVersion || result.Claim.LeaderEpoch != evidence.Fence.LeaderEpoch || result.Claim.RunnerEpoch != evidence.Fence.RunnerEpoch || result.Claim.Worktree != worktree || result.Claim.WorktreeIdentity != identity || result.Claim.BaseSHA != base || result.Claim.CommandDigest != commandDigest || !candidatePolicyMatches(evidence.Snapshot.CommandPolicyDigest, result.Claim.PolicyDigest) {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	finished, err := time.Parse(time.RFC3339Nano, providerFinished)
	if err != nil || result.Result.ObservedAt.Before(finished) {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	request := commandEvidenceRequest(RepositoryCommandPurposePostbuildCandidate, evidence.Ref, evidence.ExpectedVersion, evidence.Fence, evidence.BuilderResult, intent, proof, checkpoint, commandDigest, result)
	if err := assertCommandEvidenceRequest(request, result); err != nil {
		return RepositoryCommandResult{}, RepositoryCommandResultBinding{}, err
	}
	return result, resultBinding(result, ""), nil
}

func ensureCandidateCommandBinding(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, generation uint64, binding RepositoryCommandResultBinding) error {
	var stored RepositoryCommandResultBinding
	err := conn.QueryRowContext(ctx, `SELECT binding_ticket_version,leader_epoch,runner_epoch,semantic_key,claim_epoch,command_digest,spec_digest,policy_digest,executable_path,executable_digest FROM candidate_command_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, ref.Channel, ref.Project, ref.Ticket, generation).Scan(&stored.TicketVersion, &stored.LeaderEpoch, &stored.RunnerEpoch, &stored.Key.SemanticKey, &stored.Key.ClaimEpoch, &stored.CommandDigest, &stored.SpecDigest, &stored.PolicyDigest, &stored.ExecutablePath, &stored.ExecutableDigest)
	if err == nil {
		if stored == binding {
			return nil
		}
		return ErrEvidenceConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = conn.ExecContext(ctx, `INSERT INTO candidate_command_result_bindings(channel,project_id,ticket_id,generation,binding_ticket_version,leader_epoch,runner_epoch,semantic_key,claim_epoch,command_digest,spec_digest,policy_digest,executable_path,executable_digest) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ref.Channel, ref.Project, ref.Ticket, generation, binding.TicketVersion, binding.LeaderEpoch, binding.RunnerEpoch, binding.Key.SemanticKey, binding.Key.ClaimEpoch, binding.CommandDigest, binding.SpecDigest, binding.PolicyDigest, binding.ExecutablePath, binding.ExecutableDigest)
	return err
}

func loadCandidateCommandBinding(ctx context.Context, q repositoryResultQuerier, ref domain.TicketRef, generation uint64) (RepositoryCommandResultBinding, error) {
	var binding RepositoryCommandResultBinding
	err := q.QueryRowContext(ctx, `SELECT binding_ticket_version,leader_epoch,runner_epoch,semantic_key,claim_epoch,command_digest,spec_digest,policy_digest,executable_path,executable_digest FROM candidate_command_result_bindings WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, ref.Channel, ref.Project, ref.Ticket, generation).Scan(&binding.TicketVersion, &binding.LeaderEpoch, &binding.RunnerEpoch, &binding.Key.SemanticKey, &binding.Key.ClaimEpoch, &binding.CommandDigest, &binding.SpecDigest, &binding.PolicyDigest, &binding.ExecutablePath, &binding.ExecutableDigest)
	if err != nil {
		return RepositoryCommandResultBinding{}, err
	}
	result, found, err := loadRepositoryCommandResult(ctx, q, binding.Key, true)
	if err != nil || !found || result.Claim.TicketRef != ref || binding.ExpectedOutcome != "" || !matchingBinding(binding, result) || result.Result.ExitCode != 0 {
		return RepositoryCommandResultBinding{}, ErrEvidenceConflict
	}
	return binding, nil
}

func (s *Store) reauthenticateStoredCandidateCommand(ctx context.Context, ref domain.TicketRef, stored StoredCandidate) error {
	result, err := s.LoadRepositoryCommandResult(ctx, stored.CommandBinding.Key)
	if err != nil || result.Claim.TicketRef != ref || !matchingBinding(stored.CommandBinding, result) || stored.CommandBinding.TicketVersion != stored.TicketVersion || stored.CommandBinding.LeaderEpoch != stored.Fence.LeaderEpoch || stored.CommandBinding.RunnerEpoch != stored.Fence.RunnerEpoch || result.Result.ExitCode != 0 || !candidatePolicyMatches(stored.Snapshot.CommandPolicyDigest, result.Claim.PolicyDigest) {
		return ErrEvidenceConflict
	}
	var builderCreated string
	if err := s.db.QueryRowContext(ctx, `SELECT created_at FROM provider_attempt_results WHERE provider_attempt_id=?`, stored.BuilderResult.AttemptID).Scan(&builderCreated); err != nil {
		return ErrEvidenceConflict
	}
	created, err := time.Parse(time.RFC3339Nano, builderCreated)
	if err != nil || result.Result.ObservedAt.Before(created) {
		return ErrEvidenceConflict
	}
	verification, err := s.CurrentVerification(ctx, ref)
	if err != nil || stored.Snapshot.VerificationIntentDigest != verification.Revision.IntentDigest || stored.Snapshot.ProofDigest != verification.Revision.ProofDigest || stored.Commit.ParentOID != verification.Checkpoint.CommitOID {
		return ErrEvidenceConflict
	}
	worktree, err := s.Worktree(ctx, ref)
	if err != nil || stored.Snapshot.BaseSHA != worktree.BaseSHA || result.Claim.Worktree != worktree.Path || result.Claim.WorktreeIdentity != string(worktree.IdentityJSON) || result.Claim.BaseSHA != worktree.BaseSHA {
		return ErrEvidenceConflict
	}
	ticket, err := s.Ticket(ctx, ref)
	if err != nil {
		return ErrEvidenceConflict
	}
	argv, err := frozenVerifyArgv(ticket.ConfigSnapshot, ticket.ConfigDigest)
	if err != nil {
		return ErrEvidenceConflict
	}
	commandDigest, err := exactRepositoryCommandDigest(argv)
	if err != nil || commandDigest != result.Claim.CommandDigest {
		return ErrEvidenceConflict
	}
	request := commandEvidenceRequest(RepositoryCommandPurposePostbuildCandidate, ref, stored.TicketVersion, stored.Fence, stored.BuilderResult, stored.Snapshot.VerificationIntentDigest, stored.Snapshot.ProofDigest, verification.Revision.CheckpointID, commandDigest, result)
	return assertCommandEvidenceRequest(request, result)
}

// commandResultIsExact is retained as a small testable assertion for paths
// which already loaded the immutable record and a binding independently.
func commandResultIsExact(left, right RepositoryCommandResult) bool {
	return left.Key == right.Key && left.Claim == right.Claim && left.Result.ExitCode == right.Result.ExitCode && bytes.Equal(left.Result.Stdout, right.Result.Stdout) && bytes.Equal(left.Result.Stderr, right.Result.Stderr) && bytes.Equal(left.Result.OutputLastMessage, right.Result.OutputLastMessage) && left.Result.StdoutTruncated == right.Result.StdoutTruncated && left.Result.StderrTruncated == right.Result.StderrTruncated && left.Result.OutputLastMessageTruncated == right.Result.OutputLastMessageTruncated && left.Result.Duration == right.Result.Duration && left.Result.ObservedAt.Equal(right.Result.ObservedAt) && left.CreatedAt.Equal(right.CreatedAt) && left.ResultDigest == right.ResultDigest
}

var _ = contracts.RepositoryCommandResultKey{}
