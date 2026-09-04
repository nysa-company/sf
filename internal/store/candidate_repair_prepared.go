package store

import (
	"context"
	"database/sql"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

// CandidateRepairPreparedCandidateWitness is the complete immutable authority
// for the narrow crash window after a correction Builder's post-build command
// and prepared Git child have completed but before RecordCandidate appends the
// successor generation. The witness is observation-only recovery authority;
// it never authorizes another repository command or Git mutation.
type CandidateRepairPreparedCandidateWitness struct {
	Ref          domain.TicketRef
	Version      uint64
	Fence        domain.Fence
	Project      Project
	Worktree     StoredWorktree
	Repair       CandidateRepairBuildContext
	Verification StoredVerification
	Plan         StoredPlan
	Builder      ProviderAttemptResultKey
	// BuilderArtifact is the exact immutable artifact authenticated by the
	// provider result and the prepared Git request digest. Runtime compares its
	// supplied artifact again before observation-only replay.
	BuilderArtifact phaseartifact.Builder
	Command         RepositoryCommandResult
	Commit          CommitObservation
	Claim           contracts.GitMutationClaim
	EffectState     EffectState
}

// CandidateRepairRecoverablePreparedCandidateWitness authenticates a single
// confirmed repair child across any signed runner-recovery suffix. Clean
// absence means the command and Git mutation have not crossed this boundary;
// any partial, ambiguous, or malformed tuple fails closed.
func (s *Store) CandidateRepairRecoverablePreparedCandidateWitness(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, builderKey ProviderAttemptResultKey) (CandidateRepairPreparedCandidateWitness, bool, error) {
	if s == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 || builderKey.Ref != ref || builderKey.Phase != domain.PhaseBuild || builderKey.AttemptID <= 0 || builderKey.Attempt <= 0 {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	if ctx == nil {
		ctx = context.Background()
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "ROLLBACK") }()
	return s.candidateRepairPreparedCandidateWitnessFrom(ctx, conn, ref, version, fence, builderKey)
}

func (s *Store) candidateRepairPreparedCandidateWitnessFrom(ctx context.Context, conn *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence, builderKey ProviderAttemptResultKey) (CandidateRepairPreparedCandidateWitness, bool, error) {
	if s == nil || conn == nil || ref.Validate() != nil || version == 0 || fence.LeaderEpoch == 0 || fence.RunnerEpoch == 0 || fence.ClaimEpoch != 0 || builderKey.Ref != ref || builderKey.Phase != domain.PhaseBuild || builderKey.AttemptID <= 0 || builderKey.Attempt <= 0 {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	authority, err := s.candidateRepairBuildAuthorityAt(ctx, conn, ref, version, fence)
	if err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, err
	}
	if err := s.authenticateOperatorSourceResumeRepairPredecessor(ctx, conn, ref, authority); err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}

	predecessor, err := s.latestCandidateFrom(ctx, conn, ref, false)
	if err != nil || predecessor.Snapshot.Generation != authority.context.PredecessorGeneration || predecessor.Snapshot.HeadSHA != authority.context.PredecessorHeadSHA || predecessor.Snapshot.TreeSHA != authority.context.PredecessorTreeSHA {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	var targetCandidates, targetCompletions int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_snapshots WHERE channel=? AND project_id=? AND ticket_id=? AND generation=?`, ref.Channel, ref.Project, ref.Ticket, authority.context.TargetGeneration).Scan(&targetCandidates); err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM candidate_repair_completions WHERE channel=? AND project_id=? AND ticket_id=? AND target_generation=?`, ref.Channel, ref.Project, ref.Ticket, authority.context.TargetGeneration).Scan(&targetCompletions); err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	if targetCandidates != 0 || targetCompletions != 0 {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}

	worktree, project, err := operatorSourceWorktreeFrom(ctx, conn, ref)
	if err != nil || !validRepositoryWorktreeIdentity(string(worktree.IdentityJSON), project.Path, worktree.Path, worktree.Branch, project.BaseRef, worktree.BaseSHA) || worktree.BaseSHA != predecessor.Snapshot.BaseSHA {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	project.Channel, project.ID = ref.Channel, ref.Project
	if err := conn.QueryRowContext(ctx, `SELECT p.current_config_generation,COALESCE(c.digest,''),COALESCE(c.snapshot_bytes,X'') FROM projects p LEFT JOIN project_configurations c ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation WHERE p.channel=? AND p.id=?`, ref.Channel, ref.Project).Scan(&project.ConfigGeneration, &project.ConfigDigest, &project.ConfigSnapshot); err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}

	plan, err := operatorSourcePlanFrom(ctx, conn, ref)
	if err != nil || plan.Document.ProviderResult == nil || plan.Document.Planner == nil {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	planner, plannerParsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, *plan.Document.ProviderResult)
	if err != nil || planner.Claim.Ref != ref || planner.Claim.Phase != domain.PhasePlanning || planner.Claim.Role != "planner" || plannerParsed.Planner == nil {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	planIdentity, err := workflowprompt.NewPlanIdentity(*plannerParsed.Planner)
	if err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}

	verification := authority.context.Verification
	reviewer, reviewerParsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, verification.ProviderResult)
	if err != nil || reviewer.Claim.Ref != ref || reviewer.Claim.Phase != domain.PhaseVerification || reviewer.Claim.Role != "reviewer" || reviewerParsed.Verify == nil || reviewerParsed.Verify.AcceptanceDigest != planIdentity.Digest {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	verificationIdentity, err := workflowprompt.NewVerificationIdentity(*reviewerParsed.Verify, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Revision.CheckpointID)
	if err != nil || verification.Checkpoint.CommitOID != verification.Revision.CheckpointID {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}

	builder, builderParsed, err := s.loadHistoricalProviderAttemptResult(ctx, conn, builderKey)
	if err != nil || builder.Claim.Ref != ref || builder.Claim.Phase != domain.PhaseBuild || builder.Claim.Role != "builder" || builderParsed.Builder == nil || builder.Claim.Repository != project.Path || builder.Claim.Worktree != worktree.Path || builder.Claim.WorktreeIdentity != string(worktree.IdentityJSON) || builder.Claim.BaseSHA != worktree.BaseSHA || candidateRepairBuilderEntryResultReachesFence(ctx, conn, builderKey, builder, version, fence) != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}

	gitRows, err := conn.QueryContext(ctx, `SELECT semantic_key FROM git_mutation_intents WHERE channel=? AND project_id=? AND ticket_id=? AND operation='commit' AND repository_path=? AND worktree_path=? AND branch_ref=? AND ticket_version>=? AND prepared_commit_oid<>'' AND prepared_tree_oid<>'' ORDER BY semantic_key`, ref.Channel, ref.Project, ref.Ticket, project.Path, worktree.Path, worktree.Branch, authority.context.EntryTicketVersion)
	if err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	var gitKeys []string
	for gitRows.Next() {
		var key string
		if err := gitRows.Scan(&key); err != nil {
			gitRows.Close()
			return CandidateRepairPreparedCandidateWitness{}, false, err
		}
		gitKeys = append(gitKeys, key)
	}
	if err := gitRows.Err(); err != nil {
		gitRows.Close()
		return CandidateRepairPreparedCandidateWitness{}, false, err
	}
	if err := gitRows.Close(); err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, err
	}

	commandRows, err := conn.QueryContext(ctx, `SELECT semantic_key,claim_epoch FROM repository_command_results WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>=? AND semantic_key LIKE ? ORDER BY semantic_key,claim_epoch`, ref.Channel, ref.Project, ref.Ticket, authority.context.EntryTicketVersion, "repository-command-evidence/"+RepositoryCommandPurposePostbuildCandidate+"/%")
	if err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, normalizeBusy(ctx, err)
	}
	var commandKeys []contracts.RepositoryCommandResultKey
	for commandRows.Next() {
		var key contracts.RepositoryCommandResultKey
		if err := commandRows.Scan(&key.SemanticKey, &key.ClaimEpoch); err != nil {
			commandRows.Close()
			return CandidateRepairPreparedCandidateWitness{}, false, err
		}
		commandKeys = append(commandKeys, key)
	}
	if err := commandRows.Err(); err != nil {
		commandRows.Close()
		return CandidateRepairPreparedCandidateWitness{}, false, err
	}
	if err := commandRows.Close(); err != nil {
		return CandidateRepairPreparedCandidateWitness{}, false, err
	}
	if len(gitKeys) == 0 && len(commandKeys) == 0 {
		return CandidateRepairPreparedCandidateWitness{}, false, nil
	}
	if len(gitKeys) != 1 || len(commandKeys) == 0 {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}

	facts, err := gitMutationIntentFactsFrom(ctx, conn, gitKeys[0])
	if err != nil || facts.Claim.Operation != "commit" || facts.Claim.TicketRef != ref || facts.Claim.Repository != project.Path || facts.Claim.Worktree != worktree.Path || facts.Claim.Branch != worktree.Branch || facts.Claim.BaseRef != project.BaseRef || facts.Claim.ExpectedBaseOID != worktree.BaseSHA || facts.Claim.ExpectedHeadOID != authority.context.PredecessorHeadSHA || facts.PreparedCommitOID == "" || facts.PreparedTreeOID == "" || facts.Effect.State != EffectConfirmed {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	claimFence := domain.Fence{LeaderEpoch: facts.Claim.LeaderEpoch, RunnerEpoch: facts.Claim.RunnerEpoch}
	if facts.Claim.TicketVersion != version || claimFence != fence {
		if validateRunnerRecoveryLedger(ctx, conn, ref, facts.Claim.TicketVersion, facts.Claim.RunnerEpoch, facts.Claim.LeaderEpoch, version, fence.RunnerEpoch, fence.LeaderEpoch) != nil {
			return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
		}
	}
	if builder.Claim.ExpectedVersion != facts.Claim.TicketVersion || builder.Claim.RunnerEpoch != facts.Claim.RunnerEpoch || builder.Claim.LeaderEpoch != facts.Claim.LeaderEpoch {
		if validateRunnerRecoveryLedgerPrefix(ctx, conn, ref, builder.Claim.ExpectedVersion, builder.Claim.RunnerEpoch, builder.Claim.LeaderEpoch, facts.Claim.TicketVersion, facts.Claim.RunnerEpoch, facts.Claim.LeaderEpoch) != nil {
			return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
		}
	}

	var matched *RepositoryCommandResult
	for _, key := range commandKeys {
		command, found, loadErr := loadRepositoryCommandResult(ctx, conn, key, true)
		if loadErr != nil || !found {
			return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
		}
		if command.Claim.TicketRef != ref || command.Claim.TicketVersion != facts.Claim.TicketVersion || command.Claim.LeaderEpoch != facts.Claim.LeaderEpoch || command.Claim.RunnerEpoch != facts.Claim.RunnerEpoch || command.Claim.Repository != project.Path || command.Claim.Worktree != worktree.Path || command.Claim.WorktreeIdentity != string(worktree.IdentityJSON) || command.Claim.Branch != worktree.Branch || command.Claim.BaseRef != project.BaseRef || command.Claim.BaseSHA != worktree.BaseSHA {
			continue
		}
		snapshot := domain.CandidateSnapshot{BaseSHA: worktree.BaseSHA, VerificationIntentDigest: verification.Revision.IntentDigest, ProofDigest: verification.Revision.ProofDigest, CommandPolicyDigest: strings.TrimPrefix(command.Claim.PolicyDigest, "sha256:")}
		evidence := CandidateEvidence{Ref: ref, Snapshot: snapshot, BuilderResult: builderKey, CommandResult: command.Key}
		if _, _, err := authenticateCandidateCommandEvidence(ctx, conn, evidence, builder, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Revision.CheckpointID); err != nil {
			continue
		}
		digest := CanonicalRepositoryCommitDigest("candidate", ref, facts.Claim.TicketVersion, claimFence, worktree, builderKey, command.Key, command.ResultDigest, struct {
			Plan         workflowprompt.PlanIdentity
			Verification workflowprompt.VerificationIdentity
			Builder      phaseartifact.Builder
		}{planIdentity, verificationIdentity, *builderParsed.Builder})
		if facts.Claim.RequestDigest != digest {
			continue
		}
		if matched != nil {
			return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
		}
		copy := command
		matched = &copy
	}
	if matched == nil {
		return CandidateRepairPreparedCandidateWitness{}, false, ErrEvidenceConflict
	}
	return CandidateRepairPreparedCandidateWitness{
		Ref: ref, Version: version, Fence: fence, Project: project, Worktree: worktree,
		Repair: authority.context, Verification: verification, Plan: plan, Builder: builderKey, BuilderArtifact: *builderParsed.Builder,
		Command: *matched, Commit: CommitObservation{CommitOID: facts.PreparedCommitOID, ParentOID: authority.context.PredecessorHeadSHA, TreeOID: facts.PreparedTreeOID},
		Claim: facts.Claim, EffectState: facts.Effect.State,
	}, true, nil
}
