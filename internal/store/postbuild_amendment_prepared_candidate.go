package store

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/workflowprompt"
)

// This witness permits only observation/recording of the already prepared first
// candidate. It grants neither a command retry nor another Git mutation.
type PostbuildAmendmentPreparedCandidateWitness struct {
	Ref             domain.TicketRef
	Version         uint64
	Fence           domain.Fence
	Project         Project
	Worktree        StoredWorktree
	Verification    StoredVerification
	Plan            StoredPlan
	Builder         ProviderAttemptResultKey
	BuilderArtifact phaseartifact.Builder
	Command         RepositoryCommandResult
	Commit          CommitObservation
	Claim           contracts.GitMutationClaim
	EffectState     EffectState
}

func (s *Store) PostbuildAmendmentPreparedCandidateWitness(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, builderKey ProviderAttemptResultKey) (PostbuildAmendmentPreparedCandidateWitness, bool, error) {
	var result PostbuildAmendmentPreparedCandidateWitness
	value, err := s.PostbuildVerificationAmendmentContext(ctx, ref, version, fence)
	if errors.Is(err, ErrNotFound) {
		return result, false, nil
	}
	if err != nil {
		return result, false, err
	}
	if value.Decision != VerificationAmendmentAccepted || value.Candidate != nil || builderKey.Ref != ref || builderKey.Phase != domain.PhaseBuild {
		return result, false, ErrEvidenceConflict
	}
	return s.postbuildPreparedCandidateWitness(ctx, ref, version, fence, builderKey, func(q *sql.Conn) (postbuildCandidateSource, error) {
		boundary, err := loadVerificationAmendmentBoundary(ctx, q, ref, version, fence)
		if err != nil || boundary.Decision != VerificationAmendmentAccepted || !reflect.DeepEqual(boundary.Amendment, value.Amendment) {
			return postbuildCandidateSource{}, ErrEvidenceConflict
		}
		binding, _, err := loadPostbuildAmendmentBinding(ctx, q, boundary.Amendment)
		if err != nil || !reflect.DeepEqual(binding, value.Binding) {
			return postbuildCandidateSource{}, ErrEvidenceConflict
		}
		verification, err := s.verificationEvidenceForIdentityFrom(ctx, q, ref, "", "", "")
		if err != nil || !reflect.DeepEqual(verification, value.CurrentVerification) {
			return postbuildCandidateSource{}, ErrEvidenceConflict
		}
		return postbuildCandidateSource{EntryVersion: boundary.DecisionVersion, Verification: verification, Worktree: value.Worktree, Plan: value.Plan}, nil
	})
}

type postbuildCandidateSource struct {
	EntryVersion uint64
	Verification StoredVerification
	Worktree     StoredWorktree
	Plan         StoredPlan
}

func (s *Store) postbuildPreparedCandidateWitness(ctx context.Context, ref domain.TicketRef, version uint64, fence domain.Fence, builderKey ProviderAttemptResultKey, load func(*sql.Conn) (postbuildCandidateSource, error)) (PostbuildAmendmentPreparedCandidateWitness, bool, error) {
	var result PostbuildAmendmentPreparedCandidateWitness
	found := false
	err := s.readProtectedBaseRefreshSnapshot(ctx, func(q *sql.Conn) error {
		value, err := load(q)
		if err != nil {
			return err
		}
		result, found, err = s.postbuildPreparedCandidateWitnessFrom(ctx, q, ref, version, fence, builderKey, value)
		return err
	})
	return result, found && err == nil, err
}

func (s *Store) postbuildPreparedCandidateWitnessFrom(ctx context.Context, q *sql.Conn, ref domain.TicketRef, version uint64, fence domain.Fence, builderKey ProviderAttemptResultKey, value postbuildCandidateSource) (PostbuildAmendmentPreparedCandidateWitness, bool, error) {
	var result PostbuildAmendmentPreparedCandidateWitness
	found := false
	err := func() error {
		if err := s.assertTicketFence(ctx, q, ref, version, fence); err != nil {
			return err
		}
		var state domain.State
		if q.QueryRowContext(ctx, `SELECT state FROM tickets WHERE channel=? AND project_id=? AND id=?`, ref.Channel, ref.Project, ref.Ticket).Scan(&state) != nil || state != domain.StateBuilding || assertNoVerificationAmendmentDownstream(ctx, q, ref) != nil {
			return ErrEvidenceConflict
		}
		if value.EntryVersion == 0 || value.EntryVersion > version || builderKey.Ref != ref || builderKey.Phase != domain.PhaseBuild {
			return ErrEvidenceConflict
		}
		verification := value.Verification
		worktree, project, err := operatorSourceWorktreeFrom(ctx, q, ref)
		if err != nil || worktree.Path != value.Worktree.Path || worktree.Branch != value.Worktree.Branch || worktree.BaseSHA != value.Worktree.BaseSHA || string(worktree.IdentityJSON) != string(value.Worktree.IdentityJSON) {
			return ErrEvidenceConflict
		}
		project.Channel, project.ID = ref.Channel, ref.Project
		if q.QueryRowContext(ctx, `SELECT p.current_config_generation,COALESCE(c.digest,''),COALESCE(c.snapshot_bytes,X'') FROM projects p LEFT JOIN project_configurations c ON c.channel=p.channel AND c.project_id=p.id AND c.generation=p.current_config_generation WHERE p.channel=? AND p.id=?`, ref.Channel, ref.Project).Scan(&project.ConfigGeneration, &project.ConfigDigest, &project.ConfigSnapshot) != nil {
			return ErrEvidenceConflict
		}
		plan, err := s.planFrom(ctx, q, ref)
		if err != nil || plan.Document.Planner == nil || !reflect.DeepEqual(plan, value.Plan) {
			return ErrEvidenceConflict
		}
		planIdentity, err := workflowprompt.NewPlanIdentity(*plan.Document.Planner)
		if err != nil {
			return ErrEvidenceConflict
		}
		_, parsedReviewer, err := s.loadHistoricalProviderAttemptResult(ctx, q, verification.ProviderResult)
		if err != nil || parsedReviewer.Verify == nil || parsedReviewer.Verify.AcceptanceDigest != planIdentity.Digest {
			return ErrEvidenceConflict
		}
		verificationIdentity, err := workflowprompt.NewVerificationIdentity(*parsedReviewer.Verify, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Revision.CheckpointID)
		if err != nil {
			return ErrEvidenceConflict
		}
		builder, parsed, err := s.loadHistoricalProviderAttemptResult(ctx, q, builderKey)
		if err != nil || parsed.Builder == nil || parsed.Builder.AmendmentRequest != nil || builder.Claim.ExpectedVersion < value.EntryVersion || builder.Claim.Repository != project.Path || builder.Claim.Worktree != worktree.Path || builder.Claim.WorktreeIdentity != string(worktree.IdentityJSON) || builder.Claim.BaseSHA != worktree.BaseSHA || assertNewestBoundResult(ctx, q, ref, domain.PhaseBuild, "builder", builderKey) != nil || providerResultReachesFence(ctx, q, builderKey, builder, version, fence) != nil {
			return ErrEvidenceConflict
		}
		var count int
		if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_phase_attempt_entries WHERE channel=? AND project_id=? AND ticket_id=? AND phase='build' AND role='builder' AND provider_attempt_id=? AND attempt=? AND entry_ticket_version=?`, ref.Channel, ref.Project, ref.Ticket, builderKey.AttemptID, builderKey.Attempt, value.EntryVersion).Scan(&count) != nil || count != 1 {
			return ErrEvidenceConflict
		}
		rows, err := q.QueryContext(ctx, `SELECT semantic_key FROM git_mutation_intents WHERE channel=? AND project_id=? AND ticket_id=? AND operation='commit' AND ticket_version>=? ORDER BY semantic_key LIMIT 2`, ref.Channel, ref.Project, ref.Ticket, value.EntryVersion)
		if err != nil {
			return err
		}
		var keys []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				rows.Close()
				return err
			}
			keys = append(keys, key)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}
		if len(keys) != 1 {
			return ErrEvidenceConflict
		}
		facts, err := gitMutationIntentFactsFrom(ctx, q, keys[0])
		if err != nil || facts.Claim.Repository != project.Path || facts.Claim.Worktree != worktree.Path || facts.Claim.Branch != worktree.Branch || facts.Claim.BaseRef != project.BaseRef || facts.Claim.ExpectedBaseOID != worktree.BaseSHA || facts.Claim.ExpectedHeadOID != verification.Checkpoint.CommitOID || !validOID(facts.PreparedCommitOID) || !validOID(facts.PreparedTreeOID) {
			return ErrEvidenceConflict
		}
		if repositoryHasGitWriter(ctx, q, project.Path) != nil || repositoryHasProviderWriter(ctx, q, project.Path) != nil || repositoryHasCommandWriter(ctx, q, project.Path) != nil {
			return ErrControlNotDrained
		}
		if q.QueryRowContext(ctx, `SELECT COUNT(*) FROM effects WHERE channel=? AND project_id=? AND ticket_id=? AND semantic_key<>? AND state IN ('planned','executing','uncertain')`, ref.Channel, ref.Project, ref.Ticket, keys[0]).Scan(&count) != nil || count != 0 {
			return ErrControlNotDrained
		}
		claimFence := domain.Fence{LeaderEpoch: facts.Claim.LeaderEpoch, RunnerEpoch: facts.Claim.RunnerEpoch}
		if facts.Effect.State == EffectConfirmed {
			if facts.Effect.ObservedIdentity != facts.PreparedCommitOID {
				return ErrEvidenceConflict
			}
		} else if (facts.Effect.State != EffectExecuting && facts.Effect.State != EffectUncertain) || facts.Claim.TicketVersion != version || claimFence != fence || facts.Effect.TicketVersion != version || facts.Effect.LeaderEpoch != fence.LeaderEpoch || facts.Effect.RunnerEpoch != fence.RunnerEpoch || facts.Effect.ClaimEpoch != facts.Claim.ClaimEpoch || facts.Effect.ObservedIdentity != "" {
			return ErrEvidenceConflict
		}
		if postbuildRepairSignedSourcePrefix(ctx, q, ref, builder.Claim.ExpectedVersion, domain.Fence{LeaderEpoch: builder.Claim.LeaderEpoch, RunnerEpoch: builder.Claim.RunnerEpoch}, facts.Claim.TicketVersion, claimFence) != nil || postbuildRepairSignedSourcePrefix(ctx, q, ref, facts.Claim.TicketVersion, claimFence, version, fence) != nil {
			return ErrEvidenceConflict
		}
		rows, err = q.QueryContext(ctx, `SELECT semantic_key,claim_epoch FROM repository_command_results WHERE channel=? AND project_id=? AND ticket_id=? AND ticket_version>=? AND semantic_key LIKE ? ORDER BY semantic_key,claim_epoch LIMIT 65`, ref.Channel, ref.Project, ref.Ticket, value.EntryVersion, "repository-command-evidence/"+RepositoryCommandPurposePostbuildCandidate+"/%")
		if err != nil {
			return err
		}
		var commands []contracts.RepositoryCommandResultKey
		for rows.Next() {
			var key contracts.RepositoryCommandResultKey
			if err := rows.Scan(&key.SemanticKey, &key.ClaimEpoch); err != nil {
				rows.Close()
				return err
			}
			commands = append(commands, key)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(commands) > 64 {
			return ErrEvidenceConflict
		}
		var matched *RepositoryCommandResult
		for _, key := range commands {
			command, exists, err := loadRepositoryCommandResult(ctx, q, key, true)
			if err != nil || !exists {
				return ErrEvidenceConflict
			}
			if command.Claim.TicketVersion != facts.Claim.TicketVersion || command.Claim.LeaderEpoch != claimFence.LeaderEpoch || command.Claim.RunnerEpoch != claimFence.RunnerEpoch || command.Claim.Repository != project.Path || command.Claim.Branch != worktree.Branch || command.Claim.BaseRef != project.BaseRef {
				continue
			}
			evidence := CandidateEvidence{Ref: ref, BuilderResult: builderKey, CommandResult: key, Snapshot: domain.CandidateSnapshot{BaseSHA: worktree.BaseSHA, CommandPolicyDigest: strings.TrimPrefix(command.Claim.PolicyDigest, "sha256:")}}
			if _, _, err := authenticateCandidateCommandEvidence(ctx, q, evidence, builder, verification.Revision.IntentDigest, verification.Revision.ProofDigest, verification.Revision.CheckpointID); err != nil {
				continue
			}
			digest := CanonicalRepositoryCommitDigest("candidate", ref, facts.Claim.TicketVersion, claimFence, worktree, builderKey, key, command.ResultDigest, struct {
				Plan         workflowprompt.PlanIdentity
				Verification workflowprompt.VerificationIdentity
				Builder      phaseartifact.Builder
			}{planIdentity, verificationIdentity, *parsed.Builder})
			if facts.Claim.RequestDigest != digest {
				continue
			}
			intent := GitMutationIntent{EffectFence: EffectFence{Ref: ref, TicketVersion: facts.Claim.TicketVersion, Fence: claimFence}, RequestDigest: digest, Repository: project.Path, Worktree: worktree.Path, Branch: worktree.Branch, Operation: "commit", BaseRef: project.BaseRef, ExpectedBaseOID: worktree.BaseSHA, ExpectedHeadOID: verification.Checkpoint.CommitOID}
			if facts.Claim.SemanticKey != CanonicalGitMutationSemanticKey(intent) || matched != nil {
				return ErrEvidenceConflict
			}
			copy := command
			matched = &copy
		}
		if matched == nil {
			return ErrEvidenceConflict
		}
		result = PostbuildAmendmentPreparedCandidateWitness{Ref: ref, Version: version, Fence: fence, Project: project, Worktree: worktree, Verification: verification, Plan: plan, Builder: builderKey, BuilderArtifact: *parsed.Builder, Command: *matched, Commit: CommitObservation{CommitOID: facts.PreparedCommitOID, ParentOID: verification.Checkpoint.CommitOID, TreeOID: facts.PreparedTreeOID}, Claim: facts.Claim, EffectState: facts.Effect.State}
		found = true
		return nil
	}()
	return result, found && err == nil, err
}
