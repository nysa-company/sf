package store

// Protected-base refresh has a separate evidence lineage from red-CI repair.
// These canonical payloads are structural building blocks, not launch or
// transition authority. The eventual Store transaction must authenticate every
// referenced row before persisting them; parsing a payload never grants a lease.

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/nysa-company/sf/internal/domain"
)

const maxBaseRefreshPayload = 128 << 10

type protectedBaseRefreshIntent struct {
	Format               string
	Ref                  domain.TicketRef
	TicketVersion        uint64
	Fence                domain.Fence
	SourceDigest         string
	ConfigGeneration     uint64
	ConfigDigest         string
	ConfigSnapshotDigest string
	Repository           string
	BaseRef              string
	Candidate            StoredCandidate
	Worktree             StoredWorktree
	NewBaseSHA           string
	BaseProofSemanticKey string
	ProtectedPaths       []string
}

func canonicalProtectedBaseRefreshIntent(value protectedBaseRefreshIntent) ([]byte, string, error) {
	candidate := value.Candidate
	if value.Format != "sf.protected-base-refresh.v1" || value.Ref.Validate() != nil ||
		value.TicketVersion == 0 || value.TicketVersion == ^uint64(0) ||
		value.Fence.LeaderEpoch == 0 || value.Fence.RunnerEpoch == 0 || value.Fence.ClaimEpoch != 0 ||
		value.ConfigGeneration == 0 || !validDigest(value.SourceDigest) || !validDigest(value.ConfigDigest) || !validDigest(value.ConfigSnapshotDigest) ||
		!validStorePath(value.Repository) || value.BaseRef == "" || len(value.BaseRef) > 1024 ||
		value.BaseProofSemanticKey == "" || len(value.BaseProofSemanticKey) > 1024 ||
		candidate.Snapshot.Generation == 0 || candidate.Snapshot.Generation == ^uint64(0) ||
		candidate.TicketVersion == 0 || candidate.TicketVersion >= value.TicketVersion ||
		candidate.Fence.LeaderEpoch == 0 || candidate.Fence.RunnerEpoch == 0 ||
		candidate.Snapshot.SourceDigest != value.SourceDigest ||
		!validGitOIDWidth(candidate.Snapshot.BaseSHA, candidate.Snapshot.HeadSHA, candidate.Snapshot.TreeSHA, value.NewBaseSHA) ||
		candidate.Snapshot.HeadSHA == "" || candidate.Snapshot.TreeSHA == "" || value.NewBaseSHA == "" ||
		value.NewBaseSHA == candidate.Snapshot.BaseSHA || value.NewBaseSHA == candidate.Snapshot.HeadSHA ||
		candidate.Commit.CommitOID != candidate.Snapshot.HeadSHA || candidate.Commit.TreeOID != candidate.Snapshot.TreeSHA ||
		!validStoreOID(candidate.Commit.ParentOID) || len(candidate.Commit.ParentOID) != len(candidate.Snapshot.BaseSHA) ||
		candidate.BuilderResult.Ref != value.Ref || candidate.BuilderResult.Phase != domain.PhaseBuild || candidate.BuilderResult.AttemptID <= 0 || candidate.BuilderResult.Attempt <= 0 ||
		value.Worktree.State != "registered" || value.Worktree.BaseSHA != candidate.Snapshot.BaseSHA ||
		!validRepositoryWorktreeIdentity(string(value.Worktree.IdentityJSON), value.Repository, value.Worktree.Path, value.Worktree.Branch, value.BaseRef, value.Worktree.BaseSHA) || validOwnedFiles(value.ProtectedPaths) != nil {
		return nil, "", ErrEvidenceConflict
	}
	for _, digest := range []string{candidate.Snapshot.VerificationIntentDigest, candidate.Snapshot.ProofDigest, candidate.Snapshot.CommandPolicyDigest, candidate.Snapshot.BuilderEvidenceDigest} {
		if !validDigest(digest) {
			return nil, "", ErrEvidenceConflict
		}
	}
	payload, err := json.Marshal(value)
	if err != nil || len(payload) > maxBaseRefreshPayload {
		return nil, "", ErrEvidenceConflict
	}
	return payload, ciAuthorityDigest(payload), nil
}

func decodeProtectedBaseRefreshIntent(payload []byte, digest string) (protectedBaseRefreshIntent, error) {
	var value protectedBaseRefreshIntent
	if len(payload) == 0 || len(payload) > maxBaseRefreshPayload || !validCIAuthorityDigest(digest) || ciAuthorityDigest(payload) != digest {
		return value, ErrEvidenceConflict
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF {
		return protectedBaseRefreshIntent{}, ErrEvidenceConflict
	}
	canonical, expectedDigest, err := canonicalProtectedBaseRefreshIntent(value)
	if err != nil || expectedDigest != digest || !bytes.Equal(canonical, payload) {
		return protectedBaseRefreshIntent{}, ErrEvidenceConflict
	}
	return value, nil
}
