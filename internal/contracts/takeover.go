package contracts

import (
	"context"

	"github.com/nysa-company/sf/internal/domain"
)

// TakeoverInspection is the read-only, authenticated handback boundary. A
// registered worktree is reported only after its repository, worktree,
// branch, remotes, base, and filesystem identity have all been rechecked.
// Clean means Git reports no uncommitted operator changes. ChangeKind then
// distinguishes an existing durable checkpoint from an authenticated clean
// operator source commit or an unadopted commit.
type TakeoverInspection struct {
	Registered   bool     `json:"registered"`
	Path         string   `json:"path"`
	Branch       string   `json:"branch"`
	Repository   string   `json:"repository"`
	Origin       string   `json:"origin,omitempty"`
	PushOrigin   string   `json:"push_origin,omitempty"`
	BaseSHA      string   `json:"base_sha"`
	HeadSHA      string   `json:"head_sha"`
	Clean        bool     `json:"clean"`
	ChangeKind   string   `json:"change_kind"`
	ChangedFiles []string `json:"changed_files,omitempty"`

	RemoteCandidatePresent bool   `json:"remote_candidate_present"`
	RemoteCandidateSHA     string `json:"remote_candidate_sha,omitempty"`
	RemoteBaseSHA          string `json:"remote_base_sha,omitempty"`
	RemoteIdentityExact    bool   `json:"remote_identity_exact"`

	RetainedProofDigest  string `json:"retained_proof_digest,omitempty"`
	RetainedPolicyDigest string `json:"retained_policy_digest,omitempty"`
	RetainedVersion      uint64 `json:"retained_version,omitempty"`
	RetainedLeaderEpoch  uint64 `json:"retained_leader_epoch,omitempty"`
	RetainedRunnerEpoch  uint64 `json:"retained_runner_epoch,omitempty"`

	// SourceResumable means the inspector authenticated a clean, single-parent
	// operator source commit whose parent is the retained verification
	// checkpoint. Dirty edits are inspectable but are never executable input.
	SourceResumable bool                 `json:"source_resumable"`
	SourceCommit    OperatorSourceCommit `json:"source_commit,omitempty"`
}

// OperatorSourceChange is the complete canonical file identity of one
// operator-authored commit delta. Status is deliberately restricted by the
// Git observer to A, M, or D; renames and type changes fail closed.
type OperatorSourceChange struct {
	Status string `json:"status"`
	Path   string `json:"path"`
}

// OperatorSourceCommit binds the clean commit the operator asks sf to adopt.
// ParentOID is the retained verification checkpoint, never an arbitrary
// branch ancestor.
type OperatorSourceCommit struct {
	CommitOID string                 `json:"commit_oid"`
	ParentOID string                 `json:"parent_oid"`
	TreeOID   string                 `json:"tree_oid"`
	Changes   []OperatorSourceChange `json:"changes,omitempty"`
}

// TakeoverInspector deliberately has no mutation methods. The daemon uses it
// after draining a ticket and again before resuming it; neither the CLI nor a
// workflow worker can self-attest a handoff as clean.
type TakeoverInspector interface {
	InspectTakeover(context.Context, domain.TicketRef) (TakeoverInspection, error)
}
