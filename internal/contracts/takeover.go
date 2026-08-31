package contracts

import (
	"context"

	"github.com/nysa-company/sf/internal/domain"
)

// TakeoverInspection is the read-only, authenticated handback boundary. A
// registered worktree is reported only after its repository, worktree,
// branch, remotes, base, and filesystem identity have all been rechecked.
// Clean means the current head is an existing durable registration,
// verification, or candidate checkpoint and Git reports no uncommitted
// operator changes.
type TakeoverInspection struct {
	Registered   bool     `json:"registered"`
	Path         string   `json:"path"`
	Branch       string   `json:"branch"`
	Repository   string   `json:"repository"`
	BaseSHA      string   `json:"base_sha"`
	HeadSHA      string   `json:"head_sha"`
	Clean        bool     `json:"clean"`
	ChangeKind   string   `json:"change_kind"`
	ChangedFiles []string `json:"changed_files,omitempty"`
	// SourceResumable means the inspector authenticated a bounded set of
	// uncommitted source changes against the retained verification checkpoint.
	// It is intentionally not an adoption claim: the Builder must still emit a
	// new result and RecordCandidate must still authenticate that result.
	SourceResumable bool `json:"source_resumable"`
}

// TakeoverInspector deliberately has no mutation methods. The daemon uses it
// after draining a ticket and again before resuming it; neither the CLI nor a
// workflow worker can self-attest a handoff as clean.
type TakeoverInspector interface {
	InspectTakeover(context.Context, domain.TicketRef) (TakeoverInspection, error)
}
