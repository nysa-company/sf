package publication_test

import (
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/publication"
)

// Model a sibling's hosted merge after this ticket's immutable candidate was
// built. This is a real private bare remote, not a fabricated base observation.
// Refusal must remain mutation-free on every scheduler retry; it is not proof
// that the unfinished ticket can automatically refresh and finish.
func TestSharedBaseMovementNeverPublishesStaleCandidateOnRetry(t *testing.T) {
	f := newPublicationFixture(t)
	defer f.close()
	hosted := filepath.Join(t.TempDir(), "hosted")
	runGit(t, filepath.Dir(hosted), "clone", "--branch", "main", f.bare, hosted)
	moved := makeCommit(t, hosted, "sibling ticket merged", "sibling delivery\n")
	runGit(t, hosted, "push", "origin", "main:refs/heads/main")
	if moved == f.candidate.Snapshot.BaseSHA {
		t.Fatal("fixture did not advance protected base")
	}
	worker := publication.Worker{Store: f.db, Git: f.runner, GitHub: f.github}
	for attempt := 0; attempt < 3; attempt++ {
		if result, err := worker.Run(f.ctx, f.ref, f.fence); err == nil || result.Transitioned {
			t.Fatalf("retry %d accepted stale base: result=%+v err=%v", attempt, result, err)
		}
		if f.gitPushCount != 0 || f.github.MutationCount("pr_create") != 0 {
			t.Fatalf("retry %d mutated stale candidate: pushes=%d creates=%d", attempt, f.gitPushCount, f.github.MutationCount("pr_create"))
		}
	}
	ticket, err := f.db.Ticket(f.ctx, f.ref)
	if err != nil || ticket.State != domain.StatePublishing {
		t.Fatalf("unexpected ticket after refusal: %+v %v", ticket, err)
	}
	candidate, err := f.db.RecoverableCandidate(f.ctx, f.ref)
	if err != nil || candidate.Snapshot != f.candidate.Snapshot {
		t.Fatalf("refusal rewrote immutable candidate: %+v %v", candidate, err)
	}
}
