package worktreecoord

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/git"
)

// A hosted merge advances the remote without updating the operator's primary
// checkout. The next ticket must include that merge without rewriting the
// operator's branch. All repositories here are private temporary fixtures.
func TestNewTicketUsesRemoteBaseAfterHostedMerge(t *testing.T) {
	f := setupCoordinator(t, "SF-next-after-hosted-merge")
	oldBase := mustGit(t, f.project.Path, "rev-parse", "main")
	remote := mustGit(t, f.project.Path, "remote", "get-url", "origin")
	hostedCheckout := filepath.Join(t.TempDir(), "hosted")
	mustGit(t, filepath.Dir(hostedCheckout), "clone", "--branch", "main", remote, hostedCheckout)
	mustGit(t, hostedCheckout, "config", "user.name", "hosted-fixture")
	mustGit(t, hostedCheckout, "config", "user.email", "hosted@example.test")
	if err := os.WriteFile(filepath.Join(hostedCheckout, "src", "previous-ticket.txt"), []byte("previous ticket delivered\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mustGit(t, hostedCheckout, "add", "src/previous-ticket.txt")
	mustGit(t, hostedCheckout, "commit", "-m", "previous ticket hosted merge")
	mustGit(t, hostedCheckout, "push", "origin", "main:refs/heads/main")
	newBase := mustGit(t, hostedCheckout, "rev-parse", "HEAD")
	if newBase == oldBase || mustGit(t, f.project.Path, "rev-parse", "main") != oldBase {
		t.Fatal("fixture did not preserve a stale primary checkout after hosted merge")
	}

	registered, err := coordinatorFor(f).Ensure(context.Background(), f.request)
	if err != nil {
		t.Fatalf("create next ticket from fresh protected base: %v", err)
	}
	if registered.BaseSHA != newBase || registered.HeadSHA != newBase {
		t.Fatalf("next ticket bound stale local base: base=%s head=%s; remote=%s local=%s", registered.BaseSHA, registered.HeadSHA, newBase, oldBase)
	}
	if got := mustGit(t, f.project.Path, "rev-parse", "main"); got != oldBase {
		t.Fatalf("factory rewrote operator primary branch: got=%s want=%s", got, oldBase)
	}
	if _, err := os.Stat(filepath.Join(registered.Path, "src", "previous-ticket.txt")); err != nil {
		t.Fatalf("next ticket omitted previous delivered work: %v", err)
	}
	if err := f.runner.ValidateDiff(context.Background(), registered.Path, f.project.BaseRef, git.DiffPolicy{AllowedPaths: []string{"src/next-ticket.txt"}}); err != nil {
		t.Fatalf("prior delivered files were incorrectly counted as this ticket's changes: %v", err)
	}
	// Remote movement after registration cannot silently rebase this ticket.
	// Its own base must remain pinned across ordinary Ensure/restart replay.
	mustGit(t, hostedCheckout, "commit", "--allow-empty", "-m", "later hosted merge")
	mustGit(t, hostedCheckout, "push", "origin", "main:refs/heads/main")
	replayed, err := coordinatorFor(f).Ensure(context.Background(), f.request)
	if err != nil || replayed.BaseSHA != newBase || !bytes.Equal(replayed.IdentityJSON, registered.IdentityJSON) {
		t.Fatalf("registered ticket drifted after later hosted merge: %+v %v", replayed, err)
	}
	pinnedRef := mustGit(t, f.project.Path, "for-each-ref", "--format=%(refname)", "refs/sf/worktree-base/")
	mustGit(t, f.project.Path, "update-ref", pinnedRef, oldBase)
	if _, err := coordinatorFor(f).Ensure(context.Background(), f.request); err == nil {
		t.Fatal("tampered pinned base was accepted as the registered worktree")
	}
}
