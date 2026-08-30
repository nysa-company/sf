package git

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
)

type observerCountingAuthority struct{ calls int }

func (a *observerCountingAuthority) AcquireGitMutation(context.Context, contracts.GitMutationClaim) (contracts.GitMutationLease, error) {
	a.calls++
	return nil, errors.New("observer must not acquire mutation authority")
}

func observerWorktree(t *testing.T) (context.Context, Runner, Worktree, string) {
	t.Helper()
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, "dev", "project", "SF-observer")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main", createClaim(t, repository, path, branch, "main"))
	if err != nil {
		t.Fatal(err)
	}
	return ctx, runner, worktree, repository
}

func TestObserveCommitReturnsExactSingleParentAndTree(t *testing.T) {
	ctx, runner, worktree, _ := observerWorktree(t)
	if err := os.WriteFile(filepath.Join(worktree.Path, "src", "candidate.txt"), []byte("candidate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rawGit(t, worktree.Path, "add", "--", "src/candidate.txt")
	rawGit(t, worktree.Path, "commit", "-m", "candidate")
	wantCommit := rawGit(t, worktree.Path, "rev-parse", "HEAD^{commit}")
	wantParent := rawGit(t, worktree.Path, "rev-parse", "HEAD^1")
	wantTree := rawGit(t, worktree.Path, "rev-parse", "HEAD^{tree}")

	got, err := runner.ObserveCommit(ctx, worktree)
	if err != nil {
		t.Fatal(err)
	}
	if got.CommitOID != wantCommit || got.ParentOID != wantParent || got.TreeOID != wantTree {
		t.Fatalf("observation=%+v want commit=%s parent=%s tree=%s", got, wantCommit, wantParent, wantTree)
	}
}

func observerInjectedRunner(t *testing.T, worktree Worktree, commit, parent, tree string, finalCommit string) (Runner, *observerCountingAuthority, *[][]string, *bool) {
	t.Helper()
	base := Runner{Home: filepath.Join(t.TempDir(), "git-home"), ExecHelper: testExecHelper, TestLocalTransport: true}
	config, err := base.command(context.Background(), worktree.Path, "config", "--null", "--list", "--show-origin")
	if err != nil {
		t.Fatal(err)
	}
	configKeys, err := base.command(context.Background(), worktree.Path, "config", "--null", "--name-only", "--list", "--show-origin")
	if err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	head := []string{commit, finalCommit}
	identityPasses := 0
	moveDuringFinalReauth := false
	base.Run = func(_ context.Context, _ string, argv, _ []string) ([]byte, error) {
		calls = append(calls, append([]string(nil), argv...))
		suffix := func(want ...string) bool {
			if len(argv) < len(want) {
				return false
			}
			for i := range want {
				if argv[len(argv)-len(want)+i] != want[i] {
					return false
				}
			}
			return true
		}
		switch {
		case suffix("rev-parse", "--show-toplevel"):
			identityPasses++
			if identityPasses == 2 && moveDuringFinalReauth {
				head[0] = strings.Repeat("f", 40)
			}
			return []byte(worktree.Path + "\n"), nil
		case suffix("rev-parse", "--path-format=absolute", "--git-common-dir"):
			return []byte(worktree.Identity.CommonDir + "\n"), nil
		case suffix("remote", "get-url", "origin"):
			return []byte(worktree.Identity.Origin + "\n"), nil
		case suffix("remote", "get-url", "--all", "--push", "origin"):
			return []byte(worktree.Identity.PushOrigin + "\n"), nil
		case suffix("rev-parse", "--verify", worktree.Identity.BaseRef+"^{commit}"):
			return []byte(worktree.Identity.BaseHead + "\n"), nil
		case suffix("symbolic-ref", "--quiet", "--short", "HEAD"):
			return []byte(worktree.Branch + "\n"), nil
		case suffix("config", "--null", "--list", "--show-origin"):
			return config, nil
		case suffix("config", "--null", "--name-only", "--list", "--show-origin"):
			return configKeys, nil
		case suffix("submodule", "status", "--recursive"), suffix("for-each-ref", "--format=%(refname)", "refs/replace"):
			return nil, nil
		case suffix("rev-parse", "--verify", "HEAD^{commit}"):
			if len(head) == 0 {
				return nil, errors.New("unexpected extra HEAD read")
			}
			value := head[0]
			head = head[1:]
			return []byte(value + "\n"), nil
		case suffix("rev-list", "--parents", "-n", "1", "HEAD"):
			return []byte(commit + " " + parent + "\n"), nil
		case suffix("rev-parse", "--verify", commit+"^{tree}"):
			return []byte(tree + "\n"), nil
		default:
			return nil, fmt.Errorf("unexpected injected git argv: %v", argv)
		}
	}
	authority := &observerCountingAuthority{}
	base.MutationAuthority = authority
	return base, authority, &calls, &moveDuringFinalReauth
}

func TestObserveCommitInjectedRunnerUsesReadOnlyExactArgvAndFreshHead(t *testing.T) {
	ctx, realRunner, worktree, _ := observerWorktree(t)
	commit := strings.Repeat("c", 40)
	parent := strings.Repeat("d", 40)
	tree := strings.Repeat("e", 40)
	runner, authority, calls, _ := observerInjectedRunner(t, worktree, commit, parent, tree, commit)
	worktree.Identity.ConfigHash = realRunnerIdentityConfigHash(t, realRunner, worktree)
	got, err := runner.ObserveCommit(ctx, worktree)
	if err != nil {
		t.Fatal(err)
	}
	if got != (CommitObservation{CommitOID: commit, ParentOID: parent, TreeOID: tree}) {
		t.Fatalf("observation=%+v", got)
	}
	if authority.calls != 0 {
		t.Fatalf("mutation authority acquired %d times", authority.calls)
	}
	wantCommands := [][]string{
		{"rev-parse", "--verify", "HEAD^{commit}"},
		{"rev-list", "--parents", "-n", "1", "HEAD"},
		{"rev-parse", "--verify", commit + "^{tree}"},
	}
	for _, want := range wantCommands {
		found := false
		for _, argv := range *calls {
			if len(argv) >= len(want) {
				match := true
				for i := range want {
					match = match && argv[len(argv)-len(want)+i] == want[i]
				}
				if match {
					found = true
				}
			}
		}
		if !found {
			t.Fatalf("exact read argv %v not observed in %v", want, *calls)
		}
	}
	for _, argv := range *calls {
		for _, forbidden := range []string{"add", "write-tree", "commit-tree", "update-ref", "push", "fetch"} {
			for _, arg := range argv {
				if arg == forbidden {
					t.Fatalf("mutating argv observed: %v", argv)
				}
			}
		}
	}
	other := strings.Repeat("f", 40)
	staleRunner, _, _, _ := observerInjectedRunner(t, worktree, commit, parent, tree, other)
	if _, err := staleRunner.ObserveCommit(ctx, worktree); !errors.Is(err, ErrUnexpectedRemote) {
		t.Fatalf("HEAD movement accepted: %v", err)
	}
	movingRunner, _, _, moveDuringReauth := observerInjectedRunner(t, worktree, commit, parent, tree, commit)
	*moveDuringReauth = true
	if _, err := movingRunner.ObserveCommit(ctx, worktree); !errors.Is(err, ErrUnexpectedRemote) {
		t.Fatalf("HEAD movement during final reauthentication accepted: %v", err)
	}
}

func realRunnerIdentityConfigHash(t *testing.T, runner Runner, worktree Worktree) string {
	t.Helper()
	config, err := runner.command(context.Background(), worktree.Path, "config", "--null", "--list", "--show-origin")
	if err != nil {
		t.Fatal(err)
	}
	return digest(config)
}

func TestObserveCommitRejectsRootAndMerge(t *testing.T) {
	t.Run("root", func(t *testing.T) {
		ctx, runner, worktree, _ := observerWorktree(t)
		if _, err := runner.ObserveCommit(ctx, worktree); err == nil {
			t.Fatal("root commit accepted")
		}
	})
	t.Run("merge", func(t *testing.T) {
		ctx, runner, worktree, _ := observerWorktree(t)
		tree := rawGit(t, worktree.Path, "rev-parse", "HEAD^{tree}")
		base := worktree.Identity.BaseHead
		first := rawGit(t, worktree.Path, "commit-tree", tree, "-p", base, "-m", "first")
		second := rawGit(t, worktree.Path, "commit-tree", tree, "-p", base, "-m", "second")
		merge := rawGit(t, worktree.Path, "commit-tree", tree, "-p", first, "-p", second, "-m", "merge")
		rawGit(t, worktree.Path, "update-ref", "refs/heads/"+worktree.Branch, merge, base)
		if _, err := runner.ObserveCommit(ctx, worktree); err == nil {
			t.Fatal("merge commit accepted")
		}
	})
}

func TestObserveRemoteBranchAbsencePresenceAndAuthority(t *testing.T) {
	ctx, runner, worktree, repository := observerWorktree(t)
	authority := &observerCountingAuthority{}
	runner.MutationAuthority = authority

	missing, err := runner.ObserveRemoteBranch(ctx, worktree, worktree.Identity.Origin, worktree.Branch)
	if err != nil {
		t.Fatal(err)
	}
	if missing.Branch != worktree.Branch || missing.OID != "" {
		t.Fatalf("absence observation=%+v", missing)
	}
	rawGit(t, repository, "push", "origin", worktree.Branch+":refs/heads/"+worktree.Branch)
	want := rawGit(t, repository, "rev-parse", "refs/heads/"+worktree.Branch)
	present, err := runner.ObserveRemoteBranch(ctx, worktree, worktree.Identity.Origin, worktree.Branch)
	if err != nil {
		t.Fatal(err)
	}
	if present.OID != want {
		t.Fatalf("presence observation=%+v want oid=%s", present, want)
	}
	if authority.calls != 0 {
		t.Fatalf("observer acquired mutation authority %d times", authority.calls)
	}
}

func TestObserveRemoteBranchRejectsUnboundBranchAndOrigin(t *testing.T) {
	ctx, runner, worktree, _ := observerWorktree(t)
	otherBranch := "sf/dev/0123456789abcdef/0123456789abcdef-0123456789abcdef0123456789abcdef"
	if _, err := runner.ObserveRemoteBranch(ctx, worktree, worktree.Identity.Origin, otherBranch); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("branch mismatch error=%v", err)
	}
	if _, err := runner.ObserveRemoteBranch(ctx, worktree, "https://github.com/other/repository.git", worktree.Branch); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("origin mismatch error=%v", err)
	}
}

func TestSafeOriginRejectsCredentialBearingSSHOrigin(t *testing.T) {
	_, err := safeOrigin("ssh://git:secret@ssh.github.com:443/owner/repository.git")
	if !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("credential-bearing SSH origin accepted: %v", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("SSH credential leaked in error: %v", err)
	}
}

func TestObserverParsersRejectMalformedAndAcceptCanonicalShapes(t *testing.T) {
	oid40 := strings.Repeat("a", 40)
	oid64 := strings.Repeat("b", 64)
	for _, tc := range []struct {
		name string
		data string
		want string
	}{
		{"sha1", oid40 + "\n", oid40},
		{"sha256", oid64, oid64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSingleOID([]byte(tc.data), "commit")
			if err != nil || got != tc.want {
				t.Fatalf("oid=%q err=%v", got, err)
			}
		})
	}
	for _, data := range []string{
		oid40 + "\n",
		oid40 + " " + oid40 + " " + oid40 + "\n",
		oid40 + " " + oid64 + "\n",
		oid40 + "  " + oid40 + "\n",
	} {
		if _, _, err := parseCommitParents([]byte(data)); err == nil {
			t.Fatalf("malformed parent output accepted: %q", data)
		}
	}
	if _, _, err := parseCommitParents([]byte(oid64 + " " + oid64 + "\n")); err != nil {
		t.Fatalf("sha-256 parent output rejected: %v", err)
	}
	branch := "sf/dev/0123456789abcdef/0123456789abcdef-0123456789abcdef0123456789abcdef"
	row := oid40 + "\trefs/heads/" + branch + "\n"
	if got, err := parseRemoteBranchOutput([]byte(row), branch); err != nil || got != oid40 {
		t.Fatalf("remote row=%q err=%v", got, err)
	}
	if got, err := parseRemoteBranchOutput(nil, branch); err != nil || got != "" {
		t.Fatalf("remote absence=%q err=%v", got, err)
	}
	for _, data := range []string{
		"\n",
		row + row,
		oid40 + "\trefs/heads/" + branch + "^{},\n",
		oid40 + "\trefs/heads/other\n",
		oid40 + "\trefs/heads/" + branch + " extra\n",
	} {
		if _, err := parseRemoteBranchOutput([]byte(data), branch); err == nil {
			t.Fatalf("malformed remote output accepted: %q", data)
		}
	}
}
