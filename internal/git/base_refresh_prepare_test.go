package git

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

type refreshPrepareAuthority struct {
	recordErr   error
	calls       int
	recordCalls int
	prepared    BaseRefreshPreparation
}
type refreshPrepareLease struct{ authority *refreshPrepareAuthority }
type refreshPrepareNoRecorderAuthority struct{}

func (refreshPrepareNoRecorderAuthority) AcquireGitMutation(context.Context, contracts.GitMutationClaim) (contracts.GitMutationLease, error) {
	return testMutationLease{}, nil
}

func (a *refreshPrepareAuthority) AcquireGitMutation(context.Context, contracts.GitMutationClaim) (contracts.GitMutationLease, error) {
	a.calls++
	return refreshPrepareLease{authority: a}, nil
}
func (refreshPrepareLease) Check(context.Context) error { return nil }
func (refreshPrepareLease) Release() error              { return nil }

func (l refreshPrepareLease) PreparedBaseRefresh(context.Context) (contracts.GitBaseRefreshPreparation, bool, error) {
	return l.authority.prepared, l.authority.prepared.CommitOID != "", nil
}

func (l refreshPrepareLease) RecordBaseRefreshPreparation(_ context.Context, commit, tree string, parents [2]string) error {
	l.authority.recordCalls++
	if l.authority.recordErr == nil {
		l.authority.prepared = BaseRefreshPreparation{CommitOID: commit, TreeOID: tree, Parents: parents}
	}
	return l.authority.recordErr
}

func TestPrepareProtectedBaseRefreshCreatesOnlyDeterministicObject(t *testing.T) {
	for _, crashPoint := range []string{"before_checkout", "after_checkout"} {
		t.Run(crashPoint, func(t *testing.T) {
			ctx, baseRunner, repository, remote := fixture(t)
			branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-refresh")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "worktree")
			worktree, err := baseRunner.CreateWorktree(ctx, repository, path, branch, "main", createClaim(t, repository, path, branch, "main"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(path, "src", "candidate.txt"), []byte("candidate\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			rawGit(t, path, "add", "src/candidate.txt")
			rawGit(t, path, "commit", "-m", "candidate")
			candidate := rawGit(t, path, "rev-parse", "HEAD")
			oldBase := worktree.Identity.BaseHead
			newBase := exactBaseProofCommit(t, repository, "base-refresh.txt", "new base\n", "new base")
			rawGit(t, repository, "push", "origin", "main")

			input := baseRefreshRunnerInput{Format: "sf.protected-base-refresh.v1", Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "project", Ticket: "SF-refresh"}, TicketVersion: 2, Fence: domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}, Repository: worktree.Identity.Repository, BaseRef: "main", NewBaseSHA: newBase, ProtectedPaths: []string{"src/verification.txt"}}
			input.Candidate.Snapshot.BaseSHA, input.Candidate.Snapshot.HeadSHA = oldBase, candidate
			input.Candidate.Snapshot.TreeSHA = rawGit(t, path, "rev-parse", "HEAD^{tree}")
			input.Worktree.Path, input.Worktree.Branch, input.Worktree.State, input.Worktree.BaseSHA = worktree.Path, worktree.Branch, "registered", oldBase
			input.Worktree.IdentityJSON, _ = json.Marshal(worktree.Identity)
			payload, _ := json.Marshal(input)
			sum := sha256.Sum256(payload)
			claim := createClaim(t, repository, path, branch, "main")
			claim.TicketRef, claim.TicketVersion, claim.LeaderEpoch, claim.RunnerEpoch = input.Ref, input.TicketVersion, input.Fence.LeaderEpoch, input.Fence.RunnerEpoch
			claim.Operation, claim.BaseRef, claim.ExpectedBaseOID, claim.ExpectedHeadOID = "refresh-base", "main", newBase, candidate
			claim.SemanticKey, claim.RequestDigest = "refresh/SF-refresh", "sha256:"+hex.EncodeToString(sum[:])
			authority := &refreshPrepareAuthority{}
			runner := baseRunner
			runner.MutationAuthority = authority
			beforeHead := rawGit(t, path, "rev-parse", "HEAD")
			beforeStatus := rawGit(t, path, "status", "--porcelain")
			prepared, err := runner.PrepareProtectedBaseRefresh(ctx, payload, claim)
			if err != nil {
				t.Fatal(err)
			}
			if prepared.Parents != [2]string{candidate, newBase} {
				t.Fatalf("parents=%v", prepared.Parents)
			}
			if got := rawGit(t, path, "show", prepared.CommitOID+":src/candidate.txt"); got != "candidate" {
				t.Fatalf("candidate content lost: %q", got)
			}
			if got := rawGit(t, path, "show", prepared.CommitOID+":src/base-refresh.txt"); got != "new base" {
				t.Fatalf("sibling content lost: %q", got)
			}
			if got := rawGit(t, path, "rev-parse", worktreeBaseRef(branch)); got != oldBase {
				t.Fatal("preparation changed pinned base")
			}
			if got := rawGit(t, path, "rev-parse", "HEAD"); got != beforeHead {
				t.Fatalf("HEAD changed: %s -> %s", beforeHead, got)
			}
			if got := rawGit(t, path, "status", "--porcelain"); got != beforeStatus {
				t.Fatalf("worktree changed: %q -> %q", beforeStatus, got)
			}
			if rawGit(t, path, "diff", "--cached", "--quiet") != "" {
				t.Fatal("index changed")
			}
			if got := rawGit(t, remote, "rev-parse", "refs/heads/main"); got != newBase {
				t.Fatalf("remote base=%s", got)
			}
			second, err := runner.PrepareProtectedBaseRefresh(ctx, payload, claim)
			if err != nil || second != prepared {
				t.Fatalf("replay=%+v err=%v", second, err)
			}
			if authority.calls != 2 {
				t.Fatalf("acquisitions=%d", authority.calls)
			}
			t.Run("apply and lost response replay", func(t *testing.T) {
				// A mismatched old base must roll back BOTH refs, not update the
				// candidate branch first and leave a half-applied refresh.
				badCAS, err := baseRefreshRefTransaction(branch, strings.Repeat("0", len(oldBase)), candidate, newBase, prepared.CommitOID)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := runner.commandEnvInputExpectedWithHandoff(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, nil, badCAS, nil, "update-ref", "--no-deref", "--stdin"); err == nil {
					t.Fatal("wrong expected-old base CAS succeeded")
				}
				if rawGit(t, path, "rev-parse", "HEAD") != candidate || rawGit(t, path, "rev-parse", worktreeBaseRef(branch)) != oldBase {
					t.Fatal("failed paired CAS changed a ref")
				}
				if crashPoint == "after_checkout" {
					// Simulate a process exit after successful checkout projection but
					// before the paired ref CAS. Only this exact prepared tree may resume.
					rawGit(t, path, "read-tree", "-u", "-m", candidate, prepared.CommitOID)
					if rawGit(t, path, "rev-parse", "HEAD") != candidate {
						t.Fatal("staged crash fixture advanced ref")
					}
				}
				applied, err := runner.ApplyProtectedBaseRefresh(ctx, payload, claim)
				if err != nil {
					t.Fatal(err)
				}
				if applied.Preparation != prepared || applied.Identity.BaseHead != newBase {
					t.Fatal("applied identity mismatch")
				}
				if rawGit(t, path, "rev-parse", "HEAD") != prepared.CommitOID || rawGit(t, path, "rev-parse", worktreeBaseRef(branch)) != newBase || rawGit(t, path, "status", "--porcelain") != "" {
					t.Fatal("refresh did not atomically expose clean prepared checkout")
				}
				replay, err := runner.ApplyProtectedBaseRefresh(ctx, payload, claim)
				if err != nil || replay != applied {
					t.Fatalf("lost response replay: %+v %v", replay, err)
				}
				// A foreign remote branch head must fail closed even after the
				// local refresh completed; no local projection may be overwritten.
				rawGit(t, remote, "update-ref", "refs/heads/"+branch, newBase)
				beforeHead, beforeBase, beforeStatus := rawGit(t, path, "rev-parse", "HEAD"), rawGit(t, path, "rev-parse", worktreeBaseRef(branch)), rawGit(t, path, "status", "--porcelain")
				if _, err := runner.ApplyProtectedBaseRefresh(ctx, payload, claim); err == nil {
					t.Fatal("foreign remote branch head accepted")
				}
				if rawGit(t, path, "rev-parse", "HEAD") != beforeHead || rawGit(t, path, "rev-parse", worktreeBaseRef(branch)) != beforeBase || rawGit(t, path, "status", "--porcelain") != beforeStatus || rawGit(t, path, "diff", "--cached", "--quiet") != "" {
					t.Fatal("foreign remote refusal changed local checkout")
				}
			})
		})
	}
}

func TestPrepareProtectedBaseRefreshRefusesTamperProtectedFilesAndRecorder(t *testing.T) {
	ctx, runner, repository, _ := fixture(t)
	branch, err := allocatorForTest().Allocate(ctx, domain.ChannelDev, "project", "SF-refresh-refuse")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worktree")
	worktree, err := runner.CreateWorktree(ctx, repository, path, branch, "main", createClaim(t, repository, path, branch, "main"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "src", "candidate.txt"), []byte("candidate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rawGit(t, path, "add", "src/candidate.txt")
	rawGit(t, path, "commit", "-m", "candidate")
	candidate, oldBase := rawGit(t, path, "rev-parse", "HEAD"), worktree.Identity.BaseHead
	newBase := exactBaseProofCommit(t, repository, "base-refresh.txt", "new\n", "new")
	rawGit(t, repository, "push", "origin", "main")
	makePayload := func(tip string, paths []string) ([]byte, contracts.GitMutationClaim) {
		in := baseRefreshRunnerInput{Format: "sf.protected-base-refresh.v1", Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "project", Ticket: "SF-refresh-refuse"}, TicketVersion: 2, Fence: domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}, Repository: worktree.Identity.Repository, BaseRef: "main", NewBaseSHA: tip, ProtectedPaths: paths}
		in.Candidate.Snapshot.BaseSHA, in.Candidate.Snapshot.HeadSHA, in.Candidate.Snapshot.TreeSHA = oldBase, candidate, rawGit(t, path, "rev-parse", "HEAD^{tree}")
		in.Worktree.Path, in.Worktree.Branch, in.Worktree.State, in.Worktree.BaseSHA = worktree.Path, worktree.Branch, "registered", oldBase
		in.Worktree.IdentityJSON, _ = json.Marshal(worktree.Identity)
		p, _ := json.Marshal(in)
		sum := sha256.Sum256(p)
		c := createClaim(t, repository, path, branch, "main")
		c.TicketRef, c.TicketVersion, c.LeaderEpoch, c.RunnerEpoch, c.Operation, c.BaseRef, c.ExpectedBaseOID, c.ExpectedHeadOID, c.SemanticKey, c.RequestDigest = in.Ref, 2, 1, 1, "refresh-base", "main", tip, candidate, "refresh/refuse", "sha256:"+hex.EncodeToString(sum[:])
		return p, c
	}
	payload, claim := makePayload(newBase, []string{"src/verification.txt"})
	authority := &refreshPrepareAuthority{recordErr: errors.New("recorder failed")}
	runner.MutationAuthority = authority
	if _, err := runner.PrepareProtectedBaseRefresh(ctx, payload, claim); !errors.Is(err, authority.recordErr) || authority.recordCalls != 1 {
		t.Fatalf("recorder failure not reached: calls=%d error=%v", authority.recordCalls, err)
	}
	payload, claim = makePayload(newBase, []string{"src/verification.txt"})
	payload = append(payload, ' ')
	if _, err := runner.PrepareProtectedBaseRefresh(ctx, payload, claim); err == nil {
		t.Fatal("tampered payload accepted")
	}
	if authority.calls != 1 {
		t.Fatalf("tamper acquired lease: %d", authority.calls)
	}
	newBase = exactBaseProofCommit(t, repository, "base-refresh.txt", "changed\n", "changed")
	rawGit(t, repository, "push", "origin", "main")
	payload, claim = makePayload(newBase, []string{"src/base-refresh.txt"})
	noRecorder := refreshPrepareAuthority{}
	runner.MutationAuthority = &noRecorder
	if _, err := runner.PrepareProtectedBaseRefresh(ctx, payload, claim); !errors.Is(err, ErrUnsafeWorktree) {
		t.Fatalf("protected-file change err=%v", err)
	}
	if noRecorder.prepared.CommitOID != "" {
		t.Fatal("protected-file refusal recorded preparation")
	}
	runner.MutationAuthority = refreshPrepareNoRecorderAuthority{}
	if _, err := runner.PrepareProtectedBaseRefresh(ctx, payload, claim); err == nil {
		t.Fatal("missing recorder accepted")
	}
}
