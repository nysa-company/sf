package processsupervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
)

func TestRepositoryCommandDrainerFailsClosedOnUnclearIdentity(t *testing.T) {
	d := RepositoryCommandDrainer{}
	if err := d.DrainRepositoryCommand(context.Background(), contracts.RepositoryCommandLaunch{PID: 42, PGID: 42}); err == nil {
		t.Fatal("drainer accepted an identity without boot/start proofs")
	}
}

func TestRepositoryCommandWorktreeReplacementBetweenPreflightAndOpenRefuses(t *testing.T) {
	root := t.TempDir()
	worktree := filepath.Join(root, "worktree")
	replacement := filepath.Join(root, "replacement")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(replacement, 0o700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(worktree)
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	identity := gitboundary.Identity{Repository: root, RepositoryDev: 1, RepositoryIno: 1, Worktree: worktree, WorktreeDev: uint64(stat.Dev), WorktreeIno: uint64(stat.Ino), GitFile: worktree + "/.git", GitFileDev: 1, GitFileIno: 1, CommonDir: root + "/.git", CommonDirDev: 1, CommonDirIno: 1, Origin: "ssh://example.test/repo", PushOrigin: "/tmp/origin", PushOriginDev: 1, PushOriginIno: 1, BaseRef: "main", BaseHead: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", HeadRef: "branch", ConfigHash: "x", HooksHash: "y"}
	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	claim := contracts.RepositoryCommandClaim{TicketRef: domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "t"}, Repository: root, Worktree: worktree, WorktreeIdentity: string(raw), BaseRef: "main", BaseSHA: identity.BaseHead, Branch: "branch"}
	s := RepositoryCommandSupervisor{beforeWorktreeOpen: func() {
		if err := os.Rename(worktree, filepath.Join(root, "old")); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(replacement, worktree); err != nil {
			t.Fatal(err)
		}
	}}
	if opened, err := s.openAuthenticatedWorktree(claim, identity); err == nil {
		_ = opened.Close()
		t.Fatal("replacement worktree was accepted")
	}
}

func TestRepositoryCommandStagesExecutableBeforeFinalPathReplacement(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "tool")
	original := []byte("#!/bin/sh\necho original\n")
	if err := os.WriteFile(source, original, 0o700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(original)
	staged, err := stageExecutable(source, "sha256:"+hex.EncodeToString(sum[:]))
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(filepath.Dir(staged))
	if err := os.WriteFile(source, []byte("#!/bin/sh\necho replacement\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(staged)
	if err != nil || string(got) != string(original) {
		t.Fatalf("staged executable changed after source replacement: %q err=%v", got, err)
	}
}
