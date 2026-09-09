package git

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const retainedWorktreeMaxBytes = 8 << 20
const retainedWorktreeMaxFileBytes = 1 << 20

// RetainedWorktreeInspection is an observation, not permission to launch a
// writer. The caller must bind Changes.Head and Digest to its Store authority.
type RetainedWorktreeInspection struct {
	Changes WorktreeChanges
	Digest  string
}

type retainedFile struct {
	Path    string
	Deleted bool
	Mode    uint32
	Size    int64
	Digest  string
}

type retainedSnapshot struct {
	Schema   string
	Identity Identity
	Changes  WorktreeChanges
	Staged   []byte
	Unstaged []byte
	Files    []retainedFile
}

// InspectRetainedWorktree fingerprints bounded retained edits without changing
// the index, refs, or files. Two complete observations detect ordinary concurrent
// edits; this is not containment against a hostile same-UID process. HEAD is
// included in the digest, but its expected value is authenticated by the caller.
func (r Runner) InspectRetainedWorktree(ctx context.Context, worktree Worktree) (RetainedWorktreeInspection, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	root, err := openPinnedDirectory(worktree.Path)
	if err != nil {
		return RetainedWorktreeInspection{}, err
	}
	defer root.Close()
	if root.dev() != worktree.Identity.WorktreeDev || root.ino() != worktree.Identity.WorktreeIno {
		return RetainedWorktreeInspection{}, ErrIdentityMismatch
	}
	first, err := r.retainedSnapshot(ctx, worktree, root)
	if err != nil {
		return RetainedWorktreeInspection{}, err
	}
	second, err := r.retainedSnapshot(ctx, worktree, root)
	if err != nil {
		return RetainedWorktreeInspection{}, err
	}
	if !reflect.DeepEqual(first, second) {
		return RetainedWorktreeInspection{}, ErrUnsafeWorktree
	}
	if err := root.verify(); err != nil {
		return RetainedWorktreeInspection{}, err
	}
	if err := r.InspectWorktree(ctx, worktree); err != nil {
		return RetainedWorktreeInspection{}, err
	}
	if err := ctx.Err(); err != nil {
		return RetainedWorktreeInspection{}, err
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		return RetainedWorktreeInspection{}, err
	}
	sum := sha256.Sum256(encoded)
	return RetainedWorktreeInspection{Changes: first.Changes, Digest: "sha256:" + hex.EncodeToString(sum[:])}, nil
}

func (r Runner) retainedSnapshot(ctx context.Context, worktree Worktree, root *pinnedDirectory) (retainedSnapshot, error) {
	var result retainedSnapshot
	changes, err := r.InspectWorktreeChanges(ctx, worktree)
	if err != nil {
		return result, err
	}
	read := func(args ...string) ([]byte, error) {
		return r.commandExpected(ctx, worktree.Path, worktree.Identity.WorktreeDev, worktree.Identity.WorktreeIno, args...)
	}
	ignored, err := read("ls-files", "--others", "--ignored", "--exclude-standard", "-z")
	if err != nil {
		return result, err
	}
	if len(ignored) != 0 || len(changes.Paths) > 256 {
		return result, ErrUnsafeWorktree
	}
	result.Schema, result.Identity, result.Changes = "sf.retained-worktree/v1", worktree.Identity, changes
	// Existing command output bounds apply independently to each diff.
	result.Staged, err = read("diff", "--cached", "--binary", "--no-ext-diff", "--no-textconv", "--no-renames", "--")
	if err != nil {
		return result, err
	}
	result.Unstaged, err = read("diff", "--binary", "--no-ext-diff", "--no-textconv", "--no-renames", "--")
	if err != nil {
		return result, err
	}
	remaining := retainedWorktreeMaxBytes - len(result.Staged) - len(result.Unstaged)
	for _, path := range changes.Paths {
		file, err := readRetainedFile(ctx, int(root.file.Fd()), path, &remaining)
		if err != nil {
			return result, err
		}
		result.Files = append(result.Files, file)
	}
	after, err := r.InspectWorktreeChanges(ctx, worktree)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(changes, after) {
		return result, ErrUnsafeWorktree
	}
	for _, check := range []struct {
		argv []string
		want []byte
	}{
		{[]string{"ls-files", "--others", "--ignored", "--exclude-standard", "-z"}, ignored},
		{[]string{"diff", "--cached", "--binary", "--no-ext-diff", "--no-textconv", "--no-renames", "--"}, result.Staged},
		{[]string{"diff", "--binary", "--no-ext-diff", "--no-textconv", "--no-renames", "--"}, result.Unstaged},
	} {
		got, err := read(check.argv...)
		if err != nil {
			return result, err
		}
		if !bytes.Equal(got, check.want) {
			return result, ErrUnsafeWorktree
		}
	}
	if err := root.verify(); err != nil {
		return result, err
	}
	return result, nil
}

// Resolve every component below an authenticated root descriptor. Missing
// components represent deletions; symlinks, devices, FIFOs and directories are
// refused. No source bytes are exposed in the returned observation or errors.
func readRetainedFile(ctx context.Context, root int, path string, remaining *int) (retainedFile, error) {
	result := retainedFile{Path: path}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if !validRepoPath(path) || remaining == nil || *remaining < 0 {
		return result, ErrUnsafeWorktree
	}
	parent := root
	var opened []int
	defer func() {
		for _, fd := range opened {
			_ = unix.Close(fd)
		}
	}()
	parts := strings.Split(path, "/")
	for index, part := range parts {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK
		if index < len(parts)-1 {
			flags |= unix.O_DIRECTORY
		}
		fd, err := unix.Openat(parent, part, flags, 0)
		if errors.Is(err, unix.ENOENT) {
			result.Deleted = true
			return result, nil
		}
		if err != nil {
			return result, ErrUnsafeWorktree
		}
		if index < len(parts)-1 {
			opened = append(opened, fd)
			parent = fd
			continue
		}
		file := os.NewFile(uintptr(fd), "retained-source")
		defer file.Close()
		before, err := file.Stat()
		if err != nil || !before.Mode().IsRegular() || before.Size() > retainedWorktreeMaxFileBytes || before.Size() > int64(*remaining) {
			return result, ErrUnsafeWorktree
		}
		data, err := io.ReadAll(io.LimitReader(file, before.Size()+1))
		if err != nil || int64(len(data)) != before.Size() {
			return result, ErrUnsafeWorktree
		}
		if err := ctx.Err(); err != nil {
			return result, err
		}
		after, err := file.Stat()
		var named, current unix.Stat_t
		if err != nil || !samePathIdentity(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) || unix.Fstat(fd, &current) != nil || unix.Fstatat(parent, part, &named, unix.AT_SYMLINK_NOFOLLOW) != nil || current.Dev != named.Dev || current.Ino != named.Ino || current.Mode != named.Mode {
			return result, ErrUnsafeWorktree
		}
		*remaining -= len(data)
		sum := sha256.Sum256(data)
		result.Mode, result.Size, result.Digest = uint32(before.Mode()), before.Size(), hex.EncodeToString(sum[:])
		return result, nil
	}
	return result, ErrUnsafeWorktree
}
