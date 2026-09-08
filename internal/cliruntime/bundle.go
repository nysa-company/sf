// Package cliruntime authenticates immutable launch snapshots for the supported
// Claude and Cursor installations. It never executes a provider or reads auth.
package cliruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"syscall"
	"time"
)

const (
	MaxEntries           = 4096
	MaxFileBytes   int64 = 256 << 20
	MaxBundleBytes int64 = 1 << 30
	MaxDuration          = 30 * time.Second
)

var ErrBundle = errors.New("CLI runtime bundle is unavailable or changed")

type member struct {
	Name       string
	Directory  bool
	Executable bool
	Size       int64
	Digest     string
}

// Bundle fields are private so callers cannot add unqualified members or change
// a source path under an old digest. Digest covers kind, relative names, modes,
// sizes and bytes; it deliberately excludes location so a stage matches source.
type Bundle struct {
	kind, root, entry, digest string
	members                   []member
}

func (b Bundle) Digest() string     { return b.digest }
func (b Bundle) Kind() string       { return b.kind }
func (b Bundle) Executable() string { return filepath.Join(b.root, b.entry) }

// Resolve accepts a PATH symlink only at initial selection. Every resolved
// ancestor and every member must be trusted and non-symlinked. Cursor's entire
// version directory is bound, including native modules, JS chunks and helpers.
func Resolve(ctx context.Context, kind, executable string) (Bundle, error) {
	ctx, cancel := context.WithTimeout(ctx, MaxDuration)
	defer cancel()
	if !cleanAbsolute(executable) {
		return Bundle{}, ErrBundle
	}
	path, err := filepath.EvalSymlinks(executable)
	if err != nil || privateParents(filepath.Dir(path)) != nil {
		return Bundle{}, ErrBundle
	}
	root, entry := filepath.Dir(path), filepath.Base(path)
	if kind != "claude" && kind != "cursor" || kind == "cursor" && entry != "cursor-agent" {
		return Bundle{}, ErrBundle
	}
	return scan(ctx, kind, root, entry)
}

func scan(ctx context.Context, kind, root, entry string) (Bundle, error) {
	b := Bundle{kind: kind, root: root, entry: entry}
	var total int64
	add := func(path, name string, info os.FileInfo) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if len(b.members) >= MaxEntries || !trusted(info) || info.Mode().Perm()&0022 != 0 || info.Mode()&os.ModeSymlink != 0 {
			return ErrBundle
		}
		m := member{Name: name, Directory: info.IsDir(), Executable: info.Mode().Perm()&0111 != 0}
		if !info.IsDir() {
			if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > MaxFileBytes || info.Size() > MaxBundleBytes-total {
				return ErrBundle
			}
			total += info.Size()
			m.Size = info.Size()
			digest, err := readMember(ctx, path, info, io.Discard)
			if err != nil {
				return err
			}
			m.Digest = digest
		}
		b.members = append(b.members, m)
		return nil
	}
	if kind == "claude" {
		info, err := os.Lstat(filepath.Join(root, entry))
		if err != nil || add(filepath.Join(root, entry), entry, info) != nil {
			return Bundle{}, ErrBundle
		}
	} else {
		err := walkBounded(ctx, root, root, 0, add)
		if err != nil {
			return Bundle{}, err
		}
	}
	required := map[string]bool{entry: true}
	if kind == "cursor" {
		required["node"] = true
		required["index.js"] = false
	}
	for name, exec := range required {
		found := false
		for _, m := range b.members {
			if m.Name == name && !m.Directory && m.Size > 0 && (!exec || m.Executable) {
				found = true
			}
		}
		if !found {
			return Bundle{}, ErrBundle
		}
	}
	payload, err := json.Marshal(struct {
		Version, Kind, Entry string
		Members              []member
	}{"cli-runtime/v1", kind, entry, b.members})
	if err != nil {
		return Bundle{}, ErrBundle
	}
	sum := sha256.Sum256(payload)
	b.digest = hex.EncodeToString(sum[:])
	return b, nil
}

func walkBounded(ctx context.Context, root, path string, depth int, add func(string, string, os.FileInfo) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth > 64 {
		return ErrBundle
	}
	before, err := os.Lstat(path)
	if err != nil {
		return ErrBundle
	}
	name, err := filepath.Rel(root, path)
	if err != nil {
		return ErrBundle
	}
	if err := add(path, name, before); err != nil {
		return err
	}
	if !before.IsDir() {
		return nil
	}
	dir, err := os.Open(path)
	if err != nil {
		return ErrBundle
	}
	after, statErr := dir.Stat()
	if statErr != nil || !os.SameFile(before, after) {
		dir.Close()
		return ErrBundle
	}
	entries, readErr := dir.ReadDir(MaxEntries + 1)
	closeErr := dir.Close()
	if closeErr != nil || readErr != nil && readErr != io.EOF || len(entries) > MaxEntries {
		return ErrBundle
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if err := walkBounded(ctx, root, filepath.Join(path, entry.Name()), depth+1, add); err != nil {
			return err
		}
	}
	return nil
}

// Stage creates a private snapshot. Failure removes only that newly-created
// directory. On success its lifetime belongs to supervisor process completion,
// not the caller's early cancellation return. No existing stage is overwritten.
func (b Bundle) Stage(ctx context.Context, parent string) (Bundle, error) {
	ctx, cancel := context.WithTimeout(ctx, MaxDuration)
	defer cancel()
	if b.digest == "" || !cleanAbsolute(parent) || privateParents(parent) != nil {
		return Bundle{}, ErrBundle
	}
	current, err := Resolve(ctx, b.kind, b.Executable())
	if err != nil || !reflect.DeepEqual(current, b) {
		return Bundle{}, ErrBundle
	}
	dir, err := os.MkdirTemp(parent, "sf-cli-runtime-")
	if err != nil {
		return Bundle{}, err
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(dir)
		}
	}()
	for _, m := range b.members {
		if ctx.Err() != nil {
			return Bundle{}, ctx.Err()
		}
		target := filepath.Join(dir, m.Name)
		if m.Directory {
			if m.Name != "." {
				if err := os.Mkdir(target, 0700); err != nil {
					return Bundle{}, err
				}
			}
			continue
		}
		before, err := os.Lstat(filepath.Join(b.root, m.Name))
		if err != nil || !before.Mode().IsRegular() || !trusted(before) || before.Mode().Perm()&0022 != 0 || before.Size() != m.Size || (before.Mode().Perm()&0111 != 0) != m.Executable {
			return Bundle{}, ErrBundle
		}
		mode := os.FileMode(0400)
		if m.Executable {
			mode = 0500
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
		if err != nil {
			return Bundle{}, err
		}
		digest, copyErr := readMember(ctx, filepath.Join(b.root, m.Name), before, out)
		syncErr, closeErr := out.Sync(), out.Close()
		if copyErr != nil || syncErr != nil || closeErr != nil || digest != m.Digest {
			return Bundle{}, ErrBundle
		}
	}
	staged, err := scan(ctx, b.kind, dir, b.entry)
	if err != nil || staged.digest != b.digest {
		return Bundle{}, ErrBundle
	}
	keep = true
	return staged, nil
}

func readMember(ctx context.Context, path string, before os.FileInfo, destination io.Writer) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", ErrBundle
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() || before.Size() != after.Size() {
		return "", ErrBundle
	}
	h := sha256.New()
	buffer := make([]byte, 64<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		n, err := f.Read(buffer)
		total += int64(n)
		if total > before.Size() {
			return "", ErrBundle
		}
		if n > 0 {
			if _, writeErr := io.MultiWriter(destination, h).Write(buffer[:n]); writeErr != nil {
				return "", writeErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	end, err := f.Stat()
	if err != nil || total != before.Size() || end.Size() != before.Size() || !end.ModTime().Equal(before.ModTime()) {
		return "", ErrBundle
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func cleanAbsolute(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != "/"
}
func trusted(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Uid == 0 || stat.Uid == uint32(os.Getuid()))
}
func privateParents(path string) error {
	for {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || !trusted(info) {
			return ErrBundle
		}
		stat := info.Sys().(*syscall.Stat_t)
		if info.Mode().Perm()&0022 != 0 && !(info.Mode()&os.ModeSticky != 0 && stat.Uid == 0) {
			return ErrBundle
		}
		next := filepath.Dir(path)
		if next == path {
			return nil
		}
		path = next
	}
}
