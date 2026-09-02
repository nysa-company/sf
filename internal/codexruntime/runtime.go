// Package codexruntime authenticates the local Codex runtime bundle.
//
// Recent Codex releases launch the sibling codex-code-mode-host executable.
// Treating only the codex launcher as a runtime left the staged launcher
// unable to find, and therefore unable to authenticate, that required host.
package codexruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const (
	// CodeModeHost is the fixed sibling required by the supported Codex CLI.
	CodeModeHost = "codex-code-mode-host"
	// MaxExecutableSize bounds each member of the runtime bundle.
	MaxExecutableSize int64 = 256 << 20
)

var ErrUnavailable = errors.New("Codex runtime bundle is unavailable")

// File is an authenticated executable member of a runtime bundle. Its path is
// canonical and its digest is SHA-256 over the bytes read through a descriptor
// whose identity was checked against the path entry.
type File struct {
	Path   string
	Digest string
	Size   int64
}

// Bundle names the exact two executable files Codex needs at launch.
// Digest is a canonical digest of both names, byte digests, and sizes.
type Bundle struct {
	Codex  File
	Host   File
	Digest string
}

// Resolve validates Codex and its exact non-symlinked sibling host. The main
// executable may initially be selected through a PATH symlink, but after that
// resolution both bundle members and every parent directory are checked.
func Resolve(executable string) (Bundle, error) {
	if executable == "" || !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return Bundle{}, ErrUnavailable
	}
	resolved, err := filepath.EvalSymlinks(executable)
	if err != nil || !filepath.IsAbs(resolved) || filepath.Clean(resolved) != resolved {
		return Bundle{}, ErrUnavailable
	}
	codex, err := authenticate(resolved, true)
	if err != nil {
		return Bundle{}, ErrUnavailable
	}
	hostPath := filepath.Join(filepath.Dir(resolved), CodeModeHost)
	host, err := authenticate(hostPath, true)
	if err != nil {
		return Bundle{}, ErrUnavailable
	}
	bundle := Bundle{Codex: codex, Host: host}
	bundle.Digest = digestBundle(bundle)
	return bundle, nil
}

// ResolveStaged authenticates a supervisor-owned, already-private staging
// directory. Unlike Resolve it deliberately does not require every ancestor
// to be private: conventional temporary roots such as /tmp are sticky and
// world-writable on Linux. The caller must separately lstat and own the leaf
// directory before using this narrow form.
func ResolveStaged(executable string) (Bundle, error) {
	if executable == "" || !filepath.IsAbs(executable) || filepath.Clean(executable) != executable {
		return Bundle{}, ErrUnavailable
	}
	codex, err := authenticate(executable, false)
	if err != nil {
		return Bundle{}, ErrUnavailable
	}
	host, err := authenticate(filepath.Join(filepath.Dir(executable), CodeModeHost), false)
	if err != nil {
		return Bundle{}, ErrUnavailable
	}
	bundle := Bundle{Codex: codex, Host: host}
	bundle.Digest = digestBundle(bundle)
	return bundle, nil
}

// Verify re-resolves the bundle and proves it still names exactly the bundle
// described by value. Callers use it immediately before staging or execution.
func Verify(value Bundle) error {
	if value.Codex.Path == "" || value.Host.Path == "" || value.Digest == "" {
		return ErrUnavailable
	}
	current, err := Resolve(value.Codex.Path)
	if err != nil || current != value {
		return ErrUnavailable
	}
	return nil
}

// CopyTo copies a verified bundle to a private, pre-created directory with its
// required sibling spellings. It re-resolves first, then verifies every source
// descriptor while hashing and copying, so a changed helper cannot be staged
// under an old qualification digest.
func CopyTo(value Bundle, directory string) (string, error) {
	if err := Verify(value); err != nil {
		return "", err
	}
	if directory == "" || !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return "", ErrUnavailable
	}
	for _, member := range []struct {
		file File
		name string
	}{
		{value.Codex, "codex"},
		{value.Host, CodeModeHost},
	} {
		if err := copyMember(member.file, filepath.Join(directory, member.name)); err != nil {
			return "", err
		}
	}
	return filepath.Join(directory, "codex"), nil
}

func digestBundle(value Bundle) string {
	valueBytes := fmt.Sprintf("codex-runtime-bundle/v1\x00codex\x00%s\x00%d\x00%s\x00%s\x00%d", value.Codex.Digest, value.Codex.Size, CodeModeHost, value.Host.Digest, value.Host.Size)
	sum := sha256.Sum256([]byte(valueBytes))
	return hex.EncodeToString(sum[:])
}

func authenticate(path string, requirePrivateParents bool) (File, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return File{}, ErrUnavailable
	}
	if requirePrivateParents {
		if err := privateParents(filepath.Dir(path)); err != nil {
			return File{}, err
		}
	}
	before, err := os.Lstat(path)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !validExecutable(before) {
		return File{}, ErrUnavailable
	}
	file, err := os.Open(path)
	if err != nil {
		return File{}, ErrUnavailable
	}
	defer file.Close()
	after, err := file.Stat()
	if err != nil || !sameIdentity(before, after) || !validExecutable(after) {
		return File{}, ErrUnavailable
	}
	hash := sha256.New()
	copied, err := io.Copy(hash, io.LimitReader(file, MaxExecutableSize+1))
	if err != nil || copied != after.Size() {
		return File{}, ErrUnavailable
	}
	return File{Path: path, Digest: hex.EncodeToString(hash.Sum(nil)), Size: after.Size()}, nil
}

func copyMember(expected File, targetPath string) error {
	if expected.Path == "" || expected.Digest == "" || expected.Size <= 0 || expected.Size > MaxExecutableSize {
		return ErrUnavailable
	}
	before, err := os.Lstat(expected.Path)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !validExecutable(before) {
		return ErrUnavailable
	}
	source, err := os.Open(expected.Path)
	if err != nil {
		return ErrUnavailable
	}
	defer source.Close()
	after, err := source.Stat()
	if err != nil || !sameIdentity(before, after) || !validExecutable(after) || after.Size() != expected.Size {
		return ErrUnavailable
	}
	target, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o500)
	if err != nil {
		return err
	}
	hash := sha256.New()
	copied, copyErr := io.Copy(io.MultiWriter(target, hash), io.LimitReader(source, MaxExecutableSize+1))
	syncErr := target.Sync()
	closeErr := target.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil || copied != expected.Size || hex.EncodeToString(hash.Sum(nil)) != expected.Digest {
		return ErrUnavailable
	}
	return nil
}

func validExecutable(info os.FileInfo) bool {
	return info != nil && info.Mode().IsRegular() && info.Mode()&0o111 != 0 && info.Mode()&0o022 == 0 && info.Size() > 0 && info.Size() <= MaxExecutableSize && trustedOwner(info)
}

func privateParents(directory string) error {
	for current := directory; ; current = filepath.Dir(current) {
		entry, err := os.Lstat(current)
		if err != nil || entry.Mode()&os.ModeSymlink != 0 || !entry.IsDir() || !trustedOwner(entry) {
			return ErrUnavailable
		}
		// A root-owned sticky temporary root (for example /tmp) protects a
		// private child from other UIDs deleting or renaming it. The documented
		// same-UID source-replacement limitation still applies, as it does to
		// every source runtime path; arbitrary writable ancestors do not pass.
		if entry.Mode().Perm()&0o022 != 0 && !(entry.Mode()&os.ModeSticky != 0 && ownerIsRoot(entry)) {
			return ErrUnavailable
		}
		parent := filepath.Dir(current)
		if parent == current {
			return nil
		}
	}
}

func ownerIsRoot(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0
}

func trustedOwner(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (stat.Uid == uint32(os.Getuid()) || stat.Uid == 0)
}

func sameIdentity(left, right os.FileInfo) bool {
	leftStat, leftOK := left.Sys().(*syscall.Stat_t)
	rightStat, rightOK := right.Sys().(*syscall.Stat_t)
	return leftOK && rightOK && leftStat.Dev == rightStat.Dev && leftStat.Ino == rightStat.Ino && left.Mode() == right.Mode() && left.Size() == right.Size()
}
