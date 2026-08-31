// Package nodeclosure defines the deliberately small local Node 22 recipe.
// It validates source before Node ever receives a repository pathname; this is
// not an npm/package-manager adapter.
package nodeclosure

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	MaxPackageJSONBytes int64 = 1 << 20
	maxEntries                = 10_000
	maxDepth                  = 32
	maxBytes            int64 = 64 << 20
	maxWalkTime               = 3 * time.Second
)

var (
	ErrInvalid      = errors.New("invalid dependency-free Node repository")
	ErrDependencies = errors.New("Node dependencies require CI or operator takeover")
	ErrNoTests      = errors.New("Node local recipe requires an official discovered test file")
)

// Validate accepts only a bounded, dependency-free JavaScript/CJS/MJS
// worktree with at least one file Node's built-in --test discovery will run.
// It deliberately rejects every symlink, native addon, node_modules tree, and
// package-manager dependency declaration rather than trying to interpret one.
func Validate(worktree string) error {
	if !filepath.IsAbs(worktree) || filepath.Clean(worktree) != worktree {
		return ErrInvalid
	}
	root, err := unix.Open(worktree, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ErrInvalid
	}
	defer unix.Close(root)
	return validateDirectoryFD(root)
}

// ValidateDirectoryFD repeats the bounded source admission check through an
// already-authenticated directory descriptor.  It never resolves a child from
// an ambient worktree pathname, so a rename/replacement after Git has proved
// the worktree identity cannot redirect this final launch-time validation.
// The caller retains ownership of rootFD.
func ValidateDirectoryFD(rootFD int) error {
	if rootFD < 0 {
		return ErrInvalid
	}
	copyFD, err := unix.Dup(rootFD)
	if err != nil {
		return ErrInvalid
	}
	defer unix.Close(copyFD)
	return validateDirectoryFD(copyFD)
}

func validateDirectoryFD(rootFD int) error {
	var root unix.Stat_t
	if err := unix.Fstat(rootFD, &root); err != nil || root.Mode&unix.S_IFMT != unix.S_IFDIR {
		return ErrInvalid
	}
	data, err := readPackageAt(rootFD)
	if err != nil {
		return ErrInvalid
	}
	var doc map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&doc); err != nil || doc == nil {
		return ErrInvalid
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ErrInvalid
	}
	for _, key := range []string{"dependencies", "devDependencies", "optionalDependencies", "peerDependencies", "bundledDependencies", "bundleDependencies", "workspaces"} {
		if _, ok := doc[key]; ok {
			return ErrDependencies
		}
	}

	deadline := time.Now().Add(maxWalkTime)
	entries, varBytes, tests := 0, int64(0), 0
	var walk func(int, string, int) error
	walk = func(dirFD int, relative string, depth int) error {
		if time.Now().After(deadline) || depth > maxDepth {
			return ErrInvalid
		}
		readFD, err := unix.Dup(dirFD)
		if err != nil {
			return ErrInvalid
		}
		dir := os.NewFile(uintptr(readFD), "node-closure-directory")
		if dir == nil {
			_ = unix.Close(readFD)
			return ErrInvalid
		}
		defer dir.Close()
		for {
			children, readErr := dir.ReadDir(128)
			for _, child := range children {
				name := child.Name()
				if name == "." || name == ".." || strings.ContainsRune(name, filepath.Separator) {
					return ErrInvalid
				}
				childRel := name
				if relative != "" {
					childRel = filepath.Join(relative, name)
				}
				var before unix.Stat_t
				if err := unix.Fstatat(dirFD, name, &before, unix.AT_SYMLINK_NOFOLLOW); err != nil || before.Mode&unix.S_IFMT == unix.S_IFLNK {
					return ErrInvalid
				}
				entries++
				if entries > maxEntries {
					return ErrInvalid
				}
				switch before.Mode & unix.S_IFMT {
				case unix.S_IFDIR:
					if childRel == ".git" { // Git control plane is authenticated elsewhere.
						continue
					}
					if name == "node_modules" {
						return ErrDependencies
					}
					childFD, err := unix.Openat(dirFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
					if err != nil {
						return ErrInvalid
					}
					var opened unix.Stat_t
					err = unix.Fstat(childFD, &opened)
					if err == nil && (opened.Mode&unix.S_IFMT != unix.S_IFDIR || opened.Dev != before.Dev || opened.Ino != before.Ino) {
						err = ErrInvalid
					}
					if err == nil {
						err = walk(childFD, childRel, depth+1)
					}
					_ = unix.Close(childFD)
					if err != nil {
						return err
					}
				case unix.S_IFREG:
					if before.Size < 0 || before.Size > maxBytes-varBytes {
						return ErrInvalid
					}
					fileFD, err := unix.Openat(dirFD, name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
					if err != nil {
						return ErrInvalid
					}
					var opened unix.Stat_t
					err = unix.Fstat(fileFD, &opened)
					_ = unix.Close(fileFD)
					if err != nil || opened.Mode&unix.S_IFMT != unix.S_IFREG || opened.Dev != before.Dev || opened.Ino != before.Ino || opened.Size != before.Size {
						return ErrInvalid
					}
					varBytes += before.Size
					base := filepath.Base(name)
					if strings.HasSuffix(base, ".node") {
						return ErrDependencies
					}
					// This recipe deliberately has no transpiler, loader, or strip-types
					// authority. A TypeScript source file is therefore a CI/operator case,
					// even if its test name would otherwise resemble Node discovery.
					if strings.HasSuffix(base, ".ts") || strings.HasSuffix(base, ".cts") || strings.HasSuffix(base, ".mts") {
						return ErrDependencies
					}
					if officialTestFile(filepath.ToSlash(childRel)) {
						tests++
					}
				default:
					return ErrInvalid
				}
			}
			if readErr == io.EOF {
				return nil
			}
			if readErr != nil {
				return ErrInvalid
			}
		}
	}
	if err := walk(rootFD, "", 0); err != nil {
		return err
	}
	if tests == 0 {
		return ErrNoTests
	}
	return nil
}

func readPackageAt(rootFD int) ([]byte, error) {
	var before unix.Stat_t
	if err := unix.Fstatat(rootFD, "package.json", &before, unix.AT_SYMLINK_NOFOLLOW); err != nil || before.Mode&unix.S_IFMT != unix.S_IFREG || before.Size > MaxPackageJSONBytes {
		return nil, ErrInvalid
	}
	fd, err := unix.Openat(rootFD, "package.json", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrInvalid
	}
	f := os.NewFile(uintptr(fd), "node-package-json")
	defer f.Close()
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil || opened.Mode&unix.S_IFMT != unix.S_IFREG || opened.Dev != before.Dev || opened.Ino != before.Ino || opened.Size != before.Size {
		return nil, ErrInvalid
	}
	b, err := io.ReadAll(io.LimitReader(f, MaxPackageJSONBytes+1))
	if err != nil || int64(len(b)) > MaxPackageJSONBytes {
		return nil, ErrInvalid
	}
	return b, nil
}

func officialTestFile(relative string) bool {
	base := filepath.Base(relative)
	stem, ok := nodeTestStem(base)
	if !ok {
		return false
	}
	if strings.HasSuffix(stem, ".test") || strings.HasSuffix(stem, "-test") || strings.HasSuffix(stem, "_test") || strings.HasPrefix(stem, "test-") || stem == "test" {
		return true
	}
	return strings.Contains("/"+relative, "/test/")
}

func nodeTestStem(base string) (string, bool) {
	for _, extension := range []string{".cjs", ".mjs", ".js"} {
		if strings.HasSuffix(base, extension) {
			return strings.TrimSuffix(base, extension), true
		}
	}
	return "", false
}
