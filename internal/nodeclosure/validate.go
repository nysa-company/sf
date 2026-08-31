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
	root, err := os.Lstat(worktree)
	if err != nil || root.Mode()&os.ModeSymlink != 0 || !root.IsDir() {
		return ErrInvalid
	}
	data, err := readPackage(filepath.Join(worktree, "package.json"))
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
	var walk func(string, string, int) error
	walk = func(path, relative string, depth int) error {
		if time.Now().After(deadline) || depth > maxDepth {
			return ErrInvalid
		}
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return ErrInvalid
		}
		entries++
		if entries > maxEntries {
			return ErrInvalid
		}
		if info.IsDir() {
			if relative == ".git" { // Git control plane is authenticated elsewhere.
				return nil
			}
			if filepath.Base(path) == "node_modules" {
				return ErrDependencies
			}
			dir, err := os.Open(path)
			if err != nil {
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
					if err := walk(filepath.Join(path, name), childRel, depth+1); err != nil {
						return err
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
		if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxBytes-varBytes {
			return ErrInvalid
		}
		varBytes += info.Size()
		if strings.HasSuffix(strings.ToLower(filepath.Base(path)), ".node") {
			return ErrDependencies
		}
		// This recipe deliberately has no transpiler, loader, or strip-types
		// authority. A TypeScript source file is therefore a CI/operator case,
		// even if its test name would otherwise resemble Node discovery.
		lower := strings.ToLower(filepath.Base(path))
		if strings.HasSuffix(lower, ".ts") || strings.HasSuffix(lower, ".cts") || strings.HasSuffix(lower, ".mts") {
			return ErrDependencies
		}
		if officialTestFile(filepath.ToSlash(relative)) {
			tests++
		}
		return nil
	}
	if err := walk(worktree, "", 0); err != nil {
		return err
	}
	if tests == 0 {
		return ErrNoTests
	}
	return nil
}

func readPackage(path string) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() || before.Size() > MaxPackageJSONBytes {
		return nil, ErrInvalid
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, ErrInvalid
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(before, opened) {
		return nil, ErrInvalid
	}
	b, err := io.ReadAll(io.LimitReader(f, MaxPackageJSONBytes+1))
	if err != nil || int64(len(b)) > MaxPackageJSONBytes {
		return nil, ErrInvalid
	}
	return b, nil
}

func officialTestFile(relative string) bool {
	base := strings.ToLower(filepath.Base(relative))
	valid := strings.HasSuffix(base, ".js") || strings.HasSuffix(base, ".cjs") || strings.HasSuffix(base, ".mjs")
	if !valid {
		return false
	}
	if strings.Contains(base, ".test.") || strings.Contains(base, "-test.") || strings.HasPrefix(base, "test-") {
		return true
	}
	return strings.HasPrefix(relative, "test/")
}
