// Package nysapure defines the deliberately narrow nysa_api_pure_v1 Node
// recipe.  It is not a TypeScript package-manager adapter: it admits one
// explicit .test.ts entrypoint and the bounded, relative, dependency-free
// TypeScript closure reachable from it.
package nysapure

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const sourceManifestVersion = "nysa-api-pure-source-manifest-v1"

// SourceStage is the owner-only, per-launch snapshot of a validated TypeScript
// closure. Files retain their repository-relative layout; Manifest is the
// canonical byte-level claim the factory loader enforces before resolving
// either the test entrypoint or an imported implementation.
type SourceStage struct {
	Root           string
	TestPath       string
	Files          []string
	Sources        []SourceFile
	Manifest       []byte
	ManifestDigest string
	cleanup        func()
}

// SourceFile is the exact FD-read evidence admitted for one source file. The
// inode identity closes replacement races while the digest/size binds the
// staged bytes and the loader manifest.
type SourceFile struct {
	Path   string
	Digest string
	Size   int64
	Device uint64
	Inode  uint64
}

// Close removes the private source snapshot. Callers retain it until the
// supervised process is known gone; an ambiguous process outcome intentionally
// leaves the snapshot in place rather than revoking files from a live child.
func (stage *SourceStage) Close() {
	if stage != nil && stage.cleanup != nil {
		stage.cleanup()
		stage.cleanup = nil
	}
}

// Validate confirms the sealed staged bytes still match the already-admitted
// static closure evidence and canonical manifest. It is intentionally useful
// before launch and in focused tamper tests; the loader repeats the byte check
// at every resolution.
func (stage *SourceStage) Validate() error {
	if stage == nil || !filepath.IsAbs(stage.Root) || filepath.Clean(stage.Root) != stage.Root || !ValidTestPath(stage.TestPath) || len(stage.Files) == 0 || len(stage.Manifest) == 0 {
		return ErrInvalid
	}
	sum := sha256.Sum256(stage.Manifest)
	if stage.ManifestDigest != "sha256:"+hex.EncodeToString(sum[:]) {
		return ErrInvalid
	}
	var manifest sourceManifest
	if err := json.Unmarshal(stage.Manifest, &manifest); err != nil {
		return ErrInvalid
	}
	canonical, err := canonicalManifest(manifest)
	if err != nil || !bytes.Equal(canonical, stage.Manifest) {
		return ErrInvalid
	}
	if !canonicalSources(stage.Sources) || len(stage.Sources) != len(stage.Files) || len(manifest.Entries) != len(stage.Sources) {
		return ErrInvalid
	}
	manifestByPath := make(map[string]sourceManifestEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		manifestByPath[entry.Path] = entry
	}
	if len(manifestByPath) != len(stage.Sources) {
		return ErrInvalid
	}
	for index, source := range stage.Sources {
		if index > 0 && stage.Sources[index-1].Path >= source.Path {
			return ErrInvalid
		}
		file := filepath.Join(stage.Root, filepath.FromSlash(source.Path))
		if stage.Files[index] != file {
			return ErrInvalid
		}
		data, err := os.ReadFile(file)
		entry, ok := manifestByPath[source.Path]
		sum := sha256.Sum256(data)
		info, statErr := os.Lstat(file)
		if err != nil || statErr != nil || !ok || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o400 || source.Size != int64(len(data)) || source.Digest != "sha256:"+hex.EncodeToString(sum[:]) || entry.Size != source.Size || entry.SHA256 != source.Digest {
			return ErrInvalid
		}
	}
	return validateStageTree(stage.Root, stage.Sources)
}

type sourceManifest struct {
	Version string                `json:"version"`
	Entries []sourceManifestEntry `json:"entries"`
}

type sourceManifestEntry struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

const (
	RecipeFlag      = "--sf-nysa-api-pure-v1"
	MaxPathBytes    = 512
	maxEntries      = 128
	maxSourceBytes  = int64(4 << 20)
	maxFileBytes    = int64(1 << 20)
	maxDepth        = 24
	maxValidateTime = 3 * time.Second
	loaderVersion   = "nysa-api-pure-loader-v1"
)

var (
	ErrInvalid       = errors.New("invalid nysa_api_pure_v1 TypeScript closure")
	errMissingSource = errors.New("missing relative TypeScript implementation")

	// These intentionally over-match comments and unsupported dynamic forms.
	// A false refusal is preferable to handing Node a module graph that this
	// small recipe cannot authenticate.
	moduleSpecifier = regexp.MustCompile(`(?m)\b(?:import|export)\s+(?:[^'";]*?\s+from\s+)?['"]([^'"\\\r\n]+)['"]`)
	dynamicImport   = regexp.MustCompile(`\b(?:import|require)\s*\(`)
)

// ValidTestPath validates the command-level path before a worktree exists.
// It accepts a canonical slash-separated repository-relative .test.ts path.
func ValidTestPath(value string) bool {
	if value == "" || len(value) > MaxPathBytes || filepath.IsAbs(value) || strings.Contains(value, "\\") || strings.ContainsRune(value, '\x00') || path.Clean(value) != value {
		return false
	}
	if !strings.HasSuffix(value, ".test.ts") {
		return false
	}
	parts := strings.Split(value, "/")
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || part == "node_modules" || strings.HasPrefix(part, ".") {
			return false
		}
	}
	return true
}

// Validate performs a bounded admission using a private root descriptor.
func Validate(worktree, testPath string) error {
	if !filepath.IsAbs(worktree) || filepath.Clean(worktree) != worktree || !ValidTestPath(testPath) {
		return ErrInvalid
	}
	fd, err := unix.Open(worktree, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return ErrInvalid
	}
	defer unix.Close(fd)
	return ValidateDirectoryFD(fd, testPath)
}

// ValidateDirectoryFD repeats admission from the authenticated worktree
// descriptor immediately before Node's fd-gated exec.
func ValidateDirectoryFD(rootFD int, testPath string) error {
	_, err := ValidateDirectoryFDPaths(rootFD, testPath)
	return err
}

// ValidateDirectoryFDPaths performs the same admission as
// ValidateDirectoryFD and returns the exact regular source files in the
// authenticated closure. Callers use this list to construct a least-privilege
// runtime filesystem policy; it is never derived from the mutable path alone.
func ValidateDirectoryFDPaths(rootFD int, testPath string) ([]string, error) {
	sources, err := ValidateDirectoryFDSources(rootFD, testPath)
	if err != nil {
		return nil, err
	}
	paths := make([]string, len(sources))
	for index, source := range sources {
		paths[index] = source.Path
	}
	return paths, nil
}

// ValidateDirectoryFDSources returns canonical FD-read source evidence for
// staging. A later pathname scan alone is never sufficient authority to copy
// source bytes.
func ValidateDirectoryFDSources(rootFD int, testPath string) ([]SourceFile, error) {
	if rootFD < 0 || !ValidTestPath(testPath) {
		return nil, ErrInvalid
	}
	var root unix.Stat_t
	if err := unix.Fstat(rootFD, &root); err != nil || root.Mode&unix.S_IFMT != unix.S_IFDIR {
		return nil, ErrInvalid
	}
	deadline := time.Now().Add(maxValidateTime)
	seen := make(map[string]struct{})
	sources := make([]SourceFile, 0, maxEntries)
	entries, bytesRead := 0, int64(0)
	var walk func(string, int) error
	walk = func(relative string, depth int) error {
		if depth > maxDepth || time.Now().After(deadline) {
			return ErrInvalid
		}
		if _, ok := seen[relative]; ok {
			return nil
		}
		seen[relative] = struct{}{}
		entries++
		if entries > maxEntries {
			return ErrInvalid
		}
		source, data, err := readSourceFileAt(rootFD, relative)
		// Verification intentionally runs before implementation. A missing
		// relative implementation is therefore a safe, expected red outcome;
		// topology still has to be canonical and every existing source remains
		// authenticated. The test entrypoint itself can never be missing.
		if errors.Is(err, errMissingSource) && depth > 0 {
			return nil
		}
		if err != nil || int64(len(data)) > maxFileBytes || int64(len(data)) > maxSourceBytes-bytesRead {
			return ErrInvalid
		}
		sources = append(sources, source)
		bytesRead += int64(len(data))
		if dynamicImport.Match(data) {
			return ErrInvalid
		}
		matches := moduleSpecifier.FindAllSubmatch(data, -1)
		for _, match := range matches {
			specifier := string(match[1])
			if strings.HasPrefix(specifier, "node:") {
				if ValidBuiltinSpecifier(specifier) {
					continue
				}
				return ErrInvalid
			}
			if !strings.HasPrefix(specifier, "./") && !strings.HasPrefix(specifier, "../") {
				return ErrInvalid // no packages, import maps, URLs, or node_modules.
			}
			if strings.ContainsAny(specifier, "?#\\\x00") || !strings.HasSuffix(specifier, ".js") {
				return ErrInvalid
			}
			next := path.Clean(path.Join(path.Dir(relative), strings.TrimSuffix(specifier, ".js")+".ts"))
			if !ValidSourcePath(next) {
				return ErrInvalid
			}
			if err := walk(next, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(testPath, 0); err != nil {
		return nil, err
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].Path < sources[j].Path })
	return sources, nil
}

// ValidBuiltinSpecifier is the intentionally tiny builtin surface needed by
// the pure Nysa test contract. Filesystem/module-loader/vm/process builtins
// would let a test escape the authenticated static closure.
func ValidBuiltinSpecifier(value string) bool {
	switch value {
	case "node:test", "node:assert", "node:assert/strict":
		return true
	default:
		return false
	}
}

func ValidSourcePath(value string) bool {
	if value == "" || len(value) > MaxPathBytes || strings.Contains(value, "\\") || path.Clean(value) != value || !strings.HasSuffix(value, ".ts") || strings.HasSuffix(value, ".tsx") {
		return false
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." || part == "node_modules" || strings.HasPrefix(part, ".") {
			return false
		}
	}
	return true
}

func readSourceFileAt(rootFD int, relative string) (SourceFile, []byte, error) {
	if !ValidSourcePath(relative) {
		return SourceFile{}, nil, ErrInvalid
	}
	dirFD, err := unix.Dup(rootFD)
	if err != nil {
		return SourceFile{}, nil, ErrInvalid
	}
	defer func() { _ = unix.Close(dirFD) }()
	parts := strings.Split(relative, "/")
	for _, component := range parts[:len(parts)-1] {
		var before unix.Stat_t
		if err := unix.Fstatat(dirFD, component, &before, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			if errors.Is(err, unix.ENOENT) {
				return SourceFile{}, nil, errMissingSource
			}
			return SourceFile{}, nil, ErrInvalid
		}
		if before.Mode&unix.S_IFMT != unix.S_IFDIR {
			return SourceFile{}, nil, ErrInvalid
		}
		next, err := unix.Openat(dirFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return SourceFile{}, nil, ErrInvalid
		}
		var opened unix.Stat_t
		if err := unix.Fstat(next, &opened); err != nil || opened.Mode&unix.S_IFMT != unix.S_IFDIR || opened.Dev != before.Dev || opened.Ino != before.Ino {
			_ = unix.Close(next)
			return SourceFile{}, nil, ErrInvalid
		}
		_ = unix.Close(dirFD)
		dirFD = next
	}
	leaf := parts[len(parts)-1]
	var before unix.Stat_t
	if err := unix.Fstatat(dirFD, leaf, &before, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return SourceFile{}, nil, errMissingSource
		}
		return SourceFile{}, nil, ErrInvalid
	}
	if before.Mode&unix.S_IFMT != unix.S_IFREG || before.Size < 0 || before.Size > maxFileBytes {
		return SourceFile{}, nil, ErrInvalid
	}
	fd, err := unix.Openat(dirFD, leaf, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return SourceFile{}, nil, ErrInvalid
	}
	f := os.NewFile(uintptr(fd), "nysa-pure-source")
	defer f.Close()
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil || opened.Mode&unix.S_IFMT != unix.S_IFREG || opened.Dev != before.Dev || opened.Ino != before.Ino || opened.Size != before.Size {
		return SourceFile{}, nil, ErrInvalid
	}
	data, err := io.ReadAll(io.LimitReader(f, maxFileBytes+1))
	if err != nil || int64(len(data)) != before.Size {
		return SourceFile{}, nil, ErrInvalid
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil || after.Dev != opened.Dev || after.Ino != opened.Ino || after.Size != opened.Size {
		return SourceFile{}, nil, ErrInvalid
	}
	sum := sha256.Sum256(data)
	return SourceFile{Path: relative, Digest: "sha256:" + hex.EncodeToString(sum[:]), Size: int64(len(data)), Device: uint64(opened.Dev), Inode: uint64(opened.Ino)}, data, nil
}

// StageDirectoryFD copies exactly the prior FD-read closure evidence into a
// new private directory. Each re-opened source must retain the recorded inode,
// digest, and size; the copied tree is then sealed before it is accepted.
func StageDirectoryFD(rootFD int, testPath string, expected []SourceFile) (*SourceStage, error) {
	if rootFD < 0 || !ValidTestPath(testPath) || len(expected) == 0 || len(expected) > maxEntries {
		return nil, ErrInvalid
	}
	if !canonicalSources(expected) {
		return nil, ErrInvalid
	}
	deadline := time.Now().Add(maxValidateTime)
	current, err := ValidateDirectoryFDSources(rootFD, testPath)
	if err != nil || time.Now().After(deadline) || !sameSourceFiles(current, expected) {
		return nil, ErrInvalid
	}
	base, err := os.MkdirTemp("", "sf-nysa-pure-source-")
	if err != nil {
		return nil, err
	}
	created := base
	base, err = filepath.EvalSymlinks(base)
	if err != nil || !filepath.IsAbs(base) || filepath.Clean(base) != base {
		_ = os.RemoveAll(created)
		return nil, ErrInvalid
	}
	cleanup := func() { removeSealedSourceStage(base) }
	if err := os.Chmod(base, 0o700); err != nil {
		cleanup()
		return nil, err
	}
	entries := make([]sourceManifestEntry, 0, len(expected))
	files := make([]string, 0, len(expected))
	for _, source := range expected {
		if time.Now().After(deadline) {
			cleanup()
			return nil, ErrInvalid
		}
		current, data, err := readSourceFileAt(rootFD, source.Path)
		if err != nil || !sameSourceFile(current, source) {
			cleanup()
			return nil, ErrInvalid
		}
		if err := stageSourceFile(base, source.Path, data); err != nil {
			cleanup()
			return nil, err
		}
		entries = append(entries, sourceManifestEntry{Path: source.Path, Size: source.Size, SHA256: source.Digest})
		files = append(files, filepath.Join(base, filepath.FromSlash(source.Path)))
	}
	if time.Now().After(deadline) || sealSourceStage(base, files) != nil {
		cleanup()
		return nil, ErrInvalid
	}
	manifest, err := canonicalManifest(sourceManifest{Version: sourceManifestVersion, Entries: entries})
	if err != nil {
		cleanup()
		return nil, err
	}
	sum := sha256.Sum256(manifest)
	stage := &SourceStage{Root: base, TestPath: testPath, Files: files, Sources: append([]SourceFile(nil), expected...), Manifest: manifest, ManifestDigest: "sha256:" + hex.EncodeToString(sum[:]), cleanup: cleanup}
	if time.Now().After(deadline) || stage.Validate() != nil || time.Now().After(deadline) {
		cleanup()
		return nil, ErrInvalid
	}
	return stage, nil
}

func stageSourceFile(root, relative string, data []byte) error {
	if !cleanStageRelative(relative) || int64(len(data)) > maxFileBytes {
		return ErrInvalid
	}
	directory := filepath.Join(root, filepath.FromSlash(path.Dir(relative)))
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	target := filepath.Join(root, filepath.FromSlash(relative))
	file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(target)
		return ErrInvalid
	}
	info, err := os.Lstat(target)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || int64(len(data)) != info.Size() {
		return ErrInvalid
	}
	return nil
}

func canonicalSources(sources []SourceFile) bool {
	if len(sources) == 0 || len(sources) > maxEntries {
		return false
	}
	for index, source := range sources {
		digest, err := hex.DecodeString(strings.TrimPrefix(source.Digest, "sha256:"))
		if !ValidSourcePath(source.Path) || source.Size < 0 || source.Size > maxFileBytes || len(source.Digest) != len("sha256:")+64 || !strings.HasPrefix(source.Digest, "sha256:") || err != nil || len(digest) != sha256.Size || source.Device == 0 || source.Inode == 0 || (index > 0 && sources[index-1].Path >= source.Path) {
			return false
		}
	}
	return true
}

func sameSourceFile(left, right SourceFile) bool {
	return left.Path == right.Path && left.Digest == right.Digest && left.Size == right.Size && left.Device == right.Device && left.Inode == right.Inode
}

func sameSourceFiles(left, right []SourceFile) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !sameSourceFile(left[index], right[index]) {
			return false
		}
	}
	return true
}

func removeSealedSourceStage(root string) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return
	}
	_ = filepath.WalkDir(root, func(current string, entry os.DirEntry, err error) error {
		if err == nil && entry.IsDir() && entry.Type()&os.ModeSymlink == 0 {
			_ = os.Chmod(current, 0o700)
		}
		return nil
	})
	_ = os.RemoveAll(root)
}

func sealSourceStage(root string, files []string) error {
	if len(files) == 0 {
		return ErrInvalid
	}
	for _, file := range files {
		if err := os.Chmod(file, 0o400); err != nil {
			return err
		}
	}
	// Walk deepest-first so all source directories remain writable while the
	// snapshot is assembled, then become read/execute-only as one final step.
	var directories []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			directories = append(directories, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(directories, func(i, j int) bool { return len(directories[i]) > len(directories[j]) })
	for _, directory := range directories {
		if err := os.Chmod(directory, 0o500); err != nil {
			return err
		}
	}
	return nil
}

func validateStageTree(root string, sources []SourceFile) error {
	wantFiles, wantDirectories := map[string]bool{}, map[string]bool{root: true}
	for _, source := range sources {
		file := filepath.Join(root, filepath.FromSlash(source.Path))
		wantFiles[file] = true
		for directory := filepath.Dir(file); directory != root; directory = filepath.Dir(directory) {
			wantDirectories[directory] = true
		}
	}
	return filepath.WalkDir(root, func(current string, entry os.DirEntry, err error) error {
		if err != nil || entry.Type()&os.ModeSymlink != 0 {
			return ErrInvalid
		}
		info, err := entry.Info()
		if err != nil {
			return ErrInvalid
		}
		if entry.IsDir() {
			if !wantDirectories[current] || info.Mode().Perm() != 0o500 {
				return ErrInvalid
			}
			return nil
		}
		if !wantFiles[current] || !info.Mode().IsRegular() || info.Mode().Perm() != 0o400 {
			return ErrInvalid
		}
		return nil
	})
}

func cleanStageRelative(value string) bool {
	return ValidSourcePath(value) && !filepath.IsAbs(value) && filepath.Clean(filepath.FromSlash(value)) == filepath.FromSlash(value)
}

func canonicalManifest(value sourceManifest) ([]byte, error) {
	if value.Version != sourceManifestVersion || len(value.Entries) == 0 || len(value.Entries) > maxEntries {
		return nil, ErrInvalid
	}
	for index, entry := range value.Entries {
		if !ValidSourcePath(entry.Path) || entry.Size < 0 || entry.Size > maxFileBytes || !strings.HasPrefix(entry.SHA256, "sha256:") || len(entry.SHA256) != len("sha256:")+64 || (index > 0 && value.Entries[index-1].Path >= entry.Path) {
			return nil, ErrInvalid
		}
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'}), nil
}

// LoaderSource is copied into the private staged Node closure. It only maps
// relative .js source specifiers to manifest-bound regular non-symlink .ts
// files below the private source snapshot. It deliberately refuses every
// package, URL, extensionless, dynamic, JSX, and escaping resolution form.
const LoaderSource = `import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'node:url';

const configuredRoot = process.env.SF_NYSA_API_PURE_ROOT;
if (!configuredRoot || !path.isAbsolute(configuredRoot) || path.resolve(configuredRoot) !== configuredRoot) throw new Error('sf nysa pure root invalid');
const manifestRaw = process.env.SF_NYSA_API_PURE_MANIFEST;
const manifestDigest = process.env.SF_NYSA_API_PURE_MANIFEST_DIGEST;
if (!manifestRaw || !manifestDigest || !/^sha256:[0-9a-f]{64}$/.test(manifestDigest)) throw new Error('sf nysa pure manifest missing');
if ('sha256:' + crypto.createHash('sha256').update(manifestRaw, 'utf8').digest('hex') !== manifestDigest) throw new Error('sf nysa pure manifest digest invalid');
let manifest;
try { manifest = JSON.parse(manifestRaw); } catch { throw new Error('sf nysa pure manifest invalid'); }
if (!manifest || manifest.version !== 'nysa-api-pure-source-manifest-v1' || !Array.isArray(manifest.entries) || manifest.entries.length < 1 || manifest.entries.length > 128) throw new Error('sf nysa pure manifest noncanonical');
const manifestEntries = new Map();
let previousManifestPath = '';
for (const entry of manifest.entries) {
  if (!entry || typeof entry.path !== 'string' || !/^[^./][^\\]*?(?:\/[^./][^\\]*?)*\.ts$/.test(entry.path) || entry.path.split('/').includes('node_modules') || entry.path.endsWith('.tsx') || !Number.isSafeInteger(entry.size) || entry.size < 0 || entry.size > 1048576 || typeof entry.sha256 !== 'string' || !/^sha256:[0-9a-f]{64}$/.test(entry.sha256) || entry.path <= previousManifestPath || manifestEntries.has(entry.path)) throw new Error('sf nysa pure manifest entry invalid');
  manifestEntries.set(entry.path, entry);
  previousManifestPath = entry.path;
}
// The launch gate fchdirs the staged source descriptor before this process
// starts. Require the descriptor-backed cwd to agree with the frozen path.
const workingRoot = process.cwd();
if (!path.isAbsolute(workingRoot) || path.resolve(workingRoot) !== configuredRoot) throw new Error('sf nysa pure root changed');
const root = workingRoot;
function confined(value) {
  const clean = path.resolve(value);
  if (clean !== root && !clean.startsWith(root + path.sep)) throw new Error('sf nysa pure path escape');
  return clean;
}
function regular(value) {
  const stat = fs.lstatSync(value);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size < 0 || stat.size > 1048576) throw new Error('sf nysa pure source invalid');
  const relative = path.relative(root, value).split(path.sep).join('/');
  const entry = manifestEntries.get(relative);
  if (!entry || stat.size !== entry.size) throw new Error('sf nysa pure source manifest mismatch');
  const digest = 'sha256:' + crypto.createHash('sha256').update(fs.readFileSync(value)).digest('hex');
  if (digest !== entry.sha256) throw new Error('sf nysa pure source digest mismatch');
}
export async function resolve(specifier, context, nextResolve) {
  if (specifier === 'node:test' || specifier === 'node:assert' || specifier === 'node:assert/strict') return nextResolve(specifier, context);
  if (specifier.startsWith('node:')) throw new Error('sf nysa pure builtin denied');
  if (specifier.startsWith('file:')) {
    const target = confined(fileURLToPath(specifier));
    if (!target.endsWith('.ts') || target.endsWith('.tsx')) throw new Error('sf nysa pure entry denied');
    regular(target);
    return { url: pathToFileURL(target).href, shortCircuit: true };
  }
  if (!specifier.startsWith('./') && !specifier.startsWith('../')) throw new Error('sf nysa pure bare import denied');
  if (specifier.includes('?') || specifier.includes('#') || specifier.includes('\\') || !specifier.endsWith('.js')) throw new Error('sf nysa pure specifier denied');
  if (!context.parentURL || !context.parentURL.startsWith('file:')) throw new Error('sf nysa pure parent denied');
  const parent = confined(fileURLToPath(context.parentURL));
  regular(parent);
  const target = confined(path.resolve(path.dirname(parent), specifier.slice(0, -3) + '.ts'));
  if (!target.endsWith('.ts') || target.endsWith('.tsx')) throw new Error('sf nysa pure extension denied');
  regular(target);
  return { url: pathToFileURL(target).href, shortCircuit: true };
}
`

// LoaderIdentity is included in the durable executable binding, so modifying
// the resolver source cannot silently change a frozen ticket's runtime.
func LoaderIdentity() string {
	sum := sha256.Sum256([]byte(loaderVersion + "\x00" + LoaderSource))
	return loaderVersion + ":sha256:" + hex.EncodeToString(sum[:])
}

// EqualLoaderSource lets tests assert the staged artifact exactly without
// exporting mutable state.
func EqualLoaderSource(value []byte) bool { return bytes.Equal(value, []byte(LoaderSource)) }
