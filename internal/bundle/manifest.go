// Package bundle verifies the complete local distribution payload. Its hashes
// establish integrity against a supplied manifest, not publisher authenticity.
package bundle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"debug/buildinfo"
	"debug/macho"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
)

const ManifestName = "sf-bundle.json"
const maxPayloadBytes = 128 << 20
const maxManifestBytes = 16 << 10

type Identity struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Channel string `json:"channel"`
	OS      string `json:"os"`
	Arch    string `json:"arch"`
}

type File struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Mode   uint32 `json:"mode"`
	SHA256 string `json:"sha256"`
}

type Manifest struct {
	Schema   string   `json:"schema"`
	Identity Identity `json:"identity"`
	Files    []File   `json:"files"`
}

var semver = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$`)
var commitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

func payloads(identity Identity) (map[string]string, error) {
	if len(identity.Version) > 128 || !semver.MatchString(identity.Version) || !commitPattern.MatchString(identity.Commit) || identity.OS != "darwin" || identity.Arch != "arm64" && identity.Arch != "amd64" || identity.Channel != "stable" && identity.Channel != "dev" {
		return nil, errors.New("invalid or unsupported bundle identity")
	}
	suffix := ""
	if identity.Channel == "dev" {
		suffix = "-dev"
	}
	return map[string]string{"sf" + suffix: "sf", "sf-ssh" + suffix: "sf-ssh", "sf-git-exec" + suffix: "sf-git-exec", "sf-git-credential" + suffix: "sf-git-credential", "github_known_hosts": ""}, nil
}

// CreateManifest accepts only an exact complete build directory. It never
// overwrites a manifest or executes any payload to inspect its identity.
func CreateManifest(ctx context.Context, directory string, identity Identity) (Manifest, error) {
	manifest, err := describe(ctx, directory, identity, false)
	if err != nil {
		return Manifest{}, err
	}
	data, err := encode(manifest)
	if err != nil {
		return Manifest{}, err
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	file, err := os.OpenFile(filepath.Join(directory, ManifestName), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0644)
	if err != nil {
		return Manifest{}, errors.New("manifest already exists or cannot be created")
	}
	_, writeErr := file.Write(data)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return Manifest{}, errors.New("manifest write failed; a partial new manifest may remain")
	}
	return manifest, nil
}

func Verify(ctx context.Context, directory string) (Manifest, error) {
	file, err := openRegular(filepath.Join(directory, ManifestName), maxManifestBytes)
	if err != nil {
		return Manifest{}, errors.New("bundle manifest is missing, unsafe or oversized")
	}
	info, statErr := file.Stat()
	if statErr != nil || info.Mode().Perm() != 0644 || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		file.Close()
		return Manifest{}, errors.New("unexpected manifest permissions")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	file.Close()
	if err != nil || len(data) > maxManifestBytes {
		return Manifest{}, errors.New("bundle manifest cannot be read")
	}
	var expected Manifest
	if json.Unmarshal(data, &expected) != nil {
		return Manifest{}, errors.New("invalid bundle manifest")
	}
	canonical, err := encode(expected)
	if err != nil || !bytes.Equal(canonical, data) {
		return Manifest{}, errors.New("bundle manifest is not canonical")
	}
	actual, err := describe(ctx, directory, expected.Identity, true)
	if err != nil {
		return Manifest{}, err
	}
	actualData, _ := encode(actual)
	if !bytes.Equal(actualData, data) {
		return Manifest{}, errors.New("bundle payload does not match manifest")
	}
	return actual, nil
}

func encode(manifest Manifest) ([]byte, error) {
	data, err := json.MarshalIndent(manifest, "", "  ")
	return append(data, '\n'), err
}

func describe(ctx context.Context, directory string, identity Identity, hasManifest bool) (Manifest, error) {
	packages, err := payloads(identity)
	if err != nil {
		return Manifest{}, err
	}
	if !filepath.IsAbs(directory) || filepath.Clean(directory) != directory {
		return Manifest{}, errors.New("bundle directory must be an absolute clean path")
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return Manifest{}, errors.New("bundle directory must be a real directory")
	}
	dir, err := os.Open(directory)
	if err != nil {
		return Manifest{}, err
	}
	entries, readErr := dir.Readdirnames(len(packages) + 2)
	dir.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return Manifest{}, errors.New("bundle inventory unavailable")
	}
	want := len(packages)
	if hasManifest {
		want++
	}
	if len(entries) != want {
		return Manifest{}, errors.New("bundle contains missing or extra files")
	}
	for _, name := range entries {
		if _, ok := packages[name]; !ok && !(hasManifest && name == ManifestName) {
			return Manifest{}, errors.New("bundle contains an unexpected file")
		}
	}
	names := make([]string, 0, len(packages))
	for name := range packages {
		names = append(names, name)
	}
	sort.Strings(names)
	manifest := Manifest{Schema: "sf.bundle/v1", Identity: identity, Files: make([]File, 0, len(names))}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return Manifest{}, err
		}
		file, err := openRegular(filepath.Join(directory, name), maxPayloadBytes)
		if err != nil {
			return Manifest{}, fmt.Errorf("unsafe bundle payload: %s", name)
		}
		entry, err := inspect(ctx, file, name, packages[name], identity)
		file.Close()
		if err != nil {
			return Manifest{}, err
		}
		manifest.Files = append(manifest.Files, entry)
	}
	return manifest, nil
}

func openRegular(path string, limit int64) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > limit {
		file.Close()
		return nil, errors.New("not a bounded regular file")
	}
	return file, nil
}

func inspect(ctx context.Context, file *os.File, name, packageName string, identity Identity) (File, error) {
	info, err := file.Stat()
	if err != nil {
		return File{}, err
	}
	mode := os.FileMode(0644)
	if packageName != "" {
		mode = 0755
	}
	if info.Mode().Perm() != mode || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return File{}, fmt.Errorf("unexpected payload permissions: %s", name)
	}
	if packageName != "" {
		build, err := buildinfo.Read(file)
		if err != nil || build.Path != "github.com/nysa-company/sf/cmd/"+packageName {
			return File{}, fmt.Errorf("unexpected executable build identity: %s", name)
		}
		settings := map[string]string{}
		for _, setting := range build.Settings {
			settings[setting.Key] = setting.Value
		}
		if settings["GOOS"] != identity.OS || settings["GOARCH"] != identity.Arch {
			return File{}, fmt.Errorf("executable platform mismatch: %s", name)
		}
		image, err := macho.NewFile(file)
		if err != nil {
			return File{}, errors.New("executable is not a supported Mach-O image")
		}
		for key, value := range map[string]string{"Version": identity.Version, "Commit": identity.Commit, "Channel": identity.Channel} {
			actual, err := embeddedString(image, "github.com/nysa-company/sf/internal/version."+key)
			if err != nil {
				return File{}, fmt.Errorf("executable %s %s metadata: %w", name, key, err)
			}
			if actual != value {
				return File{}, fmt.Errorf("executable %s %s mismatch", name, key)
			}
		}
	}
	hash := sha256.New()
	size, err := io.Copy(hash, io.LimitReader(contextReader{ctx, file}, maxPayloadBytes+1))
	after, statErr := file.Stat()
	if err != nil || statErr != nil || size != info.Size() || size > maxPayloadBytes || after.Size() != info.Size() || !after.ModTime().Equal(info.ModTime()) {
		return File{}, errors.New("payload changed or could not be hashed")
	}
	return File{Name: name, Size: size, Mode: uint32(mode), SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

// Read the linker's -X string symbol directly; -trimpath omits linker flags
// from buildinfo, and PIE pointers in string headers may need loader rebasing.
// Require both the variable and its linker string, failing closed if a future
// toolchain changes this representation. No payload is executed here.
func embeddedString(image *macho.File, name string) (string, error) {
	if image.Magic != macho.Magic64 || image.Symtab == nil {
		return "", errors.New("missing 64-bit symbol metadata")
	}
	read := func(address uint64, data []byte) error {
		for _, section := range image.Sections {
			if address >= section.Addr && address-section.Addr <= section.Size && uint64(len(data)) <= section.Size-(address-section.Addr) {
				_, err := section.ReadAt(data, int64(address-section.Addr))
				return err
			}
		}
		return errors.New("symbol address outside file sections")
	}
	var address uint64
	count, variables := 0, 0
	for _, symbol := range image.Symtab.Syms {
		if strings.TrimPrefix(symbol.Name, "_") == name {
			variables++
		}
		if strings.TrimPrefix(symbol.Name, "_") == name+".str" {
			address = symbol.Value
			count++
		}
	}
	if count != 1 || variables != 1 {
		return "", errors.New("missing or ambiguous identity symbol")
	}
	data := make([]byte, 0, 128)
	for offset := uint64(0); offset <= 128; offset++ {
		if address+offset < address {
			return "", errors.New("invalid symbol address")
		}
		value := []byte{0}
		if err := read(address+offset, value); err != nil {
			return "", err
		}
		if value[0] == 0 {
			if len(data) == 0 {
				return "", errors.New("empty identity string")
			}
			return string(data), nil
		}
		data = append(data, value[0])
	}
	return "", errors.New("identity string exceeds bound")
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(data)
}
