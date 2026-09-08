// Package pythonclosure describes prepared Python runtime/dependency snapshots.
// A manifest is content evidence, not permission to execute, a package lock
// resolver, or a replacement for Store-issued command and filesystem authority.
package pythonclosure

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

const Version = "sf-python-snapshot/v1"

var ErrInvalid = errors.New("invalid prepared Python snapshot")
var ErrLimit = errors.New("Python snapshot exceeds inspection bounds")

type Entry struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	Mode   uint32 `json:"mode"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type Manifest struct {
	Version string  `json:"version"`
	Entries []Entry `json:"entries"`
}

type limits struct {
	entries, depth int
	file, total    int64
}

var maximum = limits{20000, 32, 64 << 20, 256 << 20}

// CaptureDirectoryFD owns no filesystem authority. The caller must authenticate
// and retain rootFD and prevent writers to the prepared snapshot. Symlinks and
// special files are refused, including harmless launcher aliases: preparation
// must materialize the selected regular-file layout before this boundary.
// No path is reopened through an ambient root pathname. Each call uses its own
// directory cursor, so capture/revalidation never consumes the caller's cursor.
func CaptureDirectoryFD(ctx context.Context, rootFD int) (Manifest, error) {
	if ctx == nil || rootFD < 0 {
		return Manifest{}, ErrInvalid
	}
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return capture(bounded, rootFD, maximum)
}

func capture(ctx context.Context, rootFD int, bound limits) (Manifest, error) {
	manifest := Manifest{Version: Version, Entries: []Entry{}}
	var total int64
	var walk func(int, string, int) error
	walk = func(parent int, relative string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if depth > bound.depth {
			return ErrLimit
		}
		fd, err := unix.Openat(parent, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return ErrInvalid
		}
		directory := os.NewFile(uintptr(fd), "python-snapshot-directory")
		defer directory.Close()
		for {
			children, readErr := directory.ReadDir(128)
			for _, child := range children {
				if err := ctx.Err(); err != nil {
					return err
				}
				name := child.Name()
				rel := path.Join(relative, name)
				if !validPath(rel) || strings.Contains(name, "/") {
					return ErrInvalid
				}
				if len(manifest.Entries) >= bound.entries {
					return ErrLimit
				}
				var before unix.Stat_t
				if unix.Fstatat(fd, name, &before, unix.AT_SYMLINK_NOFOLLOW) != nil {
					return ErrInvalid
				}
				kind := before.Mode & unix.S_IFMT
				if kind != unix.S_IFREG && kind != unix.S_IFDIR {
					return ErrInvalid
				}
				flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK
				if kind == unix.S_IFDIR {
					flags |= unix.O_DIRECTORY
				}
				openedFD, err := unix.Openat(fd, name, flags, 0)
				if err != nil {
					return ErrInvalid
				}
				file := os.NewFile(uintptr(openedFD), "python-snapshot-entry")
				var opened unix.Stat_t
				err = unix.Fstat(openedFD, &opened)
				if err != nil || opened.Dev != before.Dev || opened.Ino != before.Ino || opened.Mode != before.Mode || opened.Size != before.Size {
					file.Close()
					return ErrInvalid
				}
				entry := Entry{Path: rel, Mode: uint32(opened.Mode & 07777)}
				if kind == unix.S_IFDIR {
					entry.Kind = "directory"
					manifest.Entries = append(manifest.Entries, entry)
					err = walk(openedFD, rel, depth+1)
				} else {
					entry.Kind, entry.Size = "file", opened.Size
					if entry.Size < 0 || entry.Size > bound.file || entry.Size > bound.total-total {
						file.Close()
						return ErrLimit
					}
					total += entry.Size
					prior, statErr := file.Stat()
					if statErr != nil {
						file.Close()
						return ErrInvalid
					}
					hash := sha256.New()
					var size int64
					size, err = io.Copy(hash, io.LimitReader(contextReader{ctx, file}, entry.Size+1))
					after, statErr := file.Stat()
					if err == nil && (statErr != nil || size != entry.Size || after.Size() != prior.Size() || after.Mode() != prior.Mode() || !after.ModTime().Equal(prior.ModTime())) {
						err = ErrInvalid
					}
					entry.SHA256 = hex.EncodeToString(hash.Sum(nil))
					manifest.Entries = append(manifest.Entries, entry)
				}
				file.Close()
				if err != nil {
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
	if err := walk(rootFD, "", 0); err != nil {
		return Manifest{}, err
	}
	sort.Slice(manifest.Entries, func(i, j int) bool { return manifest.Entries[i].Path < manifest.Entries[j].Path })
	if _, _, err := manifest.Canonical(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// Canonical binds ordered paths, kinds, modes, sizes and contents, not a host
// pathname or inode. Live root identity and artifact provenance are separate.
func (m Manifest) Canonical() ([]byte, string, error) {
	if m.Version != Version || len(m.Entries) == 0 || len(m.Entries) > maximum.entries {
		return nil, "", ErrInvalid
	}
	directories := map[string]bool{".": true}
	var total int64
	files := 0
	for i, entry := range m.Entries {
		if !validPath(entry.Path) || strings.Count(entry.Path, "/") > maximum.depth || (i > 0 && m.Entries[i-1].Path >= entry.Path) || !directories[path.Dir(entry.Path)] || entry.Mode & ^uint32(0777) != 0 {
			return nil, "", ErrInvalid
		}
		switch entry.Kind {
		case "directory":
			if entry.Size != 0 || entry.SHA256 != "" || strings.Count(entry.Path, "/") >= maximum.depth {
				return nil, "", ErrInvalid
			}
			directories[entry.Path] = true
		case "file":
			if entry.Size < 0 || entry.Size > maximum.file || entry.Size > maximum.total-total || len(entry.SHA256) != 64 || strings.ToLower(entry.SHA256) != entry.SHA256 {
				return nil, "", ErrInvalid
			}
			if _, err := hex.DecodeString(entry.SHA256); err != nil {
				return nil, "", ErrInvalid
			}
			total += entry.Size
			files++
		default:
			return nil, "", ErrInvalid
		}
	}
	if files == 0 {
		return nil, "", ErrInvalid
	}
	data, err := json.Marshal(m)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	return data, "sha256:" + hex.EncodeToString(sum[:]), nil
}

func VerifyDirectoryFD(ctx context.Context, rootFD int, expected Manifest) error {
	want, _, err := expected.Canonical()
	if err != nil {
		return err
	}
	actual, err := CaptureDirectoryFD(ctx, rootFD)
	if err != nil {
		return err
	}
	got, _, err := actual.Canonical()
	if err != nil || !bytes.Equal(want, got) {
		return ErrInvalid
	}
	return nil
}

func validPath(value string) bool {
	if !utf8.ValidString(value) || value == "." || value == "" || path.IsAbs(value) || path.Clean(value) != value || strings.ContainsAny(value, "\\\x00\r\n") || value == ".." || strings.HasPrefix(value, "../") {
		return false
	}
	return len(value) <= 4096
}

type contextReader struct {
	ctx   context.Context
	input io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.input.Read(p)
}
