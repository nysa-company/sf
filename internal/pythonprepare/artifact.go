package pythonprepare

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// authenticateArtifact checks retained bytes against code-owned download
// identity, rewinding for extraction. The caller excludes concurrent writers.
func authenticateArtifact(ctx context.Context, file *os.File, a Artifact) error {
	if ctx == nil || file == nil || a.Size <= 0 || a.Size > 32<<20 || len(a.SHA256) != 64 {
		return ErrArtifact
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	before, err := file.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() != a.Size {
		return ErrArtifact
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ErrArtifact
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(contextReader{bounded, file}, a.Size+1))
	if err != nil || n != a.Size || hex.EncodeToString(h.Sum(nil)) != a.SHA256 {
		return ErrArtifact
	}
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || before.Mode() != after.Mode() || !before.ModTime().Equal(after.ModTime()) {
		return ErrArtifact
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ErrArtifact
	}
	return bounded.Err()
}

func openArtifact(ctx context.Context, parent int, a Artifact) (*os.File, error) {
	if parent < 0 || !validArchivePath(a.Name) {
		return nil, ErrArtifact
	}
	// Artifact names are single components; archive entry paths are not allowed.
	for _, c := range a.Name {
		if c == '/' {
			return nil, ErrArtifact
		}
	}
	fd, err := unix.Openat(parent, a.Name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrArtifact
	}
	file := os.NewFile(uintptr(fd), "verified-python-archive")
	if err := authenticateArtifact(ctx, file, a); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}
