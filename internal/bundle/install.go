package bundle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/runtimeassets"
)

// Install returns created=true once the exclusively-created destination
// exists. A failure after that point retains the partial directory for explicit
// inspection; retries never overwrite it. No payload is executed.
func Install(ctx context.Context, source, destination string) (manifest Manifest, created bool, err error) {
	manifest, err = Verify(ctx, source)
	if err != nil {
		return Manifest{}, false, err
	}
	if manifest.Identity.OS != runtime.GOOS || manifest.Identity.Arch != runtime.GOARCH {
		return Manifest{}, false, errors.New("bundle does not match this host")
	}
	if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination {
		return Manifest{}, false, errors.New("destination must be an absolute clean path")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(destination))
	if err != nil || parent != filepath.Dir(destination) {
		return Manifest{}, false, errors.New("destination parent must exist and be canonical")
	}
	if err := runtimeassets.ValidateInstallParent(parent); err != nil {
		return Manifest{}, false, errors.New("destination ancestry is unsafe; choose an owner-controlled location with no group/world-writable ancestors, not a shared temporary directory")
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, false, err
	}
	if err := os.Mkdir(destination, 0700); err != nil {
		return Manifest{}, false, errors.New("destination exists or cannot be created; nothing was overwritten")
	}
	created = true
	for _, entry := range manifest.Files {
		if err := copyPayload(ctx, filepath.Join(source, entry.Name), filepath.Join(destination, entry.Name), entry); err != nil {
			return Manifest{}, true, err
		}
	}
	data, err := encode(manifest)
	if err != nil {
		return Manifest{}, true, err
	}
	file, err := os.OpenFile(filepath.Join(destination, ManifestName), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0644)
	if err != nil {
		return Manifest{}, true, errors.New("could not create installed manifest")
	}
	_, writeErr := file.Write(data)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return Manifest{}, true, errors.New("installed manifest write failed")
	}
	if _, err := Verify(ctx, destination); err != nil {
		return Manifest{}, true, err
	}
	primary := "sf"
	if manifest.Identity.Channel == "dev" {
		primary = "sf-dev"
	}
	executable := filepath.Join(destination, primary)
	channel := domain.Channel(manifest.Identity.Channel)
	if _, err := runtimeassets.ResolveCore(channel, executable); err != nil {
		return Manifest{}, true, errors.New("installed runtime core is unsafe; partial destination retained")
	}
	if _, err := runtimeassets.ResolvePublication(channel, executable); err != nil {
		return Manifest{}, true, errors.New("installed publication helper is unsafe; partial destination retained")
	}
	directory, err := os.Open(destination)
	if err != nil {
		return Manifest{}, true, err
	}
	err = directory.Sync()
	directory.Close()
	if err != nil {
		return Manifest{}, true, errors.New("installed directory sync failed")
	}
	parentDirectory, err := os.Open(parent)
	if err != nil {
		return Manifest{}, true, errors.New("installed parent unavailable for sync")
	}
	err = parentDirectory.Sync()
	parentDirectory.Close()
	if err != nil {
		return Manifest{}, true, errors.New("installed parent sync failed")
	}
	return manifest, true, nil
}

func copyPayload(ctx context.Context, source, destination string, entry File) error {
	input, err := openRegular(source, maxPayloadBytes)
	if err != nil {
		return errors.New("source changed before copy")
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return errors.New("could not create new payload")
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(output, hash), io.LimitReader(contextReader{ctx, input}, maxPayloadBytes+1))
	if copyErr == nil && (size != entry.Size || hex.EncodeToString(hash.Sum(nil)) != entry.SHA256) {
		copyErr = errors.New("source bytes changed during installation")
	}
	if copyErr == nil {
		copyErr = output.Chmod(os.FileMode(entry.Mode))
	}
	if copyErr == nil {
		copyErr = output.Sync()
	}
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		return errors.New("payload copy or verification failed; partial destination retained")
	}
	return nil
}
