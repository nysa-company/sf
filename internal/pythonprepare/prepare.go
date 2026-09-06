package pythonprepare

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/nysa-company/sf/internal/pythonclosure"
	"golang.org/x/sys/unix"
)

type Result struct {
	EnvironmentDigest string `json:"environment_digest"`
	LockDigest        string `json:"lock_digest"`
	AlreadyPrepared   bool   `json:"already_prepared"`
}

var ErrCacheInvalid = errors.New("existing Python cache failed verification and was retained")

// Prepare changes only the explicit caller-authenticated private channel cache.
// It does not discover HOME, modify a project, run code, or contact a daemon.
// The caller obtains download consent before invoking it. Staging is derivative
// data, not lifecycle authority; existing digest directories are never removed.
func Prepare(ctx context.Context, root string) (Result, error) {
	catalog, err := DefaultCatalog(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return Result{}, err
	}
	client := preparationHTTPClient()
	defer client.CloseIdleConnections()
	return prepare(ctx, root, catalog, client)
}

func prepare(ctx context.Context, root string, catalog Catalog, client *http.Client) (Result, error) {
	if ctx == nil || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" {
		return Result{}, ErrArtifact
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil || canonical != root {
		return Result{}, ErrArtifact
	}
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return Result{}, ErrArtifact
	}
	parent := os.NewFile(uintptr(fd), "python-prepared-channel")
	defer parent.Close()
	var identity unix.Stat_t
	if unix.Fstat(fd, &identity) != nil || identity.Uid != uint32(os.Geteuid()) || identity.Mode&0077 != 0 || identity.Mode&07000 != 0 {
		return Result{}, ErrArtifact
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	result := Result{EnvironmentDigest: catalog.EnvironmentDigest, LockDigest: LockDigest()}
	// Catalog identity is code-owned. Never derive an expected digest from an
	// existing cache manifest, even when replaying successful preparation.
	if len(catalog.EnvironmentDigest) != 71 {
		return Result{}, ErrArtifact
	}
	name := catalog.EnvironmentDigest[7:]
	var existing unix.Stat_t
	err = unix.Fstatat(fd, name, &existing, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		p, err := pythonclosure.OpenPreparedDirectoryFD(bounded, fd, catalog.EnvironmentDigest, LockDigest(), pythonclosure.BootstrapDigest())
		if err != nil {
			return Result{}, errors.Join(ErrCacheInvalid, err)
		}
		defer p.Close()
		result.AlreadyPrepared = true
		return result, nil
	}
	if !errors.Is(err, unix.ENOENT) {
		return Result{}, ErrArtifact
	}
	stage, err := os.MkdirTemp(root, ".prepare-python-")
	if err != nil {
		return Result{}, ErrArtifact
	}
	// Exact, newly created private staging only. Published snapshots move out
	// of it atomically and are never targets of this cleanup.
	defer os.RemoveAll(stage)
	for _, dir := range []string{"archives", "build", "build/runtime", "build/dependencies"} {
		if err := os.Mkdir(filepath.Join(stage, dir), 0700); err != nil {
			return Result{}, ErrArtifact
		}
	}
	archives, err := os.Open(filepath.Join(stage, "archives"))
	if err != nil {
		return Result{}, ErrArtifact
	}
	defer archives.Close()
	all := append([]Artifact{catalog.Runtime}, catalog.Wheels...)
	for _, artifact := range all {
		if err := downloadArtifact(bounded, int(archives.Fd()), artifact, client); err != nil {
			return Result{}, err
		}
	}
	runtimeArchive, err := openArtifact(bounded, int(archives.Fd()), catalog.Runtime)
	if err != nil {
		return Result{}, err
	}
	defer runtimeArchive.Close()
	runtimeRoot, err := os.Open(filepath.Join(stage, "build/runtime"))
	if err != nil {
		return Result{}, ErrArtifact
	}
	defer runtimeRoot.Close()
	if err := extractRuntime(bounded, runtimeArchive, int(runtimeRoot.Fd())); err != nil {
		return Result{}, err
	}
	var wheels []*os.File
	defer func() {
		for _, f := range wheels {
			f.Close()
		}
	}()
	for _, artifact := range catalog.Wheels {
		f, err := openArtifact(bounded, int(archives.Fd()), artifact)
		if err != nil {
			return Result{}, err
		}
		wheels = append(wheels, f)
	}
	depsRoot, err := os.Open(filepath.Join(stage, "build/dependencies"))
	if err != nil {
		return Result{}, ErrArtifact
	}
	defer depsRoot.Close()
	if err := extractWheels(bounded, wheels, int(depsRoot.Fd())); err != nil {
		return Result{}, err
	}
	runtimeManifest, err := pythonclosure.CaptureDirectoryFD(bounded, int(runtimeRoot.Fd()))
	if err != nil {
		return Result{}, err
	}
	depsManifest, err := pythonclosure.CaptureDirectoryFD(bounded, int(depsRoot.Fd()))
	if err != nil {
		return Result{}, err
	}
	environment := pythonclosure.Environment{Version: pythonclosure.EnvironmentVersion, Runtime: runtimeManifest, Dependencies: depsManifest, Interpreter: "bin/python3.13", LockDigest: LockDigest(), BootstrapDigest: pythonclosure.BootstrapDigest()}
	data, digest, err := environment.Canonical()
	if err != nil || digest != catalog.EnvironmentDigest {
		return Result{}, ErrArtifact
	}
	manifest, err := os.OpenFile(filepath.Join(stage, "build/environment.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0400)
	if err != nil {
		return Result{}, ErrArtifact
	}
	_, writeErr := manifest.Write(data)
	syncErr := manifest.Sync()
	closeErr := manifest.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return Result{}, ErrArtifact
	}
	if err := os.Rename(filepath.Join(stage, "build"), filepath.Join(stage, name)); err != nil {
		return Result{}, ErrArtifact
	}
	var named unix.Stat_t
	if unix.Lstat(root, &named) != nil || named.Dev != identity.Dev || named.Ino != identity.Ino {
		return Result{}, ErrArtifact
	}
	staging, err := os.Open(stage)
	if err != nil {
		return Result{}, ErrArtifact
	}
	defer staging.Close()
	published, err := pythonclosure.PublishPreparedDirectoryFD(bounded, fd, int(staging.Fd()), digest, LockDigest(), pythonclosure.BootstrapDigest())
	if err != nil {
		return Result{}, err
	}
	result.AlreadyPrepared = !published
	return result, nil
}
