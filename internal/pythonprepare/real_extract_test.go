package pythonprepare

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/pythonclosure"
)

func TestPinnedArchivesProduceVerifiedEnvironment(t *testing.T) {
	archives := os.Getenv("SF_TEST_PYTHON_ARCHIVES")
	if archives == "" {
		t.Skip("requires explicitly supplied pinned archives; skip is not preparation acceptance")
	}
	catalog, err := DefaultCatalog("darwin", "arm64")
	if err != nil {
		t.Fatal(err)
	}
	runtimeArchive, err := os.Open(filepath.Join(archives, "python.tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer runtimeArchive.Close()
	if err := authenticateArtifact(t.Context(), runtimeArchive, catalog.Runtime); err != nil {
		t.Fatal(err)
	}
	_, runtimeRoot := extractionRoot(t)
	if err := extractRuntime(t.Context(), runtimeArchive, int(runtimeRoot.Fd())); err != nil {
		t.Fatal("runtime extraction", err)
	}
	var wheels []*os.File
	for _, artifact := range catalog.Wheels {
		f, err := os.Open(filepath.Join(archives, "wheels", artifact.Name))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if err := authenticateArtifact(t.Context(), f, artifact); err != nil {
			t.Fatal(err)
		}
		wheels = append(wheels, f)
	}
	_, depsRoot := extractionRoot(t)
	if err := extractWheels(t.Context(), wheels, int(depsRoot.Fd())); err != nil {
		t.Fatal("wheel extraction", err)
	}
	runtimeManifest, err := pythonclosure.CaptureDirectoryFD(t.Context(), int(runtimeRoot.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	depsManifest, err := pythonclosure.CaptureDirectoryFD(t.Context(), int(depsRoot.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	e := pythonclosure.Environment{Version: pythonclosure.EnvironmentVersion, Runtime: runtimeManifest, Dependencies: depsManifest, Interpreter: "bin/python3.13", LockDigest: LockDigest(), BootstrapDigest: pythonclosure.BootstrapDigest()}
	data, digest, err := e.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if digest != catalog.EnvironmentDigest {
		t.Fatal("prepared environment differs from independently pinned catalog", digest)
	}
	if _, err := pythonclosure.AuthenticateEnvironmentFD(t.Context(), int(runtimeRoot.Fd()), int(depsRoot.Fd()), data, digest, LockDigest(), pythonclosure.BootstrapDigest()); err != nil {
		t.Fatal(err)
	}
	t.Logf("prepared environment %s: runtime entries=%d dependencies=%d", digest, len(runtimeManifest.Entries), len(depsManifest.Entries))
}
