package pythonclosure

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func environmentFixture(t *testing.T) (Environment, int, int, string) {
	t.Helper()
	root, runtimeFD, _ := fixture(t)
	if err := os.Chmod(filepath.Join(root, "lib/sample.py"), 0500); err != nil {
		t.Fatal(err)
	}
	runtime, err := CaptureDirectoryFD(t.Context(), runtimeFD)
	if err != nil {
		t.Fatal(err)
	}
	depsRoot, depsFD, deps := fixture(t)
	return Environment{EnvironmentVersion, runtime, deps, "lib/sample.py", "sha256:" + strings.Repeat("a", 64), "sha256:" + strings.Repeat("b", 64)}, runtimeFD, depsFD, depsRoot
}

func TestEnvironmentCanonicalRoundTripAndBinding(t *testing.T) {
	e, _, _, _ := environmentFixture(t)
	data, digest, err := e.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeEnvironment(data)
	if err != nil {
		t.Fatal(err)
	}
	again, got, err := decoded.Canonical()
	if err != nil || got != digest || !bytes.Equal(data, again) {
		t.Fatal("canonical round trip changed identity")
	}
	for _, field := range []string{"runtime", "dependencies", "lock", "bootstrap", "interpreter", "version"} {
		t.Run(field, func(t *testing.T) {
			changed, err := DecodeEnvironment(data)
			if err != nil {
				t.Fatal(err)
			}
			switch field {
			case "runtime":
				changed.Runtime.Entries[1].SHA256 = strings.Repeat("c", 64)
			case "dependencies":
				changed.Dependencies.Entries[1].SHA256 = strings.Repeat("c", 64)
			case "lock":
				changed.LockDigest = "sha256:" + strings.Repeat("c", 64)
			case "bootstrap":
				changed.BootstrapDigest = "sha256:" + strings.Repeat("c", 64)
			case "interpreter":
				changed.Interpreter = "missing"
			case "version":
				changed.Version = "unknown"
			}
			_, changedDigest, err := changed.Canonical()
			if err == nil && changedDigest == digest {
				t.Fatal("mutation retained identity")
			}
		})
	}
}

func TestEnvironmentRejectsAlternateWireAndInvalidInterpreter(t *testing.T) {
	e, _, _, _ := environmentFixture(t)
	data, _, _ := e.Canonical()
	for _, value := range [][]byte{
		append([]byte(" "), data...),
		bytes.Replace(data, []byte(`"version":`), []byte(`"unknown":1,"version":`), 1),
		bytes.Replace(data, []byte(`"version":`), []byte(`"version":"sf-python-environment/v1","version":`), 1),
		bytes.Replace(data, []byte(`"version":`), []byte(`"Version":`), 1),
		append(data, '\n'),
		nil,
		bytes.Repeat([]byte(" "), maxEnvironmentBytes+1),
	} {
		if _, err := DecodeEnvironment(value); err == nil {
			t.Fatal("accepted noncanonical wire")
		}
	}
	e.Runtime.Entries[1].Mode = 0400
	if _, _, err := e.Canonical(); err == nil {
		t.Fatal("accepted non-executable interpreter")
	}
	e.Runtime.Entries[1].Mode = 0500
	e.LockDigest = strings.ToUpper(e.LockDigest)
	if _, _, err := e.Canonical(); err == nil {
		t.Fatal("accepted noncanonical lock digest")
	}
}

func TestEnvironmentVerifiesBothRoots(t *testing.T) {
	e, runtimeFD, depsFD, depsRoot := environmentFixture(t)
	if err := VerifyEnvironmentFD(t.Context(), runtimeFD, depsFD, e); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := VerifyEnvironmentFD(ctx, runtimeFD, depsFD, e); err == nil {
		t.Fatal("ignored cancellation")
	}
	if err := VerifyEnvironmentFD(nil, runtimeFD, depsFD, e); err == nil {
		t.Fatal("accepted nil context")
	}
	if err := os.WriteFile(filepath.Join(depsRoot, "lib/sample.py"), []byte("abd"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyEnvironmentFD(t.Context(), runtimeFD, depsFD, e); err == nil {
		t.Fatal("accepted changed dependency")
	}
}

func TestEnvironmentAuthenticatesIndependentExpectedBindings(t *testing.T) {
	e, runtimeFD, depsFD, _ := environmentFixture(t)
	data, digest, err := e.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AuthenticateEnvironmentFD(t.Context(), runtimeFD, depsFD, data, digest, e.LockDigest, e.BootstrapDigest); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"environment", "lock", "bootstrap", "empty"} {
		t.Run(field, func(t *testing.T) {
			want, lock, bootstrap := digest, e.LockDigest, e.BootstrapDigest
			other := "sha256:" + strings.Repeat("d", 64)
			switch field {
			case "environment":
				want = other
			case "lock":
				lock = other
			case "bootstrap":
				bootstrap = other
			case "empty":
				want = ""
			}
			if _, err := AuthenticateEnvironmentFD(t.Context(), runtimeFD, depsFD, data, want, lock, bootstrap); err == nil {
				t.Fatal("accepted mismatched independent identity")
			}
		})
	}
}

func TestEnvironmentRejectsOversizedCanonicalAllocation(t *testing.T) {
	e, _, _, _ := environmentFixture(t)
	entry := e.Runtime.Entries[1]
	entry.Path = strings.Repeat("x", 4096)
	e.Runtime.Entries = make([]Entry, maximum.entries)
	for i := range e.Runtime.Entries {
		e.Runtime.Entries[i] = entry
	}
	// Size budgeting precedes ordering validation and JSON allocation.
	if _, _, err := e.Canonical(); !errors.Is(err, ErrLimit) {
		t.Fatalf("expected pre-allocation bound, got %v", err)
	}
}
