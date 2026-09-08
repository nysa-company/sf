package pythonclosure

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
)

const EnvironmentVersion = "sf-python-environment/v1"
const maxEnvironmentBytes = 16 << 20

// Environment binds prepared content, not its provenance or permission to run.
// Store command authority and the supervisor must independently authenticate
// the roots, lock and factory-owned bootstrap before accepting this identity.
type Environment struct {
	Version         string   `json:"version"`
	Runtime         Manifest `json:"runtime"`
	Dependencies    Manifest `json:"dependencies"`
	Interpreter     string   `json:"interpreter"`
	LockDigest      string   `json:"lock_digest"`
	BootstrapDigest string   `json:"bootstrap_digest"`
}

func (e Environment) Canonical() ([]byte, string, error) {
	if e.Version != EnvironmentVersion || !validPath(e.Interpreter) || !validDigest(e.LockDigest) || !validDigest(e.BootstrapDigest) {
		return nil, "", ErrInvalid
	}
	// Bound allocation before marshalling caller-controlled paths. JSON can
	// expand a byte to a six-byte escape; reserve overhead for every entry.
	budget := 1024 + 6*len(e.Interpreter)
	for _, manifest := range []Manifest{e.Runtime, e.Dependencies} {
		if len(manifest.Entries) > maximum.entries {
			return nil, "", ErrLimit
		}
		for _, entry := range manifest.Entries {
			if len(entry.Path) > 4096 {
				return nil, "", ErrInvalid
			}
			budget += 256 + 6*len(entry.Path)
			if budget > maxEnvironmentBytes {
				return nil, "", ErrLimit
			}
		}
		if _, _, err := manifest.Canonical(); err != nil {
			return nil, "", err
		}
	}
	found := false
	for _, entry := range e.Runtime.Entries {
		if entry.Path == e.Interpreter && entry.Kind == "file" && entry.Mode&0111 != 0 {
			found = true
		}
	}
	if !found {
		return nil, "", ErrInvalid
	}
	data, err := json.Marshal(e)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	return data, "sha256:" + hex.EncodeToString(sum[:]), nil
}

// DecodeEnvironment admits only the exact canonical wire representation. This
// rejects duplicate/unknown keys as well as alternate encodings of valid data.
func DecodeEnvironment(data []byte) (Environment, error) {
	if len(data) == 0 || len(data) > maxEnvironmentBytes {
		return Environment{}, ErrLimit
	}
	var value Environment
	if json.Unmarshal(data, &value) != nil {
		return Environment{}, ErrInvalid
	}
	canonical, _, err := value.Canonical()
	if err != nil {
		return Environment{}, err
	}
	if !bytes.Equal(data, canonical) {
		return Environment{}, ErrInvalid
	}
	return value, nil
}

// AuthenticateEnvironmentFD compares prepared evidence with independently
// supplied configuration and factory-bootstrap identities, then verifies the
// retained roots. Expected identities must not be taken from the same untrusted
// manifest: the caller supplies them from the frozen command and recipe.
func AuthenticateEnvironmentFD(ctx context.Context, runtimeFD, dependenciesFD int, data []byte, expectedDigest, expectedLock, expectedBootstrap string) (Environment, error) {
	if !validDigest(expectedDigest) || !validDigest(expectedLock) || !validDigest(expectedBootstrap) {
		return Environment{}, ErrInvalid
	}
	value, err := DecodeEnvironment(data)
	if err != nil {
		return Environment{}, err
	}
	_, digest, err := value.Canonical()
	if err != nil || digest != expectedDigest || value.LockDigest != expectedLock || value.BootstrapDigest != expectedBootstrap {
		return Environment{}, ErrInvalid
	}
	if err := VerifyEnvironmentFD(ctx, runtimeFD, dependenciesFD, value); err != nil {
		return Environment{}, err
	}
	return value, nil
}

// VerifyEnvironmentFD checks both retained roots under one overall deadline.
// The caller must exclude writers throughout verification and execution.
func VerifyEnvironmentFD(ctx context.Context, runtimeFD, dependenciesFD int, expected Environment) error {
	if ctx == nil || runtimeFD < 0 || dependenciesFD < 0 {
		return ErrInvalid
	}
	if _, _, err := expected.Canonical(); err != nil {
		return err
	}
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := VerifyDirectoryFD(bounded, runtimeFD, expected.Runtime); err != nil {
		return err
	}
	return VerifyDirectoryFD(bounded, dependenciesFD, expected.Dependencies)
}

func validDigest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value[7:])
	return err == nil
}
