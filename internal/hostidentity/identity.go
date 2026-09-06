// Package hostidentity reads OS facts for same-host reboot recovery. Its facts
// are observations, not permission to release a process or mutation lease.
package hostidentity

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
)

var ErrUnavailable = errors.New("trusted host identity is unavailable")

type Identity struct {
	MachineDigest string
	BootID        string
}

func (i Identity) Valid() bool {
	digest, err := hex.DecodeString(i.MachineDigest)
	boot, bootErr := canonicalUUID(i.BootID)
	return err == nil && len(digest) == sha256.Size && strings.ToLower(i.MachineDigest) == i.MachineDigest && bootErr == nil && boot == i.BootID
}

type boundedOutput struct{ buffer bytes.Buffer }

func (b *boundedOutput) Write(p []byte) (int, error) {
	if len(p) > 64*1024-b.buffer.Len() {
		return 0, ErrUnavailable
	}
	return b.buffer.Write(p)
}

func (b *boundedOutput) Bytes() []byte { return b.buffer.Bytes() }

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
var platformUUIDPattern = regexp.MustCompile(`(?m)^[ \t]*"IOPlatformUUID" = "([^"]*)"[ \t]*$`)

func canonicalUUID(value string) (string, error) {
	if !uuidPattern.MatchString(value) || value == "00000000-0000-0000-0000-000000000000" {
		return "", ErrUnavailable
	}
	return strings.ToLower(value), nil
}

func platformMachineDigest(output []byte) (string, error) {
	if bytes.Count(output, []byte(`"IOPlatformUUID"`)) != 1 {
		return "", ErrUnavailable
	}
	matches := platformUUIDPattern.FindAllSubmatch(output, -1)
	if len(matches) != 1 {
		return "", ErrUnavailable
	}
	uuid, err := canonicalUUID(string(matches[0][1]))
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte("sf/host-machine/v1\x00" + uuid))
	return hex.EncodeToString(digest[:]), nil
}
