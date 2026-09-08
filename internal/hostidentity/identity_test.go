package hostidentity

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
)

func TestHostOutputBoundCannotBeBypassedByCopy(t *testing.T) {
	var output boundedOutput
	_, err := io.Copy(&output, io.LimitReader(strings.NewReader(strings.Repeat("x", 128*1024)), 128*1024))
	if !errors.Is(err, ErrUnavailable) || len(output.Bytes()) > 64*1024 {
		t.Fatal("host output exceeded its hard byte bound")
	}
}

func TestPlatformIdentityRequiresExactlyOneValidUUID(t *testing.T) {
	valid := `    "IOPlatformUUID" = "12345678-ABCD-1234-ABCD-123456789ABC"`
	digest, err := platformMachineDigest([]byte(valid))
	if err != nil || len(digest) != 64 || strings.Contains(digest, "12345678") {
		t.Fatalf("identity digest was not produced: %v", err)
	}
	lower, err := platformMachineDigest([]byte(strings.ReplaceAll(valid, "ABCD", "abcd")))
	if err != nil || lower != digest {
		t.Fatal("UUID casing changed machine identity")
	}
	for _, invalid := range []string{"", valid + "\n" + valid, valid + "\n" + `"IOPlatformUUID" = malformed`, `"IOPlatformUUID" = "secret"`, `"IOPlatformUUID" = "00000000-0000-0000-0000-000000000000"`, valid + " trailing"} {
		if _, err := platformMachineDigest([]byte(invalid)); !errors.Is(err, ErrUnavailable) {
			t.Fatal("ambiguous/malformed machine identity was accepted")
		}
	}
}

func TestObserveRequiresLiveContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if value, err := Observe(ctx); !errors.Is(err, ErrUnavailable) || value != (Identity{}) {
		t.Fatal("canceled observation returned host evidence")
	}
}

func TestObserveHostIdentity(t *testing.T) {
	first, err := Observe(context.Background())
	if runtime.GOOS != "darwin" {
		if !errors.Is(err, ErrUnavailable) {
			t.Fatal("unsupported host produced recovery evidence")
		}
		return
	}
	if err != nil || len(first.MachineDigest) != 64 || first.BootID == "" {
		t.Fatalf("OS identity unavailable: %v", err)
	}
	second, err := Observe(context.Background())
	if err != nil || first != second {
		t.Fatal("consecutive observations did not bind the same host and boot")
	}
}
