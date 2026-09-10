package runtimeassets

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/gitssh"
)

func TestResolveSSHExactBundle(t *testing.T) {
	for _, channel := range []domain.Channel{domain.ChannelStable, domain.ChannelDev} {
		t.Run(string(channel), func(t *testing.T) {
			root := privateDirectory(t)
			suffix := ""
			if channel == domain.ChannelDev {
				suffix = "-dev"
			}
			primary := executable(t, root, "sf"+suffix, 0o700)
			helper := executable(t, root, "sf-ssh"+suffix, 0o700)
			known := filepath.Join(root, "github_known_hosts")
			if err := os.WriteFile(known, []byte(gitssh.PinnedKnownHosts), 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := ResolveSSH(channel, primary)
			canonicalHelper, _ := filepath.EvalSymlinks(helper)
			canonicalKnown, _ := filepath.EvalSymlinks(known)
			if err != nil || got.Helper != canonicalHelper || got.KnownHosts != canonicalKnown {
				t.Fatalf("bundle=%+v err=%v", got, err)
			}
			if repeated, err := ResolveSSH(channel, root+"///sf"+suffix); err != nil || repeated != got {
				t.Fatalf("repeated separators changed bundle: %+v %v", repeated, err)
			}
			if err := os.WriteFile(known, []byte("untrusted"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := ResolveSSH(channel, primary); err == nil {
				t.Fatal("accepted replaced host keys")
			}
			if err := os.Remove(known); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(helper, known); err != nil {
				t.Fatal(err)
			}
			if _, err := ResolveSSH(channel, primary); err == nil {
				t.Fatal("accepted symlinked host keys")
			}
		})
	}
}
