package runtimeassets

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestResolveCoreExactChannelBundle(t *testing.T) {
	for _, test := range []struct {
		channel         domain.Channel
		primary, helper string
		foreignPrimary  string
		foreignHelper   string
	}{
		{domain.ChannelStable, "sf", "sf-git-exec", "sf-dev", "sf-git-exec-dev"},
		{domain.ChannelDev, "sf-dev", "sf-git-exec-dev", "sf", "sf-git-exec"},
	} {
		t.Run(string(test.channel), func(t *testing.T) {
			root := privateDirectory(t)
			primary := executable(t, root, test.primary, 0o700)
			helper := executable(t, root, test.helper, 0o700)
			got, err := ResolveCore(test.channel, primary)
			if err != nil {
				t.Fatal(err)
			}
			wantPrimary, _ := filepath.EvalSymlinks(primary)
			wantHelper, _ := filepath.EvalSymlinks(helper)
			if got.Executable != wantPrimary || got.GitExec != wantHelper {
				t.Fatalf("bundle=%+v want primary=%q helper=%q", got, wantPrimary, wantHelper)
			}
			if _, err := ResolveCore(test.channel, executable(t, root, test.foreignPrimary, 0o700)); !errors.Is(err, ErrUnsafeBundle) {
				t.Fatalf("foreign primary err=%v", err)
			}
			if err := os.Rename(helper, helper+".saved"); err != nil {
				t.Fatal(err)
			}
			executable(t, root, test.foreignHelper, 0o700)
			if _, err := ResolveCore(test.channel, primary); !errors.Is(err, ErrUnsafeBundle) {
				t.Fatalf("foreign-only helper err=%v", err)
			}
		})
	}
}

func TestResolveCoreRejectsUnsafeExecutableFacts(t *testing.T) {
	t.Run("invalid-channel", func(t *testing.T) {
		if _, err := ResolveCore(domain.Channel("other"), "/tmp/sf"); !errors.Is(err, ErrUnsafeBundle) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("relative", func(t *testing.T) {
		if _, err := ResolveCore(domain.ChannelDev, "sf-dev"); !errors.Is(err, ErrUnsafeBundle) {
			t.Fatalf("err=%v", err)
		}
	})
	for _, test := range []struct {
		name string
		mode os.FileMode
	}{
		{"not-executable", 0o600},
		{"group-writable", 0o720},
		{"world-writable", 0o702},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := privateDirectory(t)
			primary := executable(t, root, "sf-dev", test.mode)
			executable(t, root, "sf-git-exec-dev", 0o700)
			if _, err := ResolveCore(domain.ChannelDev, primary); !errors.Is(err, ErrUnsafeBundle) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	t.Run("symlink-primary", func(t *testing.T) {
		root := privateDirectory(t)
		target := executable(t, root, "real-sf-dev", 0o700)
		primary := filepath.Join(root, "sf-dev")
		if err := os.Symlink(target, primary); err != nil {
			t.Fatal(err)
		}
		executable(t, root, "sf-git-exec-dev", 0o700)
		if _, err := ResolveCore(domain.ChannelDev, primary); !errors.Is(err, ErrUnsafeBundle) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("symlink-helper", func(t *testing.T) {
		root := privateDirectory(t)
		primary := executable(t, root, "sf-dev", 0o700)
		target := executable(t, root, "real-helper", 0o700)
		if err := os.Symlink(target, filepath.Join(root, "sf-git-exec-dev")); err != nil {
			t.Fatal(err)
		}
		if _, err := ResolveCore(domain.ChannelDev, primary); !errors.Is(err, ErrUnsafeBundle) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("hardlinked-helper", func(t *testing.T) {
		root := privateDirectory(t)
		primary := executable(t, root, "sf-dev", 0o700)
		target := executable(t, root, "real-helper", 0o700)
		if err := os.Link(target, filepath.Join(root, "sf-git-exec-dev")); err != nil {
			t.Fatal(err)
		}
		if _, err := ResolveCore(domain.ChannelDev, primary); !errors.Is(err, ErrUnsafeBundle) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("unsafe-parent", func(t *testing.T) {
		root := privateDirectory(t)
		if err := os.Chmod(root, 0o770); err != nil {
			t.Fatal(err)
		}
		primary := executable(t, root, "sf-dev", 0o700)
		executable(t, root, "sf-git-exec-dev", 0o700)
		if _, err := ResolveCore(domain.ChannelDev, primary); !errors.Is(err, ErrUnsafeBundle) {
			t.Fatalf("err=%v", err)
		}
	})
}

func privateDirectory(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func executable(t *testing.T, root, name string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.WriteFile(path, []byte("bundle fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Chmod after creation makes the unsafe-mode fixtures independent of the
	// developer machine's umask.
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}
