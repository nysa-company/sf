package bundle

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/runtimeassets"
)

func TestRealBundleManifestRejectsPayloadAndMetadataTamper(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("macOS bundle acceptance")
	}
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	identity := Identity{Version: "1.2.3-dev.1", Commit: strings.Repeat("a", 40), Channel: "dev", OS: "darwin", Arch: runtime.GOARCH}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "make", "-s", "build-dev", "BIN_DIR="+directory, "DEV_VERSION="+identity.Version, "COMMIT="+identity.Commit)
	build.Dir = "../.."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build=%v %s", err, output)
	}
	manifest, err := CreateManifest(ctx, directory, identity)
	if err != nil || len(manifest.Files) != 6 {
		t.Fatalf("manifest=%+v err=%v", manifest, err)
	}
	if _, err := Verify(ctx, directory); err != nil {
		t.Fatal(err)
	}
	t.Run("license required and authenticated", func(t *testing.T) {
		path := filepath.Join(directory, "LICENSE")
		original, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(original), "MIT License") {
			t.Fatalf("license absent: %v", err)
		}
		defer os.WriteFile(path, original, 0644)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if _, err := Verify(ctx, directory); err == nil {
			t.Fatal("accepted missing license")
		}
		if err := os.WriteFile(path, []byte("changed license"), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := Verify(ctx, directory); err == nil {
			t.Fatal("accepted changed license")
		}
	})
	if _, err := CreateManifest(ctx, directory, identity); err == nil {
		t.Fatal("overwrote manifest")
	}
	t.Run("changed bytes", func(t *testing.T) {
		path := filepath.Join(directory, "github_known_hosts")
		original, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		defer os.WriteFile(path, original, 0644)
		if err := os.WriteFile(path, []byte("changed host key\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := Verify(ctx, directory); err == nil {
			t.Fatal("accepted changed bytes")
		}
	})
	t.Run("permissions", func(t *testing.T) {
		path := filepath.Join(directory, "sf-dev")
		if err := os.Chmod(path, 0644); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(path, 0755)
		if _, err := Verify(ctx, directory); err == nil {
			t.Fatal("accepted nonexecutable binary")
		}
	})
	t.Run("extra file", func(t *testing.T) {
		path := filepath.Join(directory, "extra")
		if err := os.WriteFile(path, []byte("extra"), 0600); err != nil {
			t.Fatal(err)
		}
		defer os.Remove(path)
		if _, err := Verify(ctx, directory); err == nil {
			t.Fatal("accepted extra file")
		}
	})
	t.Run("missing file", func(t *testing.T) {
		path := filepath.Join(directory, "github_known_hosts")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		defer os.WriteFile(path, data, 0644)
		if _, err := Verify(ctx, directory); err == nil {
			t.Fatal("accepted missing file")
		}
	})
	t.Run("symlink", func(t *testing.T) {
		path := filepath.Join(directory, "github_known_hosts")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(t.TempDir(), "hosts")
		if err := os.WriteFile(target, data, 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		defer func() { os.Remove(path); os.WriteFile(path, data, 0644) }()
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
		if _, err := Verify(ctx, directory); err == nil {
			t.Fatal("accepted symlink")
		}
	})
	t.Run("manifest duplicate field", func(t *testing.T) {
		path := filepath.Join(directory, ManifestName)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		defer os.WriteFile(path, data, 0644)
		changed := strings.Replace(string(data), `"schema": "sf.bundle/v1",`, `"schema": "sf.bundle/v1", "schema": "sf.bundle/v1",`, 1)
		if err := os.WriteFile(path, []byte(changed), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := Verify(ctx, directory); err == nil {
			t.Fatal("accepted duplicate manifest field")
		}
	})
	t.Run("wrong embedded version", func(t *testing.T) {
		wrong := identity
		wrong.Version = "9.9.9"
		if _, err := describe(ctx, directory, wrong, true); err == nil {
			t.Fatal("accepted different executable version")
		}
	})
	if _, err := Verify(ctx, directory); err != nil {
		t.Fatalf("fixture not restored: %v", err)
	}
	t.Run("reject writable ancestor before creating destination", func(t *testing.T) {
		root, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(root, 0o777); err != nil {
			t.Fatal(err)
		}
		private := filepath.Join(root, "private")
		if err := os.Mkdir(private, 0o700); err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(private, "installed")
		if _, created, err := Install(ctx, directory, destination); err == nil || created {
			t.Fatalf("accepted runtime-unusable ancestry: created=%v err=%v", created, err)
		}
		if _, err := os.Lstat(destination); !os.IsNotExist(err) {
			t.Fatal("unsafe installation created files")
		}
	})
	t.Run("install and execute local intake", func(t *testing.T) {
		parent, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(parent, "installed")
		installed, created, err := Install(ctx, directory, destination)
		if err != nil || !created || installed.Identity != identity {
			t.Fatalf("installed=%+v created=%v err=%v", installed, created, err)
		}
		if _, err := Verify(ctx, destination); err != nil {
			t.Fatal(err)
		}
		license, err := os.ReadFile(filepath.Join(destination, "LICENSE"))
		if err != nil || !strings.Contains(string(license), "MIT License") {
			t.Fatalf("installed license: %v", err)
		}
		if _, err := runtimeassets.ResolveCore(domain.ChannelDev, filepath.Join(destination, "sf-dev")); err != nil {
			t.Fatalf("installed core unusable: %v", err)
		}
		if _, err := runtimeassets.ResolvePublication(domain.ChannelDev, filepath.Join(destination, "sf-dev")); err != nil {
			t.Fatalf("installed publication helper unusable: %v", err)
		}
		if _, created, err := Install(ctx, directory, destination); err == nil || created {
			t.Fatalf("overwrote destination: created=%v err=%v", created, err)
		}
		home := filepath.Join(parent, "home")
		if err := os.Mkdir(home, 0700); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"version", "--json"}, {"--help"}, {"ticket", "template"}, {"bundle", "verify", destination, "--json"}} {
			command := exec.CommandContext(ctx, filepath.Join(destination, "sf-dev"), args...)
			command.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin:/usr/sbin:/sbin", "TMPDIR=" + parent}
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("installed %v: %v %s", args, err, output)
			}
		}
		cliDestination := filepath.Join(parent, "cli-installed")
		for i := 0; i < 2; i++ {
			command := exec.CommandContext(ctx, filepath.Join(destination, "sf-dev"), "bundle", "install", directory, "--to", cliDestination, "--json")
			command.Env = []string{"HOME=" + home, "PATH=/usr/bin:/bin:/usr/sbin:/sbin", "TMPDIR=" + parent}
			output, runErr := command.CombinedOutput()
			var response api.Response
			if json.Unmarshal(output, &response) != nil || response.OK != (i == 0) || (runErr == nil) != (i == 0) || response.Mutation.Attempted != (i == 0) {
				t.Fatalf("CLI install iteration=%d err=%v output=%s", i, runErr, output)
			}
		}
		if _, err := Verify(ctx, cliDestination); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(home)
		if err != nil || len(entries) != 0 {
			t.Fatalf("intake wrote HOME: %v %v", entries, err)
		}
	})
	t.Run("cancel before install", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		parent, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(parent, "cancelled")
		if _, created, err := Install(cancelled, directory, destination); err == nil || created {
			t.Fatal("cancelled installation proceeded")
		}
		if _, err := os.Lstat(destination); !os.IsNotExist(err) {
			t.Fatal("cancelled installation created directory")
		}
	})
}

func TestBundleIdentityRequiresSupportedExplicitBuild(t *testing.T) {
	identity := Identity{Version: "1.2.3", Commit: strings.Repeat("a", 40), Channel: "stable", OS: "darwin", Arch: "arm64"}
	for _, change := range []func(*Identity){func(v *Identity) { v.Commit = "unknown" }, func(v *Identity) { v.Version = "latest" }, func(v *Identity) { v.Channel = "other" }, func(v *Identity) { v.OS = "linux" }, func(v *Identity) { v.Arch = "386" }} {
		value := identity
		change(&value)
		if _, err := payloads(value); err == nil {
			t.Fatalf("accepted %+v", value)
		}
	}
}
