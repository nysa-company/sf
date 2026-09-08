package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func TestPublicationCapabilityDoesNotExposeUnauthenticatedConfiguration(t *testing.T) {
	home, explicit, _ := githubConfigFixture(t)
	for _, missingBinary := range []bool{false, true} {
		calls := 0
		owner, binary, directory, disabled, err := publicationCapabilityWith(home, domain.ChannelDev, explicit, "",
			func(string) (string, error) {
				if missingBinary {
					return "", errors.New("not installed")
				}
				return "/tool/gh", nil
			}, func(string, string, string) (bool, error) { calls++; return false, nil })
		if err != nil || !disabled || owner != "" || binary != "" || directory != "" || missingBinary && calls != 0 || !missingBinary && calls != 1 {
			t.Fatalf("unauthenticated capability: %q %q %q %v %v calls=%d", owner, binary, directory, disabled, err, calls)
		}
	}
}

func TestPublicationConfigRejectsInvalidXDGWithoutDefaultFallback(t *testing.T) {
	home, _, _ := githubConfigFixture(t)
	if _, err := selectedGitHubConfigDirectory(home, "", "relative"); err == nil {
		t.Fatal("invalid XDG path fell back to existing default configuration")
	}
}

func githubConfigFixture(t *testing.T) (string, string, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home, explicit, xdg := filepath.Join(root, "home"), filepath.Join(root, "external-gh"), filepath.Join(root, "xdg")
	for _, path := range []string{filepath.Join(home, ".config", "gh"), explicit, filepath.Join(xdg, "gh")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return home, explicit, xdg
}

func TestPublicationCapabilityHonorsGitHubConfigPrecedence(t *testing.T) {
	home, explicit, xdg := githubConfigFixture(t)
	for _, test := range []struct{ name, override, xdg, want string }{
		{"explicit", explicit, xdg, explicit},
		{"explicit ignores invalid lower precedence", explicit, "relative", explicit},
		{"xdg", "", xdg, filepath.Join(xdg, "gh")},
		{"default", "", "", filepath.Join(home, ".config", "gh")},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			owner, binary, directory, disabled, err := publicationCapabilityWith(home, domain.ChannelDev, test.override, test.xdg,
				func(string) (string, error) { return "/tool/gh", nil },
				func(gotBinary, gotHome, gotDirectory string) (bool, error) {
					calls++
					if gotBinary != "/tool/gh" || gotHome != home || gotDirectory != test.want {
						t.Fatalf("probe=%q %q %q", gotBinary, gotHome, gotDirectory)
					}
					return true, nil
				})
			if err != nil || disabled || calls != 1 || owner != home || binary != "/tool/gh" || directory != test.want {
				t.Fatalf("result=%q %q %q %v %v", owner, binary, directory, disabled, err)
			}
		})
	}
}

func TestPublicationConfigMissingDoesNotFallBackToOtherAccount(t *testing.T) {
	home, _, _ := githubConfigFixture(t)
	missing := filepath.Join(home, "missing-gh")
	owner, binary, directory, disabled, err := publicationCapabilityWith(home, domain.ChannelDev, missing, "",
		func(string) (string, error) { return "/tool/gh", nil },
		func(string, string, string) (bool, error) { t.Fatal("used fallback account"); return false, nil })
	if err != nil || !disabled || owner != "" || binary != "" || directory != "" {
		t.Fatalf("result=%q %q %q %v %v", owner, binary, directory, disabled, err)
	}
	if _, err := os.Lstat(missing); !os.IsNotExist(err) {
		t.Fatalf("created config directory: %v", err)
	}
}

func TestPublicationConfigRejectsUnsafePathsBeforeAuthentication(t *testing.T) {
	home, explicit, _ := githubConfigFixture(t)
	link := filepath.Join(home, "link")
	if err := os.Symlink(explicit, link); err != nil {
		t.Fatal(err)
	}
	writable := filepath.Join(home, "writable")
	if err := os.Mkdir(writable, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(writable, 0o777); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"relative-secret-value", "/", explicit + "/../external-gh", explicit + "\n", link, writable} {
		_, _, _, _, err := publicationCapabilityWith(home, domain.ChannelDev, path, "",
			func(string) (string, error) { return "/tool/gh", nil },
			func(string, string, string) (bool, error) {
				t.Fatal("unsafe configuration reached auth")
				return false, nil
			})
		if err == nil || strings.Contains(err.Error(), "secret-value") {
			t.Fatalf("unsafe path error=%v", err)
		}
	}
}
