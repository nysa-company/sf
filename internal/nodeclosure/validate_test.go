package nodeclosure

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func writeNodeFixture(t *testing.T, packageJSON string, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(packageJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, contents := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestValidateAcceptsDependencyFreeDiscoveredJavaScript(t *testing.T) {
	root := writeNodeFixture(t, `{"name":"proof","private":true}`, map[string]string{"test/smoke.test.mjs": `import test from 'node:test'; test('ok', () => {});`})
	if err := Validate(root); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRefusesDependencyAndWorktreeEscapes(t *testing.T) {
	for name, prepare := range map[string]func(*testing.T, string){
		"dependency": func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(`{"dependencies":{"x":"1"}}`), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"workspace": func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(`{"workspaces":["packages/*"]}`), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"empty dependency section": func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(`{"dependencies":{}}`), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"bundle dependencies alias": func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(`{"bundleDependencies":[]}`), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"native addon": func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "addon.node"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"TypeScript": func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "src.ts"), []byte("export {}"), 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"node modules": func(t *testing.T, root string) {
			if err := os.Mkdir(filepath.Join(root, "node_modules"), 0o700); err != nil {
				t.Fatal(err)
			}
		},
		"symlink": func(t *testing.T, root string) {
			if err := os.Symlink(filepath.Join(root, "test", "smoke.test.js"), filepath.Join(root, "escape.js")); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := writeNodeFixture(t, `{"name":"proof"}`, map[string]string{"test/smoke.test.js": ""})
			prepare(t, root)
			if err := Validate(root); err == nil || (!errors.Is(err, ErrDependencies) && !errors.Is(err, ErrInvalid)) {
				t.Fatalf("Validate error=%v", err)
			}
		})
	}
}

func TestOfficialTestFileMatchesNodeDiscoveryPatterns(t *testing.T) {
	for _, test := range []struct {
		name string
		want bool
	}{
		{"nested/smoke.test.js", true},
		{"nested/smoke-test.cjs", true},
		{"nested/smoke_test.mjs", true},
		{"nested/test-smoke.js", true},
		{"nested/test.js", true},
		{"test/smoke.js", true},
		{"nested/test/deeper/smoke.mjs", true},
		{"nested/TEST/smoke.js", false},
		{"nested/SMOKE.TEST.JS", false},
		{"nested/smoke.TEST.js", false},
		{"nested/test-smoke.ts", false},
		{"src/smoke.js", false},
		{"test/smoke.ts", false},
	} {
		if got := officialTestFile(test.name); got != test.want {
			t.Fatalf("officialTestFile(%q)=%v want %v", test.name, got, test.want)
		}
	}
}

func TestValidateDirectoryFDStaysBoundToOpenedWorktree(t *testing.T) {
	parent := t.TempDir()
	worktree := filepath.Join(parent, "worktree")
	if err := os.Rename(writeNodeFixture(t, `{"name":"proof"}`, map[string]string{"test/smoke.test.js": ""}), worktree); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Open(worktree, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	replacement := writeNodeFixture(t, `{"dependencies":{"x":"1"}}`, map[string]string{"test/smoke.test.js": ""})
	if err := os.Rename(worktree, filepath.Join(parent, "original")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, worktree); err != nil {
		t.Fatal(err)
	}
	if err := Validate(worktree); !errors.Is(err, ErrDependencies) {
		t.Fatalf("replacement path validation error=%v", err)
	}
	if err := ValidateDirectoryFD(fd); err != nil {
		t.Fatalf("opened worktree FD was redirected by pathname swap: %v", err)
	}
}

func TestValidateRequiresOfficialNodeDiscoveryFileAndBoundsPackage(t *testing.T) {
	root := writeNodeFixture(t, `{"name":"proof"}`, map[string]string{"src/example.js": ""})
	if err := Validate(root); !errors.Is(err, ErrNoTests) {
		t.Fatalf("no test error=%v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "package.json"), []byte(strings.Repeat(" ", int(MaxPackageJSONBytes)+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Validate(root); !errors.Is(err, ErrInvalid) {
		t.Fatalf("oversized package error=%v", err)
	}
}
