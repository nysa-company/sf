package nodeclosure

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	for _, name := range []string{"test/smoke.js", "nested/test-smoke.cjs", "nested/smoke-test.mjs", "nested/smoke.test.js"} {
		if !officialTestFile(name) {
			t.Fatalf("%q was not recognized", name)
		}
	}
	for _, name := range []string{"nested/test-smoke.ts", "src/smoke.js", "test/smoke.ts"} {
		if officialTestFile(name) {
			t.Fatalf("%q was incorrectly recognized", name)
		}
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
