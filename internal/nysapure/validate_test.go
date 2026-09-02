package nysapure

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func writeFixture(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, contents := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestValidateAcceptsBoundedRelativeTypeScriptClosure(t *testing.T) {
	root := writeFixture(t, map[string]string{
		"apps/api/tests/kernel.test.ts": `import test from 'node:test'; import { value } from '../src/kernel.js'; test('ok', () => { if (value !== 1) throw new Error('bad'); });`,
		"apps/api/src/kernel.ts":        `export const value: number = 1;`,
	})
	if err := Validate(root, "apps/api/tests/kernel.test.ts"); err != nil {
		t.Fatalf("validate pure closure: %v", err)
	}
	if got := LoaderIdentity(); got == "" || len(got) != len("nysa-api-pure-loader-v1:sha256:")+64 {
		t.Fatalf("loader identity=%q", got)
	}
}

func TestValidateAllowsMissingRelativeImplementationForRedThenGreenVerification(t *testing.T) {
	root := writeFixture(t, map[string]string{
		"apps/api/tests/kernel.test.ts": `import { value } from '../src/kernel.js'; if (value) throw new Error('unreachable');`,
	})
	if err := Validate(root, "apps/api/tests/kernel.test.ts"); err != nil {
		t.Fatalf("missing implementation must remain a safe red verification: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "apps", "api", "src"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "apps", "api", "src", "kernel.ts"), []byte(`export const value = 0;`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Validate(root, "apps/api/tests/kernel.test.ts"); err != nil {
		t.Fatalf("green implementation refused: %v", err)
	}
}

func TestStageDirectoryFDPreservesValidatedSourceMappingAndManifest(t *testing.T) {
	root := writeFixture(t, map[string]string{
		"apps/api/tests/kernel.test.ts": `import { value } from '../src/kernel.js'; if (value !== 1) throw new Error('bad');`,
		"apps/api/src/kernel.ts":        `export const value = 1;`,
	})
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	paths, err := ValidateDirectoryFDSources(fd, "apps/api/tests/kernel.test.ts")
	if err != nil {
		t.Fatal(err)
	}
	stage, err := StageDirectoryFD(fd, "apps/api/tests/kernel.test.ts", paths)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	if stage.Root == root || len(stage.Files) != 2 || stage.ManifestDigest == "" || stage.Validate() != nil {
		t.Fatalf("stage=%+v", stage)
	}
	if info, err := os.Lstat(stage.Root); err != nil || info.Mode().Perm() != 0o500 {
		t.Fatalf("stage root info=%v err=%v", info, err)
	}
	if info, err := os.Lstat(stage.Files[0]); err != nil || info.Mode().Perm() != 0o400 {
		t.Fatalf("stage file info=%v err=%v", info, err)
	}
	if got, err := os.ReadFile(filepath.Join(stage.Root, "apps", "api", "src", "kernel.ts")); err != nil || string(got) != "export const value = 1;" {
		t.Fatalf("staged mapping data=%q err=%v", got, err)
	}
	if err := os.WriteFile(filepath.Join(root, "apps", "api", "src", "kernel.ts"), []byte("export const value = 2;"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(stage.Root, "apps", "api", "src", "kernel.ts")); err != nil || string(got) != "export const value = 1;" {
		t.Fatalf("staged source changed with worktree data=%q err=%v", got, err)
	}
}

func TestStageDirectoryFDRejectsPathMismatchAndManifestTamper(t *testing.T) {
	root := writeFixture(t, map[string]string{
		"apps/api/tests/kernel.test.ts": `import { value } from '../src/kernel.js'; if (value !== 1) throw new Error('bad');`,
		"apps/api/src/kernel.ts":        `export const value = 1;`,
	})
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	paths, err := ValidateDirectoryFDSources(fd, "apps/api/tests/kernel.test.ts")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := StageDirectoryFD(fd, "apps/api/tests/kernel.test.ts", paths[:1]); !errors.Is(err, ErrInvalid) {
		t.Fatalf("path mismatch err=%v", err)
	}
	stage, err := StageDirectoryFD(fd, "apps/api/tests/kernel.test.ts", paths)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	stage.Manifest[0] ^= 1
	if err := stage.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("manifest tamper err=%v", err)
	}
}

func TestStageDirectoryFDBindsInitialSourceEvidenceAndExactCardinality(t *testing.T) {
	root := writeFixture(t, map[string]string{
		"apps/api/tests/kernel.test.ts": `import { value } from '../src/kernel.js'; if (value !== 1) throw new Error('bad');`,
		"apps/api/src/kernel.ts":        `export const value = 1;`,
	})
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	sources, err := ValidateDirectoryFDSources(fd, "apps/api/tests/kernel.test.ts")
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "apps", "api", "src", "kernel.ts")
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("export const value = 1;"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := StageDirectoryFD(fd, "apps/api/tests/kernel.test.ts", sources); !errors.Is(err, ErrInvalid) {
		t.Fatalf("replacement with identical bytes accepted: %v", err)
	}

	// Fresh evidence stages successfully, but any manifest/source cardinality
	// drift is refused before launch.
	sources, err = ValidateDirectoryFDSources(fd, "apps/api/tests/kernel.test.ts")
	if err != nil {
		t.Fatal(err)
	}
	stage, err := StageDirectoryFD(fd, "apps/api/tests/kernel.test.ts", sources)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	stage.Sources = append(stage.Sources, stage.Sources[0])
	if err := stage.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("source cardinality drift accepted: %v", err)
	}
}

func TestValidateRefusesUnsupportedResolutionAndFilesystemShapes(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"bare package": {
			"apps/api/tests/kernel.test.ts": `import x from 'tsx'; export { x };`,
		},
		"path escape": {
			"apps/api/tests/kernel.test.ts": `import x from '../../../../outside.js'; export { x };`,
		},
		"extensionless": {
			"apps/api/tests/kernel.test.ts": `import x from '../src/kernel'; export { x };`,
		},
		"direct ts import": {
			"apps/api/tests/kernel.test.ts": `import x from '../src/kernel.ts'; export { x };`,
			"apps/api/src/kernel.ts":        `export default 1;`,
		},
		"dynamic import": {
			"apps/api/tests/kernel.test.ts": `const x = import('../src/kernel.js'); export { x };`,
			"apps/api/src/kernel.ts":        `export default 1;`,
		},
		"node modules import": {
			"apps/api/tests/kernel.test.ts": `import x from '../node_modules/kernel.js'; export { x };`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := Validate(writeFixture(t, files), "apps/api/tests/kernel.test.ts"); !errors.Is(err, ErrInvalid) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	t.Run("symlink", func(t *testing.T) {
		root := writeFixture(t, map[string]string{
			"apps/api/tests/kernel.test.ts": `import x from '../src/kernel.js'; export { x };`,
			"outside.ts":                    `export default 1;`,
		})
		if err := os.MkdirAll(filepath.Join(root, "apps", "api", "src"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(root, "outside.ts"), filepath.Join(root, "apps", "api", "src", "kernel.ts")); err != nil {
			t.Fatal(err)
		}
		if err := Validate(root, "apps/api/tests/kernel.test.ts"); !errors.Is(err, ErrInvalid) {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestValidTestPathRejectsEscapesAndNonCanonicalForms(t *testing.T) {
	for _, value := range []string{"", "/apps/api/tests/a.test.ts", "apps//api/tests/a.test.ts", "apps/api/tests/a.ts", "apps/api/tests/.a.test.ts", "apps/api/tests/a.test.ts?x", "apps\\api\\tests\\a.test.ts", "../apps/api/tests/a.test.ts", "apps/node_modules/tests/a.test.ts"} {
		if ValidTestPath(value) {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestNysaRetrievalFusionClosureReadOnly(t *testing.T) {
	if os.Getenv("SF_NYSA_LOCAL_PROOF") != "1" {
		t.Skip("set SF_NYSA_LOCAL_PROOF=1 to run the read-only local Nysa compatibility proof")
	}
	const nysa = "/Users/sofiagonzalez-2/Projects/nysa-company/nysa-app"
	if _, err := os.Stat(nysa); err != nil {
		t.Skip("local Nysa checkout is unavailable")
	}
	if err := Validate(nysa, "apps/api/tests/retrieval-fusion.test.ts"); err != nil {
		t.Fatalf("Nysa pure API closure was refused: %v", err)
	}
	runStrictNodeTest(t, nysa, "apps/api/tests/retrieval-fusion.test.ts")
}

func TestStrictLoaderRunsOnlyConfinedJSIntoTSMapping(t *testing.T) {
	root := writeFixture(t, map[string]string{
		"apps/api/tests/kernel.test.ts": `import test from 'node:test'; import { value } from '../src/kernel.js'; test('kernel', () => { if (value !== 1) throw new Error('bad'); });`,
		"apps/api/src/kernel.ts":        `export const value: number = 1;`,
	})
	runStrictNodeTest(t, root, "apps/api/tests/kernel.test.ts")
}

func runStrictNodeTest(t *testing.T, root, testPath string) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node is unavailable")
	}
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(rootFD)
	paths, err := ValidateDirectoryFDSources(rootFD, testPath)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := StageDirectoryFD(rootFD, testPath, paths)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	loader := filepath.Join(t.TempDir(), "nysa-api-pure-loader.mjs")
	if err := os.WriteFile(loader, []byte(LoaderSource), 0o500); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(node, "--experimental-transform-types", "--experimental-loader", loader, "--test", testPath)
	command.Dir = stage.Root
	command.Env = append(os.Environ(), "SF_NYSA_API_PURE_ROOT="+stage.Root, "SF_NYSA_API_PURE_MANIFEST="+string(stage.Manifest), "SF_NYSA_API_PURE_MANIFEST_DIGEST="+stage.ManifestDigest)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("strict loader test %s failed: %v: %s", testPath, err, output)
	}
}
