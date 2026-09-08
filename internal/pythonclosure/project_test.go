package pythonclosure

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPythonTestPathRejectsSymlinksAndSpecialFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "tests"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "tests/test_ok.py"), []byte("def test_ok(): pass\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("tests", filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("tests/test_ok.py", filepath.Join(root, "linked.py")); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"tests", "tests/test_ok.py"} {
		if err := ValidateTestPath(root, p); err != nil {
			t.Fatal(p, err)
		}
	}
	for _, p := range []string{"linked/test_ok.py", "linked.py", "../test_ok.py", "missing.py", "tests/../tests/test_ok.py", "tests/test_ok.py/child.py"} {
		if err := ValidateTestPath(root, p); err == nil {
			t.Fatal("accepted", p)
		}
	}
}
