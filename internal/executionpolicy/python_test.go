package executionpolicy

import (
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/pythonclosure"
)

func TestPreparedPythonPolicyBindsExactRecipe(t *testing.T) {
	argv := []string{"python3", pythonclosure.RecipeFlag, "sha256:" + strings.Repeat("a", 64), "sha256:" + strings.Repeat("b", 64), "tests"}
	snapshot, err := NewCommandSnapshot(argv)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Authorize(argv); err != nil {
		t.Fatal(err)
	}
	for i := range argv {
		changed := append([]string(nil), argv...)
		changed[i] += "x"
		if snapshot.Authorize(changed) == nil {
			t.Fatalf("changed field %d authorized", i)
		}
	}
	for _, bad := range [][]string{
		{"python3", "-m", "pytest"}, {"python3", "-c", "print(1)"}, {"python3", "test.py"},
		{"/usr/bin/python3", argv[1], argv[2], argv[3], argv[4]},
		{argv[0], argv[1], argv[2], argv[3], "../tests"},
		{argv[0], argv[1], argv[2], argv[3], argv[4], "-p", "plugin"},
	} {
		if _, err := NewCommandSnapshot(bad); err == nil {
			t.Fatalf("unsafe recipe allowed: %q", bad)
		}
	}
}
