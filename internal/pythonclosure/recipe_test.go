package pythonclosure

import (
	"strings"
	"testing"
)

func TestPreparedPytestRecipeIsExact(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	argv := []string{"python3", RecipeFlag, digest, digest, "tests"}
	value, err := ParseRecipe(argv)
	if err != nil || value.EnvironmentDigest != digest || value.LockDigest != digest || value.TestPath != "tests" {
		t.Fatalf("recipe: %+v %v", value, err)
	}
	for _, test := range []string{"tests/test_example.py", "app/tests", "test_example.py"} {
		copy := append([]string(nil), argv...)
		copy[4] = test
		if _, err := ParseRecipe(copy); err != nil {
			t.Fatal(test, err)
		}
	}
	for _, bad := range [][]string{
		nil,
		{"python3", "-c", "print(1)"},
		{"/usr/bin/python3", RecipeFlag, digest, digest, "tests"},
		{"python", RecipeFlag, digest, digest, "tests"},
		{"python3", RecipeFlag, digest, digest, "tests", "-p", "custom"},
		{"python3", RecipeFlag, "unknown", digest, "tests"},
		{"python3", RecipeFlag, digest, "unknown", "tests"},
	} {
		if _, err := ParseRecipe(bad); err == nil {
			t.Fatal("accepted nonrecipe argv")
		}
	}
	for _, bad := range []string{"", ".", "..", "../tests", "/tmp/tests", "tests/../tests", "--help.py", "tests/*.py", "tests/te?st.py", "tests\\x.py", "tests/x\n.py", "setup.cfg"} {
		copy := append([]string(nil), argv...)
		copy[4] = bad
		if _, err := ParseRecipe(copy); err == nil {
			t.Fatalf("accepted path %q", bad)
		}
	}
	if !validDigest(BootstrapDigest()) || BootstrapSource == "" {
		t.Fatal("invalid factory bootstrap identity")
	}
}
