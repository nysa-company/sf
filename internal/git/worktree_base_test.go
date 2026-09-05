package git

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestPinnedWorktreeBaseRequiresExactRefIdentity(t *testing.T) {
	branch := "sf/dev/pinned-base"
	ref := worktreeBaseRef(branch)
	oid := strings.Repeat("a", 40)
	for _, test := range []struct {
		name, output, want string
		invalid            bool
	}{
		{"absent-legacy", "", "", false},
		{"exact", ref + " " + oid + "\n", oid, false},
		{"child-not-exact-ref", ref + "/child " + oid + "\n", "", true},
		{"multiple", ref + " " + oid + "\n" + ref + "/child " + oid + "\n", "", true},
		{"invalid-object", ref + " bad\n", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			runner := Runner{Home: filepath.Join(t.TempDir(), "private-home"), Run: func(context.Context, string, []string, []string) ([]byte, error) {
				return []byte(test.output), nil
			}}
			got, err := runner.pinnedWorktreeBase(context.Background(), dir, branch)
			if (err != nil) != test.invalid || got != test.want {
				t.Fatalf("got %q, %v", got, err)
			}
			if test.invalid && !errors.Is(err, ErrIdentityMismatch) {
				t.Fatalf("fixture did not reach ref authentication: %v", err)
			}
		})
	}
}
