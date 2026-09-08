package worktreecoord

import (
	"github.com/nysa-company/sf/internal/git"
	"testing"
)

func TestRefreshBuilderChangesRequireExactAnchorAndBoundedSource(t *testing.T) {
	for _, tc := range []struct {
		name, head             string
		paths, declared, scope []string
		want                   bool
	}{
		{"source", "anchor", []string{"app/source.js"}, []string{"app/source.js"}, []string{"app"}, true},
		{"clean", "anchor", nil, []string{"app/source.js"}, []string{"app"}, true},
		{"foreign head", "other", []string{"app/source.js"}, []string{"app/source.js"}, []string{"app"}, false},
		{"undeclared", "anchor", []string{"app/other.js"}, []string{"app/source.js"}, []string{"app"}, false},
		{"outside plan", "anchor", []string{"other/source.js"}, []string{"other/source.js"}, []string{"app"}, false},
		{"protected", "anchor", []string{"app/tests/source.test.js"}, []string{"app/tests/source.test.js"}, []string{"app"}, false},
		{"missing plan", "anchor", nil, nil, nil, false},
		{"unbounded plan", "anchor", []string{"app/source.js"}, []string{"app/source.js"}, []string{"."}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := refreshBuilderChangesMatch(git.WorktreeChanges{Head: tc.head, Paths: tc.paths}, "anchor", tc.declared, tc.scope, []string{"app/tests"}); got != tc.want {
				t.Fatalf("got=%t want=%t", got, tc.want)
			}
		})
	}
}
