//go:build sf_e2e

package git

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestRewriteE2ERemoteArgvOnlyRewritesExactRemoteSlots(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bare := filepath.Join(root, "origin.git")
	for _, path := range []string{bare, filepath.Join(bare, "objects"), filepath.Join(bare, "refs")} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for name, value := range map[string]string{"HEAD": "ref: refs/heads/main\n", "config": "[core]\n\tbare = true\n"} {
		if err := os.WriteFile(filepath.Join(bare, name), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("SF_E2E_GIT_BARE", bare)

	for name, test := range map[string]struct {
		argv []string
		want []string
	}{
		"push remote": {
			argv: []string{"push", e2eGitHubRemote, "refs/heads/source:refs/heads/source"},
			want: []string{"push", bare, "refs/heads/source:refs/heads/source"},
		},
		"ls remote": {
			argv: []string{"ls-remote", "--heads", e2eGitHubRemote, "refs/heads/source"},
			want: []string{"ls-remote", "--heads", bare, "refs/heads/source"},
		},
		"fetch remote": {
			argv: []string{"fetch", "--no-write-fetch-head", "--no-tags", e2eGitHubRemote, "refs/heads/main:refs/sf/proof"},
			want: []string{"fetch", "--no-write-fetch-head", "--no-tags", bare, "refs/heads/main:refs/sf/proof"},
		},
		"URL in refspec": {
			argv: []string{"push", "origin", e2eGitHubRemote},
			want: []string{"push", "origin", e2eGitHubRemote},
		},
		"extra push flag": {
			argv: []string{"push", "--force", e2eGitHubRemote, "refs/heads/source:refs/heads/source"},
			want: []string{"push", "--force", e2eGitHubRemote, "refs/heads/source:refs/heads/source"},
		},
		"URL in wrong ls remote slot": {
			argv: []string{"ls-remote", e2eGitHubRemote, "--heads", "refs/heads/source"},
			want: []string{"ls-remote", e2eGitHubRemote, "--heads", "refs/heads/source"},
		},
		"extra fetch flag": {
			argv: []string{"fetch", "--force", "--no-write-fetch-head", "--no-tags", e2eGitHubRemote, "refs/heads/main:refs/sf/proof"},
			want: []string{"fetch", "--force", "--no-write-fetch-head", "--no-tags", e2eGitHubRemote, "refs/heads/main:refs/sf/proof"},
		},
		"unknown argv": {
			argv: []string{"status", e2eGitHubRemote},
			want: []string{"status", e2eGitHubRemote},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := rewriteE2ERemoteArgv(test.argv)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("rewrite=%q want=%q", got, test.want)
			}
			if len(got) > 0 {
				got[0] = "mutated"
				if test.argv[0] == "mutated" {
					t.Fatal("rewrite mutated caller-owned argv")
				}
			}
		})
	}
}
