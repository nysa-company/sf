package publication

import "testing"

func TestGitHubRepositorySSHMatchesHTTPS(t *testing.T) {
	want, ok := githubRepository("https://github.com/owner/repo.git")
	if !ok {
		t.Fatal("HTTPS fixture")
	}
	for _, raw := range []string{"git@github.com:owner/repo.git", "ssh://git@github.com/owner/repo.git", "ssh://git@github.com:22/owner/repo.git", "ssh://git@ssh.github.com:443/owner/repo.git"} {
		if got, ok := githubRepository(raw); !ok || got != want {
			t.Fatalf("origin=%q got=%+v", raw, got)
		}
	}
	for _, raw := range []string{"git@other.com:owner/repo.git", "ssh://git@github.com:444/owner/repo.git", "git@github.com:owner/repo.git?token=secret"} {
		if _, ok := githubRepository(raw); ok {
			t.Fatal("accepted invalid SSH origin")
		}
	}
}
