package store

import "testing"

func TestPublicationGitHubRepoSSH(t *testing.T) {
	for _, raw := range []string{"https://github.com/owner/repo.git", "git@github.com:owner/repo.git", "ssh://git@github.com/owner/repo.git", "ssh://git@github.com:22/owner/repo.git", "ssh://git@ssh.github.com:443/owner/repo.git"} {
		owner, repo, ok := publicationGitHubRepo(raw)
		if !ok || owner != "owner" || repo != "repo" {
			t.Fatalf("origin=%q rejected", raw)
		}
	}
	for _, raw := range []string{"git@other.com:owner/repo.git", "ssh://git@github.com:444/owner/repo.git", "ssh://git:secret@github.com/owner/repo.git"} {
		if _, _, ok := publicationGitHubRepo(raw); ok {
			t.Fatal("accepted invalid SSH origin")
		}
	}
}
