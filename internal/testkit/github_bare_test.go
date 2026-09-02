package testkit

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/contracts"
)

func TestFakeGHBareSourceWitnessSupportsPackedRefs(t *testing.T) {
	bare := fakeBareRepository(t, strings.Repeat("1", 40))
	feature := strings.Repeat("2", 40)
	if err := os.WriteFile(filepath.Join(bare, "packed-refs"), []byte("# pack-refs with: peeled fully-peeled sorted\n"+feature+" refs/heads/feature\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	remote, err := NewFakeGH(filepath.Join(t.TempDir(), "remote.json"), contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.UseBareRepositoryForTest(bare); err != nil {
		t.Fatal(err)
	}
	output, err := remote.Run([]string{"api", "repos/example/app/git/ref/heads/feature"})
	if err != nil {
		t.Fatal(err)
	}
	var observed struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if err := json.Unmarshal(output, &observed); err != nil || observed.Object.SHA != feature {
		t.Fatalf("packed ref witness=%q err=%v output=%s", observed.Object.SHA, err, output)
	}
}

func TestFakeGHRealAllASourceOIDIsNotAReadThroughSentinel(t *testing.T) {
	bare := fakeBareRepository(t, strings.Repeat("1", 40))
	if err := os.MkdirAll(filepath.Join(bare, "refs", "heads", "sf"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bare, "refs", "heads", "sf", "ticket"), []byte(strings.Repeat("2", 40)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	remote, err := NewFakeGH(filepath.Join(t.TempDir(), "remote.json"), contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.UseBareRepositoryForTest(bare); err != nil {
		t.Fatal(err)
	}
	identity := fakePRIdentity()
	identity.HeadRef = "sf/ticket"
	identity.HeadOID = strings.Repeat("a", 40)
	identity.FactoryOwned = true
	if err := remote.InjectPullRequestForTest(PullRequest{Identity: identity, Draft: true}); err != nil {
		t.Fatal(err)
	}
	output, err := remote.Run([]string{"api", "repos/example/app/git/ref/heads/sf/ticket"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(output), strings.Repeat("a", 40)) || strings.Contains(string(output), strings.Repeat("2", 40)) {
		t.Fatalf("PR witness was replaced by bare read-through: %s", output)
	}
}

func TestCompiledFakeGHReconcilesBareAfterLostMergeResponse(t *testing.T) {
	base, merged := strings.Repeat("1", 40), strings.Repeat("3", 40)
	bare := fakeBareRepository(t, base)
	configRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(configRoot, "gh")
	if err := os.Mkdir(config, 0o700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(config, "sf-fake-gh.json")
	remote, err := NewFakeGH(statePath, contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	if err := remote.SetBaseHeadOIDForTest(base); err != nil {
		t.Fatal(err)
	}
	if err := remote.SetMergeCommitForTest(merged); err != nil {
		t.Fatal(err)
	}
	identity := fakePRIdentity()
	identity.Number = 1
	identity.HeadOID = strings.Repeat("2", 40)
	identity.BaseOID = base
	identity.FactoryOwned = true
	if err := remote.InjectPullRequestForTest(PullRequest{Identity: identity, Draft: false, Ready: true}); err != nil {
		t.Fatal(err)
	}
	if err := remote.SetResponse("pr_merge", ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, "sf-fake-gh-bare"), []byte(bare+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	binary := buildFixture(t, "./cmd/fake-gh")
	command := exec.Command(binary, "pr", "merge", "1", "--repo", "example/app", "--match-head-commit", identity.HeadOID, "--squash")
	command.Env = append(os.Environ(), "HOME="+filepath.Dir(config), "GH_CONFIG_DIR="+config)
	if output, err := command.CombinedOutput(); err == nil || !strings.Contains(string(output), "response lost after mutation") {
		t.Fatalf("lost merge response err=%v output=%s", err, output)
	}
	data, err := os.ReadFile(filepath.Join(bare, "refs", "heads", "main"))
	if err != nil || strings.TrimSpace(string(data)) != merged {
		t.Fatalf("bare main=%q err=%v, want durable merge %s", strings.TrimSpace(string(data)), err, merged)
	}
	snapshot := remote.Snapshot()
	if snapshot.BaseHeadOID != merged || len(snapshot.PRs) != 1 || !snapshot.PRs[0].Merged || snapshot.PRs[0].MergeCommit != merged {
		t.Fatalf("durable hosted state diverged from bare: %+v", snapshot)
	}
}

func fakeBareRepository(t *testing.T, main string) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bare := filepath.Join(root, "origin.git")
	for _, path := range []string{bare, filepath.Join(bare, "objects"), filepath.Join(bare, "refs"), filepath.Join(bare, "refs", "heads")} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bare, "HEAD"), []byte("ref: refs/heads/main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bare, "config"), []byte("[core]\n\tbare = true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bare, "refs", "heads", "main"), []byte(main+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return bare
}
