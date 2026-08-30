package testkit

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestFakeClockSleepOnlyAdvancesWhenTold(t *testing.T) {
	clock := NewFakeClock(time.Unix(100, 0))
	done := make(chan error, 1)
	go func() { done <- clock.Sleep(context.Background(), time.Minute) }()
	select {
	case <-done:
		t.Fatal("fake sleep returned before the clock advanced")
	case <-time.After(10 * time.Millisecond):
	}
	clock.Advance(time.Minute)
	if err := <-done; err != nil {
		t.Fatalf("fake sleep: %v", err)
	}
}

func TestIDsAreDeterministicAndChannelReferencesRemainDistinct(t *testing.T) {
	ids := NewIDs(7)
	stable := ids.Ticket(domain.ChannelStable, "nysa")
	dev := domain.TicketRef{Channel: domain.ChannelDev, Project: stable.Project, Ticket: stable.Ticket}
	if stable.Ticket != "SF-00000001" {
		t.Fatalf("stable ticket = %q", stable.Ticket)
	}
	if stable.Ticket != dev.Ticket || stable.Channel == dev.Channel {
		t.Fatal("fixture should model same generated ID as distinct channel-scoped references")
	}
	if got := ids.BranchSuffix(); got != "00000009" {
		t.Fatalf("branch suffix = %q", got)
	}
}

func TestFakeGHSeparatesMutationFromResponseDeliveryAndBindsIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remote.json")
	remote, err := NewFakeGH(path, contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	if err := remote.SetResponse("pr_create", ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	identity := contracts.PullRequestIdentity{
		Repository:     contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"},
		HeadOwner:      "example",
		HeadRepository: "app",
		HeadRef:        "sf/dev/nysa/SF-00000001-abc123",
		HeadOID:        "0123456789012345678901234567890123456789",
		BaseRef:        "main",
	}
	if _, err := remote.CreateDraftPullRequest(context.Background(), EffectClaimForTest("draft_pr", identity, "title", "body"), identity, "title", "body"); err == nil {
		t.Fatal("expected dropped response")
	}
	if got := remote.MutationCount("pr_create"); got != 1 {
		t.Fatalf("mutation count = %d, want 1", got)
	}
	if got := remote.DeliveryCount("pr_create"); got != 0 {
		t.Fatalf("delivery count = %d, want 0", got)
	}
	identity.Number = 1
	identity.FactoryOwned = true
	found, ok, err := remote.FindPullRequest(context.Background(), identity)
	if err != nil || !ok || found.HeadOID != identity.HeadOID || !found.FactoryOwned {
		t.Fatalf("full identity lookup = %#v, found=%v, err=%v", found, ok, err)
	}
	wrongHead := identity
	wrongHead.HeadRepository = "fork"
	if _, ok, err := remote.FindPullRequest(context.Background(), wrongHead); err != nil || ok {
		t.Fatalf("fork identity unexpectedly adopted: found=%v err=%v", ok, err)
	}
}

func TestFakeGHRequiresOwnedRecoveryAndRejectsHumanLookalike(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remote.json")
	remote, err := NewFakeGH(path, contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	identity := fakePRIdentity()
	identity.Number = 7
	if err := remote.InjectPullRequestForTest(PullRequest{Identity: identity, Draft: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := remote.FindPullRequest(context.Background(), identity); err == nil {
		t.Fatal("lookup without factory ownership unexpectedly succeeded")
	}
	identity.FactoryOwned = true
	if _, found, err := remote.FindPullRequest(context.Background(), identity); err != nil || found {
		t.Fatalf("human-owned lookalike adopted: found=%v err=%v", found, err)
	}
	identity.FactoryOwned = false
	if _, err := remote.CreateDraftPullRequest(context.Background(), EffectClaimForTest("draft_pr", identity, "title", "body"), identity, "title", "body"); err == nil || !strings.Contains(err.Error(), "human-owned") {
		t.Fatalf("human-owned lookalike did not block a factory create: %v", err)
	}
}

func TestFakeGHRejectsUnfencedMutationClaimsAndBadMergeBinding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remote.json")
	remote, err := NewFakeGH(path, contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	identity := fakePRIdentity()
	if _, err := remote.CreateDraftPullRequest(context.Background(), domain.ExternalEffectClaim{}, identity, "title", "body"); err == nil {
		t.Fatal("unclaimed create succeeded")
	}
	created, err := remote.CreateDraftPullRequest(context.Background(), EffectClaimForTest("draft_pr", identity, "title", "body"), identity, "title", "body")
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.UpdatePullRequest(context.Background(), domain.ExternalEffectClaim{}, created, "next", "body"); err == nil {
		t.Fatal("unclaimed edit succeeded")
	}
	if err := remote.MarkReady(context.Background(), domain.ExternalEffectClaim{}, created); err == nil {
		t.Fatal("unclaimed ready succeeded")
	}
	if err := remote.MarkReady(context.Background(), EffectClaimForTest("pr_ready", created), created); err != nil {
		t.Fatal(err)
	}
	base := created.BaseOID
	authorization := domain.MergeAuthorization{ReviewedHead: created.HeadOID, CurrentHead: created.HeadOID, ReviewedBaseSHA: base, CurrentBaseSHA: base, ReviewedBaseHeadOID: base, CurrentBaseHeadOID: base, Approved: true, GatesGreen: true}
	if err := remote.MergeExactHead(context.Background(), domain.ExternalEffectClaim{}, created, created.HeadOID, "squash", authorization); err == nil {
		t.Fatal("unclaimed merge succeeded")
	}
	badAuthorization := authorization
	badAuthorization.CurrentBaseHeadOID = strings.Repeat("d", 40)
	badClaim := EffectClaimForTest("merge", created, created.HeadOID, "squash", badAuthorization.ReviewedBaseSHA, badAuthorization.CurrentBaseSHA, badAuthorization.ReviewedBaseHeadOID, badAuthorization.CurrentBaseHeadOID)
	if err := remote.MergeExactHead(context.Background(), badClaim, created, created.HeadOID, "squash", badAuthorization); err == nil {
		t.Fatal("split local/GitHub base binding merged")
	}
	claim := EffectClaimForTest("merge", created, created.HeadOID, "squash", authorization.ReviewedBaseSHA, authorization.CurrentBaseSHA, authorization.ReviewedBaseHeadOID, authorization.CurrentBaseHeadOID)
	if err := remote.SetBypassForcePushAllowancesForTest(1); err != nil {
		t.Fatal(err)
	}
	if err := remote.MergeExactHead(context.Background(), claim, created, created.HeadOID, "squash", authorization); err == nil {
		t.Fatal("merge with force-push bypass allowance succeeded")
	}
	if err := remote.SetBypassForcePushAllowancesForTest(0); err != nil {
		t.Fatal(err)
	}
	if err := remote.SetBaseHeadOIDForTest(strings.Repeat("d", 40)); err != nil {
		t.Fatal(err)
	}
	if err := remote.MergeExactHead(context.Background(), claim, created, created.HeadOID, "squash", authorization); err == nil {
		t.Fatal("merge after protected-base move succeeded")
	}
}

func TestFakeGHSubprocessRecoveryCreatesOnePR(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remote.json")
	remote, err := NewFakeGH(path, contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	if err := remote.SetResponse("pr_create", ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	fixture := buildFixture(t, "./cmd/fake-gh")
	body := "body\n\n<!-- sf:v1 repository=example/app head=example/app:sf/dev/nysa/SF-00000001-abc123 oid=0123456789012345678901234567890123456789 base=main -->"
	args := []string{"pr", "create", "--repo", "example/app", "--head", "example:sf/dev/nysa/SF-00000001-abc123", "--base", "main", "--draft", "--title", "title", "--body", body}
	start := make(chan struct{})
	results := make(chan error, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			command := exec.Command(fixture, args...)
			command.Env = append(os.Environ(), "SF_FAKE_GH_STATE="+path)
			_, err := command.CombinedOutput()
			results <- err
		}()
	}
	close(start)
	group.Wait()
	close(results)
	for err := range results {
		if err == nil {
			t.Fatal("lost-response create unexpectedly reported success")
		}
	}
	state := remote.Snapshot()
	if len(state.PRs) != 1 || remote.MutationCount("pr_create") != 1 || remote.DeliveryCount("pr_create") != 0 {
		t.Fatalf("concurrent recovery state = %#v", state)
	}
	want := fakePRIdentity()
	want.Number = 1
	want.FactoryOwned = true
	if _, found, err := remote.FindPullRequest(context.Background(), want); err != nil || !found {
		t.Fatalf("lost-response recovery did not find the one owned PR: found=%v err=%v", found, err)
	}
}

func TestFakeGHRejectsFakeOnlyAndUnknownArguments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remote.json")
	remote, err := NewFakeGH(path, contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.Run([]string{"pr", "create", "--repo", "example/app", "--head", "example:branch", "--base", "main", "--draft", "--title", "title", "--body", "body", "--head-oid", strings.Repeat("a", 40)}); err == nil {
		t.Fatal("fake-only head-oid flag was accepted")
	}
}

func TestWriteWithinRejectsHostileSymlinkWithoutMutatingSentinel(t *testing.T) {
	root := repositoryRoot(t)
	hostile := filepath.Join(root, "testdata", "hostile-repository")
	target := filepath.Join(root, "testdata", "parent-sentinel")
	if link, err := os.Readlink(filepath.Join(hostile, "parent-sentinel")); err != nil || link != "../parent-sentinel" {
		t.Fatalf("hostile fixture symlink = %q, err=%v", link, err)
	}
	before, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeWithin(hostile, "parent-sentinel", []byte("must not write\n")); err == nil {
		t.Fatal("symlink escape unexpectedly accepted")
	}
	after, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("controlled sentinel mutated: %q", after)
	}
}

func TestScriptedProviderWritesInSortedOrderAndRejectsSymlinkEscape(t *testing.T) {
	worktree := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(worktree, "a")); err != nil {
		t.Fatal(err)
	}
	provider := NewScriptedProvider(domain.ProviderIdentity{Provider: "fixture", Model: "fixture", Family: "fixture", Version: "v1"})
	provider.Default = ProviderStep{WriteFiles: map[string][]byte{
		"z.txt":        []byte("must not be written before a failing path\n"),
		"a/newdir/out": []byte("must not escape\n"),
	}}
	_, err := provider.Parse(context.Background(), contracts.PhaseInput{
		Ticket:   domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-1"},
		Phase:    domain.PhaseBuild,
		Worktree: worktree,
	}, contracts.CommandResult{})
	if err == nil {
		t.Fatal("symlink escape unexpectedly accepted")
	}
	if _, err := os.Stat(filepath.Join(worktree, "z.txt")); !os.IsNotExist(err) {
		t.Fatalf("nondeterministic partial write occurred before rejected path: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "newdir")); !os.IsNotExist(err) {
		t.Fatalf("symlinked parent was mutated before rejection: %v", err)
	}
}

func TestScriptedProviderProbeRequiresCompleteIdentity(t *testing.T) {
	complete := domain.ProviderIdentity{Provider: "fixture", Model: "model", Family: "family", Version: "v1"}
	for _, missing := range []string{"provider", "model", "family", "version"} {
		identity := complete
		switch missing {
		case "provider":
			identity.Provider = ""
		case "model":
			identity.Model = ""
		case "family":
			identity.Family = ""
		case "version":
			identity.Version = ""
		}
		if _, err := NewScriptedProvider(identity).Probe(context.Background()); err == nil {
			t.Fatalf("Probe accepted identity missing %s", missing)
		}
	}
	if got, err := NewScriptedProvider(complete).Probe(context.Background()); err != nil || got != complete {
		t.Fatalf("Probe complete identity = %#v, err=%v", got, err)
	}
}

func TestFakeProviderRejectsEscapingWritesAndBoundsChildren(t *testing.T) {
	fixture := buildFixture(t, "./cmd/fake-provider")
	worktree := filepath.Join(t.TempDir(), "worktree")
	if err := os.Mkdir(worktree, 0o755); err != nil {
		t.Fatal(err)
	}
	escape := filepath.Join(filepath.Dir(worktree), "escape")
	command := exec.Command(fixture, "--write=../escape")
	command.Dir = worktree
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "fixture path escapes worktree") {
		t.Fatalf("escaping write result: err=%v output=%q", err, output)
	}
	if _, err := os.Stat(escape); !os.IsNotExist(err) {
		t.Fatalf("escaping write created %s: %v", escape, err)
	}
	outside := filepath.Join(filepath.Dir(worktree), "outside")
	if err := os.Mkdir(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(worktree, "link")); err != nil {
		t.Fatal(err)
	}
	command = exec.Command(fixture, "--write=link/newdir/escape")
	command.Dir = worktree
	if output, err = command.CombinedOutput(); err == nil || !strings.Contains(string(output), "escapes worktree through symlink") {
		t.Fatalf("symlink write result: err=%v output=%q", err, output)
	}
	if _, err := os.Stat(filepath.Join(outside, "newdir")); !os.IsNotExist(err) {
		t.Fatalf("subprocess symlinked parent was mutated before rejection: %v", err)
	}
	command = exec.Command(fixture, "--duration=3s")
	if output, err = command.CombinedOutput(); err == nil || !strings.Contains(string(output), "duration must be") {
		t.Fatalf("overlong duration result: err=%v output=%q", err, output)
	}
	started := time.Now()
	command = exec.Command(fixture, "--scenario=double-fork", "--duration=50ms")
	output, err = command.CombinedOutput()
	if err != nil {
		t.Fatalf("bounded double-fork: %v (%s)", err, output)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("escaped child exceeded bounded cleanup window: %s", elapsed)
	}
	for _, marker := range []string{"escaped-child-pid=", "escaped-child-start", "escaped-child-exit"} {
		if !strings.Contains(string(output), marker) {
			t.Fatalf("missing bounded-child marker %q in %q", marker, output)
		}
	}
}

func fakePRIdentity() contracts.PullRequestIdentity {
	return contracts.PullRequestIdentity{
		Repository:     contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"},
		HeadOwner:      "example",
		HeadRepository: "app",
		HeadRef:        "sf/dev/nysa/SF-00000001-abc123",
		HeadOID:        "0123456789012345678901234567890123456789",
		BaseRef:        "main",
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func buildFixture(t *testing.T, packagePath string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), filepath.Base(packagePath))
	command := exec.Command("go", "build", "-o", binary, packagePath)
	command.Dir = repositoryRoot(t)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build fixture %s: %v (%s)", packagePath, err, output)
	}
	return binary
}

func TestFakeGHRejectsAmbiguousIdentityLookup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "remote.json")
	remote, err := NewFakeGH(path, contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	base := contracts.PullRequestIdentity{
		Repository:     contracts.RepositoryIdentity{Host: "github.com", Owner: "example", Name: "app"},
		HeadOwner:      "example",
		HeadRepository: "app",
		HeadRef:        "sf/dev/nysa/SF-00000001-abc123",
		BaseRef:        "main",
	}
	for _, oid := range []string{"0123456789012345678901234567890123456789", "abcdefabcdefabcdefabcdefabcdefabcdefabcd"} {
		identity := base
		identity.HeadOID = oid
		identity.FactoryOwned = true
		if err := remote.InjectPullRequestForTest(PullRequest{Identity: identity, Title: "title", Body: "body", Draft: true}); err != nil {
			t.Fatal(err)
		}
	}
	base.FactoryOwned = true
	if _, found, err := remote.FindPullRequest(context.Background(), base); err == nil || !strings.Contains(err.Error(), "ambiguous") || found {
		t.Fatalf("ambiguous lookup = found=%v err=%v", found, err)
	}
}

func TestCrashControllerIsOneShot(t *testing.T) {
	crash := NewCrashController()
	crash.Arm(AfterRemoteMutationBeforeResp)
	if err := crash.Hit(AfterRemoteMutationBeforeResp); err == nil {
		t.Fatal("expected injected crash")
	}
	if err := crash.Hit(AfterRemoteMutationBeforeResp); err != nil {
		t.Fatalf("one-shot crash fired twice: %v", err)
	}
	if got := crash.Hits(); len(got) != 1 || got[0] != AfterRemoteMutationBeforeResp {
		t.Fatalf("hits = %#v", got)
	}
}
