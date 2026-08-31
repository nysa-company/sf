package publication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
	githubboundary "github.com/nysa-company/sf/internal/github"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

func TestPublicationKeysAreDeterministicAndCandidateBound(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-publication"}
	worktree := store.StoredWorktree{Path: "/tmp/publication", Branch: "sf/dev/1111111111111111/2222222222222222-33333333333333333333333333333333"}
	candidate := store.StoredCandidate{Snapshot: domain.CandidateSnapshot{BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}}
	first := pushRequestDigest(ref, "/tmp/repository", worktree, candidate, "")
	if first != pushRequestDigest(ref, "/tmp/repository", worktree, candidate, "") || !strings.HasPrefix(first, "sha256:") {
		t.Fatalf("push request is not stable: %q", first)
	}
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "nysa", Name: "app"}, HeadOwner: "nysa", HeadRepository: "app", HeadRef: worktree.Branch, HeadOID: candidate.Snapshot.HeadSHA, BaseRef: "main", BaseOID: candidate.Snapshot.BaseSHA, FactoryOwned: true}
	title, body := "sf: SF-publication", "typed evidence"
	if got, want := draftKey(identity, title, body), "github/draft-pr/v1/"+githubboundary.CanonicalDraftPullRequestRequestDigest(identity, title, body); got != want {
		t.Fatalf("draft semantic key=%q want %q", got, want)
	}
	changed := candidate
	changed.Snapshot.HeadSHA = strings.Repeat("c", 40)
	if pushRequestDigest(ref, "/tmp/repository", worktree, changed, "") == first {
		t.Fatal("candidate head did not affect push request")
	}
	if pushRequestDigest(ref, "/tmp/repository", worktree, candidate, strings.Repeat("a", 40)) == first {
		t.Fatal("prior candidate head did not affect push request")
	}
}

func TestSamePRRequiresThePublishedBaseAndOwnedIdentity(t *testing.T) {
	base := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "nysa", Name: "app"}, Number: 2, HeadOwner: "nysa", HeadRepository: "app", HeadRef: "sf/dev/ref", HeadOID: strings.Repeat("b", 40), BaseRef: "main", BaseOID: strings.Repeat("a", 40), FactoryOwned: true}
	if !samePR(base, base) {
		t.Fatal("exact PR rejected")
	}
	changed := base
	changed.BaseOID = strings.Repeat("c", 40)
	if samePR(changed, base) {
		t.Fatal("base drift accepted")
	}
	changed = base
	changed.FactoryOwned = false
	if samePR(changed, base) {
		t.Fatal("foreign PR accepted")
	}
	changed = base
	changed.Number++
	if samePR(changed, base) {
		t.Fatal("different numbered PR accepted for an exact persisted identity")
	}
	// The initial create lookup has no durable PR number yet, so a positive
	// observed number is valid when the expected identity is intentionally
	// numberless.
	numberless := base
	numberless.Number = 0
	if !samePR(base, numberless) {
		t.Fatal("numberless create identity rejected")
	}
}

func TestCorrectionWorktreeIdentityPermitsOnlyProtectedBaseRefresh(t *testing.T) {
	prior := gitboundary.Identity{Repository: "/repo", RepositoryDev: 1, RepositoryIno: 2, Worktree: "/worktree", WorktreeDev: 3, WorktreeIno: 4, GitFile: "/worktree/.git", GitFileDev: 5, GitFileIno: 6, CommonDir: "/repo/.git", CommonDirDev: 7, CommonDirIno: 8, Origin: "https://github.com/acme/app.git", PushOrigin: "https://github.com/acme/app.git", PushOriginDev: 9, PushOriginIno: 10, BaseRef: "main", BaseHead: strings.Repeat("a", 40), HeadRef: "sf/dev/correction", ConfigHash: "config", HooksHash: "hooks"}
	current := prior
	current.BaseHead = strings.Repeat("b", 40)
	if !sameCorrectionWorktreeIdentity(prior, current) {
		t.Fatal("protected-base refresh rejected")
	}
	current = prior
	current.BaseHead = strings.Repeat("b", 40)
	current.PushOrigin = "https://github.com/acme/other.git"
	if sameCorrectionWorktreeIdentity(prior, current) {
		t.Fatal("push-origin substitution accepted during base refresh")
	}
}

func TestDraftCorrectionReplansUnappliedUpdateAfterFenceBump(t *testing.T) {
	runDraftCorrectionFenceRecovery(t, testkit.ResponseErrorBefore, 0, 1)
}

func TestDraftCorrectionReconcilesLostAppliedUpdateAfterFenceBump(t *testing.T) {
	runDraftCorrectionFenceRecovery(t, testkit.ResponseDropAfterCall, 1, 1)
}

func runDraftCorrectionFenceRecovery(t *testing.T, response testkit.ResponseMode, beforeRecovery, afterRecovery int) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "sf.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	projectPath := filepath.Join(t.TempDir(), "repo")
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("app", projectPath), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "app", Path: projectPath, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "app", Ticket: "SF-correction-recovery"}
	source := []byte("correction recovery")
	sum := sha256.Sum256(source)
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: hex.EncodeToString(sum[:]), Source: source, Type: domain.TicketFeature, MergeMode: domain.MergeGuarded}); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "correction-recovery")
	if err != nil {
		t.Fatal(err)
	}
	started, err := db.StartOrAdopt(ctx, ref, 1, "correction-recovery", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	oldHead, newHead, base := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	repository := contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}
	branch := "sf/dev/correction-recovery"
	old := contracts.PullRequestIdentity{Repository: repository, Number: 7, HeadOwner: "acme", HeadRepository: "app", HeadRef: branch, HeadOID: oldHead, BaseRef: "main", BaseOID: base, FactoryOwned: true}
	current := old
	current.HeadOID = newHead
	marker := "<!-- sf:v1 repository=acme/app head=acme/app:" + branch + " oid=" + oldHead + " base=main -->"
	fake, err := testkit.NewFakeGH(filepath.Join(t.TempDir(), "github.json"), repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := fake.SetAuthenticated(true); err != nil {
		t.Fatal(err)
	}
	if err := fake.InjectPullRequestForTest(testkit.PullRequest{Identity: old, Draft: true, Body: marker, Title: "old"}); err != nil {
		t.Fatal(err)
	}
	if err := fake.SetPullRequestHeadOIDForTest(old.Number, newHead); err != nil {
		t.Fatal(err)
	}
	key := draftUpdateKey(current, "new", "new body")
	oldFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	if err := fake.SetResponse("pr_edit", response); err != nil {
		t.Fatal(err)
	}
	worker := Worker{Store: db, GitHub: fake}
	if _, _, err := worker.ensureDraftCorrection(ctx, started, oldFence, current, "new", "new body", old); err == nil {
		t.Fatal("simulated update loss unexpectedly succeeded")
	}
	firstEffect, err := db.Effect(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if response == testkit.ResponseErrorBefore && firstEffect.State != store.EffectFailed {
		t.Fatalf("worker did not durably fail pre-start update: %+v", firstEffect)
	}
	if response == testkit.ResponseDropAfterCall && firstEffect.State != store.EffectUncertain {
		t.Fatalf("worker did not durably quarantine post-handoff update: %+v", firstEffect)
	}
	if got := fake.MutationCount("pr_edit"); got != beforeRecovery {
		t.Fatalf("before recovery mutations=%d want=%d", got, beforeRecovery)
	}

	newLeader, err := db.AcquireLeader(ctx, domain.ChannelDev, "correction-recovery-restart")
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := db.InvalidateRunner(ctx, ref, started.Version, domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: started.RunnerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	newFence := domain.Fence{LeaderEpoch: newLeader, RunnerEpoch: advanced.RunnerEpoch}
	got, _, err := worker.ensureDraftCorrection(ctx, advanced, newFence, current, "new", "new body", old)
	if err != nil {
		t.Fatalf("recover correction update=%v", err)
	}
	if !samePR(got, current) || fake.MutationCount("pr_edit") != afterRecovery {
		t.Fatalf("recovered=%+v mutations=%d want=%d", got, fake.MutationCount("pr_edit"), afterRecovery)
	}
	effect, err := db.Effect(ctx, key)
	if err != nil || effect.State != store.EffectConfirmed || effect.TicketVersion != advanced.Version || effect.LeaderEpoch != newFence.LeaderEpoch || effect.RunnerEpoch != newFence.RunnerEpoch || effect.ClaimEpoch != firstEffect.ClaimEpoch+1 {
		t.Fatalf("effect=%+v err=%v", effect, err)
	}
}
