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
	githubboundary "github.com/nysa-company/sf/internal/github"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

func TestPublicationKeysAreDeterministicAndCandidateBound(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-publication"}
	worktree := store.StoredWorktree{Path: "/tmp/publication", Branch: "sf/dev/1111111111111111/2222222222222222-33333333333333333333333333333333"}
	candidate := store.StoredCandidate{Snapshot: domain.CandidateSnapshot{BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}}
	first := pushRequestDigest(ref, "/tmp/repository", worktree, candidate)
	if first != pushRequestDigest(ref, "/tmp/repository", worktree, candidate) || !strings.HasPrefix(first, "sha256:") {
		t.Fatalf("push request is not stable: %q", first)
	}
	identity := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "nysa", Name: "app"}, HeadOwner: "nysa", HeadRepository: "app", HeadRef: worktree.Branch, HeadOID: candidate.Snapshot.HeadSHA, BaseRef: "main", BaseOID: candidate.Snapshot.BaseSHA, FactoryOwned: true}
	title, body := "sf: SF-publication", "typed evidence"
	if got, want := draftKey(identity, title, body), "github/draft-pr/v1/"+githubboundary.CanonicalDraftPullRequestRequestDigest(identity, title, body); got != want {
		t.Fatalf("draft semantic key=%q want %q", got, want)
	}
	changed := candidate
	changed.Snapshot.HeadSHA = strings.Repeat("c", 40)
	if pushRequestDigest(ref, "/tmp/repository", worktree, changed) == first {
		t.Fatal("candidate head did not affect push request")
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

func TestDraftCorrectionReconcilesAppliedUpdateAfterFenceBump(t *testing.T) {
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
	request := githubboundary.CanonicalPullRequestUpdateRequestDigest(current, "new", "new body")
	oldFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: started.RunnerEpoch}
	if _, err := db.PlanEffect(ctx, store.EffectPlan{SemanticKey: key, Ref: ref, Kind: store.PublicationPRUpdateEffectKind, TicketVersion: started.Version, Fence: oldFence, RequestDigest: request}); err != nil {
		t.Fatal(err)
	}
	claim, err := db.ClaimEffect(ctx, store.EffectFence{SemanticKey: key, Ref: ref, TicketVersion: started.Version, Fence: oldFence})
	if err != nil || !claim.Claimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	if err := fake.SetResponse("pr_edit", testkit.ResponseDropAfterCall); err != nil {
		t.Fatal(err)
	}
	if err := fake.UpdatePullRequest(ctx, claim.ExternalClaim(), current, "new", "new body"); err == nil {
		t.Fatal("lost update response unexpectedly succeeded")
	}
	if got := fake.MutationCount("pr_edit"); got != 1 {
		t.Fatalf("update mutations=%d", got)
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
	worker := Worker{Store: db, GitHub: fake}
	got, _, err := worker.ensureDraftCorrection(ctx, advanced, newFence, current, "new", "new body", old)
	if err != nil {
		t.Fatalf("reconcile applied update=%v", err)
	}
	if !samePR(got, current) || fake.MutationCount("pr_edit") != 1 {
		t.Fatalf("reconciled=%+v mutations=%d", got, fake.MutationCount("pr_edit"))
	}
	effect, err := db.Effect(ctx, key)
	if err != nil || effect.State != store.EffectConfirmed {
		t.Fatalf("effect=%+v err=%v", effect, err)
	}
}
