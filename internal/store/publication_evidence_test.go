package store

import (
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestPublicationEvidenceSchemaAndNarrowAbsence(t *testing.T) {
	db, ctx := openTestStore(t)
	if err := db.validateSchema(ctx); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-publication-absence"}
	if ok, err := db.PublishedCandidateExists(ctx, ref); err != nil || ok { // missing row is not a publication identity
		t.Fatalf("absence query=%v err=%v", ok, err)
	}
	if got := CanonicalPublicationPushObservation("sf/dev/branch", strings.Repeat("a", 40)); got == "" || !strings.HasPrefix(got, "sha256:") || strings.ContainsRune(got, '\x00') {
		t.Fatalf("invalid bounded canonical push identity: %q", got)
	}
}

func TestPublicationObservationIdentitiesAreExactAndDistinct(t *testing.T) {
	pr := contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "o", Name: "r"}, Number: 1, HeadOwner: "o", HeadRepository: "r", HeadRef: "sf/dev/branch", HeadOID: strings.Repeat("a", 40), BaseRef: "main", BaseOID: strings.Repeat("b", 40), FactoryOwned: true}
	push := CanonicalPublicationPushObservation(pr.HeadRef, pr.HeadOID)
	prID := CanonicalPublicationPRObservation(pr, "OPEN", true)
	if push == prID || !strings.HasPrefix(prID, "sha256:") {
		t.Fatalf("push=%q pr=%q", push, prID)
	}
}

func TestPublicationRebindDigestIsCanonicalAndChainsVersions(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-rebind"}
	head := strings.Repeat("a", 40)
	first := PublicationRebind{Ref: ref, CandidateGeneration: 1, CandidateHeadOID: head, PriorWitnessDigest: "sha256:" + strings.Repeat("b", 64), PriorTicketVersion: 5, PriorFence: domain.Fence{LeaderEpoch: 2, RunnerEpoch: 3}, TicketVersion: 6, Fence: domain.Fence{LeaderEpoch: 4, RunnerEpoch: 4}}
	firstPayload, err := publicationRebindPayload(first)
	if err != nil {
		t.Fatal(err)
	}
	first.RebindDigest = publicationIdentityDigest(firstPayload)
	second := PublicationRebind{Ref: ref, CandidateGeneration: 1, CandidateHeadOID: head, PriorWitnessDigest: first.PriorWitnessDigest, PriorTicketVersion: first.TicketVersion, PriorFence: first.Fence, TicketVersion: 7, Fence: domain.Fence{LeaderEpoch: 5, RunnerEpoch: 5}}
	secondPayload, err := publicationRebindPayload(second)
	if err != nil {
		t.Fatal(err)
	}
	if publicationIdentityDigest(secondPayload) == first.RebindDigest || second.PriorTicketVersion != first.TicketVersion || second.PriorFence != first.Fence {
		t.Fatal("successive recovery rebind did not preserve an exact predecessor")
	}
}

func TestPublicationEffectDigestSpellingsAreStrict(t *testing.T) {
	base := PublicationEffectEvidence{SemanticKey: "effect", Kind: PublicationPushEffectKind, RequestDigest: strings.Repeat("a", 64), ClaimEpoch: 1, ObservedIdentity: "sha256:" + strings.Repeat("b", 64)}
	if !validPublicationEffect(base) {
		t.Fatal("plain GitHub digest should be accepted")
	}
	base.RequestDigest = "sha256:" + strings.Repeat("a", 64)
	if !validPublicationEffect(base) {
		t.Fatal("typed Git digest should be accepted")
	}
	for _, malformed := range []string{"digest", "SHA256:" + strings.Repeat("a", 64), "sha256:" + strings.Repeat("a", 63), strings.Repeat("a", 63)} {
		base.RequestDigest = malformed
		if validPublicationEffect(base) {
			t.Fatalf("malformed request digest accepted: %q", malformed)
		}
	}
}

func TestPublishedCandidateValidationAcceptsCanonicalPushWitness(t *testing.T) {
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "nysa", Ticket: "SF-published"}
	base, head, tree, parent := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40), strings.Repeat("d", 40)
	remoteBase := base
	branch := "sf/dev/nysa/SF-published"
	identity := []byte(`{"Origin":"https://github.com/acme/app.git","PushOrigin":"git@github.com:acme/app.git","BaseRef":"main","BaseHead":"` + remoteBase + `","HeadRef":"` + branch + `"}`)
	policy := strings.Repeat("1", 64)
	command := RepositoryCommandResultBinding{Key: contracts.RepositoryCommandResultKey{SemanticKey: "command", ClaimEpoch: 1}, TicketVersion: 5, LeaderEpoch: 1, RunnerEpoch: 1, CommandDigest: "sha256:" + strings.Repeat("2", 64), SpecDigest: "sha256:" + strings.Repeat("3", 64), PolicyDigest: "sha256:" + policy, ExecutablePath: "/usr/bin/true", ExecutableDigest: "sha256:" + strings.Repeat("4", 64)}
	value := PublishedCandidateEvidence{Ref: ref, TicketVersion: 6, Fence: domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}, Candidate: StoredCandidate{Snapshot: domain.CandidateSnapshot{Generation: 1, BaseSHA: base, HeadSHA: head, TreeSHA: tree, SourceDigest: strings.Repeat("5", 64), VerificationIntentDigest: strings.Repeat("6", 64), ProofDigest: strings.Repeat("7", 64), CommandPolicyDigest: policy, BuilderEvidenceDigest: strings.Repeat("8", 64)}, TicketVersion: 5, Fence: domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}, BuilderResult: ProviderAttemptResultKey{AttemptID: 1, Ref: ref, Phase: domain.PhaseBuild, Attempt: 1}, Commit: CommitObservation{CommitOID: head, ParentOID: parent, TreeOID: tree}, CommandBinding: command}, ConfigGeneration: 1, ConfigDigest: strings.Repeat("9", 64), ConfigSnapshotDigest: strings.Repeat("a", 64), Worktree: StoredWorktree{Path: "/tmp/SF-published", Branch: branch, State: "registered", IdentityJSON: identity, BaseSHA: base, HeadSHA: parent, TicketVersion: 2, Fence: domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}}, RemoteBranchRef: branch, RemoteBranchOID: head, RemoteBaseOID: remoteBase, PushEffect: PublicationEffectEvidence{SemanticKey: "push", Kind: PublicationPushEffectKind, RequestDigest: strings.Repeat("b", 64), ClaimEpoch: 1, ObservedIdentity: CanonicalPublicationPushObservation(branch, head)}, PullRequest: contracts.PullRequestIdentity{Repository: contracts.RepositoryIdentity{Host: "github.com", Owner: "acme", Name: "app"}, Number: 1, HeadOwner: "acme", HeadRepository: "app", HeadRef: branch, HeadOID: head, BaseRef: "main", BaseOID: remoteBase, FactoryOwned: true}, PullRequestState: "OPEN", PullRequestDraft: true, PullRequestObservedAt: time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC), PRCreateOrUpdateEffect: PublicationEffectEvidence{SemanticKey: "pr", Kind: PublicationPRCreateEffectKind, RequestDigest: "sha256:" + strings.Repeat("c", 64), ClaimEpoch: 1}}
	value.PRCreateOrUpdateEffect.ObservedIdentity = CanonicalPublicationPRObservation(value.PullRequest, value.PullRequestState, value.PullRequestDraft)
	if err := validPublishedCandidateEvidence(value); err != nil {
		t.Fatalf("canonical publication witness rejected: %v", err)
	}
	if !publicationEqual(value, value) {
		t.Fatal("canonical publication witness is not exactly replayable")
	}
}
