package store

import (
	"bytes"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

// This is a serialization fixture, not a substitute for the real Store
// authority fixtures required before refresh can mint a Git claim.
func baseRefreshPayloadFixture(t *testing.T) protectedBaseRefreshIntent {
	t.Helper()
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "relay", Ticket: "refresh"}
	d := strings.Repeat("d", 64)
	base, head, tree := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	fence := domain.Fence{LeaderEpoch: 1, RunnerEpoch: 1}
	return protectedBaseRefreshIntent{
		Format: "sf.protected-base-refresh.v1", Ref: ref, TicketVersion: 5, Fence: fence,
		SourceDigest: d, ConfigGeneration: 1, ConfigDigest: d, ConfigSnapshotDigest: d,
		Repository: "/tmp/relay", BaseRef: "main", NewBaseSHA: strings.Repeat("e", 40), BaseProofSemanticKey: "git/protected-proof",
		ProtectedPaths: []string{"tests/verification.test.js"},
		Candidate: StoredCandidate{TicketVersion: 4, Fence: fence,
			Snapshot:      domain.CandidateSnapshot{Generation: 1, BaseSHA: base, HeadSHA: head, TreeSHA: tree, SourceDigest: d, VerificationIntentDigest: d, ProofDigest: d, CommandPolicyDigest: d, BuilderEvidenceDigest: d},
			BuilderResult: ProviderAttemptResultKey{Ref: ref, Phase: domain.PhaseBuild, AttemptID: 3, Attempt: 1},
			Commit:        CommitObservation{CommitOID: head, TreeOID: tree, ParentOID: strings.Repeat("f", 40)},
		},
		Worktree: StoredWorktree{Path: "/tmp/relay/worktree", Branch: "sf/dev/refresh", State: "registered", BaseSHA: base, HeadSHA: base, TicketVersion: 2, Fence: fence,
			IdentityJSON: []byte(repositoryCommandIdentity(t, "/tmp/relay", "/tmp/relay/worktree", "sf/dev/refresh", "main"))},
	}
}

func TestProtectedBaseRefreshPayloadRoundTripsExactBytes(t *testing.T) {
	value := baseRefreshPayloadFixture(t)
	payload, digest, err := canonicalProtectedBaseRefreshIntent(value)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeProtectedBaseRefreshIntent(payload, digest)
	if err != nil {
		t.Fatal(err)
	}
	again, againDigest, err := canonicalProtectedBaseRefreshIntent(decoded)
	if err != nil || !bytes.Equal(payload, again) || digest != againDigest {
		t.Fatalf("changed roundtrip: %v", err)
	}
	for _, malformed := range [][]byte{
		append([]byte(" "), payload...),
		append(append([]byte(nil), payload...), '\n'),
		append(append([]byte(nil), payload...), []byte("{}")...),
		bytes.Replace(payload, []byte(`"Format":`), []byte(`"format":`), 1),
		bytes.Replace(payload, []byte(`"Format":`), []byte(`"Unknown":1,"Format":`), 1),
		bytes.Replace(payload, []byte(`"Format":`), []byte(`"Format":"sf.protected-base-refresh.v1","Format":`), 1),
	} {
		if _, err := decodeProtectedBaseRefreshIntent(malformed, ciAuthorityDigest(malformed)); err == nil {
			t.Fatalf("noncanonical payload accepted: %q", malformed)
		}
	}
	if _, err := decodeProtectedBaseRefreshIntent(payload, "sha256:"+strings.Repeat("0", 64)); err == nil {
		t.Fatal("tampered digest accepted")
	}
}

func TestProtectedBaseRefreshPayloadBindsEveryLineage(t *testing.T) {
	for name, mutate := range map[string]func(*protectedBaseRefreshIntent){
		"new base":        func(v *protectedBaseRefreshIntent) { v.NewBaseSHA = strings.Repeat("f", 40) },
		"proof":           func(v *protectedBaseRefreshIntent) { v.BaseProofSemanticKey += "/different" },
		"config":          func(v *protectedBaseRefreshIntent) { v.ConfigGeneration++ },
		"version":         func(v *protectedBaseRefreshIntent) { v.TicketVersion++ },
		"leader":          func(v *protectedBaseRefreshIntent) { v.Fence.LeaderEpoch++ },
		"runner":          func(v *protectedBaseRefreshIntent) { v.Fence.RunnerEpoch++ },
		"provider":        func(v *protectedBaseRefreshIntent) { v.Candidate.BuilderResult.AttemptID++ },
		"command":         func(v *protectedBaseRefreshIntent) { v.Candidate.CommandBinding.Key.SemanticKey = "different" },
		"protected paths": func(v *protectedBaseRefreshIntent) { v.ProtectedPaths = []string{"tests/different.test.js"} },
	} {
		t.Run(name, func(t *testing.T) {
			value := baseRefreshPayloadFixture(t)
			_, original, err := canonicalProtectedBaseRefreshIntent(value)
			if err != nil {
				t.Fatal(err)
			}
			mutate(&value)
			_, changed, err := canonicalProtectedBaseRefreshIntent(value)
			if err == nil && original == changed {
				t.Fatal("changed lineage preserved digest")
			}
		})
	}
}

func TestProtectedBaseRefreshPayloadRejectsImpossibleRefresh(t *testing.T) {
	for name, mutate := range map[string]func(*protectedBaseRefreshIntent){
		"unchanged base":          func(v *protectedBaseRefreshIntent) { v.NewBaseSHA = v.Worktree.BaseSHA },
		"missing base":            func(v *protectedBaseRefreshIntent) { v.NewBaseSHA = "" },
		"mixed format":            func(v *protectedBaseRefreshIntent) { v.NewBaseSHA = strings.Repeat("e", 64) },
		"future candidate":        func(v *protectedBaseRefreshIntent) { v.Candidate.TicketVersion = v.TicketVersion },
		"source mismatch":         func(v *protectedBaseRefreshIntent) { v.SourceDigest = strings.Repeat("e", 64) },
		"worktree mismatch":       func(v *protectedBaseRefreshIntent) { v.Worktree.BaseSHA = v.NewBaseSHA },
		"opaque identity":         func(v *protectedBaseRefreshIntent) { v.Worktree.IdentityJSON = []byte(`{}`) },
		"wrong provider":          func(v *protectedBaseRefreshIntent) { v.Candidate.BuilderResult.Ref.Ticket = "other" },
		"claim fence":             func(v *protectedBaseRefreshIntent) { v.Fence.ClaimEpoch = 1 },
		"missing leader":          func(v *protectedBaseRefreshIntent) { v.Fence.LeaderEpoch = 0 },
		"generation overflow":     func(v *protectedBaseRefreshIntent) { v.Candidate.Snapshot.Generation = ^uint64(0) },
		"missing protected paths": func(v *protectedBaseRefreshIntent) { v.ProtectedPaths = nil },
	} {
		t.Run(name, func(t *testing.T) {
			value := baseRefreshPayloadFixture(t)
			mutate(&value)
			if _, _, err := canonicalProtectedBaseRefreshIntent(value); err == nil {
				t.Fatal("invalid refresh accepted")
			}
		})
	}
}
