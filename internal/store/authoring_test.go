package store

import (
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"strings"
	"testing"
)

func TestAuthoringReservationAuthorityIdempotencyAndDrain(t *testing.T) {
	db, ctx := openTestStore(t)
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	epoch, err := db.AcquireLeader(ctx, domain.ChannelDev, "authoring-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, epoch, signer.PublicKey()); err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	session := AuthoringSession{Channel: domain.ChannelDev, ID: "draft-session", Purpose: "ticket_draft", Project: "nysa", ContextDigest: digest, Capability: contracts.AuthoringCapability{Identity: domain.ProviderIdentity{Provider: "claude", Model: "opus", Family: "claude", Version: "2.1.263"}, BinaryDigest: digest, AuthDigest: digest, PolicyDigest: digest}}
	if err := db.CreateAuthoringSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	forged := session
	forged.ContextDigest = strings.Repeat("b", 64)
	if _, _, err := db.ReserveAuthoringTurn(ctx, forged, "turn", digest, epoch); err == nil {
		t.Fatal("forged session accepted")
	}
	turn, created, err := db.ReserveAuthoringTurn(ctx, session, "turn", digest, epoch)
	if err != nil || !created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	replay, created, err := db.ReserveAuthoringTurn(ctx, session, "turn", digest, epoch)
	if err != nil || created || replay.Claim != turn.Claim {
		t.Fatal("reservation replay changed identity")
	}
	if _, _, err := db.ReserveAuthoringTurn(ctx, session, "other", digest, epoch); err == nil {
		t.Fatal("undrained channel admitted another turn")
	}
	proof, err := signer.ProveAuthoringDrained(turn.Claim, epoch)
	if err != nil {
		t.Fatal(err)
	}
	result := contracts.AuthoringResult{Kind: "draft", Title: "Count jobs", Problem: "Return count", Scope: []string{}, Acceptance: []string{"Empty returns zero"}, Assumptions: []string{}}
	if err := db.FinishAuthoringTurn(ctx, turn.Claim, proof, "success", &result); err == nil {
		t.Fatal("success before launch accepted")
	}
	if err := db.FinishAuthoringTurn(ctx, turn.Claim, proof, "cancelled", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.FinishAuthoringTurn(ctx, turn.Claim, proof, "cancelled", nil); err != nil {
		t.Fatal("exact finish replay failed", err)
	}
	if err := db.FinishAuthoringTurn(ctx, turn.Claim, proof, "failed", nil); err == nil {
		t.Fatal("mismatched finish replay accepted")
	}
	for n := 2; n <= 4; n++ {
		key := strings.Repeat("x", n)
		next, created, err := db.ReserveAuthoringTurn(ctx, session, key, digest, epoch)
		if err != nil || !created {
			t.Fatal(err)
		}
		proof, _ := signer.ProveAuthoringDrained(next.Claim, epoch)
		if err := db.FinishAuthoringTurn(ctx, next.Claim, proof, "cancelled", nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := db.ReserveAuthoringTurn(ctx, session, "fifth", digest, epoch); err == nil {
		t.Fatal("fifth turn accepted")
	}
}
