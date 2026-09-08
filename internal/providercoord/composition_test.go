package providercoord

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

type compositionProcess struct {
	*testkit.Supervisor
	registrations int
}

func (p *compositionProcess) RegisterRuntime(b contracts.RuntimeBinding, _, _ string) (string, error) {
	p.registrations++
	return b.BinaryDigest, nil
}

type compositionProvider struct {
	*testkit.ScriptedProvider
	binding contracts.RuntimeBinding
	route   string
}

func (p *compositionProvider) Name() string { return p.route }
func (p *compositionProvider) Binding(context.Context) (contracts.RuntimeBinding, error) {
	return p.binding, nil
}

func TestComposeQualifiedUsesExactThreeRoleSelection(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "store.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	process := &compositionProcess{Supervisor: testkit.NewSupervisor()}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "composition")
	if err != nil || db.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, process.PublicKey()) != nil {
		t.Fatal("authority setup failed")
	}
	var candidates []RuntimeCandidate
	var ids []int64
	for i, item := range []struct{ provider, family, mode string }{{"cursor", "xai-grok", "cursor_browser"}, {"claude", "anthropic-claude", "claude_subscription"}, {"codex", "openai-gpt-5.6", "chatgpt_subscription"}} {
		identity := domain.ProviderIdentity{Provider: item.provider, Model: "fixture-model", Family: item.family, Version: "fixture-version"}
		binding := contracts.RuntimeBinding{Identity: identity, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), AuthDigest: strings.Repeat("d", 64), AuthMode: item.mode}
		created := time.Now().UTC()
		run := strings.Repeat(string(rune('a'+i)), 32)
		proof, err := process.Signer.SignQualification(contracts.QualificationAttestation{Channel: domain.ChannelDev, RunID: run, Identity: identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, AuthDigest: binding.AuthDigest, AuthMode: item.mode, ProbeDigest: strings.Repeat("e", 64), Profile: contracts.ProfileGuarded, CreatedUnixNanos: created.UnixNano(), LeaderEpoch: leader, Nonce: run})
		if err != nil {
			t.Fatal(err)
		}
		q, _, err := db.RecordAttestedProviderQualification(ctx, store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, AuthDigest: binding.AuthDigest, AuthMode: item.mode, ProbeDigest: proof.ProbeDigest, Profile: store.QualificationGuarded, CreatedAt: created}, proof)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, q.ID)
		provider := &compositionProvider{ScriptedProvider: testkit.NewScriptedProvider(identity), binding: binding, route: "route-" + item.provider}
		candidates = append(candidates, RuntimeCandidate{Provider: provider, Executable: "/fixture/bin", AuthHome: "/fixture/home"})
	}
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, ids[0], ids[1], ids[2], time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	coordinator, err := ComposeQualified(ctx, domain.ChannelDev, db, process, candidates, 2)
	if err != nil {
		t.Fatal(err)
	}
	for role, name := range map[Role]string{RolePlanner: "route-cursor", RoleBuilder: "route-claude", RoleReviewer: "route-codex"} {
		route := coordinator.routes[role]
		if route.Primary != name || route.Capacity != 2 || route.Fallback != "" {
			t.Fatalf("incorrect %s route: %+v", role, route)
		}
	}
	if process.registrations != 3 {
		t.Fatal("missing registration")
	}
	assertUnavailable := func(candidates []RuntimeCandidate) {
		t.Helper()
		c, err := ComposeQualified(ctx, domain.ChannelDev, db, process, candidates, 1)
		if err != nil || len(c.routes) != 0 {
			t.Fatal("partial or stale set composed", err)
		}
	}
	assertUnavailable(candidates[:2])
	assertUnavailable(append(append([]RuntimeCandidate(nil), candidates...), candidates[0]))
	p := candidates[0].Provider.(*compositionProvider)
	p.binding.AuthDigest = strings.Repeat("f", 64)
	assertUnavailable(candidates)
	p.binding.AuthDigest = strings.Repeat("d", 64)
	if _, err := db.AcquireLeader(ctx, domain.ChannelDev, "restart"); err != nil {
		t.Fatal(err)
	}
	assertUnavailable(candidates)
}
