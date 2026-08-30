package providercoord

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
)

func TestMalformedOutputFallsBackAndNeverPersistsSecrets(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	identity := `{"repository":"/tmp/p"}`
	t.Cleanup(func() { db.Close() })
	raw := []byte("frozen")
	sum := sha256.Sum256(raw)
	digest := fmt.Sprintf("%x", sum)
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "p", Path: "/tmp/p", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: raw}); err != nil {
		t.Fatal(err)
	}
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "test")
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-1"}
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "source", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.StartOrAdopt(ctx, ref, 1, "dev/p/SF-1", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Path: root, Branch: "dev/p/SF-1", IdentityJSON: []byte(identity), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	primary := testkit.NewScriptedProvider(id("cursor", "cursor-family"))
	secret := strings.Join([]string{"token", "must-not-persist"}, "=")
	primary.Add(domain.PhasePlanning, testkit.ProviderStep{Behavior: testkit.ProviderMalformed, Transcript: secret})
	fallback := testkit.NewScriptedProvider(id("claude", "claude-family"))
	fallback.Add(domain.PhasePlanning, testkit.ProviderStep{Artifact: plannerArtifact()})
	recordQual(t, db, primary)
	recordQual(t, db, fallback)
	primaryQualification, _ := db.LatestProviderQualification(ctx, domain.ChannelDev, id("cursor", "cursor-family"))
	fallbackQualification, _ := db.LatestProviderQualification(ctx, domain.ChannelDev, id("claude", "claude-family"))
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, primaryQualification.ID, primaryQualification.ID, fallbackQualification.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := registry.Register(ctx, primary); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(ctx, fallback); err != nil {
		t.Fatal(err)
	}
	c, err := New(registry, map[Role]Route{RolePlanner: {Primary: "cursor", Fallback: "claude"}}, db, nil, testkit.NewSupervisor())
	if err != nil {
		t.Fatal(err)
	}
	r := Request{Role: RolePlanner, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, ConfigDigest: digest, Validation: phaseartifact.Validation{TicketType: domain.TicketFeature}, Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, Prompt: "x", Repository: "/tmp/p", Worktree: root, WorktreeIdentity: identity, BaseSHA: strings.Repeat("a", 40), AllowedPaths: []string{"x"}, Timeout: time.Second, Profile: contracts.ProfileGuarded, Schema: []byte("schema")}}
	result := c.Run(ctx, r)
	if result.Code != Failed || len(result.Attempts) != 1 {
		t.Fatalf("result=%+v", result)
	}
	rawSecret := sha256.Sum256([]byte(secret))
	if result.Attempts[0].TranscriptDigest == "" || result.Attempts[0].TranscriptDigest == "sha256:"+fmt.Sprintf("%x", rawSecret) {
		t.Fatalf("unsafe digest=%+v", result.Attempts[0])
	}
}

type undrainedProvider struct {
	*testkit.ScriptedProvider
	drained bool
	request contracts.DrainRequest
}

type refusingSupervisor struct{ *testkit.Supervisor }

func (s refusingSupervisor) Drain(context.Context, contracts.DrainRequest) (contracts.DrainProof, error) {
	return contracts.DrainProof{}, fmt.Errorf("tracked process group remains live")
}

func (p *undrainedProvider) Drain(_ context.Context, request contracts.DrainRequest) (contracts.DrainResult, error) {
	p.request = request
	return contracts.DrainResult{Drained: p.drained}, nil
}

func TestCancellationQuarantinesWhenProviderDoesNotDrain(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "db.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	identity := `{"repository":"/tmp/p"}`
	t.Cleanup(func() { _ = db.Close() })
	raw := []byte("frozen")
	sum := sha256.Sum256(raw)
	digest := fmt.Sprintf("%x", sum)
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "p", Path: "/tmp/p", BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: raw}); err != nil {
		t.Fatal(err)
	}
	leader, _ := db.AcquireLeader(ctx, domain.ChannelDev, "cancel-test")
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-cancel"}
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "source", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.StartOrAdopt(ctx, ref, 1, "dev/p/SF-cancel", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, Path: root, Branch: "dev/p/SF-cancel", IdentityJSON: []byte(identity), BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40)}); err != nil {
		t.Fatal(err)
	}
	primary := &undrainedProvider{ScriptedProvider: testkit.NewScriptedProvider(id("cursor", "cursor-family")), drained: false}
	primary.Add(domain.PhasePlanning, testkit.ProviderStep{Behavior: testkit.ProviderHang})
	fallback := testkit.NewScriptedProvider(id("claude", "claude-family"))
	recordQual(t, db, primary.ScriptedProvider)
	recordQual(t, db, fallback)
	primaryQualification, _ := db.LatestProviderQualification(ctx, domain.ChannelDev, id("cursor", "cursor-family"))
	fallbackQualification, _ := db.LatestProviderQualification(ctx, domain.ChannelDev, id("claude", "claude-family"))
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, primaryQualification.ID, primaryQualification.ID, fallbackQualification.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	if err := registry.Register(ctx, primary); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(ctx, fallback); err != nil {
		t.Fatal(err)
	}
	c, err := New(registry, map[Role]Route{RolePlanner: {Primary: "cursor", Fallback: "claude"}}, db, nil, refusingSupervisor{testkit.NewSupervisor()})
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Role: RolePlanner, ExpectedVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}, ConfigDigest: digest, Validation: phaseartifact.Validation{TicketType: domain.TicketFeature}, Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, Prompt: "x", Repository: "/tmp/p", Worktree: root, WorktreeIdentity: identity, BaseSHA: strings.Repeat("a", 40), AllowedPaths: []string{"x"}, Timeout: time.Second, Profile: contracts.ProfileGuarded, Schema: []byte("schema")}}
	// Leave enough startup budget for the race-instrumented SQLite admission
	// path to persist the claim before cancellation exercises quarantine.
	callCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	result := c.Run(callCtx, request)
	if !result.NeedsOperator {
		t.Fatalf("cancellation result=%+v", result)
	}
	attempts, err := db.ProviderAttempts(ctx, ref)
	if err != nil || len(attempts) != 1 || attempts[0].State != "quarantined" {
		t.Fatalf("quarantined attempts=%+v err=%v", attempts, err)
	}
	claim := attempts[0]
	_ = claim // the adapter cannot self-attest: only the refusing supervisor was consulted.
}
func id(name, family string) domain.ProviderIdentity {
	return domain.ProviderIdentity{Provider: name, Model: name + "-model", Family: family, Version: "v1"}
}
func plannerArtifact() []byte {
	return []byte(`{"schema":"sf.planner/v1","acceptance":["works"],"proof":{"kind":"acceptance","command":["go","test"],"details":"x"},"paths":["x"],"commands":[["go","test"]],"risks":["x"]}`)
}
func recordQual(t *testing.T, db *store.Store, p *testkit.ScriptedProvider) {
	t.Helper()
	b, e := p.Binding(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	runSum := sha256.Sum256([]byte(b.Identity.Provider + b.Identity.Model + b.Identity.Version))
	run := fmt.Sprintf("%x", runSum)[:32]
	_, _, e = db.RecordProviderQualification(context.Background(), store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: b.Identity, BinaryDigest: b.BinaryDigest, PolicyDigest: b.PolicyDigest, FixtureDigest: b.FixtureDigest, Profile: store.QualificationGuarded, CreatedAt: time.Now().UTC()})
	if e != nil {
		t.Fatal(e)
	}
}
