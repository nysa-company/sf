package providercoord

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	gitboundary "github.com/nysa-company/sf/internal/git"
	"github.com/nysa-company/sf/internal/phaseartifact"
	"github.com/nysa-company/sf/internal/store"
	"github.com/nysa-company/sf/internal/testkit"
	"github.com/nysa-company/sf/internal/worktreecoord"
)

// This is a signed, credential-free protocol fixture, not a native Claude
// permission verdict. It exercises the real coordinator and Store accounting.
type estimatedProvider struct {
	*testkit.ScriptedProvider
	binding contracts.RuntimeBinding
}

func (p *estimatedProvider) Binding(context.Context) (contracts.RuntimeBinding, error) {
	return p.binding, nil
}

func (p *estimatedProvider) Parse(ctx context.Context, input contracts.PhaseInput, command contracts.CommandResult) (contracts.PhaseResult, error) {
	result, err := p.ScriptedProvider.Parse(ctx, input, command)
	result.UsageTrusted, result.UsageUnits = false, 0
	return result, err // unknown estimate is deliberately not verified zero
}

func estimatedRetryFixture(t *testing.T, strictCheckpoint ...bool) (*store.Store, Request, *Coordinator, *estimatedProvider, string) {
	return estimatedRetryFixtureWithGit(t, false, strictCheckpoint...)
}

func estimatedRetryFixtureWithGit(t *testing.T, physical bool, strictCheckpoint ...bool) (*store.Store, Request, *Coordinator, *estimatedProvider, string) {
	t.Helper()
	ctx := context.Background()
	repository := "/tmp/p"
	if physical {
		root := t.TempDir()
		repository = filepath.Join(root, "repository")
		if err := os.Mkdir(repository, 0700); err != nil {
			t.Fatal(err)
		}
		canonical, err := filepath.EvalSymlinks(repository)
		if err != nil {
			t.Fatal(err)
		}
		repository = canonical
		command := func(dir string, args ...string) {
			t.Helper()
			commandCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(commandCtx, "/usr/bin/git", args...)
			cmd.Dir = dir
			if err := cmd.Run(); err != nil {
				t.Fatal("disposable Git setup failed", err)
			}
		}
		command(root, "init", "--bare", filepath.Join(root, "remote.git"))
		command(repository, "init", "-b", "main")
		command(repository, "config", "user.name", "fixture")
		command(repository, "config", "user.email", "fixture@example.test")
		if err := os.WriteFile(filepath.Join(repository, "baseline.txt"), []byte("baseline\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repository, ".gitignore"), []byte("ignored.tmp\n"), 0600); err != nil {
			t.Fatal(err)
		}
		command(repository, "add", "baseline.txt", ".gitignore")
		command(repository, "commit", "-m", "fixture baseline")
		command(repository, "remote", "add", "origin", filepath.Join(root, "remote.git"))
		command(repository, "push", "origin", "main")
	}
	path := filepath.Join(t.TempDir(), "store.sqlite")
	db, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("p", repository), config.TicketOverride{})
	if err != nil {
		t.Fatal(err)
	}
	effective.Providers.Planner, effective.Providers.Builder = []string{"claude"}, []string{"claude"}
	raw, digest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateProject(ctx, store.Project{Channel: domain.ChannelDev, ID: "p", Path: repository, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: digest, ConfigSnapshot: raw}); err != nil {
		t.Fatal(err)
	}
	leader, err := db.AcquireLeader(ctx, domain.ChannelDev, "estimated-retry")
	if err != nil {
		t.Fatal(err)
	}
	process := testkit.NewSupervisor()
	if err := db.SetRecoveryAuthority(ctx, domain.ChannelDev, leader, process.PublicKey()); err != nil {
		t.Fatal(err)
	}
	var ids []int64
	var primary *estimatedProvider
	registry := NewRegistry()
	for index, item := range []struct{ name, family, mode string }{{"claude", "anthropic-claude", "claude_subscription"}, {"codex", "openai", "chatgpt_subscription"}} {
		binding := contracts.RuntimeBinding{Identity: domain.ProviderIdentity{Provider: item.name, Model: "fixture-model", Family: item.family, Version: "fixture-version"}, BinaryDigest: strings.Repeat("a", 64), PolicyDigest: strings.Repeat("b", 64), FixtureDigest: strings.Repeat("c", 64), AuthDigest: strings.Repeat("d", 64), AuthMode: item.mode}
		created, run := time.Now().UTC(), strings.Repeat(string(rune('a'+index)), 32)
		proof, err := process.Signer.SignQualification(contracts.QualificationAttestation{Channel: domain.ChannelDev, RunID: run, Identity: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, ProbeDigest: strings.Repeat("e", 64), Profile: contracts.ProfileGuarded, CreatedUnixNanos: created.UnixNano(), LeaderEpoch: leader, Nonce: run})
		if err != nil {
			t.Fatal(err)
		}
		q, _, err := db.RecordAttestedProviderQualification(ctx, store.ProviderQualification{Channel: domain.ChannelDev, RunID: run, Provider: binding.Identity, BinaryDigest: binding.BinaryDigest, PolicyDigest: binding.PolicyDigest, FixtureDigest: binding.FixtureDigest, AuthDigest: binding.AuthDigest, AuthMode: binding.AuthMode, ProbeDigest: proof.ProbeDigest, Profile: store.QualificationGuarded, CreatedAt: created}, proof)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, q.ID)
		provider := &estimatedProvider{ScriptedProvider: testkit.NewScriptedProvider(binding.Identity), binding: binding}
		if err := registry.Register(ctx, provider); err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			primary = provider
		}
	}
	if _, _, err := db.SelectProviderSet(ctx, domain.ChannelDev, ids[0], ids[0], ids[1], time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	ref := domain.TicketRef{Channel: domain.ChannelDev, Project: "p", Ticket: "SF-estimated-retry"}
	if err := db.CreateTicket(ctx, store.Ticket{Ref: ref, SourceDigest: "source", Type: domain.TicketFeature, MergeMode: domain.MergeGuarded, CreatedAt: time.Now().UTC(), MaxDuration: time.Hour, MaxCostMicroUSD: 100}); err != nil {
		t.Fatal(err)
	}
	ticket, err := db.StartOrAdopt(ctx, ref, 1, "dev/p/SF-estimated-retry", domain.Fence{LeaderEpoch: leader, RunnerEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	fence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: ticket.RunnerEpoch}
	root, identity := t.TempDir(), `{"repository":"/tmp/p"}`
	branch, head := "dev/p/SF-estimated-retry", strings.Repeat("b", 40)
	base := strings.Repeat("a", 40)
	if physical {
		coordinator := worktreecoord.Coordinator{Store: db, Git: rejectionPhysicalGit(t, db)}
		registered, err := coordinator.Ensure(ctx, worktreecoord.EnsureRequest{Ref: ref, Version: ticket.Version, Fence: fence})
		if err != nil {
			t.Fatal("register physical retry checkout", err)
		}
		root, identity, branch, head, base = registered.Path, string(registered.IdentityJSON), registered.Branch, registered.HeadSHA, registered.BaseSHA
	} else if len(strictCheckpoint) != 0 && strictCheckpoint[0] {
		part := func(value string) string { sum := sha256.Sum256([]byte(value)); return hex.EncodeToString(sum[:8]) }
		branch = "sf/dev/" + part(string(ref.Project)) + "/" + part(string(ref.Ticket)) + "-" + strings.Repeat("b", 32)
		if _, err := db.LoadOrStoreBranchUnderFence(ctx, string(ref.Channel)+"\x00"+string(ref.Project)+"\x00"+string(ref.Ticket), branch, ticket.Version, fence); err != nil {
			t.Fatal(err)
		}
		root, err = db.TicketWorktreePath(ref)
		if err != nil {
			t.Fatal(err)
		}
		head = strings.Repeat("a", 40)
		// Structural Store fixture only. No physical filesystem verdict is
		// claimed by these synthetic device/inode values.
		value := gitboundary.Identity{Repository: "/tmp/p", RepositoryDev: 1, RepositoryIno: 2, Worktree: root, WorktreeDev: 3, WorktreeIno: 4, GitFile: "gitdir: " + root + "/.git", GitFileDev: 5, GitFileIno: 6, CommonDir: "/tmp/p/.git", CommonDirDev: 7, CommonDirIno: 8, Origin: "git@example.test:p.git", PushOrigin: "/tmp/p-origin", PushOriginDev: 9, PushOriginIno: 10, BaseRef: "main", BaseHead: head, HeadRef: branch, ConfigHash: strings.Repeat("b", 64), HooksHash: strings.Repeat("c", 64)}
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		identity = string(payload)
		intent := store.GitMutationIntent{EffectFence: store.EffectFence{Ref: ref, TicketVersion: ticket.Version, Fence: fence}, RequestDigest: "sha256:" + strings.Repeat("0", 64), Repository: "/tmp/p", Worktree: root, Branch: branch, Operation: "create-worktree", BaseRef: "main", ExpectedBaseOID: head, ExpectedHeadOID: head}
		intent.SemanticKey = store.CanonicalGitMutationSemanticKey(intent)
		if _, err := db.PlanEffect(ctx, store.EffectPlan{SemanticKey: intent.SemanticKey, Ref: ref, Kind: "git/create-worktree", TicketVersion: ticket.Version, Fence: fence, RequestDigest: intent.RequestDigest}); err != nil {
			t.Fatal(err)
		}
		claim, err := db.IssueGitMutationClaim(ctx, intent)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ConfirmEffect(ctx, store.EffectFence{SemanticKey: claim.SemanticKey, Ref: ref, TicketVersion: ticket.Version, Fence: domain.Fence{LeaderEpoch: fence.LeaderEpoch, RunnerEpoch: fence.RunnerEpoch, ClaimEpoch: claim.ClaimEpoch}}, identity); err != nil {
			t.Fatal(err)
		}
	}
	if !physical {
		if err := db.RegisterWorktree(ctx, store.WorktreeRegistration{Ref: ref, ExpectedVersion: ticket.Version, Fence: fence, Path: root, Branch: branch, IdentityJSON: []byte(identity), BaseSHA: base, HeadSHA: head}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.ApproveProviderEstimatedAccounting(ctx, ref, ticket.Version, fence); err != nil {
		t.Fatal(err)
	}
	c, err := New(registry, map[Role]Route{RolePlanner: {Primary: "claude", Capacity: 1}}, db, nil, process)
	if err != nil {
		t.Fatal(err)
	}
	r := Request{Role: RolePlanner, ExpectedProvider: "claude", ExpectedVersion: ticket.Version, Fence: fence, ConfigDigest: digest, Validation: phaseartifact.Validation{TicketType: domain.TicketFeature}, Input: contracts.PhaseInput{Ticket: ref, Phase: domain.PhasePlanning, Prompt: "plan", Repository: repository, Worktree: root, WorktreeIdentity: identity, BaseSHA: base, AllowedPaths: []string{"x"}, Timeout: time.Second, Profile: contracts.ProfileGuarded, Schema: []byte("{}")}}
	return db, r, c, primary, path
}

// Real Git/filesystem inspection, with an explicit command adapter. This does
// not claim native process-supervisor or credential qualification coverage.
func rejectionPhysicalGit(t *testing.T, db *store.Store) gitboundary.Runner {
	t.Helper()
	return gitboundary.Runner{Home: filepath.Join(t.TempDir(), "git-home"), TestLocalTransport: true, MutationAuthority: db,
		Run: func(ctx context.Context, _ string, args, env []string) ([]byte, error) {
			cmd := exec.CommandContext(ctx, "/usr/bin/git", args...)
			cmd.Env = env
			cmd.WaitDelay = time.Second
			return cmd.Output()
		}}
}

func TestEstimatedClaudeRepairAndUncertaintySurviveReopen(t *testing.T) {
	for _, scenario := range []string{"repair", "exhausted", "partial-error"} {
		t.Run(scenario, func(t *testing.T) {
			db, r, c, p, path := estimatedRetryFixture(t)
			p.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Artifact: []byte(`{"schema":"bad"}`)}, {Artifact: plannerArtifact()}}
			want, count := Completed, 2
			if scenario == "exhausted" {
				p.Steps[domain.PhasePlanning][1].Artifact = []byte(`{"schema":"bad"}`)
				want = AttemptExhausted
			}
			if scenario == "partial-error" {
				p.Steps[domain.PhasePlanning] = []testkit.ProviderStep{{Behavior: testkit.ProviderExitBefore, WriteFiles: map[string][]byte{"x": []byte("retained")}}}
				want, count = ResultIndeterminate, 1
			}
			result := c.Run(context.Background(), r)
			if result.Code != want || len(p.CallsSnapshot()) != count {
				t.Fatalf("code=%s calls=%d expected=%s/%d", result.Code, len(p.CallsSnapshot()), want, count)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := store.Open(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			restarted, err := New(c.registry, c.routes, reopened, nil, c.supervisor)
			if err != nil {
				t.Fatal(err)
			}
			replay := restarted.Run(context.Background(), r)
			if replay.Code != want || len(p.CallsSnapshot()) != count {
				t.Fatalf("replay=%s calls=%d", replay.Code, len(p.CallsSnapshot()))
			}
			attempts, err := reopened.ProviderAttempts(context.Background(), r.Input.Ticket)
			if err != nil || len(attempts) != count {
				t.Fatalf("attempt count=%d err=%v", len(attempts), err)
			}
			for _, attempt := range attempts {
				if attempt.Binding != p.binding || attempt.Role != "planner" || attempt.Phase != domain.PhasePlanning {
					t.Fatal("repair changed binding or role")
				}
				_, known, err := reopened.ProviderCostEstimate(context.Background(), attempt.ProviderAttemptClaim)
				if err != nil || known {
					t.Fatalf("unknown accounting not retained: known=%v err=%v", known, err)
				}
			}
			if scenario == "partial-error" {
				raw, err := os.ReadFile(filepath.Join(r.Input.Worktree, "x"))
				if err != nil || string(raw) != "retained" {
					t.Fatal("partial writes discarded")
				}
			}
		})
	}
}
