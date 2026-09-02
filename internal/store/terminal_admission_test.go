package store

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestTerminalTicketCannotMintWritersAfterCapacityIsReassigned(t *testing.T) {
	db, ctx := openTestStore(t)
	gitIntent := unplannedGitIntentFixture(t, db, ctx, "SF-terminal-admission")
	leader := gitIntent.Fence.LeaderEpoch

	capacity := []LeaseRequest{{Scope: "global", Resource: "machine", Capacity: 1}}
	if _, err := db.AcquireLeases(ctx, gitIntent.Ref, gitIntent.TicketVersion, gitIntent.Fence, capacity, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	second := createLeaseTicket(t, db, 90)
	secondQueued, err := db.Ticket(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.StartWithOwnership(ctx, second, secondQueued.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: secondQueued.RunnerEpoch}, "dev/nysa/SF-lease-90/planning", capacity, time.Now().UTC()); !errors.Is(err, ErrLeaseCapacity) {
		t.Fatalf("capacity was not held before terminal transition: %v", err)
	}

	genericPlan := EffectPlan{
		SemanticKey:   "terminal-admission/effect",
		Ref:           gitIntent.Ref,
		Kind:          "branch_push",
		TicketVersion: gitIntent.TicketVersion,
		Fence:         gitIntent.Fence,
		RequestDigest: "terminal-effect-request",
	}
	if _, err := db.PlanEffect(ctx, genericPlan); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{
		SemanticKey: gitIntent.SemanticKey, Ref: gitIntent.Ref, Kind: "git/" + gitIntent.Operation,
		TicketVersion: gitIntent.TicketVersion, Fence: gitIntent.Fence, RequestDigest: gitIntent.RequestDigest,
	}); err != nil {
		t.Fatal(err)
	}

	repositoryIntent := RepositoryCommandIntent{
		EffectFence: EffectFence{
			SemanticKey:   "repository-command/terminal-admission",
			Ref:           gitIntent.Ref,
			TicketVersion: gitIntent.TicketVersion,
			Fence:         gitIntent.Fence,
		},
		RequestDigest:    repositoryCommandDigest("4"),
		Repository:       gitIntent.Repository,
		Worktree:         gitIntent.Worktree,
		WorktreeIdentity: repositoryCommandIdentity(t, gitIntent.Repository, gitIntent.Worktree, gitIntent.Branch, gitIntent.BaseRef),
		Branch:           gitIntent.Branch,
		BaseRef:          gitIntent.BaseRef,
		BaseSHA:          gitIntent.ExpectedBaseOID,
		CommandDigest:    repositoryCommandDigest("5"),
		SpecDigest:       repositoryCommandDigest("6"),
		PolicyDigest:     repositoryCommandDigest("7"),
		ExecutablePath:   "/usr/bin/true",
		ExecutableDigest: repositoryCommandDigest("8"),
	}
	if _, err := db.PlanEffect(ctx, EffectPlan{
		SemanticKey: repositoryIntent.SemanticKey, Ref: repositoryIntent.Ref, Kind: "repository_command",
		TicketVersion: repositoryIntent.TicketVersion, Fence: repositoryIntent.Fence, RequestDigest: repositoryIntent.RequestDigest,
	}); err != nil {
		t.Fatal(err)
	}

	terminalResult, err := db.Transition(ctx, Transition{
		Ref: gitIntent.Ref, ExpectedVersion: gitIntent.TicketVersion, From: domain.StatePlanning, To: domain.StateDone,
		Trigger: "test_terminal", Fence: gitIntent.Fence, EventPayload: `{}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := db.Ticket(ctx, gitIntent.Ref)
	if err != nil || terminal.State != domain.StateDone || terminal.Version != terminalResult.Version {
		t.Fatalf("terminal ticket=%+v transition=%+v err=%v", terminal, terminalResult, err)
	}
	terminalFence := domain.Fence{LeaderEpoch: leader, RunnerEpoch: terminal.RunnerEpoch}

	secondStarted, _, err := db.StartWithOwnership(ctx, second, secondQueued.Version, domain.Fence{LeaderEpoch: leader, RunnerEpoch: secondQueued.RunnerEpoch}, "dev/nysa/SF-lease-90/planning", capacity, time.Now().UTC())
	if err != nil {
		t.Fatalf("released capacity was not reassigned: %v", err)
	}

	// A current terminal tuple is still observable, but it carries no authority
	// to create or rebind an operation that could cross an external boundary.
	if err := db.ValidateTicketFence(ctx, terminal.Ref, terminal.Version, terminalFence); err != nil {
		t.Fatalf("current terminal observation fence=%v", err)
	}
	if version, fence, err := db.CurrentTicketFence(ctx, terminal.Ref); err != nil || version != terminal.Version || fence != terminalFence {
		t.Fatalf("current terminal tuple version=%d fence=%+v err=%v", version, fence, err)
	}

	genericPlan.TicketVersion, genericPlan.Fence = terminal.Version, terminalFence
	if _, err := db.PlanEffect(ctx, genericPlan); !errors.Is(err, ErrStaleFence) {
		t.Errorf("postterminal PlanEffect error=%v", err)
	}
	if _, err := db.ClaimEffect(ctx, EffectFence{SemanticKey: genericPlan.SemanticKey, Ref: terminal.Ref, TicketVersion: terminal.Version, Fence: terminalFence}); !errors.Is(err, ErrStaleFence) {
		t.Errorf("postterminal ClaimEffect error=%v", err)
	}

	gitIntent.TicketVersion, gitIntent.Fence = terminal.Version, terminalFence
	if _, err := db.PlanEffect(ctx, EffectPlan{
		SemanticKey: gitIntent.SemanticKey, Ref: terminal.Ref, Kind: "git/" + gitIntent.Operation,
		TicketVersion: terminal.Version, Fence: terminalFence, RequestDigest: gitIntent.RequestDigest,
	}); !errors.Is(err, ErrStaleFence) {
		t.Errorf("postterminal Git effect rebind error=%v", err)
	}
	if _, err := db.IssueGitMutationClaim(ctx, gitIntent); !errors.Is(err, ErrStaleFence) {
		t.Errorf("postterminal Git claim error=%v", err)
	}

	repositoryIntent.TicketVersion, repositoryIntent.Fence = terminal.Version, terminalFence
	if _, err := db.PlanEffect(ctx, EffectPlan{
		SemanticKey: repositoryIntent.SemanticKey, Ref: terminal.Ref, Kind: "repository_command",
		TicketVersion: terminal.Version, Fence: terminalFence, RequestDigest: repositoryIntent.RequestDigest,
	}); !errors.Is(err, ErrStaleFence) {
		t.Errorf("postterminal repository effect rebind error=%v", err)
	}
	if _, err := db.IssueRepositoryCommandClaim(ctx, repositoryIntent); !errors.Is(err, ErrStaleFence) {
		t.Errorf("postterminal repository command claim error=%v", err)
	}

	builder, _ := setupProviderPair(t, db, ctx)
	binding := runtime(builder)
	providerRequest := ProviderAttemptRequest{
		Ref: terminal.Ref, ExpectedVersion: terminal.Version, Fence: terminalFence,
		Phase: domain.PhasePlanning, Role: "planner", Binding: binding,
		ConfigDigest: terminal.ConfigDigest, Capacity: 1, At: time.Now().UTC(),
		Repository: gitIntent.Repository, Worktree: gitIntent.Worktree,
		WorktreeIdentity: repositoryIntent.WorktreeIdentity, BaseSHA: gitIntent.ExpectedBaseOID,
		SupervisorKey: providerTestSigner.PublicKey(),
	}
	providerRequest.Input = contracts.PhaseInput{
		Ticket: terminal.Ref, Phase: domain.PhasePlanning,
		LeaderEpoch: leader, RunnerEpoch: terminal.RunnerEpoch, ExpectedVersion: terminal.Version,
		Prompt: "terminal admission must fail", Repository: providerRequest.Repository,
		Worktree: providerRequest.Worktree, WorktreeIdentity: providerRequest.WorktreeIdentity,
		BaseSHA: providerRequest.BaseSHA, AllowedPaths: []string{"."}, Provider: binding.Identity,
		AuthMode: binding.AuthMode, Timeout: time.Minute, Profile: contracts.ProfileGuarded,
		Schema: []byte(`{"type":"object"}`),
	}
	if _, err := db.BeginProviderAttempt(ctx, providerRequest); !errors.Is(err, ErrStaleFence) {
		t.Errorf("postterminal provider claim error=%v", err)
	}

	if effects, err := db.ReconcileEffects(ctx, domain.ChannelDev, leader); err != nil || len(effects) != 0 {
		t.Fatalf("terminal reconciliation effects=%+v err=%v", effects, err)
	}
	if effect, err := db.Effect(ctx, genericPlan.SemanticKey); err != nil || effect.State != EffectPlanned || effect.TicketVersion != gitIntent.TicketVersion-1 {
		t.Fatalf("historical planned effect mutated=%+v err=%v", effect, err)
	}

	for _, table := range []string{"git_mutation_intents", "repository_command_intents", "provider_attempts"} {
		var count int
		if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE channel=? AND project_id=? AND ticket_id=?`, terminal.Ref.Channel, terminal.Ref.Project, terminal.Ref.Ticket).Scan(&count); err != nil || count != 0 {
			t.Fatalf("postterminal writer persisted table=%s count=%d err=%v", table, count, err)
		}
	}
	leasing, err := db.Leases(ctx, domain.ChannelDev)
	if err != nil || len(leasing) != 1 || leasing[0].Ref != secondStarted.Ref {
		t.Fatalf("capacity overlap leases=%+v err=%v", leasing, err)
	}
	if strings.TrimSpace(secondStarted.WorkflowID) == "" {
		t.Fatal("second ticket was not admitted with durable ownership")
	}
}
