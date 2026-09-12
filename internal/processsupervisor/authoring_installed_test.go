package processsupervisor_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/authoring"
	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/config"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/processsupervisor"
	"github.com/nysa-company/sf/internal/redact"
	"github.com/nysa-company/sf/internal/store"
)

// TestInstalledClaudeAuthoringPurposes is skipped unless explicitly enabled.
// SF_TEST_CLAUDE_AUTHORING=1 authorizes exactly two SF authoring turns on a
// successful run (one draft, one home intent); each may make bounded internal
// provider calls. These are NOT single API requests and cost is unknown.
// The first failed turn stops the test: no automatic retry or substitution.
// It uses installed OAuth credentials and the actual pinned runtime, native
// sandbox, credential staging, CI-built sf gate and durable Store launch path.
// SF_TEST_AUTHORING_GATE and SF_TEST_AUTHORING_GATE_SHA256 must identify the
// trusted CI artifact for the same reviewed commit as this test binary. A
// caller-supplied checksum does not itself attest artifact provenance. This
// harness never builds or downloads a gate and has no compilation fallback.
func TestInstalledClaudeAuthoringPurposes(t *testing.T) {
	if os.Getenv("SF_TEST_CLAUDE_AUTHORING") != "1" {
		t.Skip("real Claude authoring requires explicit SF_TEST_CLAUDE_AUTHORING=1; cost unknown")
	}
	if runtime.GOOS != "darwin" {
		t.Fatal("opted-in Claude authoring requires macOS")
	}
	const model = "claude-sonnet-4-6"
	if _, supported := claudeprovider.ModelFamily(model); !supported {
		t.Fatal("the fixed acceptance model is not supported by this adapter")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	root := t.TempDir()
	gate := installedAuthoringGate(t, root)
	db, err := store.Open(ctx, filepath.Join(root, "authoring.sqlite"))
	if err != nil {
		t.Fatal("could not open isolated authoring Store")
	}
	t.Cleanup(func() {
		if db.Close() != nil {
			t.Error("isolated Store close failed")
		}
	})
	supervisor, err := processsupervisor.New(nil)
	if err != nil {
		t.Fatal("could not construct real process supervisor")
	}
	// This is only the compiled sf entrypoint location. There are no test
	// overrides for credentials, runtime registration, argv or sandbox policy.
	supervisor.Executable = gate
	t.Cleanup(func() {
		if supervisor.Close() != nil {
			t.Error("authoring process drain or staged-runtime retirement failed")
		}
	})
	projectPath := filepath.Join(root, "project")
	if os.Mkdir(projectPath, 0700) != nil {
		t.Fatal("could not create isolated project root")
	}
	effective, err := config.Resolve(config.DefaultMachineLimits(), config.DefaultProject("authoring-acceptance", projectPath), config.TicketOverride{})
	if err != nil {
		t.Fatal("could not construct isolated project configuration")
	}
	snapshot, configDigest, err := config.Snapshot(effective)
	if err != nil {
		t.Fatal("could not snapshot isolated project configuration")
	}
	project := store.Project{Channel: domain.ChannelDev, ID: "authoring-acceptance", Path: projectPath, BaseRef: "main", ConfigGeneration: 1, ConfigDigest: configDigest, ConfigSnapshot: snapshot}
	if db.CreateProject(ctx, project) != nil {
		t.Fatal("could not register isolated project")
	}
	epoch, err := db.AcquireLeader(ctx, domain.ChannelDev, "installed-authoring-acceptance")
	if err != nil || db.SetRecoveryAuthority(ctx, domain.ChannelDev, epoch, supervisor.PublicKey()) != nil {
		t.Fatal("could not install isolated authoring recovery authority")
	}
	capability, err := supervisor.PrepareAuthoring(ctx, model)
	if err != nil || capability.Identity.Provider != "claude" || capability.Identity.Model != model {
		t.Fatal("installed Claude does not provide the required pinned authoring capability")
	}
	t.Logf("installed authoring capability: model=%s version=%s binary_digest=%s policy_digest=%s", capability.Identity.Model, capability.Identity.Version, capability.BinaryDigest, capability.PolicyDigest)
	for _, test := range []struct{ purpose, prompt string }{
		{"ticket_draft", "Return a draft, not a question: add a pure function that counts completed items in a supplied in-memory list. Empty lists return zero. No filesystem, network, or UI change is needed. Include one small scope item and observable acceptance criteria; do not use tools."},
		{"home_intent", "List tickets in the current project. Return the list action with an empty selector; do not use tools."},
	} {
		session := store.AuthoringSession{Channel: domain.ChannelDev, ID: "installed-" + test.purpose, Purpose: test.purpose, Project: project.ID, Capability: capability, ContextDigest: contracts.AuthoringDigest(nil)}
		if db.CreateAuthoringSession(ctx, session) != nil {
			t.Fatal("no-call session creation failed")
		}
		input := contracts.AuthoringInput{Purpose: test.purpose, Prompt: test.prompt}
		if _, err := authoring.EncodeRequest(input); err != nil {
			t.Fatal("fixed acceptance input exceeds operation bounds")
		}
		turn, created, err := db.ReserveAuthoringTurn(ctx, session, "only-turn", contracts.AuthoringInputDigest(input), epoch)
		if err != nil || !created {
			t.Fatal("durable turn reservation failed")
		}
		replay, created, err := db.ReserveAuthoringTurn(ctx, session, "only-turn", contracts.AuthoringInputDigest(input), epoch)
		if err != nil || created || replay.Claim != turn.Claim {
			t.Fatal("reserved idempotency key was not stable")
		}
		turnCtx, stopTurn := context.WithTimeout(ctx, contracts.AuthoringTurnTimeout)
		launches := 0
		result, proof, runErr := supervisor.RunAuthoring(turnCtx, turn.Claim, input, func(recordCtx context.Context, launch contracts.ProviderLaunch) error {
			launches++
			return db.RecordAuthoringLaunch(recordCtx, turn.Claim, launch)
		})
		stopTurn()
		// Finalization is independent of timeout/cancellation of inference, but
		// bounded. Unclear drain retains the channel slot and stops this run.
		finishCtx, stopFinish := context.WithTimeout(context.Background(), 10*time.Second)
		if runErr != nil || !contracts.VerifyAuthoringProof(supervisor.PublicKey(), turn.Claim, epoch, proof) {
			if db.FinishAuthoringTurn(finishCtx, turn.Claim, proof, "failed", nil) != nil {
				quarantineCtx, stopQuarantine := context.WithTimeout(context.Background(), 5*time.Second)
				_ = db.MarkAuthoringUncertain(quarantineCtx, turn.Claim)
				stopQuarantine()
			}
			stopFinish()
			t.Fatal("real authoring turn failed or did not prove process drain; no retry attempted")
		}
		storedLaunch, err := db.AuthoringTurn(finishCtx, domain.ChannelDev, session.ID, "only-turn")
		if err != nil || launches != 1 || storedLaunch.State != "launched" || storedLaunch.Launch.PID <= 0 || storedLaunch.Launch.PID != storedLaunch.Launch.PGID || storedLaunch.Launch.BootIdentity == "" || storedLaunch.Launch.ProcessStartIdentity == "" {
			stopFinish()
			t.Fatal("actual provider launch was not durably authenticated")
		}
		clean, err := authoring.SanitizePurpose(test.purpose, result, redact.NewPolicy("", nil))
		if err != nil || test.purpose == "ticket_draft" && clean.Kind != "draft" || test.purpose == "home_intent" && (clean.Intent == nil || clean.Intent.Action != "list" || clean.Intent.Selector != "") {
			_ = db.FinishAuthoringTurn(finishCtx, turn.Claim, proof, "failed", nil)
			stopFinish()
			t.Fatal("real provider did not return the fixed purpose-specific typed result")
		}
		if test.purpose == "ticket_draft" {
			if _, err := authoring.Markdown(clean); err != nil {
				_ = db.FinishAuthoringTurn(finishCtx, turn.Claim, proof, "failed", nil)
				stopFinish()
				t.Fatal("real provider draft cannot render as a valid ticket")
			}
		}
		if db.FinishAuthoringTurn(finishCtx, turn.Claim, proof, "success", &clean) != nil {
			stopFinish()
			t.Fatal("typed result and drain proof did not commit")
		}
		completed, created, err := db.ReserveAuthoringTurn(finishCtx, session, "only-turn", contracts.AuthoringInputDigest(input), epoch)
		pending, pendingErr := db.PendingAuthoringTurns(finishCtx, domain.ChannelDev)
		stopFinish()
		if err != nil || created || completed.State != "completed" || completed.Outcome != "success" || completed.Result == nil || pendingErr != nil || len(pending) != 0 {
			t.Fatal("completed turn replay or drained channel accounting is inconsistent")
		}
		t.Logf("installed authoring purpose=%s outcome=completed signed_drain=true launches=1", test.purpose)
	}
	inspectCtx, stopInspect := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopInspect()
	tickets, err := db.Tickets(inspectCtx, domain.ChannelDev, project.ID, 1)
	if err != nil || len(tickets) != 0 {
		t.Fatal("authoring acceptance unexpectedly mutated ticket inventory")
	}
}

func installedAuthoringGate(t *testing.T, root string) string {
	t.Helper()
	path, expected := os.Getenv("SF_TEST_AUTHORING_GATE"), os.Getenv("SF_TEST_AUTHORING_GATE_SHA256")
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(expected) != 64 || strings.ToLower(expected) != expected {
		t.Fatal("a canonical trusted CI gate path and lowercase SHA256 are required")
	}
	if _, err := hex.DecodeString(expected); err != nil {
		t.Fatal("invalid trusted CI gate digest")
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		t.Fatal("CI gate path must not contain symlinks")
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm()&0111 == 0 || before.Size() <= 0 || before.Size() > 256<<20 {
		t.Fatal("CI gate is not a bounded regular executable")
	}
	source, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal("could not open trusted CI gate")
	}
	defer source.Close()
	opened, err := source.Stat()
	if err != nil || !os.SameFile(before, opened) {
		t.Fatal("CI gate identity changed during open")
	}
	// Freeze the approved bytes in private temporary storage before execution;
	// a replacement at the supplied artifact path cannot change this test run.
	pinned := filepath.Join(root, "sf-authoring-acceptance")
	target, err := os.OpenFile(pinned, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0700)
	if err != nil {
		t.Fatal("could not pin trusted CI gate")
	}
	digest := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(target, digest), io.LimitReader(source, (256<<20)+1))
	closeErr := target.Close()
	if copyErr != nil || closeErr != nil || size != before.Size() || hex.EncodeToString(digest.Sum(nil)) != expected {
		t.Fatal("trusted CI gate bytes did not match the approved digest")
	}
	return pinned
}
