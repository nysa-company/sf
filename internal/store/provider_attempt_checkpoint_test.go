package store

import (
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
)

func TestProviderAttemptCheckpointForRequestAuthenticatesPersistedClaim(t *testing.T) {
	f := newProviderRetryWorktreeFixture(t, "checkpoint-request", 40)
	claim, err := f.db.BeginProviderAttempt(f.ctx, f.request(t, domain.PhasePlanning, "planner", runtime(f.builder)))
	if err != nil {
		t.Fatal(err)
	}
	request := drainRequestForClaim(claim)
	got, err := f.db.ProviderAttemptCheckpointForRequest(f.ctx, request)
	if err != nil || got.AttemptID != claim.ID || got.ExpectedHead != f.registeredHead {
		t.Fatal("exact supervisor request refused")
	}
	for name, mutate := range map[string]func(*contracts.DrainRequest){
		"model":    func(r *contracts.DrainRequest) { r.Identity.Model += "other" },
		"policy":   func(r *contracts.DrainRequest) { r.PolicyDigest += "other" },
		"auth":     func(r *contracts.DrainRequest) { r.AuthDigest += "other" },
		"request":  func(r *contracts.DrainRequest) { r.RequestDigest += "other" },
		"identity": func(r *contracts.DrainRequest) { r.WorktreeIdentity += " " },
		"lease":    func(r *contracts.DrainRequest) { r.LeaseKey += "other" },
		"fence":    func(r *contracts.DrainRequest) { r.RunnerEpoch++ },
	} {
		t.Run(name, func(t *testing.T) {
			copy := request
			mutate(&copy)
			if value, err := f.db.ProviderAttemptCheckpointForRequest(f.ctx, copy); err == nil || value.AttemptID != 0 {
				t.Fatal("altered request accepted")
			}
		})
	}
}

func TestProviderAttemptCheckpointDerivesPhaseHead(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseVerification, domain.PhaseBuild, domain.PhaseReview} {
		t.Run(string(phase), func(t *testing.T) {
			f := newProviderRetryWorktreeFixture(t, "checkpoint-head-"+string(phase), 40)
			role, binding, head := "reviewer", runtime(f.reviewer), f.registeredHead
			switch phase {
			case domain.PhaseVerification:
				f.enterVerification(t)
			case domain.PhaseBuild:
				f.enterBuilding(t)
				role, binding, head = "builder", runtime(f.builder), f.verificationHead
			case domain.PhaseReview:
				f.enterReviewing(t)
				head = f.candidateHead
			}
			claim, err := f.db.BeginProviderAttempt(f.ctx, f.request(t, phase, role, binding))
			if err != nil {
				t.Fatal(err)
			}
			got, err := f.db.ProviderAttemptCheckpoint(f.ctx, claim)
			if err != nil || got.ExpectedHead != head || got.Phase != phase || got.AttemptID != claim.ID {
				t.Fatal("phase checkpoint was not derived from authenticated evidence")
			}
		})
	}
}

func TestProviderAttemptCheckpointRequiresExactActiveClaim(t *testing.T) {
	f := newProviderRetryWorktreeFixture(t, "checkpoint-active", 40)
	claim, err := f.db.BeginProviderAttempt(f.ctx, f.request(t, domain.PhasePlanning, "planner", runtime(f.builder)))
	if err != nil {
		t.Fatal(err)
	}
	value, err := f.db.ProviderAttemptCheckpoint(f.ctx, claim)
	if err != nil || value.AttemptID != claim.ID || value.ExpectedHead != f.registeredHead || value.Worktree.Path != claim.Worktree || value.Version != claim.ExpectedVersion {
		t.Fatal("exact active checkpoint was not authenticated")
	}
	for name, mutate := range map[string]func(*ProviderAttemptClaim){
		"attempt": func(c *ProviderAttemptClaim) { c.Attempt++ },
		"head":    func(c *ProviderAttemptClaim) { c.BaseSHA = f.candidateHead },
		"lease":   func(c *ProviderAttemptClaim) { c.LeaseKey += "other" },
		"input":   func(c *ProviderAttemptClaim) { c.Input.Worktree += "/other" },
		"model":   func(c *ProviderAttemptClaim) { c.Binding.Identity.Model += "other" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := claim
			mutate(&copy)
			if got, err := f.db.ProviderAttemptCheckpoint(f.ctx, copy); err == nil || got.AttemptID != 0 {
				t.Fatal("altered claim accepted")
			}
		})
	}
	if err := f.db.FinishProviderAttempt(f.ctx, claim, proof(t, claim), f.ticket.Version, f.fence, "failed", "invalid_artifact", 1, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if got, err := f.db.ProviderAttemptCheckpoint(f.ctx, claim); err == nil || got.AttemptID != 0 {
		t.Fatal("terminal claim accepted")
	}
}

func TestProviderAttemptCheckpointRejectsLostAuthority(t *testing.T) {
	for _, mode := range []string{"leader", "lease", "phase", "registration"} {
		t.Run(mode, func(t *testing.T) {
			f := newProviderRetryWorktreeFixture(t, "checkpoint-"+mode, 40)
			claim, err := f.db.BeginProviderAttempt(f.ctx, f.request(t, domain.PhasePlanning, "planner", runtime(f.builder)))
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "leader":
				_, err = f.db.AcquireLeader(f.ctx, domain.ChannelDev, "checkpoint-new-leader")
			case "lease":
				_, err = f.db.db.ExecContext(f.ctx, `DELETE FROM leases WHERE scope='provider' AND scope_key=?`, claim.LeaseKey)
			case "phase":
				_, err = f.db.db.ExecContext(f.ctx, `UPDATE phase_runs SET state='failed',outcome='invalid_artifact' WHERE channel=? AND project_id=? AND ticket_id=?`, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket)
			case "registration":
				_, err = f.db.db.ExecContext(f.ctx, `UPDATE worktrees SET head_sha=? WHERE channel=? AND project_id=? AND ticket_id=?`, f.candidateHead, claim.Ref.Channel, claim.Ref.Project, claim.Ref.Ticket)
			}
			if err != nil {
				t.Fatal(err)
			}
			if got, err := f.db.ProviderAttemptCheckpoint(f.ctx, claim); err == nil || got.AttemptID != 0 {
				t.Fatal("lost authority accepted")
			}
		})
	}
}
