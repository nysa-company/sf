package processsupervisor

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/nysa-company/sf/internal/authoring"
	"github.com/nysa-company/sf/internal/claudeprovider"
	"github.com/nysa-company/sf/internal/cliruntime"
	"github.com/nysa-company/sf/internal/contracts"
)

func authoringPolicyDigest() string {
	return contracts.AuthoringDigest([]byte("sf.authoring.claude/v1:2.1.263:private-cwd:empty-tools:empty-mcp:bare:restricted:safe-mode:json-schema:no-persistence:max-turns3:structured-retries1:90s:64KiB:private-filesystem\x00" + authoring.Schema + "\x00" + authoring.Instruction + "\x00" + authoring.HomeSchema + "\x00" + authoring.HomeInstruction))
}

// PrepareAuthoring observes version/help/OAuth only. It does not qualify an
// execution role, create a ticket, or require an independent provider pair.
func (s *Supervisor) PrepareAuthoring(ctx context.Context, model string) (contracts.AuthoringCapability, error) {
	if s == nil || runtime.GOOS != "darwin" {
		return contracts.AuthoringCapability{}, errors.New("authoring requires the supported macOS Claude runtime")
	}
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	executable, err := exec.LookPath("claude")
	if err != nil {
		return contracts.AuthoringCapability{}, errCLIObservation
	}
	binding, err := s.observeClaudeOperation(ctx, executable, model, lookupCLISecret, true)
	if err != nil {
		return contracts.AuthoringCapability{}, err
	}
	bundle, err := cliruntime.Resolve(ctx, "claude", executable)
	if err != nil || bundle.Digest() != binding.BinaryDigest {
		return contracts.AuthoringCapability{}, errCLIObservation
	}
	trusted := trustedExecutable{path: bundle.Executable(), digest: bundle.Digest(), cliBundle: &bundle, authDigest: binding.AuthDigest, authMode: binding.AuthMode, policyDigest: authoringPolicyDigest()}
	if err := trusted.stage(); err != nil {
		return contracts.AuthoringCapability{}, errCLIObservation
	}
	s.mu.Lock()
	if s.closing || s.closed {
		s.mu.Unlock()
		os.RemoveAll(trusted.stagedDir)
		return contracts.AuthoringCapability{}, ErrUnclear
	}
	if s.authoringStages == nil {
		s.authoringStages = map[string]trustedExecutable{}
	}
	old := s.authoringStages[model]
	s.authoringStages[model] = trusted
	s.retireLocked(old)
	s.mu.Unlock()
	_ = s.reclaimRetired()
	return contracts.AuthoringCapability{Identity: binding.Identity, BinaryDigest: binding.BinaryDigest, AuthDigest: binding.AuthDigest, PolicyDigest: authoringPolicyDigest()}, nil
}

func (s *Supervisor) acquireAuthoring(claim contracts.AuthoringClaim) (trustedExecutable, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	trusted, ok := s.authoringStages[claim.Identity.Model]
	family, supported := claudeprovider.ModelFamily(claim.Identity.Model)
	if !supported || claim.Identity.Provider != "claude" || claim.Identity.Version != "2.1.263" || claim.Identity.Family != family {
		return trustedExecutable{}, nil, ErrUnclear
	}
	if !ok || s.closing || s.closed || trusted.digest != claim.BinaryDigest || trusted.authDigest != claim.AuthDigest || trusted.policyDigest != claim.PolicyDigest || claim.PolicyDigest != authoringPolicyDigest() {
		return trustedExecutable{}, nil, ErrUnclear
	}
	trusted.snapshot.refs++
	return trusted, func() { s.releaseSnapshot(trusted.snapshot) }, nil
}
