package processsupervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/nysa-company/sf/internal/authoring"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/providerjson"
)

func authoringArgv(model string) []string {
	argv, _ := authoringPurposeArgv(model, "ticket_draft")
	return argv
}
func authoringPurposeArgv(model, purpose string) ([]string, error) {
	schema, _, err := authoring.SchemaForPurpose(purpose)
	if err != nil {
		return nil, err
	}
	return []string{"--print", "--output-format", "json", "--json-schema", schema, "--model", model, "--bare", "--restricted", "--safe-mode", "--no-session-persistence", "--permission-mode", "dontAsk", "--tools", "", "--allowedTools", "", "--disallowedTools", "mcp__*", "--strict-mcp-config", "--mcp-config", `{"mcpServers":{}}`, "--max-turns", "3"}, nil
}

func authoringStdin(input contracts.AuthoringInput) ([]byte, error) {
	return authoring.EncodeRequest(input)
}

func (s *Supervisor) RunAuthoring(ctx context.Context, claim contracts.AuthoringClaim, input contracts.AuthoringInput, record func(context.Context, contracts.ProviderLaunch) error) (result contracts.AuthoringResult, proof contracts.AuthoringProof, failure error) {
	if s == nil || !contracts.ValidAuthoringClaim(claim) || contracts.AuthoringInputDigest(input) != claim.RequestDigest || contracts.AuthoringDigest([]byte(input.Context)) != claim.ContextDigest || record == nil {
		return result, proof, ErrUnclear
	}
	if input.Purpose != claim.Purpose {
		return result, proof, ErrUnclear
	}
	noLaunch := func(err error) (contracts.AuthoringResult, contracts.AuthoringProof, error) {
		p, _ := s.Signer.ProveAuthoringDrained(claim, claim.LeaderEpoch)
		return contracts.AuthoringResult{}, p, err
	}
	stdin, err := authoringStdin(input)
	if err != nil {
		return noLaunch(err)
	}
	trusted, release, err := s.acquireAuthoring(claim)
	if err != nil {
		return noLaunch(err)
	}
	releaseSafe := true
	defer func() {
		if releaseSafe {
			release()
		}
	}()
	if !stagedRuntimeMatches(trusted.snapshot, trusted.digest) {
		return noLaunch(ErrUnclear)
	}
	ctx, cancel := context.WithTimeout(ctx, contracts.AuthoringTurnTimeout)
	defer cancel()
	environment := s.authoringEnvironment
	if environment == nil {
		environment = vettedCLIEnvironment
	}
	env, tmp, cleanup, err := environment(ctx, "claude", claim.AuthDigest, lookupCLISecret)
	if err != nil {
		return noLaunch(err)
	}
	cleanSafe := true
	defer func() {
		if cleanSafe {
			cleanup()
		}
	}()
	home := ""
	for _, value := range env {
		if strings.HasPrefix(value, "HOME=") {
			home = strings.TrimPrefix(value, "HOME=")
		}
	}
	command := s.authoringCommand
	if command == nil {
		command = authoringSandboxCommand
	}
	prefix, err := command(trusted, home, tmp)
	if err != nil {
		return noLaunch(err)
	}
	self := s.Executable
	if self == "" {
		self, err = os.Executable()
		if err != nil {
			return noLaunch(err)
		}
	}
	read, write, err := os.Pipe()
	if err != nil {
		return noLaunch(err)
	}
	defer read.Close()
	defer write.Close()
	providerArgs, err := authoringPurposeArgv(claim.Identity.Model, claim.Purpose)
	if err != nil {
		return noLaunch(err)
	}
	args := append(append([]string{"__provider_gate"}, prefix...), providerArgs...)
	cmd := exec.Command(self, args...)
	cmd.Dir = tmp
	cmd.Env = append(env, "MAX_STRUCTURED_OUTPUT_RETRIES=1")
	cmd.Stdin = bytes.NewReader(stdin)
	cmd.ExtraFiles = []*os.File{read}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr limitedBuffer
	stdout.limit, stderr.limit = 64<<10, 16<<10
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	key := claim.Session + "/" + claim.TurnKey
	s.mu.Lock()
	if s.closing || s.closed || len(s.authoringRuns) != 0 || ctx.Err() != nil {
		s.mu.Unlock()
		return noLaunch(ErrUnclear)
	}
	if err := cmd.Start(); err != nil {
		s.mu.Unlock()
		return noLaunch(err)
	}
	r := &run{identity: Identity{PID: cmd.Process.Pid, PGID: cmd.Process.Pid}, worktree: tmp, done: make(chan struct{}), streams: make(chan struct{}), finished: make(chan struct{})}
	if s.authoringRuns == nil {
		s.authoringRuns = map[string]*run{}
	}
	s.authoringRuns[key] = r
	identity := s.authoringIdentity
	if identity == nil {
		identity = processStartIdentity
	}
	start, startErr := identity(cmd.Process.Pid)
	boot, bootErr := hostBootIdentity()
	r.identity.BootIdentity, r.identity.ProcessStartIdentity = boot, start
	s.mu.Unlock()
	defer close(r.finished)
	defer func() {
		if !releaseSafe {
			go func() {
				<-r.streams
				safe := false
				if startErr != nil || bootErr != nil {
					safe = syscall.Kill(-r.identity.PGID, 0) == syscall.ESRCH
				} else {
					safe = s.proveGone(r) == nil
				}
				if safe {
					cleanup()
					release()
					s.mu.Lock()
					if s.authoringRuns[key] == r {
						delete(s.authoringRuns, key)
					}
					s.mu.Unlock()
				}
			}()
		}
	}()
	read.Close()
	wait := make(chan error, 1)
	go func() { wait <- waitProcess(cmd, r) }()
	if startErr != nil || bootErr != nil {
		// The gate has never been released. Own the provisional process before
		// attempting a bounded stop; never wait while holding the registry lock.
		write.Close()
		_ = signalGroup(cmd.Process.Pid, syscall.SIGKILL)
		select {
		case <-wait:
			if syscall.Kill(-cmd.Process.Pid, 0) == syscall.ESRCH {
				s.mu.Lock()
				delete(s.authoringRuns, key)
				s.mu.Unlock()
				return noLaunch(ErrUnclear)
			}
		case <-time.After(maxDrainDuration):
		}
		cleanSafe, releaseSafe = false, false
		// Only an unreleased sf gate can exist here. Late stream completion
		// plus absent group permits resource retirement, never turn replay.
		return result, proof, ErrUnclear
	}
	launch := contracts.ProviderLaunch{PID: r.identity.PID, PGID: r.identity.PGID, BootIdentity: boot, ProcessStartIdentity: start, Worktree: tmp}
	launchErr := record(ctx, launch)
	if launchErr == nil {
		launchErr = ctx.Err()
	}
	if launchErr == nil {
		_, launchErr = write.Write([]byte{1})
	}
	if closeErr := write.Close(); launchErr == nil {
		launchErr = closeErr
	}
	if launchErr != nil {
		_ = signalGroup(r.identity.PGID, syscall.SIGKILL)
	}
	select {
	case err = <-wait:
	case <-ctx.Done():
		err = ctx.Err()
		if e := s.terminate(r); e != nil {
			cleanSafe, releaseSafe = false, false
			return result, proof, e
		}
		select {
		case <-wait:
		case <-time.After(maxDrainDuration):
			cleanSafe, releaseSafe = false, false
			return result, proof, ErrUnclear
		}
	}
	if e := s.proveGone(r); e != nil {
		cleanSafe, releaseSafe = false, false
		return result, proof, e
	}
	proof, proofErr := s.Signer.ProveAuthoringDrained(claim, claim.LeaderEpoch)
	if proofErr != nil {
		cleanSafe, releaseSafe = false, false
		return result, proof, proofErr
	}
	s.mu.Lock()
	delete(s.authoringRuns, key)
	s.mu.Unlock()
	if launchErr != nil || err != nil || stdout.truncated || stderr.truncated {
		return result, proof, errors.New("authoring stopped or exceeded its output bound")
	}
	fields, err := providerjson.Object(stdout.Bytes())
	if err != nil {
		return result, proof, authoring.ErrContent
	}
	var kind, subtype string
	var isError *bool
	if json.Unmarshal(fields["type"], &kind) != nil || json.Unmarshal(fields["subtype"], &subtype) != nil || json.Unmarshal(fields["is_error"], &isError) != nil || kind != "result" || subtype != "success" || isError == nil || *isError {
		return result, proof, authoring.ErrContent
	}
	result, err = authoring.ParsePurpose(claim.Purpose, fields["structured_output"])
	return result, proof, err
}

func authoringSandboxCommand(trusted trustedExecutable, home, tmp string) ([]string, error) {
	profile, err := authoringSandbox(trusted.stagedDir, trusted.stagedPath, home, tmp)
	if err != nil {
		return nil, err
	}
	return []string{repositorySandboxExec, repositorySandboxExec, "-p", profile, trusted.stagedPath}, nil
}

func (s *Supervisor) RecoverAuthoring(ctx context.Context, claim contracts.AuthoringClaim, launch contracts.ProviderLaunch, epoch uint64) (contracts.AuthoringProof, error) {
	if s == nil || epoch <= claim.LeaderEpoch || !contracts.ValidAuthoringClaim(claim) {
		return contracts.AuthoringProof{}, ErrUnclear
	}
	// No launch was recorded: the old leader and its unreleased gate have
	// retired under the daemon's exclusive ownership; never resend the turn.
	if launch == (contracts.ProviderLaunch{}) {
		return s.Signer.ProveAuthoringDrained(claim, epoch)
	}
	if launch.PID <= 0 || launch.PID != launch.PGID || launch.ProcessStartIdentity == "" || launch.BootIdentity == "" || !cleanAbsolute(launch.Worktree) || launch.Worktree == "/" {
		return contracts.AuthoringProof{}, ErrUnclear
	}
	boot, err := hostBootIdentity()
	if err != nil {
		return contracts.AuthoringProof{}, err
	}
	if bootIdentityChanged(launch.BootIdentity, boot) {
		return s.Signer.ProveAuthoringDrained(claim, epoch)
	}
	if syscall.Kill(launch.PID, 0) != nil {
		return contracts.AuthoringProof{}, ErrUnclear
	}
	pgid, err := syscall.Getpgid(launch.PID)
	if err != nil {
		return contracts.AuthoringProof{}, ErrUnclear
	}
	start, err := processStartIdentity(launch.PID)
	if err != nil || !persistedIdentityMatches(launch, start, pgid) {
		return contracts.AuthoringProof{}, ErrUnclear
	}
	r := &run{identity: Identity{PID: launch.PID, PGID: launch.PGID, BootIdentity: boot, ProcessStartIdentity: start}, worktree: launch.Worktree, done: make(chan struct{}), streams: make(chan struct{})}
	close(r.streams)
	bounded, cancel, err := s.drainContext(ctx)
	if err != nil {
		return contracts.AuthoringProof{}, err
	}
	defer cancel()
	go func() {
		for {
			if syscall.Kill(launch.PID, 0) == syscall.ESRCH {
				close(r.done)
				return
			}
			select {
			case <-bounded.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	if err := s.terminateContext(bounded, r); err != nil {
		return contracts.AuthoringProof{}, err
	}
	if err := s.proveGoneContext(bounded, r); err != nil {
		return contracts.AuthoringProof{}, err
	}
	return s.Signer.ProveAuthoringDrained(claim, epoch)
}

var _ contracts.AuthoringSupervisor = (*Supervisor)(nil)
