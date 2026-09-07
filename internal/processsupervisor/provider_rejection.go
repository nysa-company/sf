package processsupervisor

import (
	"context"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

// ConfigureRejectionCheckpoint installs the trusted Store+Git boundary while
// idle. Never replace the inspector while a run or shutdown owns evidence.
func (s *Supervisor) ConfigureRejectionCheckpoint(inspector contracts.RejectionCheckpointInspector) error {
	if s == nil || inspector == nil {
		return ErrUnclear
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.runs) != 0 || s.closing || s.closed {
		return ErrUnclear
	}
	s.rejectionCheckpoint = inspector
	return nil
}

// DrainServerRejection consumes only private Run-captured rejection metadata.
// Callers cannot submit output, categories, timestamps or checkpoint facts.
// It does not terminate a running process: complete Run and I/O are required.
// On refusal, ordinary Drain remains available for conservative finalization.
// A concurrent control Drain irrevocably wins over receipt issuance.
func (s *Supervisor) DrainServerRejection(ctx context.Context, request contracts.DrainRequest) (contracts.DrainProof, contracts.ServerRejectionAttestation, error) {
	if s == nil || !validRequestDigest(request.RequestDigest) {
		return contracts.DrainProof{}, contracts.ServerRejectionAttestation{}, ErrUnclear
	}
	drainCtx, cancel, err := s.drainContext(ctx)
	if err != nil {
		return contracts.DrainProof{}, contracts.ServerRejectionAttestation{}, err
	}
	defer cancel()
	s.mu.Lock()
	r := s.runs[key(request)]
	inspector := s.rejectionCheckpoint
	if inspector == nil || r == nil || r.controlDraining || r.rejection == nil || s.closing || s.closed {
		s.mu.Unlock()
		return contracts.DrainProof{}, contracts.ServerRejectionAttestation{}, ErrUnclear
	}
	rejection := *r.rejection
	s.mu.Unlock()
	select {
	case <-r.finished:
	case <-drainCtx.Done():
		return contracts.DrainProof{}, contracts.ServerRejectionAttestation{}, ErrUnclear
	}
	if err := s.proveGoneContext(drainCtx, r); err != nil {
		return contracts.DrainProof{}, contracts.ServerRejectionAttestation{}, err
	}
	head, digest, err := inspector.InspectRejectionCheckpoint(drainCtx, request)
	if err != nil || drainCtx.Err() != nil {
		return contracts.DrainProof{}, contracts.ServerRejectionAttestation{}, ErrUnclear
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runs[key(request)] != r || r.controlDraining || s.closing || s.closed || drainCtx.Err() != nil {
		return contracts.DrainProof{}, contracts.ServerRejectionAttestation{}, ErrUnclear
	}
	drain, err := s.Signer.ProveDrained(request)
	if err != nil {
		return contracts.DrainProof{}, contracts.ServerRejectionAttestation{}, ErrUnclear
	}
	receipt, err := s.Signer.SignServerRejection(request, drain, contracts.ServerRejectionEvidence{
		StreamDigest: rejection.StreamDigest, AllFailuresServerErrors: rejection.AllFailuresServerErrors,
		InternalRetries: rejection.InternalRetries, LastRetryDelayMS: rejection.LastRetryDelayMS,
		CheckpointHeadOID: head, CheckpointDigest: digest, ObservedUnixNanos: time.Now().UTC().UnixNano(),
	})
	if err != nil {
		return contracts.DrainProof{}, contracts.ServerRejectionAttestation{}, ErrUnclear
	}
	delete(s.runs, key(request))
	return drain, receipt, nil
}
