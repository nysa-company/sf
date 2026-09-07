package contracts

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"

	"github.com/nysa-company/sf/internal/domain"
)

var ErrServerRejectionProof = errors.New("provider server-rejection evidence is invalid")

// RejectionCheckpointInspector is supplied by trusted runtime composition,
// never by an adapter. It authenticates the exact active Store request and
// physical registered identity, HEAD and strict cleanliness.
type RejectionCheckpointInspector interface {
	InspectRejectionCheckpoint(context.Context, DrainRequest) (head, digest string, err error)
}

// ServerRejectionEvidence contains no provider text or credentials. The
// supervisor may supply this only after validating the complete stream and an
// independent, Store-derived physical checkpoint inspection under the claim's
// writer exclusion. This struct alone grants no authority.
type ServerRejectionEvidence struct {
	StreamDigest            string
	AllFailuresServerErrors bool
	InternalRetries         int
	LastRetryDelayMS        int
	CheckpointHeadOID       string
	CheckpointDigest        string
	ObservedUnixNanos       int64
}

// ServerRejectionAttestation is separate from DrainProof: a drained process
// does not prove a rejected API request or a pristine checkout. Store must
// authenticate this against the immutable claim's supervisor key AND exact
// expected request/checkpoint, qualification, current fence, attempt budget
// and durable backoff. Signature verification alone never admits a new run.
type ServerRejectionAttestation struct {
	Request   DrainRequest
	Evidence  ServerRejectionEvidence
	Signature []byte
}

// SignServerRejection is a supervisor-held capability, never given to provider
// adapters. It requires the same signer's drain proof for this exact claim.
// Production wiring must validate the stream and physical checkpoint before
// calling; this primitive does not inspect processes, files, or Store state.
func (s *DrainSigner) SignServerRejection(request DrainRequest, drain DrainProof, evidence ServerRejectionEvidence) (ServerRejectionAttestation, error) {
	if s == nil || len(s.privateKey) != ed25519.PrivateKeySize || !validServerRejection(request, evidence) || !VerifyDrainProof(s.publicKey, request, drain) {
		return ServerRejectionAttestation{}, ErrServerRejectionProof
	}
	value := ServerRejectionAttestation{Request: request, Evidence: evidence}
	value.Signature = ed25519.Sign(s.privateKey, serverRejectionPayload(value))
	return value, nil
}

func VerifyServerRejection(publicKey []byte, request DrainRequest, value ServerRejectionAttestation) bool {
	return len(publicKey) == ed25519.PublicKeySize && len(value.Signature) == ed25519.SignatureSize && value.Request == request &&
		validServerRejection(request, value.Evidence) && ed25519.Verify(ed25519.PublicKey(publicKey), serverRejectionPayload(value), value.Signature)
}

func serverRejectionPayload(value ServerRejectionAttestation) []byte {
	// Keep this versioned wire shape stable. Signature is never part of its
	// own payload; JSON framing avoids delimiter ambiguity in signed paths.
	payload, _ := json.Marshal(struct {
		Request  DrainRequest
		Evidence ServerRejectionEvidence
	}{value.Request, value.Evidence})
	return append([]byte("sf-provider-server-rejection/v1\x00"), payload...)
}

func validServerRejection(r DrainRequest, e ServerRejectionEvidence) bool {
	if r.ClaimID <= 0 || r.Ref.Validate() != nil || r.Attempt <= 0 || r.ExpectedVersion == 0 || r.LeaderEpoch == 0 || r.RunnerEpoch == 0 ||
		r.Identity.Provider != "claude" || r.Identity.Family != "anthropic-claude" || r.AuthMode != "claude_subscription" ||
		!rejectionBoundedText(r.Identity.Model, 128) || !rejectionBoundedText(r.Identity.Version, 64) || !rejectionBoundedText(r.LeaseKey, 512) ||
		!e.AllFailuresServerErrors || e.InternalRetries < 0 || e.InternalRetries > 15 || e.LastRetryDelayMS < 0 || e.LastRetryDelayMS > 60000 ||
		(e.InternalRetries == 0 && e.LastRetryDelayMS != 0) || e.ObservedUnixNanos <= 0 {
		return false
	}
	switch r.Phase {
	case domain.PhasePlanning:
		if r.Role != "planner" {
			return false
		}
	case domain.PhaseBuild:
		if r.Role != "builder" {
			return false
		}
	case domain.PhaseVerification, domain.PhaseReview:
		if r.Role != "reviewer" {
			return false
		}
	default:
		return false
	}
	for _, digest := range []string{r.BindingDigest, r.BinaryDigest, r.PolicyDigest, r.AuthDigest, r.RequestDigest, e.StreamDigest, e.CheckpointDigest} {
		if !rejectionHex(digest, 64) {
			return false
		}
	}
	if !(rejectionHex(r.BaseSHA, 40) || rejectionHex(r.BaseSHA, 64)) || !rejectionHex(e.CheckpointHeadOID, len(r.BaseSHA)) {
		return false
	}
	for _, path := range []string{r.Repository, r.Worktree} {
		if !rejectionBoundedText(path, 4096) || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
			return false
		}
	}
	return rejectionBoundedText(r.WorktreeIdentity, 64<<10) && json.Valid([]byte(r.WorktreeIdentity))
}

func rejectionBoundedText(value string, limit int) bool {
	return value != "" && len(value) <= limit && !strings.ContainsAny(value, "\x00\r\n")
}

func rejectionHex(value string, width int) bool {
	if len(value) != width || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
