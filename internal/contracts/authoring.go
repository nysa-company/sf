package contracts

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

const AuthoringTurnLimit = 4
const AuthoringTurnTimeout = 90 * time.Second

type AuthoringCapability struct {
	Identity     domain.ProviderIdentity `json:"identity"`
	BinaryDigest string                  `json:"binary_digest"`
	AuthDigest   string                  `json:"-"`
	PolicyDigest string                  `json:"policy_digest"`
}

type AuthoringClaim struct {
	Purpose                                                              string
	Channel                                                              domain.Channel
	Session, TurnKey                                                     string
	Turn                                                                 int
	LeaderEpoch                                                          uint64
	Identity                                                             domain.ProviderIdentity
	BinaryDigest, AuthDigest, PolicyDigest, RequestDigest, ContextDigest string
}

type AuthoringInput struct {
	Purpose string
	Prompt  string
	Context string
}

type AuthoringResult struct {
	Intent      *AuthoringIntent `json:"intent,omitempty"`
	Kind        string           `json:"kind"`
	Question    string           `json:"question"`
	Title       string           `json:"title"`
	Problem     string           `json:"problem"`
	Scope       []string         `json:"scope"`
	Acceptance  []string         `json:"acceptance"`
	Assumptions []string         `json:"assumptions"`
}

type AuthoringIntent struct {
	Action   string `json:"action"`
	Selector string `json:"selector"`
}

type AuthoringProof struct {
	Claim     AuthoringClaim
	Epoch     uint64
	Signature []byte
}

type AuthoringSupervisor interface {
	PrepareAuthoring(context.Context, string) (AuthoringCapability, error)
	RunAuthoring(context.Context, AuthoringClaim, AuthoringInput, func(context.Context, ProviderLaunch) error) (AuthoringResult, AuthoringProof, error)
	RecoverAuthoring(context.Context, AuthoringClaim, ProviderLaunch, uint64) (AuthoringProof, error)
}

func AuthoringDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
func AuthoringInputDigest(input AuthoringInput) string {
	data, _ := json.Marshal(input)
	return AuthoringDigest(data)
}
func ValidAuthoringClaim(claim AuthoringClaim) bool {
	if claim.Purpose != "ticket_draft" && claim.Purpose != "home_intent" {
		return false
	}
	if !claim.Channel.Valid() || !AuthoringID(claim.Session) || !AuthoringID(claim.TurnKey) || claim.Turn < 1 || claim.Turn > AuthoringTurnLimit || claim.LeaderEpoch == 0 || claim.Identity.Provider != "claude" || !AuthoringID(claim.Identity.Model) || !AuthoringID(claim.Identity.Family) || !AuthoringID(claim.Identity.Version) {
		return false
	}
	for _, digest := range []string{claim.BinaryDigest, claim.AuthDigest, claim.PolicyDigest, claim.RequestDigest, claim.ContextDigest} {
		if len(digest) != 64 || strings.ToLower(digest) != digest {
			return false
		}
		if _, err := hex.DecodeString(digest); err != nil {
			return false
		}
	}
	return true
}
func AuthoringID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}
func authoringProofPayload(claim AuthoringClaim, epoch uint64) []byte {
	data, _ := json.Marshal(struct {
		Domain string
		Claim  AuthoringClaim
		Epoch  uint64
	}{"sf.authoring.drained/v1", claim, epoch})
	return data
}
func (s *DrainSigner) ProveAuthoringDrained(claim AuthoringClaim, epoch uint64) (AuthoringProof, error) {
	if s == nil || len(s.privateKey) != ed25519.PrivateKeySize || !ValidAuthoringClaim(claim) || epoch < claim.LeaderEpoch {
		return AuthoringProof{}, errors.New("invalid authoring proof")
	}
	return AuthoringProof{Claim: claim, Epoch: epoch, Signature: ed25519.Sign(s.privateKey, authoringProofPayload(claim, epoch))}, nil
}
func VerifyAuthoringProof(key []byte, claim AuthoringClaim, epoch uint64, proof AuthoringProof) bool {
	return len(key) == ed25519.PublicKeySize && proof.Claim == claim && proof.Epoch == epoch && ValidAuthoringClaim(claim) && ed25519.Verify(key, authoringProofPayload(claim, epoch), proof.Signature)
}
