package contracts

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

type ExecutionProfile string

const (
	ProfileGuarded    ExecutionProfile = "qualified_guarded"
	ProfileAutonomous ExecutionProfile = "autonomous_eligible"
)

type PhaseInput struct {
	Ticket     domain.TicketRef
	Phase      domain.Phase
	Prompt     string
	Repository string
	Worktree   string
	// WorktreeIdentity and BaseSHA are the exact durable Git/worktree
	// identity authenticated by the store, never merely a caller path.
	WorktreeIdentity string
	BaseSHA          string
	AllowedPaths     []string
	Provider         domain.ProviderIdentity
	Timeout          time.Duration
	Profile          ExecutionProfile
	Schema           []byte
}

type PhaseResult struct {
	Outcome      string
	Artifact     []byte
	Transcript   string
	Provider     domain.ProviderIdentity
	ChangedFiles []string
	UsageTrusted bool
	UsageUnits   int64
}

// RuntimeBinding is re-probed immediately before a paid invocation. Its
// digests are opaque SHA-256 values; credentials themselves never cross this
// interface or enter SQLite.
type RuntimeBinding struct {
	Identity      domain.ProviderIdentity
	BinaryDigest  string
	PolicyDigest  string
	FixtureDigest string
	AuthDigest    string
}

// DrainRequest identifies exactly one provider process group. Supervisors must
// not interpret this as permission to drain every process for an account.
type DrainRequest struct {
	Identity        domain.ProviderIdentity
	Ref             domain.TicketRef
	Phase           domain.Phase
	Attempt         int
	LeaderEpoch     uint64
	RunnerEpoch     uint64
	ExpectedVersion uint64
	LeaseKey        string
	BindingDigest   string
}

// DrainResult is supplied by the process supervisor after cancellation or
// recovery. A false value keeps the durable claim quarantined.
type DrainProof struct {
	publicKey ed25519.PublicKey
	signature []byte
	request   DrainRequest
}

// DrainResult remains a compatibility value for fixture adapters. It is not
// accepted by the store and cannot release a claim.
type DrainResult struct{ Drained bool }

// DrainSigner is held only by the process supervisor. Its private key is an
// in-process capability: adapters never receive it and therefore cannot
// self-attest that a process group has drained.
type DrainSigner struct {
	publicKey  ed25519.PublicKey
	privateKey ed25519.PrivateKey
}

func NewDrainSigner() (*DrainSigner, error) {
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &DrainSigner{publicKey: pub, privateKey: private}, nil
}
func (s *DrainSigner) PublicKey() []byte {
	if s == nil {
		return nil
	}
	return append([]byte(nil), s.publicKey...)
}
func (s *DrainSigner) ProveDrained(request DrainRequest) (DrainProof, error) {
	if s == nil || len(s.privateKey) != ed25519.PrivateKeySize {
		return DrainProof{}, errors.New("drain signer unavailable")
	}
	payload := drainPayload(request)
	return DrainProof{publicKey: append(ed25519.PublicKey(nil), s.publicKey...), signature: ed25519.Sign(s.privateKey, payload), request: request}, nil
}
func VerifyDrainProof(publicKey []byte, request DrainRequest, proof DrainProof) bool {
	return len(publicKey) == ed25519.PublicKeySize && string(publicKey) == string(proof.publicKey) && proof.request == request && ed25519.Verify(ed25519.PublicKey(publicKey), drainPayload(request), proof.signature)
}
func drainPayload(r DrainRequest) []byte {
	return []byte(string(r.Ref.Channel) + "\x00" + string(r.Ref.Project) + "\x00" + string(r.Ref.Ticket) + "\x00" + string(r.Phase) + "\x00" + r.Identity.Provider + "\x00" + r.Identity.Model + "\x00" + r.Identity.Family + "\x00" + r.Identity.Version + "\x00" + r.LeaseKey + "\x00" + r.BindingDigest + "\x00" + fmtUint(r.LeaderEpoch) + "\x00" + fmtUint(r.RunnerEpoch) + "\x00" + fmtUint(r.ExpectedVersion) + "\x00" + fmtInt(r.Attempt))
}
func fmtUint(v uint64) string { return fmt.Sprintf("%d", v) }
func fmtInt(v int) string     { return fmt.Sprintf("%d", v) }

type Provider interface {
	Name() string
	Probe(context.Context) (domain.ProviderIdentity, error)
	Binding(context.Context) (RuntimeBinding, error)
	Run(context.Context, PhaseInput) (PhaseResult, error)
}

// ProcessSupervisor owns every provider process group. It must TERM, drain,
// and KILL as needed before issuing the cryptographic proof consumed by the
// store. A Provider adapter has no drain method or proof capability.
type ProcessSupervisor interface {
	PublicKey() []byte
	Run(context.Context, DrainRequest, Provider, PhaseInput) (PhaseResult, error)
	Drain(context.Context, DrainRequest) (DrainProof, error)
}
