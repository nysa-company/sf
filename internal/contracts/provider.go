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
	// UsageTrusted and UsageUnits describe a trusted monetary charge in
	// micro-USD. They alone may be charged against Ticket.MaxCostMicroUSD.
	// A provider-reported token count must never be placed here.
	UsageTrusted bool
	UsageUnits   int64
	// TokenUsage is optional provider observability. It is separate because
	// tokens cannot be compared to a monetary ceiling without an immutable
	// pricing or reservation policy.
	TokenUsageTrusted bool
	TokenUsage        int64
	// Individual provider-reported counters are retained for observability.
	// They are never interpreted as currency and are not used for budget
	// enforcement without a separately snapshotted pricing policy.
	TokenInputTokens     int64
	TokenCachedTokens    int64
	TokenOutputTokens    int64
	TokenReasoningTokens int64
}

// Invocation is an argv-only adapter proposal. The supervisor is the sole
// component allowed to start it; adapters never receive os/exec authority.
type Invocation struct {
	Argv []string
	// Stdin is a bounded adapter-supplied payload. The supervisor owns the
	// pipe and never inherits the daemon terminal.
	Stdin []byte
	// OutputSchema is materialized by the supervisor in its private temporary
	// directory. Argv must contain OutputSchemaPlaceholder exactly once when
	// this field is non-empty.
	OutputSchema []byte
	// CaptureLastMessage requests a supervisor-owned private output file. It
	// must be represented exactly once by OutputLastMessagePlaceholder in argv.
	CaptureLastMessage bool
	// AuthHome is an adapter-approved credential directory that is not
	// group/world writable. It is deliberately not persisted and is exposed
	// only under the provider's documented environment variable by the
	// supervisor.
	AuthHome string
}

// OutputSchemaPlaceholder is replaced by the supervisor with a private,
// absolute schema file immediately before exec. It prevents an adapter from
// writing schema material into a ticket worktree.
const OutputSchemaPlaceholder = "__SF_OUTPUT_SCHEMA__"

// OutputLastMessagePlaceholder is replaced by a private, bounded file path
// immediately before exec. Providers must not select their own artifact path.
const OutputLastMessagePlaceholder = "__SF_OUTPUT_LAST_MESSAGE__"

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
	ClaimID          int64
	Identity         domain.ProviderIdentity
	Ref              domain.TicketRef
	Phase            domain.Phase
	Role             string
	Attempt          int
	LeaderEpoch      uint64
	RunnerEpoch      uint64
	ExpectedVersion  uint64
	LeaseKey         string
	BindingDigest    string
	BinaryDigest     string
	PolicyDigest     string
	AuthDigest       string
	Repository       string
	Worktree         string
	WorktreeIdentity string
	BaseSHA          string
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

// QualificationAttestation is the non-secret evidence that a currently
// running supervisor observed a local, guarded qualification.  It is kept
// deliberately small: probe output and credentials never enter SQLite; their
// canonical SHA-256 digests are what the signature binds.
type QualificationAttestation struct {
	Channel          domain.Channel
	RunID            string
	Identity         domain.ProviderIdentity
	BinaryDigest     string
	PolicyDigest     string
	FixtureDigest    string
	AuthDigest       string
	ProbeDigest      string
	Profile          ExecutionProfile
	CreatedUnixNanos int64
	Nonce            string
	Signature        []byte
}

// SignQualification attests to an exact, already bounded qualification
// observation.  The same supervisor key is durably published by the daemon
// for process-recovery proof, so Store can reject a signature from a stale or
// unrelated local process.
func (s *DrainSigner) SignQualification(value QualificationAttestation) (QualificationAttestation, error) {
	if s == nil || len(s.privateKey) != ed25519.PrivateKeySize || !validQualificationAttestation(value) {
		return QualificationAttestation{}, errors.New("qualification signer unavailable or attestation invalid")
	}
	value.Signature = ed25519.Sign(s.privateKey, qualificationPayload(value))
	return value, nil
}

func VerifyQualificationAttestation(publicKey []byte, value QualificationAttestation) bool {
	return len(publicKey) == ed25519.PublicKeySize && validQualificationAttestation(value) && len(value.Signature) == ed25519.SignatureSize && ed25519.Verify(ed25519.PublicKey(publicKey), qualificationPayload(value), value.Signature)
}

func validQualificationAttestation(value QualificationAttestation) bool {
	if !value.Channel.Valid() || value.RunID == "" || value.Identity.Provider == "" || value.Identity.Model == "" || value.Identity.Family == "" || value.Identity.Version == "" || value.CreatedUnixNanos <= 0 || value.Nonce == "" || value.Profile != ProfileGuarded {
		return false
	}
	for _, digest := range []string{value.BinaryDigest, value.PolicyDigest, value.FixtureDigest, value.AuthDigest, value.ProbeDigest} {
		if len(digest) != 64 {
			return false
		}
	}
	return true
}

func qualificationPayload(value QualificationAttestation) []byte {
	return []byte("sf-qualification/v1\x00" + string(value.Channel) + "\x00" + value.RunID + "\x00" + value.Identity.Provider + "\x00" + value.Identity.Model + "\x00" + value.Identity.Family + "\x00" + value.Identity.Version + "\x00" + value.BinaryDigest + "\x00" + value.PolicyDigest + "\x00" + value.FixtureDigest + "\x00" + value.AuthDigest + "\x00" + value.ProbeDigest + "\x00" + string(value.Profile) + "\x00" + fmt.Sprintf("%d", value.CreatedUnixNanos) + "\x00" + value.Nonce)
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
	return []byte(fmt.Sprintf("%d", r.ClaimID) + "\x00" + string(r.Ref.Channel) + "\x00" + string(r.Ref.Project) + "\x00" + string(r.Ref.Ticket) + "\x00" + string(r.Phase) + "\x00" + r.Role + "\x00" + r.Identity.Provider + "\x00" + r.Identity.Model + "\x00" + r.Identity.Family + "\x00" + r.Identity.Version + "\x00" + r.LeaseKey + "\x00" + r.BindingDigest + "\x00" + r.BinaryDigest + "\x00" + r.PolicyDigest + "\x00" + r.AuthDigest + "\x00" + r.Repository + "\x00" + r.Worktree + "\x00" + r.WorktreeIdentity + "\x00" + r.BaseSHA + "\x00" + fmtUint(r.LeaderEpoch) + "\x00" + fmtUint(r.RunnerEpoch) + "\x00" + fmtUint(r.ExpectedVersion) + "\x00" + fmtInt(r.Attempt))
}
func fmtUint(v uint64) string { return fmt.Sprintf("%d", v) }
func fmtInt(v int) string     { return fmt.Sprintf("%d", v) }

type Provider interface {
	Name() string
	Probe(context.Context) (domain.ProviderIdentity, error)
	Binding(context.Context) (RuntimeBinding, error)
	Invocation(context.Context, PhaseInput) (Invocation, error)
	Parse(context.Context, PhaseInput, CommandResult) (PhaseResult, error)
}

// ProcessSupervisor owns every provider process group. It must TERM, drain,
// and KILL as needed before issuing the cryptographic proof consumed by the
// store. A Provider adapter has no drain method or proof capability.
type ProcessSupervisor interface {
	PublicKey() []byte
	Run(context.Context, DrainRequest, Invocation, PhaseInput) (CommandResult, error)
	Drain(context.Context, DrainRequest) (DrainProof, error)
}

// ProviderLaunch is the immutable local identity that was observed while the
// provider was still behind the supervisor's pre-exec gate.  The start
// identity is deliberately not a wall-clock value supplied by the caller: it
// is re-read from the operating system before recovery can signal a group.
type ProviderLaunch struct {
	PID, PGID            int
	BootIdentity         string
	ProcessStartIdentity string
	Worktree             string
}

// LaunchRecorderSetter lets the durable coordinator install the one recorder
// used by the production supervisor.  Keeping this narrow avoids giving
// provider adapters access to either SQLite or the process supervisor.
type LaunchRecorderSetter interface {
	SetLaunchRecorder(func(context.Context, DrainRequest, ProviderLaunch) error)
}
