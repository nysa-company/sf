package contracts

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/nysa-company/sf/internal/domain"
)

type ExecutionProfile string

const (
	ProfileGuarded    ExecutionProfile = "qualified_guarded"
	ProfileAutonomous ExecutionProfile = "autonomous_eligible"
)

type PhaseInput struct {
	Ticket domain.TicketRef
	Phase  domain.Phase
	// The following fields are copied from the Store-issued provider claim by
	// the coordinator immediately before launch. They make the input handed to
	// the provider and supervisor an authenticated view of that claim rather
	// than a second caller-controlled set of fence values.
	Attempt         int
	LeaderEpoch     uint64
	RunnerEpoch     uint64
	ExpectedVersion uint64
	Prompt          string
	Repository      string
	Worktree        string
	// WorktreeIdentity and BaseSHA are the exact durable Git/worktree
	// identity authenticated by the store, never merely a caller path.
	WorktreeIdentity string
	BaseSHA          string
	AllowedPaths     []string
	Provider         domain.ProviderIdentity
	// AuthMode is copied from the just-observed RuntimeBinding. It is an
	// admission value, never a credential or provider-controlled transcript.
	AuthMode string
	Timeout  time.Duration
	Profile  ExecutionProfile
	Schema   []byte
	// RequestDigest authenticates this exact launch input. It is issued by the
	// Store with the provider attempt and is not part of its own digest.
	RequestDigest string
	// Repair is an optional Store-issued marker for the one retry immediately
	// following a fully drained invalid artifact from the same provider and
	// phase entry. It carries only opaque attempt/input identifiers: never the
	// previous artifact, transcript, prompt, or credentials.
	Repair *ProviderRepairContext

	// legacyCanonicalPayload is set only by DecodeCanonicalPhaseInput after it
	// has proved that persisted pre-v53 bytes are the exact old canonical
	// representation. It is deliberately private: new proposals must always
	// serialize with the current v53 shape, including Repair:null when no
	// repair applies.
	legacyCanonicalPayload []byte
}

// phaseInputV52 is the exact pre-v53 canonical wire shape. Keep its field
// order in lock-step with PhaseInput through RequestDigest; PhaseInput.Repair
// was appended in v53 without omitempty.
type phaseInputV52 struct {
	Ticket           domain.TicketRef
	Phase            domain.Phase
	Attempt          int
	LeaderEpoch      uint64
	RunnerEpoch      uint64
	ExpectedVersion  uint64
	Prompt           string
	Repository       string
	Worktree         string
	WorktreeIdentity string
	BaseSHA          string
	AllowedPaths     []string
	Provider         domain.ProviderIdentity
	AuthMode         string
	Timeout          time.Duration
	Profile          ExecutionProfile
	Schema           []byte
	RequestDigest    string
}

func phaseInputV52Value(input PhaseInput) phaseInputV52 {
	return phaseInputV52{
		Ticket:           input.Ticket,
		Phase:            input.Phase,
		Attempt:          input.Attempt,
		LeaderEpoch:      input.LeaderEpoch,
		RunnerEpoch:      input.RunnerEpoch,
		ExpectedVersion:  input.ExpectedVersion,
		Prompt:           input.Prompt,
		Repository:       input.Repository,
		Worktree:         input.Worktree,
		WorktreeIdentity: input.WorktreeIdentity,
		BaseSHA:          input.BaseSHA,
		AllowedPaths:     input.AllowedPaths,
		Provider:         input.Provider,
		AuthMode:         input.AuthMode,
		Timeout:          input.Timeout,
		Profile:          input.Profile,
		Schema:           input.Schema,
		RequestDigest:    input.RequestDigest,
	}
}

func canonicalPhaseInputV52(input PhaseInput) ([]byte, error) {
	input.RequestDigest = ""
	return json.Marshal(phaseInputV52Value(input))
}

// ProviderRepairContext is deliberately small. Its source input digest lets
// the Store bind a repair request to exactly the failed attempt without
// exposing any provider output to the next invocation.
type ProviderRepairContext struct {
	PriorAttempt       int    `json:"prior_attempt"`
	PriorRequestDigest string `json:"prior_request_digest"`
}

// ValidProviderRepairContext is structural validation for a context already
// authenticated by a PhaseInput digest. Store remains the only component that
// derives one from durable attempt state.
func ValidProviderRepairContext(value *ProviderRepairContext) bool {
	if value == nil || value.PriorAttempt <= 0 || len(value.PriorRequestDigest) != 64 {
		return false
	}
	return strings.ToLower(value.PriorRequestDigest) == value.PriorRequestDigest && strings.Trim(value.PriorRequestDigest, "0123456789abcdef") == ""
}

// CanonicalPhaseInput returns the stable, complete representation of the
// phase input a provider is permitted to receive.  Keeping this in contracts
// lets the store issue a digest without trusting the coordinator or adapter
// to serialize the request the same way.
func CanonicalPhaseInput(input PhaseInput) ([]byte, string, error) {
	if input.legacyCanonicalPayload != nil {
		if input.Repair != nil {
			return nil, "", errors.New("legacy phase input cannot carry repair context")
		}
		legacy, err := canonicalPhaseInputV52(input)
		if err != nil || string(legacy) != string(input.legacyCanonicalPayload) {
			return nil, "", errors.New("legacy phase input no longer matches authenticated payload")
		}
		sum := sha256.Sum256(legacy)
		return append([]byte(nil), legacy...), hex.EncodeToString(sum[:]), nil
	}
	input.RequestDigest = ""
	payload, err := json.Marshal(input)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(payload)
	return payload, hex.EncodeToString(sum[:]), nil
}

func PhaseInputDigestMatches(input PhaseInput, digest string) bool {
	_, actual, err := CanonicalPhaseInput(input)
	return err == nil && len(digest) == 64 && actual == digest
}

// PhaseInputMatchesAuthenticatedClaim compares a caller's reconstructed launch
// input with a decoded durable claim. The claim may carry the private marker
// for an exact pre-v53 canonical payload; callers cannot and must not copy
// that marker. First authenticate the claim's own serialized format and
// digest, then compare the public logical fields with request digests cleared.
// New inputs always take the v53 branch in CanonicalPhaseInput.
func PhaseInputMatchesAuthenticatedClaim(proposed, claim PhaseInput, digest string) bool {
	if len(digest) != 64 {
		return false
	}
	_, actual, err := CanonicalPhaseInput(claim)
	if err != nil || actual != digest {
		return false
	}
	proposed.RequestDigest = ""
	proposed.legacyCanonicalPayload = nil
	claim.RequestDigest = ""
	claim.legacyCanonicalPayload = nil
	return reflect.DeepEqual(proposed, claim)
}

func DecodeCanonicalPhaseInput(payload []byte) (PhaseInput, error) {
	var input PhaseInput
	if err := json.Unmarshal(payload, &input); err != nil {
		return PhaseInput{}, err
	}
	canonical, _, err := CanonicalPhaseInput(input)
	if err == nil && string(canonical) == string(payload) {
		return input, nil
	}
	// v53 appended Repair without omitempty. A database created by an earlier
	// binary has exact pre-v53 canonical bytes and their SHA-256 digest. Accept
	// only that one historical shape: no Repair member at all, nil Repair after
	// decode, and a byte-for-byte match to an explicitly marshalled v52 value.
	// Never normalize or rewrite those durable bytes.
	var fields map[string]json.RawMessage
	if input.Repair != nil || json.Unmarshal(payload, &fields) != nil {
		return PhaseInput{}, errors.New("phase input is not canonical")
	}
	if _, hasRepair := fields["Repair"]; hasRepair {
		return PhaseInput{}, errors.New("phase input is not canonical")
	}
	legacy, legacyErr := canonicalPhaseInputV52(input)
	if legacyErr != nil || string(legacy) != string(payload) {
		return PhaseInput{}, errors.New("phase input is not canonical")
	}
	input.legacyCanonicalPayload = append([]byte(nil), legacy...)
	return input, nil
}

type PhaseResult struct {
	Outcome string
	// ArtifactFailureReason is a bounded, non-secret classification emitted
	// only for a clean provider completion whose final artifact is repairable.
	// It is never provider text, a transcript excerpt, or an adapter error.
	ArtifactFailureReason ArtifactFailureReason
	Artifact              []byte
	Transcript            string
	Provider              domain.ProviderIdentity
	ChangedFiles          []string
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
	TokenInputTokens      int64
	TokenCachedTokens     int64
	TokenCacheWriteTokens int64
	TokenOutputTokens     int64
	TokenReasoningTokens  int64
}

// PhaseResult outcome values are adapter-owned classifications.  In
// particular, invalid_artifact is repairable only when the adapter has
// observed a clean provider completion and can trust the subscription's zero
// monetary usage; indeterminate means the provider result or its observation
// cannot be safely interpreted.
const (
	PhaseResultCompleted       = "completed"
	PhaseResultInvalidArtifact = "invalid_artifact"
	PhaseResultIndeterminate   = "indeterminate"
)

// ArtifactFailureReason explains the small repairable subset of an
// invalid_artifact outcome. Keep this set deliberately closed: it is durable
// operator evidence, not a diagnostic channel for provider-controlled text.
type ArtifactFailureReason string

const (
	ArtifactFailureFinalMessage ArtifactFailureReason = "final_message_missing_or_malformed"
	ArtifactFailureSchema       ArtifactFailureReason = "schema_validation"
	ArtifactFailureMutationPath ArtifactFailureReason = "mutation_path"
	// ArtifactFailureAdapterDeclared covers a trusted adapter's explicit
	// invalid-artifact result when it has no more precise closed reason.
	ArtifactFailureAdapterDeclared ArtifactFailureReason = "adapter_declared_invalid_artifact"
)

func ValidArtifactFailureReason(value ArtifactFailureReason) bool {
	switch value {
	case ArtifactFailureFinalMessage, ArtifactFailureSchema, ArtifactFailureMutationPath, ArtifactFailureAdapterDeclared:
		return true
	default:
		return false
	}
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
	// AuthMode is a bounded, non-secret credential class. Codex is admitted
	// only for the explicitly supported subscription mode; an API/metered
	// login must never be mistaken for a zero-cost attempt.
	AuthMode string
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
	AuthMode         string
	Repository       string
	Worktree         string
	WorktreeIdentity string
	BaseSHA          string
	RequestDigest    string
}

// DrainProof attests only that the recorded supervised process group drained.
// Guarded v1 trusts the qualified local provider and does not represent this
// as hostile same-UID process-tree containment.
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
	AuthMode         string
	ProbeDigest      string
	Profile          ExecutionProfile
	CreatedUnixNanos int64
	LeaderEpoch      uint64
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
	if !value.Channel.Valid() || value.RunID == "" || value.Identity.Provider == "" || value.Identity.Model == "" || value.Identity.Family == "" || value.Identity.Version == "" || value.AuthMode == "" || len(value.AuthMode) > 64 || value.CreatedUnixNanos <= 0 || value.LeaderEpoch == 0 || value.Nonce == "" || value.Profile != ProfileGuarded {
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
	return []byte("sf-qualification/v2\x00" + string(value.Channel) + "\x00" + value.RunID + "\x00" + value.Identity.Provider + "\x00" + value.Identity.Model + "\x00" + value.Identity.Family + "\x00" + value.Identity.Version + "\x00" + value.BinaryDigest + "\x00" + value.PolicyDigest + "\x00" + value.FixtureDigest + "\x00" + value.AuthDigest + "\x00" + value.AuthMode + "\x00" + value.ProbeDigest + "\x00" + string(value.Profile) + "\x00" + fmt.Sprintf("%d", value.CreatedUnixNanos) + "\x00" + fmt.Sprintf("%d", value.LeaderEpoch) + "\x00" + value.Nonce)
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
	return []byte(fmt.Sprintf("%d", r.ClaimID) + "\x00" + string(r.Ref.Channel) + "\x00" + string(r.Ref.Project) + "\x00" + string(r.Ref.Ticket) + "\x00" + string(r.Phase) + "\x00" + r.Role + "\x00" + r.Identity.Provider + "\x00" + r.Identity.Model + "\x00" + r.Identity.Family + "\x00" + r.Identity.Version + "\x00" + r.LeaseKey + "\x00" + r.BindingDigest + "\x00" + r.BinaryDigest + "\x00" + r.PolicyDigest + "\x00" + r.AuthDigest + "\x00" + r.AuthMode + "\x00" + r.Repository + "\x00" + r.Worktree + "\x00" + r.WorktreeIdentity + "\x00" + r.BaseSHA + "\x00" + r.RequestDigest + "\x00" + fmtUint(r.LeaderEpoch) + "\x00" + fmtUint(r.RunnerEpoch) + "\x00" + fmtUint(r.ExpectedVersion) + "\x00" + fmtInt(r.Attempt))
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
