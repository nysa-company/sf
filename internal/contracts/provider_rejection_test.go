package contracts

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nysa-company/sf/internal/domain"
)

func serverRejectionFixture() (DrainRequest, ServerRejectionEvidence) {
	digest := strings.Repeat("a", 64)
	r := DrainRequest{ClaimID: 1, Identity: domain.ProviderIdentity{Provider: "claude", Model: "claude-sonnet-5", Family: "anthropic-claude", Version: "2.1.263"},
		Ref: domain.TicketRef{Channel: domain.ChannelDev, Project: "relay", Ticket: "SF-rejection"}, Phase: domain.PhasePlanning, Role: "planner", Attempt: 1,
		LeaderEpoch: 2, RunnerEpoch: 3, ExpectedVersion: 4, LeaseKey: "fixture-lease", BindingDigest: digest, BinaryDigest: digest,
		PolicyDigest: digest, AuthDigest: digest, AuthMode: "claude_subscription", Repository: "/private/repo", Worktree: "/private/worktree",
		WorktreeIdentity: `{"fixture":"identity"}`, BaseSHA: strings.Repeat("b", 40), RequestDigest: digest}
	e := ServerRejectionEvidence{StreamDigest: digest, AllFailuresServerErrors: true, InternalRetries: 1, LastRetryDelayMS: 1000,
		CheckpointHeadOID: strings.Repeat("c", 40), CheckpointDigest: digest, ObservedUnixNanos: 123456789}
	return r, e
}

func TestServerRejectionRequiresOwnExactDrainAndDistinctSignatureDomain(t *testing.T) {
	r, e := serverRejectionFixture()
	signer, err := NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	drain, err := signer.ProveDrained(r)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := signer.SignServerRejection(r, drain, e)
	if err != nil || !VerifyServerRejection(signer.PublicKey(), r, signed) {
		t.Fatal("valid rejection signature refused")
	}
	if VerifyServerRejection(other.PublicKey(), r, signed) || VerifyServerRejection(nil, r, signed) {
		t.Fatal("wrong key accepted")
	}
	for _, proof := range []DrainProof{{}, func() DrainProof { p, _ := other.ProveDrained(r); return p }(), func() DrainProof { copy := r; copy.Attempt++; p, _ := signer.ProveDrained(copy); return p }()} {
		value, err := signer.SignServerRejection(r, proof, e)
		if !errors.Is(err, ErrServerRejectionProof) || value.Signature != nil {
			t.Fatal("absent, foreign or wrong-attempt drain accepted")
		}
	}
	copy := signed
	copy.Signature = append([]byte(nil), drain.signature...)
	if VerifyServerRejection(signer.PublicKey(), r, copy) {
		t.Fatal("drain signature substituted for rejection signature")
	}
	drain.signature = append([]byte(nil), signed.Signature...)
	if VerifyDrainProof(signer.PublicKey(), r, drain) {
		t.Fatal("rejection signature substituted for drain signature")
	}
	raw, err := json.Marshal(signed)
	if err != nil {
		t.Fatal(err)
	}
	var restored ServerRejectionAttestation
	if json.Unmarshal(raw, &restored) != nil || !VerifyServerRejection(signer.PublicKey(), r, restored) {
		t.Fatal("exact persisted envelope lost signature")
	}
	var missing *DrainSigner
	if _, err := missing.SignServerRejection(r, DrainProof{}, e); !errors.Is(err, ErrServerRejectionProof) {
		t.Fatal("nil signer accepted")
	}
}

func TestServerRejectionBindsEveryClaimAndEvidenceField(t *testing.T) {
	r, e := serverRejectionFixture()
	signer, err := NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	drain, _ := signer.ProveDrained(r)
	signed, err := signer.SignServerRejection(r, drain, e)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range rejectionLeafMutations(reflect.ValueOf(r), "request") {
		t.Run(tc.name, func(t *testing.T) {
			changed := tc.value.Interface().(DrainRequest)
			if VerifyServerRejection(signer.PublicKey(), changed, signed) {
				t.Fatal("foreign expected request accepted")
			}
			copy := signed
			copy.Request = changed
			if VerifyServerRejection(signer.PublicKey(), changed, copy) {
				t.Fatal("altered signed request accepted")
			}
		})
	}
	for _, tc := range rejectionLeafMutations(reflect.ValueOf(e), "evidence") {
		t.Run(tc.name, func(t *testing.T) {
			copy := signed
			copy.Evidence = tc.value.Interface().(ServerRejectionEvidence)
			if VerifyServerRejection(signer.PublicKey(), r, copy) {
				t.Fatal("altered evidence accepted")
			}
		})
	}
	copy := signed
	copy.Signature = append([]byte(nil), signed.Signature...)
	copy.Signature[0] ^= 1
	if VerifyServerRejection(signer.PublicKey(), r, copy) {
		t.Fatal("altered signature accepted")
	}
}

type rejectionMutation struct {
	name  string
	value reflect.Value
}

// Enumerate every scalar leaf so a newly added signed field cannot silently
// escape the binding test. There are no pointers/slices in either payload.
func rejectionLeafMutations(value reflect.Value, prefix string) []rejectionMutation {
	var result []rejectionMutation
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		name := prefix + "." + value.Type().Field(i).Name
		copy := reflect.New(value.Type()).Elem()
		copy.Set(value)
		switch field.Kind() {
		case reflect.Struct:
			for _, child := range rejectionLeafMutations(field, name) {
				parent := reflect.New(value.Type()).Elem()
				parent.Set(value)
				parent.Field(i).Set(child.value)
				result = append(result, rejectionMutation{child.name, parent})
			}
			continue
		case reflect.String:
			copy.Field(i).SetString(field.String() + "changed")
		case reflect.Int, reflect.Int64:
			copy.Field(i).SetInt(field.Int() + 1)
		case reflect.Uint64:
			copy.Field(i).SetUint(field.Uint() + 1)
		case reflect.Bool:
			copy.Field(i).SetBool(!field.Bool())
		default:
			panic("signed payload added an unsupported field kind")
		}
		result = append(result, rejectionMutation{name, copy})
	}
	return result
}

func TestServerRejectionRefusesMalformedEvidenceBeforeSigning(t *testing.T) {
	r, e := serverRejectionFixture()
	signer, err := NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*DrainRequest, *ServerRejectionEvidence){
		"mixed":               func(_ *DrainRequest, e *ServerRejectionEvidence) { e.AllFailuresServerErrors = false },
		"retry bound":         func(_ *DrainRequest, e *ServerRejectionEvidence) { e.InternalRetries = 16 },
		"negative retries":    func(_ *DrainRequest, e *ServerRejectionEvidence) { e.InternalRetries = -1 },
		"delay bound":         func(_ *DrainRequest, e *ServerRejectionEvidence) { e.LastRetryDelayMS = 60001 },
		"delay without retry": func(_ *DrainRequest, e *ServerRejectionEvidence) { e.InternalRetries = 0 },
		"missing time":        func(_ *DrainRequest, e *ServerRejectionEvidence) { e.ObservedUnixNanos = 0 },
		"missing checkpoint":  func(_ *DrainRequest, e *ServerRejectionEvidence) { e.CheckpointDigest = "" },
		"wrong oid width":     func(_ *DrainRequest, e *ServerRejectionEvidence) { e.CheckpointHeadOID = strings.Repeat("c", 64) },
		"uppercase digest":    func(_ *DrainRequest, e *ServerRejectionEvidence) { e.StreamDigest = strings.Repeat("A", 64) },
		"nonhex digest":       func(_ *DrainRequest, e *ServerRejectionEvidence) { e.StreamDigest = strings.Repeat("x", 64) },
		"cursor":              func(r *DrainRequest, _ *ServerRejectionEvidence) { r.Identity.Provider = "cursor" },
		"wrong family":        func(r *DrainRequest, _ *ServerRejectionEvidence) { r.Identity.Family = "other" },
		"api auth":            func(r *DrainRequest, _ *ServerRejectionEvidence) { r.AuthMode = "api" },
		"wrong role":          func(r *DrainRequest, _ *ServerRejectionEvidence) { r.Role = "builder" },
		"missing claim":       func(r *DrainRequest, _ *ServerRejectionEvidence) { r.ClaimID = 0 },
		"relative path":       func(r *DrainRequest, _ *ServerRejectionEvidence) { r.Worktree = "relative" },
		"root path":           func(r *DrainRequest, _ *ServerRejectionEvidence) { r.Repository = "/" },
		"unclean path":        func(r *DrainRequest, _ *ServerRejectionEvidence) { r.Worktree = "/private/a/../b" },
		"invalid identity":    func(r *DrainRequest, _ *ServerRejectionEvidence) { r.WorktreeIdentity = "not json" },
		"lease nul":           func(r *DrainRequest, _ *ServerRejectionEvidence) { r.LeaseKey = "a\x00b" },
	} {
		t.Run(name, func(t *testing.T) {
			request, evidence := r, e
			mutate(&request, &evidence)
			// Even an exact valid drain must not authorize malformed metadata.
			drain, _ := signer.ProveDrained(request)
			if got, err := signer.SignServerRejection(request, drain, evidence); !errors.Is(err, ErrServerRejectionProof) || got.Signature != nil {
				t.Fatal("malformed evidence signed")
			}
		})
	}
}
