package operator

import (
	"errors"
	"testing"
)

func lookup(uid string) (Account, error) {
	if uid != "501" {
		return Account{}, errors.New("not found")
	}
	return Account{UID: "501", Username: "sofia"}, nil
}

func TestAuthenticatedUsernameAndConfiguredAlias(t *testing.T) {
	authenticator := Authenticator{ExpectedUID: 501, Aliases: []string{"owner"}, Lookup: lookup}
	for _, requested := range []string{"", "sofia", "owner"} {
		identity, err := authenticator.Authenticate(501, requested)
		if err != nil {
			t.Fatalf("requested=%q err=%v", requested, err)
		}
		if identity.UID != 501 || identity.Username != "sofia" || identity.Label == "" {
			t.Fatalf("identity=%+v", identity)
		}
	}
}

func TestPeerAndFreeformLabelsAreRejected(t *testing.T) {
	authenticator := Authenticator{ExpectedUID: 501, Aliases: []string{"owner"}, Lookup: lookup}
	for name, peerAndLabel := range map[string]struct {
		peer  uint32
		label string
	}{
		"wrong peer": {502, "sofia"},
		"freeform":   {501, "reviewer"},
		"control":    {501, "owner\nadmin"},
		"oversized":  {501, string(make([]byte, 129))},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := authenticator.Authenticate(peerAndLabel.peer, peerAndLabel.label); !errors.Is(err, ErrIdentityMismatch) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestLookupCannotSubstituteAnotherUID(t *testing.T) {
	authenticator := Authenticator{ExpectedUID: 501, Lookup: func(string) (Account, error) {
		return Account{UID: "0", Username: "root"}, nil
	}}
	if _, err := authenticator.Authenticate(501, "root"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("err=%v", err)
	}
}
