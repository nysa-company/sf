// Package operator binds an optional human label to the authenticated Unix
// socket peer. A label is audit metadata, never a substitute for peer UID.
package operator

import (
	"errors"
	"os/user"
	"strconv"
	"strings"
	"unicode"

	"github.com/nysa-company/sf/internal/domain"
)

var ErrIdentityMismatch = errors.New("operator label does not match the authenticated peer")

type Account struct {
	Username string
	UID      string
}

type Lookup func(string) (Account, error)

type Authenticator struct {
	ExpectedUID uint32
	Aliases     []string
	Lookup      Lookup
}

func (a Authenticator) Authenticate(peerUID uint32, requested string) (domain.OperatorIdentity, error) {
	if peerUID != a.ExpectedUID {
		return domain.OperatorIdentity{}, ErrIdentityMismatch
	}
	lookup := a.Lookup
	if lookup == nil {
		lookup = func(uid string) (Account, error) {
			account, err := user.LookupId(uid)
			if err != nil {
				return Account{}, err
			}
			return Account{Username: account.Username, UID: account.Uid}, nil
		}
	}
	account, err := lookup(strconv.FormatUint(uint64(peerUID), 10))
	if err != nil || account.Username == "" || account.UID != strconv.FormatUint(uint64(peerUID), 10) {
		return domain.OperatorIdentity{}, ErrIdentityMismatch
	}
	if requested == "" {
		requested = account.Username
	}
	if !validLabel(requested) {
		return domain.OperatorIdentity{}, ErrIdentityMismatch
	}
	allowed := requested == account.Username
	for _, alias := range a.Aliases {
		if requested == alias && validLabel(alias) {
			allowed = true
			break
		}
	}
	if !allowed {
		return domain.OperatorIdentity{}, ErrIdentityMismatch
	}
	return domain.OperatorIdentity{UID: peerUID, Username: account.Username, Label: requested}, nil
}

func validLabel(value string) bool {
	if strings.TrimSpace(value) != value || value == "" || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) || character == '\x00' {
			return false
		}
	}
	return true
}
