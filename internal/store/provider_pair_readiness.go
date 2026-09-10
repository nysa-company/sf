package store

import (
	"context"
	"errors"

	"github.com/nysa-company/sf/internal/domain"
)

// ErrProviderQualificationNotCurrent distinguishes readable historical
// selection from usable current-supervisor evidence. It grants no authority.
var ErrProviderQualificationNotCurrent = errors.New("selected provider qualification is not current")

// CurrentAttestedProviderPair is a diagnostic read. Composition and attempt
// admission must still check exact runtime bindings and current attestations.
func (s *Store) CurrentAttestedProviderPair(ctx context.Context, channel domain.Channel) (ProviderPair, error) {
	pair, err := s.ProviderPair(ctx, channel)
	if err != nil {
		return ProviderPair{}, err
	}
	for _, q := range []ProviderQualification{pair.Planner, pair.Builder, pair.Reviewer} {
		if q.Channel != channel || q.Profile != QualificationGuarded || q.AuthMode == "" || q.ProbeDigest == "" || len(q.AttestationSignature) != 64 || !s.QualificationCurrent(ctx, channel, q) {
			// QualificationCurrent intentionally fails closed on read errors.
			// Distinguish an unreadable authority database for diagnostics.
			if _, readErr := s.LeaderEpoch(ctx, channel); readErr != nil && !errors.Is(readErr, ErrNotFound) {
				return ProviderPair{}, readErr
			}
			return pair, ErrProviderQualificationNotCurrent
		}
	}
	return pair, nil
}
