package git

import (
	"errors"
	"strings"
	"testing"
)

func TestProtectedBaseChangeRequiresExactDistinctObjects(t *testing.T) {
	for _, width := range []int{40, 64} {
		expected, observed := strings.Repeat("a", width), strings.Repeat("b", width)
		err := protectedBaseChange(expected, observed)
		var change *ProtectedBaseChange
		if !errors.As(err, &change) || !errors.Is(err, ErrUnexpectedRemote) || change.Expected != expected || change.Observed != observed {
			t.Fatalf("valid change was not preserved: %v", err)
		}
		for _, invalid := range []string{"", expected, strings.Repeat("B", width), strings.Repeat("c", width+1), "unavailable"} {
			var rejected *ProtectedBaseChange
			if err := protectedBaseChange(expected, invalid); !errors.Is(err, ErrUnexpectedRemote) || errors.As(err, &rejected) {
				t.Fatalf("unproven change %q classified as refresh: %v", invalid, err)
			}
		}
	}
}
