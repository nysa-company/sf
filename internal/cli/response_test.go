package cli

import "testing"

func TestShellWordsQuotesEveryShellMetacharacter(t *testing.T) {
	got := shellWords([]string{"sf", "doctor", "a;$(touch /tmp/pwned)", "safe/path", "line\nbreak"})
	want := "sf doctor 'a;$(touch /tmp/pwned)' safe/path 'line\nbreak'"
	if got != want {
		t.Fatalf("shellWords=%q, want %q", got, want)
	}
}
