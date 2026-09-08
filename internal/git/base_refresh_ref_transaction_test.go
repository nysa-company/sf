package git

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestBaseRefreshRefTransactionExactProtocol(t *testing.T) {
	for _, width := range []int{40, 64} {
		o, n, g, b := strings.Repeat("a", width), strings.Repeat("b", width), strings.Repeat("c", width), strings.Repeat("d", width)
		got, err := baseRefreshRefTransaction("sf/dev/ticket", o, n, b, g)
		if err != nil {
			t.Fatal(err)
		}
		want := "start\nupdate refs/heads/sf/dev/ticket " + g + " " + n + "\nupdate " + worktreeBaseRef("sf/dev/ticket") + " " + b + " " + o + "\nprepare\ncommit\n"
		if string(got) != want {
			t.Fatalf("protocol=%q want=%q", got, want)
		}
	}
	for _, ref := range []string{"refs/heads/x", "bad\nref", "../escape", "sf/dev/../x"} {
		if _, err := baseRefreshRefTransaction(ref, strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40), strings.Repeat("d", 40)); !errors.Is(err, ErrIdentityMismatch) {
			t.Fatalf("ref %q err=%v", ref, err)
		}
	}
	base := strings.Repeat("a", 40)
	for _, values := range [][4]string{{base, strings.Repeat("b", 41), strings.Repeat("c", 40), strings.Repeat("d", 40)}, {base, strings.Repeat("z", 40), strings.Repeat("c", 40), strings.Repeat("d", 40)}, {base, strings.Repeat("b", 40), base, strings.Repeat("d", 40)}, {base, strings.Repeat("b", 40), strings.Repeat("c", 40), strings.Repeat("b", 40)}} {
		if _, err := baseRefreshRefTransaction("sf/dev/x", values[0], values[1], values[2], values[3]); err == nil {
			t.Fatal("invalid OIDs accepted")
		}
	}
}

func TestCommandEnvInputExpectedRejectsOversizeAndInjectedRunner(t *testing.T) {
	if _, err := (Runner{}).commandEnvInputExpectedWithHandoff(context.Background(), "", 0, 0, nil, []byte(strings.Repeat("x", 4097)), nil, "update-ref"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("oversize err=%v", err)
	}
	run := false
	r := Runner{Run: func(context.Context, string, []string, []string) ([]byte, error) { run = true; return nil, nil }}
	if _, err := r.commandEnvInputExpectedWithHandoff(context.Background(), "", 0, 0, nil, []byte("stdin"), nil, "update-ref"); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("injected runner err=%v", err)
	}
	if run {
		t.Fatal("injected runner invoked")
	}
}
