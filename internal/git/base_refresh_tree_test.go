package git

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	baseRefreshOID40A = "1111111111111111111111111111111111111111"
	baseRefreshOID40B = "2222222222222222222222222222222222222222"
	baseRefreshOID40C = "3333333333333333333333333333333333333333"
	baseRefreshOID40D = "4444444444444444444444444444444444444444"
)

func TestValidateBaseRefreshTreePreparationBindsExactInputsAndParents(t *testing.T) {
	request := baseRefreshTreeRequest{
		OriginalBaseOID:  baseRefreshOID40A,
		CandidateOID:     baseRefreshOID40B,
		RefreshedBaseOID: baseRefreshOID40C,
	}
	ancestry := baseRefreshTreeAncestryFor(request)
	got, err := validateBaseRefreshTreePreparation(request, ancestry, baseRefreshMergeTreeResponse{
		Operands: [2]string{request.CandidateOID, request.RefreshedBaseOID},
		Output:   []byte(baseRefreshOID40D + "\n"),
		ExitCode: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := baseRefreshTreePreparation{
		TreeOID: baseRefreshOID40D,
		Parents: [2]string{baseRefreshOID40B, baseRefreshOID40C},
	}
	if got != want {
		t.Fatalf("preparation=%+v want=%+v", got, want)
	}

	oid64 := func(digit byte) string { return strings.Repeat(string(digit), 64) }
	request64 := baseRefreshTreeRequest{OriginalBaseOID: oid64('1'), CandidateOID: oid64('2'), RefreshedBaseOID: oid64('3')}
	got, err = validateBaseRefreshTreePreparation(request64, baseRefreshTreeAncestryFor(request64), baseRefreshMergeTreeResponse{
		Operands: [2]string{request64.CandidateOID, request64.RefreshedBaseOID},
		Output:   []byte(oid64('4') + "\n"),
		ExitCode: 0,
	})
	if err != nil || got.TreeOID != oid64('4') || got.Parents != [2]string{oid64('2'), oid64('3')} {
		t.Fatalf("sha256 preparation=%+v err=%v", got, err)
	}
}

func TestValidateBaseRefreshTreePreparationRejectsInvalidInputs(t *testing.T) {
	valid := baseRefreshTreeRequest{
		OriginalBaseOID:  baseRefreshOID40A,
		CandidateOID:     baseRefreshOID40B,
		RefreshedBaseOID: baseRefreshOID40C,
	}
	ancestry := baseRefreshTreeAncestryFor(valid)
	response := baseRefreshMergeTreeResponse{Operands: [2]string{valid.CandidateOID, valid.RefreshedBaseOID}, Output: []byte(baseRefreshOID40D + "\n"), ExitCode: 0}

	tests := []struct {
		name     string
		request  baseRefreshTreeRequest
		ancestry baseRefreshTreeAncestry
	}{
		{name: "invalid original base", request: baseRefreshTreeRequest{OriginalBaseOID: "deadbeef", CandidateOID: valid.CandidateOID, RefreshedBaseOID: valid.RefreshedBaseOID}, ancestry: ancestry},
		{name: "uppercase candidate", request: baseRefreshTreeRequest{OriginalBaseOID: valid.OriginalBaseOID, CandidateOID: strings.Repeat("A", 40), RefreshedBaseOID: valid.RefreshedBaseOID}, ancestry: ancestry},
		{name: "mixed hash widths", request: baseRefreshTreeRequest{OriginalBaseOID: valid.OriginalBaseOID, CandidateOID: strings.Repeat("2", 64), RefreshedBaseOID: valid.RefreshedBaseOID}, ancestry: ancestry},
		{name: "candidate equals original", request: baseRefreshTreeRequest{OriginalBaseOID: valid.OriginalBaseOID, CandidateOID: valid.OriginalBaseOID, RefreshedBaseOID: valid.RefreshedBaseOID}, ancestry: ancestry},
		{name: "new base equals original", request: baseRefreshTreeRequest{OriginalBaseOID: valid.OriginalBaseOID, CandidateOID: valid.CandidateOID, RefreshedBaseOID: valid.OriginalBaseOID}, ancestry: ancestry},
		{name: "parents equal", request: baseRefreshTreeRequest{OriginalBaseOID: valid.OriginalBaseOID, CandidateOID: valid.CandidateOID, RefreshedBaseOID: valid.CandidateOID}, ancestry: ancestry},
		{name: "candidate ancestry missing", request: valid, ancestry: baseRefreshTreeAncestry{RefreshedBasePair: [2]string{valid.OriginalBaseOID, valid.RefreshedBaseOID}}},
		{name: "base ancestry missing", request: valid, ancestry: baseRefreshTreeAncestry{CandidatePair: [2]string{valid.OriginalBaseOID, valid.CandidateOID}}},
		{name: "stale candidate ancestry", request: valid, ancestry: baseRefreshTreeAncestry{CandidatePair: [2]string{valid.OriginalBaseOID, baseRefreshOID40D}, RefreshedBasePair: [2]string{valid.OriginalBaseOID, valid.RefreshedBaseOID}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, err := validateBaseRefreshTreePreparation(test.request, test.ancestry, response); !errors.Is(err, errBaseRefreshTreeInput) || got != (baseRefreshTreePreparation{}) {
				t.Fatalf("preparation=%+v err=%v", got, err)
			}
		})
	}
}

func TestValidateBaseRefreshTreePreparationRejectsMalformedOrFailedResponse(t *testing.T) {
	request := baseRefreshTreeRequest{OriginalBaseOID: baseRefreshOID40A, CandidateOID: baseRefreshOID40B, RefreshedBaseOID: baseRefreshOID40C}
	ancestry := baseRefreshTreeAncestryFor(request)

	tests := []struct {
		name   string
		output []byte
		exit   int
		is     error
	}{
		{name: "missing newline", output: []byte(baseRefreshOID40D), exit: 0, is: errBaseRefreshTreeCommand},
		{name: "crlf", output: []byte(baseRefreshOID40D + "\r\n"), exit: 0, is: errBaseRefreshTreeCommand},
		{name: "uppercase tree", output: []byte(strings.Repeat("A", 40) + "\n"), exit: 0, is: errBaseRefreshTreeCommand},
		{name: "wrong width", output: []byte(strings.Repeat("4", 64) + "\n"), exit: 0, is: errBaseRefreshTreeCommand},
		{name: "non hex", output: []byte(strings.Repeat("g", 40) + "\n"), exit: 0, is: errBaseRefreshTreeCommand},
		{name: "extra line", output: []byte(baseRefreshOID40D + "\nAuto-merging file\n"), exit: 0, is: errBaseRefreshTreeCommand},
		{name: "trailing blank line", output: []byte(baseRefreshOID40D + "\n\n"), exit: 0, is: errBaseRefreshTreeCommand},
		{name: "nul", output: append([]byte(baseRefreshOID40D), 0, '\n'), exit: 0, is: errBaseRefreshTreeCommand},
		{name: "conflict discards provisional tree", output: []byte(baseRefreshOID40D + "\n100644 blob conflict\n"), exit: 1, is: errBaseRefreshTreeConflict},
		{name: "fatal bad revision", output: []byte("fatal: not a valid object name\n"), exit: 128, is: errBaseRefreshTreeCommand},
		{name: "invalid negative status", exit: -1, is: errBaseRefreshTreeCommand},
		{name: "bounded conflict output", output: bytes.Repeat([]byte{'x'}, maxGitOutput+1), exit: 1, is: ErrOutputBound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := validateBaseRefreshTreePreparation(request, ancestry, baseRefreshMergeTreeResponse{Operands: [2]string{request.CandidateOID, request.RefreshedBaseOID}, Output: test.output, ExitCode: test.exit})
			if !errors.Is(err, test.is) || got != (baseRefreshTreePreparation{}) {
				t.Fatalf("preparation=%+v err=%v want=%v", got, err, test.is)
			}
		})
	}
}

func TestValidateBaseRefreshTreePreparationRejectsMismatchedMergeOperands(t *testing.T) {
	request := baseRefreshTreeRequest{OriginalBaseOID: baseRefreshOID40A, CandidateOID: baseRefreshOID40B, RefreshedBaseOID: baseRefreshOID40C}
	ancestry := baseRefreshTreeAncestryFor(request)
	for _, operands := range [][2]string{
		{},
		{request.RefreshedBaseOID, request.CandidateOID},
		{baseRefreshOID40D, request.RefreshedBaseOID},
	} {
		got, err := validateBaseRefreshTreePreparation(request, ancestry, baseRefreshMergeTreeResponse{Operands: operands, Output: []byte(baseRefreshOID40D + "\n")})
		if !errors.Is(err, errBaseRefreshTreeInput) || got != (baseRefreshTreePreparation{}) {
			t.Fatalf("operands=%q preparation=%+v err=%v", operands, got, err)
		}
	}
}

func TestBaseRefreshMergeTreePrivateBareFixtureIsDeterministicAndRefSafe(t *testing.T) {
	fixture := newBaseRefreshTreeFixture(t, false)
	beforeRefs := baseRefreshGit(t, fixture.environment, fixture.bare, "for-each-ref", "--format=%(refname):%(objectname)")
	if _, err := os.Stat(filepath.Join(fixture.bare, "index")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bare repository unexpectedly has an index before merge-tree: %v", err)
	}

	firstOutput, firstExit := runBaseRefreshMergeTree(t, fixture.environment, fixture.bare, fixture.candidate, fixture.refreshedBase)
	request := fixture.request()
	first, err := validateBaseRefreshTreePreparation(request, fixture.ancestry(t), baseRefreshMergeTreeResponse{Operands: [2]string{fixture.candidate, fixture.refreshedBase}, Output: firstOutput, ExitCode: firstExit})
	if err != nil {
		t.Fatal(err)
	}
	secondOutput, secondExit := runBaseRefreshMergeTree(t, fixture.environment, fixture.bare, fixture.candidate, fixture.refreshedBase)
	second, err := validateBaseRefreshTreePreparation(request, fixture.ancestry(t), baseRefreshMergeTreeResponse{Operands: [2]string{fixture.candidate, fixture.refreshedBase}, Output: secondOutput, ExitCode: secondExit})
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.Parents != [2]string{fixture.candidate, fixture.refreshedBase} {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	entries := baseRefreshGit(t, fixture.environment, fixture.bare, "ls-tree", "-r", "--name-only", first.TreeOID)
	if entries != "base.txt\ncandidate.txt\nrefreshed.txt" {
		t.Fatalf("merged tree entries=%q", entries)
	}
	if afterRefs := baseRefreshGit(t, fixture.environment, fixture.bare, "for-each-ref", "--format=%(refname):%(objectname)"); afterRefs != beforeRefs {
		t.Fatalf("merge-tree updated refs\nbefore=%q\nafter=%q", beforeRefs, afterRefs)
	}
	if _, err := os.Stat(filepath.Join(fixture.bare, "index")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("merge-tree created or changed a bare index: %v", err)
	}
}

func TestBaseRefreshMergeTreePrivateBareFixtureDetectsConflictWithoutRefOrIndexMutation(t *testing.T) {
	fixture := newBaseRefreshTreeFixture(t, true)
	beforeRefs := baseRefreshGit(t, fixture.environment, fixture.bare, "for-each-ref", "--format=%(refname):%(objectname)")
	output, exitCode := runBaseRefreshMergeTree(t, fixture.environment, fixture.bare, fixture.candidate, fixture.refreshedBase)
	got, err := validateBaseRefreshTreePreparation(fixture.request(), fixture.ancestry(t), baseRefreshMergeTreeResponse{Operands: [2]string{fixture.candidate, fixture.refreshedBase}, Output: output, ExitCode: exitCode})
	if !errors.Is(err, errBaseRefreshTreeConflict) || got != (baseRefreshTreePreparation{}) {
		t.Fatalf("conflict preparation=%+v exit=%d output=%q err=%v", got, exitCode, output, err)
	}
	if afterRefs := baseRefreshGit(t, fixture.environment, fixture.bare, "for-each-ref", "--format=%(refname):%(objectname)"); afterRefs != beforeRefs {
		t.Fatalf("conflicted merge-tree updated refs\nbefore=%q\nafter=%q", beforeRefs, afterRefs)
	}
	if _, statErr := os.Stat(filepath.Join(fixture.bare, "index")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("conflicted merge-tree created or changed a bare index: %v", statErr)
	}
}

type baseRefreshTreeFixture struct {
	environment, bare, originalBase, candidate, refreshedBase string
}

func (f baseRefreshTreeFixture) request() baseRefreshTreeRequest {
	return baseRefreshTreeRequest{OriginalBaseOID: f.originalBase, CandidateOID: f.candidate, RefreshedBaseOID: f.refreshedBase}
}

func (f baseRefreshTreeFixture) ancestry(t *testing.T) baseRefreshTreeAncestry {
	t.Helper()
	baseRefreshGit(t, f.environment, f.bare, "merge-base", "--is-ancestor", f.originalBase, f.candidate)
	baseRefreshGit(t, f.environment, f.bare, "merge-base", "--is-ancestor", f.originalBase, f.refreshedBase)
	return baseRefreshTreeAncestryFor(f.request())
}

func newBaseRefreshTreeFixture(t *testing.T, conflict bool) baseRefreshTreeFixture {
	t.Helper()
	root := t.TempDir()
	environment := filepath.Join(root, "environment")
	source := filepath.Join(root, "source")
	bare := filepath.Join(root, "objects.git")
	if err := os.Mkdir(environment, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatal(err)
	}
	baseRefreshGit(t, environment, source, "init", "-b", "main")
	baseRefreshGit(t, environment, source, "config", "user.name", "base-refresh-fixture")
	baseRefreshGit(t, environment, source, "config", "user.email", "base-refresh-fixture@example.test")
	if err := os.WriteFile(filepath.Join(source, "base.txt"), []byte("original\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	baseRefreshGit(t, environment, source, "add", "--", "base.txt")
	baseRefreshGit(t, environment, source, "commit", "-m", "original base")
	originalBase := baseRefreshGit(t, environment, source, "rev-parse", "HEAD^{commit}")

	baseRefreshGit(t, environment, source, "switch", "-c", "candidate")
	if conflict {
		if err := os.WriteFile(filepath.Join(source, "base.txt"), []byte("candidate\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	} else if err := os.WriteFile(filepath.Join(source, "candidate.txt"), []byte("candidate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	baseRefreshGit(t, environment, source, "add", "--", ".")
	baseRefreshGit(t, environment, source, "commit", "-m", "candidate")
	candidate := baseRefreshGit(t, environment, source, "rev-parse", "HEAD^{commit}")

	baseRefreshGit(t, environment, source, "switch", "main")
	if conflict {
		if err := os.WriteFile(filepath.Join(source, "base.txt"), []byte("refreshed\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	} else if err := os.WriteFile(filepath.Join(source, "refreshed.txt"), []byte("refreshed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	baseRefreshGit(t, environment, source, "add", "--", ".")
	baseRefreshGit(t, environment, source, "commit", "-m", "refreshed base")
	refreshedBase := baseRefreshGit(t, environment, source, "rev-parse", "HEAD^{commit}")

	baseRefreshGit(t, environment, root, "clone", "--bare", source, bare)
	if err := os.Chmod(bare, 0o700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(bare)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("bare fixture is not private: mode=%v", info.Mode())
	}
	return baseRefreshTreeFixture{environment: environment, bare: bare, originalBase: originalBase, candidate: candidate, refreshedBase: refreshedBase}
}

func runBaseRefreshMergeTree(t *testing.T, environment, bare, candidate, refreshedBase string) ([]byte, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/usr/bin/git", baseRefreshGitArguments(bare, "merge-tree", "--write-tree", candidate, refreshedBase)...)
	command.Env = baseRefreshGitEnvironment(environment)
	output, err := command.CombinedOutput()
	if err == nil {
		return output, 0
	}
	if ctx.Err() != nil {
		t.Fatalf("merge-tree exceeded fixture deadline: %v", ctx.Err())
	}
	var exitError *exec.ExitError
	if !errors.As(err, &exitError) {
		t.Fatalf("merge-tree could not start: %v", err)
	}
	return output, exitError.ExitCode()
}

func baseRefreshGit(t *testing.T, environment, directory string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/usr/bin/git", baseRefreshGitArguments(directory, args...)...)
	command.Env = baseRefreshGitEnvironment(environment)
	output, err := command.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			t.Fatalf("git %s exceeded fixture deadline: %v", strings.Join(args, " "), ctx.Err())
		}
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, output)
	}
	value := strings.TrimSuffix(string(output), "\n")
	if strings.ContainsAny(value, "\r\x00") {
		t.Fatalf("git %s returned noncanonical output %q", strings.Join(args, " "), output)
	}
	return value
}

func baseRefreshGitArguments(directory string, args ...string) []string {
	result := []string{
		"-C", directory,
		"-c", "core.hooksPath=/dev/null",
		"-c", "commit.gpgsign=false",
		"-c", "core.fsmonitor=false",
	}
	return append(result, args...)
}

func baseRefreshGitEnvironment(environment string) []string {
	return []string{
		"PATH=/usr/bin:/bin:/usr/sbin:/sbin",
		"LANG=C",
		"LC_ALL=C",
		"TZ=UTC",
		"HOME=" + environment,
		"TMPDIR=" + environment,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_TERMINAL_PROMPT=0",
		"GIT_OPTIONAL_LOCKS=0",
		"GIT_AUTHOR_DATE=2000-01-01T00:00:00Z",
		"GIT_COMMITTER_DATE=2000-01-01T00:00:00Z",
	}
}

func Example_validateBaseRefreshTreePreparation() {
	request := baseRefreshTreeRequest{OriginalBaseOID: baseRefreshOID40A, CandidateOID: baseRefreshOID40B, RefreshedBaseOID: baseRefreshOID40C}
	ancestry := baseRefreshTreeAncestryFor(request)
	result, _ := validateBaseRefreshTreePreparation(request, ancestry, baseRefreshMergeTreeResponse{Operands: [2]string{request.CandidateOID, request.RefreshedBaseOID}, Output: []byte(baseRefreshOID40D + "\n"), ExitCode: 0})
	fmt.Println(result.Parents[0] == request.CandidateOID, result.Parents[1] == request.RefreshedBaseOID)
	// Output: true true
}

func baseRefreshTreeAncestryFor(request baseRefreshTreeRequest) baseRefreshTreeAncestry {
	return baseRefreshTreeAncestry{
		CandidatePair:     [2]string{request.OriginalBaseOID, request.CandidateOID},
		RefreshedBasePair: [2]string{request.OriginalBaseOID, request.RefreshedBaseOID},
	}
}
