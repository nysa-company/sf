package processsupervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

func TestActivityClaudeMixedBlocksNeverExposeReasoningOrArguments(t *testing.T) {
	s := &Supervisor{}
	request := contracts.DrainRequest{ClaimID: 42}
	r := s.beginActivity(request)
	r.publish()
	var capture limitedBuffer
	capture.limit = 64 << 10
	w := activityWriter{capture: &capture, record: r, decoder: activityDecoder{provider: "claude", model: "model"}}
	stream := `{"type":"system","subtype":"init","uuid":"u1","session_id":"s1","model":"model"}` + "\n" +
		`{"type":"assistant","uuid":"u2","session_id":"s1","message":{"role":"assistant","model":"model","content":[{"type":"thinking","thinking":"PRIVATE-THOUGHT","signature":"PRIVATE-SIGNATURE"},{"type":"redacted_thinking","data":"PRIVATE-REDACTED"},{"type":"text","text":"password=secret 🦊\u001b[2J"},{"type":"tool_use","id":"tool1","name":"Read","input":{"file_path":"/private/credential"}}]}}` + "\n" +
		`{"type":"rate_limit_event","uuid":"u-rate","session_id":"s1","rate_limit_info":{"status":"PRIVATE-LIMIT"}}` + "\n" +
		`{"type":"tool_use_summary","uuid":"u-summary","session_id":"s1","summary":"PRIVATE-SUMMARY"}` + "\n" +
		`{"type":"user","uuid":"u3","session_id":"s1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool1","content":"PRIVATE-OUTPUT"}]}}` + "\n"
	// Exercise split UTF-8 and split JSON boundaries, not just complete writes.
	for _, b := range []byte(stream) {
		if _, err := w.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	got := s.ActivitySnapshot(request, 0)
	data, _ := json.Marshal(got)
	for _, forbidden := range []string{"PRIVATE", "password", "secret", "credential", "tool1", "s1", "thinking", "signature", "\x1b", "🦊"} {
		if bytes.Contains(data, []byte(forbidden)) {
			t.Fatalf("private content projected: %s", data)
		}
	}
	if !got.DetailAvailable || got.LastToolAt.IsZero() || got.LastFrameAt.IsZero() || !bytes.Contains(data, []byte("read_completed")) || !got.ExitedAt.IsZero() {
		t.Fatalf("live tool event missing: %s", data)
	}
	if capture.String() != stream {
		t.Fatal("observation changed final parser input")
	}
}

func TestActivityMalformedSessionAndFramesDisableDetailButNotBytes(t *testing.T) {
	for _, bad := range []string{`{"type":"system","subtype":"init","uuid":"u","session_id":"s","session_id":"other","model":"m"}`, "\xff\n", strings.Repeat("x", activityFrameBytes+1), `{"type":"system","subtype":"init","uuid":"u","session_id":"s","model":"m"}` + "\n" + `{"type":"result","uuid":"u2","session_id":"other"}`} {
		s := &Supervisor{}
		req := contracts.DrainRequest{ClaimID: 1}
		r := s.beginActivity(req)
		r.publish()
		var capture limitedBuffer
		capture.limit = 64 << 10
		w := activityWriter{capture: &capture, record: r, decoder: activityDecoder{provider: "claude", model: "m"}}
		_, _ = w.Write([]byte(bad + "\n"))
		_, _ = w.Write([]byte(`{"type":"result","uuid":"later","session_id":"s"}` + "\n"))
		got := s.ActivitySnapshot(req, 0)
		if got.DetailAvailable || got.Dropped == 0 || !got.Gap || got.StdoutBytes == 0 || got.LastOutputAt.IsZero() {
			t.Fatalf("bad frame accepted: %+v", got)
		}
	}
}

func TestActivityCodexCompletionIsReportedNotVerifiedAndLifetimeBounded(t *testing.T) {
	s := &Supervisor{}
	req := contracts.DrainRequest{ClaimID: 1}
	r := s.beginActivity(req)
	r.publish()
	var capture limitedBuffer
	capture.limit = 64 << 10
	w := activityWriter{capture: &capture, record: r, decoder: activityDecoder{provider: "codex"}}
	for _, frame := range []string{`{"type":"thread.started","thread_id":"session"}`, `{"type":"item.started","item":{"id":"x","type":"command_execution","command":"SECRET"}}`, `{"type":"item.completed","item":{"id":"x","type":"command_execution","aggregated_output":"12 passed","exit_code":0}}`} {
		_, _ = w.Write([]byte(frame + "\n"))
	}
	got := s.ActivitySnapshot(req, 0)
	data, _ := json.Marshal(got)
	if !bytes.Contains(data, []byte("command_completed")) || bytes.Contains(data, []byte("passed")) || bytes.Contains(data, []byte("SECRET")) {
		t.Fatal(string(data))
	}
	for i := 0; i < activitySessionEvents+1; i++ {
		_, _ = w.Write([]byte("{\"type\":\"turn.started\"}\n"))
	}
	got = s.ActivitySnapshot(req, 0)
	if got.DetailAvailable || got.Dropped == 0 || len(got.Events) > activityEvents {
		t.Fatalf("unbounded decode: %+v", got)
	}
}

func TestActivitySnapshotSequenceAndFreshnessRemainMonotonic(t *testing.T) {
	s := &Supervisor{}
	req := contracts.DrainRequest{ClaimID: 1}
	r := s.beginActivity(req)
	r.publish()
	activityAtomicMax(&r.lastOutput, 200)
	activityAtomicMax(&r.lastOutput, 100)
	if r.lastOutput.Load() != 200 {
		t.Fatal("output freshness moved backwards")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			r.observe("read_started", "provider", time.Now())
		}
	}()
	for i := 0; i < 1000; i++ {
		snapshot := s.ActivitySnapshot(req, 0)
		for _, event := range snapshot.Events {
			if event.Sequence > snapshot.Sequence {
				t.Errorf("event outran cursor")
			}
		}
	}
	wg.Wait()
}

func TestSupervisorActivityAppearsBeforeExitAndDrainIsSeparate(t *testing.T) {
	s, req, invocation, input := legacyRunFixture(t, "printf observed; sleep 30")
	t.Cleanup(func() { _ = s.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := s.Run(ctx, req, invocation, input); done <- err }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		snapshot := s.ActivitySnapshot(req, 0)
		if snapshot.Available && snapshot.StdoutBytes > 0 {
			if !snapshot.ExitedAt.IsZero() || !snapshot.DrainedAt.IsZero() {
				t.Fatal("live output mislabeled as drained")
			}
			break
		}
		select {
		case err := <-done:
			t.Fatalf("returned before live observation: %v", err)
		case <-deadline.C:
			t.Fatal("no live bytes observed")
		case <-time.After(time.Millisecond * 5):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not return")
	}
	before := s.ActivitySnapshot(req, 0)
	if before.CancellationAt.IsZero() || before.ExitedAt.IsZero() || !before.DrainedAt.IsZero() {
		t.Fatalf("conflated exit and drain: %+v", before)
	}
	if _, err := s.Drain(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if s.ActivitySnapshot(req, 0).DrainedAt.IsZero() {
		t.Fatal("signed drain proof not observed")
	}
}

func TestRecorderFailureNeverPublishesActivity(t *testing.T) {
	s, req, invocation, input := legacyRunFixture(t, "printf early")
	t.Cleanup(func() { _ = s.Close() })
	s.Recorder = recordingLaunches(func(context.Context, contracts.DrainRequest, Identity, string) error {
		return errors.New("recorder refused")
	})
	if _, err := s.Run(context.Background(), req, invocation, input); err == nil {
		t.Fatal("recorder refusal ignored")
	}
	if got := s.ActivitySnapshot(req, 0); got.Available || len(got.Events) > 0 {
		t.Fatalf("unrecorded launch exposed: %+v", got)
	}
}
