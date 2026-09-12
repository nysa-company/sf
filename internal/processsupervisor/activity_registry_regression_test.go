package processsupervisor

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/contracts"
)

func activityTestRequest(id int64) contracts.DrainRequest {
	return contracts.DrainRequest{ClaimID: id}
}

func TestActivityCountersContinueAfterBoundedCaptureTruncates(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	request := activityTestRequest(1)
	record := supervisor.beginActivity(request)
	if record == nil {
		t.Fatal("activity record was not allocated")
	}
	record.publish()
	var capture limitedBuffer
	capture.limit = 64 << 10
	writer := &activityWriter{capture: &capture, record: record}
	payload := bytes.Repeat([]byte("x"), capture.limit+1024)
	if n, err := writer.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("write n=%d err=%v", n, err)
	}
	snapshot := supervisor.ActivitySnapshot(request, 0)
	if !snapshot.Available || !capture.exceeded() || snapshot.StdoutBytes != uint64(len(payload)) || snapshot.StdoutBytes <= uint64(capture.Len()) {
		t.Fatalf("capture=%d truncated=%t snapshot=%+v", capture.Len(), capture.exceeded(), snapshot)
	}
}

func TestUnpublishedActivityIsNotVisible(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	request := activityTestRequest(2)
	record := supervisor.beginActivity(request)
	record.observe("process_started", "sf", time.Now().UTC())
	snapshot := supervisor.ActivitySnapshot(request, 0)
	if snapshot.Available || len(snapshot.Events) != 0 || snapshot.StdoutBytes != 0 {
		t.Fatalf("unpublished activity leaked: %+v", snapshot)
	}
}

func TestActivitySnapshotRequiresExactFullRequestKey(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	request := activityTestRequest(3)
	record := supervisor.beginActivity(request)
	record.publish()
	mutations := []struct {
		name string
		edit func(*contracts.DrainRequest)
	}{
		{"claim ID", func(value *contracts.DrainRequest) { value.ClaimID++ }},
		{"request digest", func(value *contracts.DrainRequest) { value.RequestDigest = "changed" }},
		{"leader epoch", func(value *contracts.DrainRequest) { value.LeaderEpoch++ }},
		{"runner epoch", func(value *contracts.DrainRequest) { value.RunnerEpoch++ }},
		{"expected version", func(value *contracts.DrainRequest) { value.ExpectedVersion++ }},
		{"project", func(value *contracts.DrainRequest) { value.Ref.Project = "other" }},
		{"binding digest", func(value *contracts.DrainRequest) { value.BindingDigest = "changed" }},
		{"lease key", func(value *contracts.DrainRequest) { value.LeaseKey = "changed" }},
		{"model", func(value *contracts.DrainRequest) { value.Identity.Model = "changed" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			wrong := request
			mutation.edit(&wrong)
			if snapshot := supervisor.ActivitySnapshot(wrong, 0); snapshot.Available || len(snapshot.Events) != 0 {
				t.Fatalf("mismatched request exposed activity: %+v", snapshot)
			}
		})
	}
}

func TestActivitySnapshotIsIndependentOfRegistryStorage(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	request := activityTestRequest(4)
	record := supervisor.beginActivity(request)
	record.publish()
	record.observe("command_completed", "provider", time.Now().UTC())
	first := supervisor.ActivitySnapshot(request, 0)
	if len(first.Events) == 0 {
		t.Fatal("missing activity event")
	}
	first.Events[0].Kind = "tampered"
	first.Events = append(first.Events, contracts.ActivityEvent{Kind: "injected"})
	second := supervisor.ActivitySnapshot(request, 0)
	if strings.Contains(second.Events[0].Kind, "tampered") || len(second.Events) != len(first.Events)-1 {
		t.Fatalf("registry storage was mutated through snapshot: first=%+v second=%+v", first, second)
	}
}

func TestActivityWriterKeepsStderrOutOfProviderFrameDecoder(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	request := activityTestRequest(5)
	record := supervisor.beginActivity(request)
	record.publish()
	var stdout, stderr limitedBuffer
	stdout.limit, stderr.limit = 64<<10, 64<<10
	stdoutWriter := &activityWriter{capture: &stdout, record: record, decoder: activityDecoder{provider: "codex"}}
	stderrWriter := &activityWriter{capture: &stderr, record: record, stderr: true, decoder: activityDecoder{provider: "codex"}}
	frame := []byte(`{"type":"thread.started","thread_id":"thread-1"}` + "\n")
	if _, err := stdoutWriter.Write(frame); err != nil {
		t.Fatal(err)
	}
	if _, err := stderrWriter.Write(frame); err != nil {
		t.Fatal(err)
	}
	snapshot := supervisor.ActivitySnapshot(request, 0)
	if !snapshot.DetailAvailable || snapshot.StdoutBytes != uint64(len(frame)) || snapshot.StderrBytes != uint64(len(frame)) {
		t.Fatalf("unexpected counters/detail: %+v", snapshot)
	}
	for _, event := range snapshot.Events {
		if event.Source == "provider" && event.Kind != "session_started" {
			t.Fatalf("stderr influenced provider events: %+v", snapshot.Events)
		}
	}
}

func TestActivityDecoderDropsOverlongFramesWithoutLeakingPayload(t *testing.T) {
	supervisor, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	request := activityTestRequest(6)
	record := supervisor.beginActivity(request)
	record.publish()
	var capture limitedBuffer
	capture.limit = 64 << 10
	writer := &activityWriter{capture: &capture, record: record, decoder: activityDecoder{provider: "codex"}}
	secret := "OVERLONG-ACTIVITY-SHOULD-NOT-BE-PROJECTED"
	frame := append(bytes.Repeat([]byte("a"), activityFrameBytes), []byte(secret+"\n")...)
	if _, err := writer.Write(frame); err != nil {
		t.Fatal(err)
	}
	snapshot := supervisor.ActivitySnapshot(request, 0)
	if snapshot.Dropped == 0 || snapshot.DetailAvailable {
		t.Fatalf("overlong frame handling leaked detail: %+v", snapshot)
	}
	for _, event := range snapshot.Events {
		if strings.Contains(event.Kind, secret) {
			t.Fatalf("overlong payload leaked into event: %+v", event)
		}
	}
}
