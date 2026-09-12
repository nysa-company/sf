package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nysa-company/sf/internal/api"
	"github.com/nysa-company/sf/internal/contracts"
	"github.com/nysa-company/sf/internal/domain"
	"github.com/nysa-company/sf/internal/transport"
)

type daemonAuthoringFixture struct {
	signer         *contracts.DrainSigner
	runs           atomic.Int32
	entered        chan contracts.AuthoringInput
	release        chan struct{}
	uncertain      bool
	prepareEntered chan struct{}
	prepareRelease chan struct{}
	ignoreCancel   bool
}

func (f *daemonAuthoringFixture) PrepareAuthoring(ctx context.Context, _ string) (contracts.AuthoringCapability, error) {
	if f.prepareEntered != nil {
		close(f.prepareEntered)
		select {
		case <-f.prepareRelease:
		case <-ctx.Done():
			return contracts.AuthoringCapability{}, ctx.Err()
		}
	}
	digest := contracts.AuthoringDigest([]byte("fixture"))
	return contracts.AuthoringCapability{Identity: domain.ProviderIdentity{Provider: "claude", Model: "model", Family: "claude", Version: "1"}, BinaryDigest: digest, AuthDigest: digest, PolicyDigest: digest}, nil
}
func (f *daemonAuthoringFixture) RunAuthoring(ctx context.Context, claim contracts.AuthoringClaim, input contracts.AuthoringInput, record func(context.Context, contracts.ProviderLaunch) error) (contracts.AuthoringResult, contracts.AuthoringProof, error) {
	f.runs.Add(1)
	f.entered <- input
	if f.ignoreCancel {
		<-f.release
	} else {
		select {
		case <-ctx.Done():
		case <-f.release:
		}
	}
	if f.uncertain {
		return contracts.AuthoringResult{}, contracts.AuthoringProof{}, errors.New("token: must-not-leak")
	}
	proof, _ := f.signer.ProveAuthoringDrained(claim, claim.LeaderEpoch)
	if ctx.Err() != nil {
		return contracts.AuthoringResult{}, proof, ctx.Err()
	}
	if err := record(ctx, contracts.ProviderLaunch{PID: 123, PGID: 123, BootIdentity: "fixture-boot", ProcessStartIdentity: "fixture-start", Worktree: "/private/tmp/sf-authoring-fixture"}); err != nil {
		return contracts.AuthoringResult{}, proof, err
	}
	return contracts.AuthoringResult{Kind: "question", Question: "token: must-not-leak"}, proof, nil
}
func (f *daemonAuthoringFixture) RecoverAuthoring(_ context.Context, claim contracts.AuthoringClaim, _ contracts.ProviderLaunch, epoch uint64) (contracts.AuthoringProof, error) {
	if f.uncertain {
		return contracts.AuthoringProof{}, errors.New("drain unknown")
	}
	return f.signer.ProveAuthoringDrained(claim, epoch)
}
func installAuthoringFixture(t *testing.T, d *Daemon) *daemonAuthoringFixture {
	t.Helper()
	signer, err := contracts.NewDrainSigner()
	if err != nil {
		t.Fatal(err)
	}
	if err := d.store.SetRecoveryAuthority(context.Background(), d.channel, d.epoch, signer.PublicKey()); err != nil {
		t.Fatal(err)
	}
	f := &daemonAuthoringFixture{signer: signer, entered: make(chan contracts.AuthoringInput, 8), release: make(chan struct{})}
	d.authoringSupervisor = f
	return f
}
func authoringDispatch(d *Daemon, method string, parameters map[string]any) api.Response {
	parameters["channel"] = d.channel
	raw, _ := json.Marshal(parameters)
	return d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid())}, api.Request{Version: api.Version, RequestID: "authoring-test", Method: method, Parameters: raw})
}
func authoringCreateFixture(t *testing.T, d *Daemon, files []string) (string, string) {
	t.Helper()
	response := authoringDispatch(d, "authoring.create", map[string]any{"project": "demo", "model": "model", "purpose": "ticket_draft", "context_files": files})
	var data struct {
		Session struct {
			ID     string `json:"id"`
			Digest string `json:"context_digest"`
		} `json:"authoring_session"`
	}
	if !response.OK || json.Unmarshal(response.Data, &data) != nil || data.Session.ID == "" {
		t.Fatalf("create: %+v", response)
	}
	return data.Session.ID, data.Session.Digest
}
func authoringAwait(t *testing.T, d *Daemon, session, key string) api.Response {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		response := authoringDispatch(d, "authoring.status", map[string]any{"session": session, "key": key})
		if !response.OK {
			t.Fatalf("status: %+v", response)
		}
		var data struct {
			Turn struct{ State string } `json:"authoring_turn"`
		}
		_ = json.Unmarshal(response.Data, &data)
		if data.Turn.State == "completed" || data.Turn.State == "uncertain" {
			return response
		}
		select {
		case <-deadline:
			t.Fatal("worker did not finish")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestAuthoringOwnerDispatchCreateNoInferenceAndAsyncFrozenSnapshot(t *testing.T) {
	d, paths, _ := testDaemon(t)
	f := installAuthoringFixture(t, d)
	root := filepath.Join(paths.Root, "repo")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "reference.txt")
	if err := os.WriteFile(file, []byte("approved original"), 0600); err != nil {
		t.Fatal(err)
	}
	raw := json.RawMessage(`{"channel":"stable","project":"demo","model":"model","purpose":"ticket_draft","context_files":[]}`)
	denied := d.Handle(context.Background(), transport.Peer{UID: uint32(os.Getuid() + 1)}, api.Request{Version: api.Version, RequestID: "foreign", Method: "authoring.create", Parameters: raw})
	if denied.OK || denied.Error.Code != "operator_identity_required" {
		t.Fatal("foreign owner admitted")
	}
	session, digest := authoringCreateFixture(t, d, []string{"reference.txt"})
	if f.runs.Load() != 0 {
		t.Fatal("create inferred")
	}
	if err := os.WriteFile(file, []byte("changed after approval"), 0600); err != nil {
		t.Fatal(err)
	}
	p := map[string]any{"session": session, "key": "one", "prompt": "draft please", "context_digest": digest}
	response := authoringDispatch(d, "authoring.turn", p)
	if !response.OK {
		t.Fatal(response)
	}
	select {
	case input := <-f.entered:
		if !strings.Contains(input.Context, "approved original") || strings.Contains(input.Context, "changed after") {
			t.Fatal("context reread")
		}
	case <-time.After(time.Second):
		t.Fatal("worker not dispatched")
	}
	if replay := authoringDispatch(d, "authoring.turn", p); !replay.OK {
		t.Fatal(replay)
	}
	if f.runs.Load() != 1 {
		t.Fatal("same-key redispatch")
	}
	if other := authoringDispatch(d, "authoring.turn", map[string]any{"session": session, "key": "two", "prompt": "draft please", "context_digest": digest}); other.OK {
		t.Fatal("concurrent channel turn admitted")
	}
	close(f.release)
	finished := authoringAwait(t, d, session, "one")
	if strings.Contains(string(finished.Data), "must-not-leak") || !strings.Contains(string(finished.Data), `"success"`) {
		t.Fatalf("result not sanitized: %s", finished.Data)
	}
}

func TestAuthoringCancellationAndShutdownOwnStartupWorker(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "close"}[shutdown], func(t *testing.T) {
			d, _, _ := testDaemon(t)
			f := installAuthoringFixture(t, d)
			session, digest := authoringCreateFixture(t, d, nil)
			if r := authoringDispatch(d, "authoring.turn", map[string]any{"session": session, "key": "one", "prompt": "draft", "context_digest": digest}); !r.OK {
				t.Fatal(r)
			}
			select {
			case <-f.entered:
			case <-time.After(time.Second):
				t.Fatal("startup not entered")
			}
			if shutdown {
				done := make(chan error, 1)
				go func() { done <- d.Close() }()
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("shutdown did not join authoring")
				}
			} else {
				if r := authoringDispatch(d, "authoring.cancel", map[string]any{"session": session, "key": "one"}); !r.OK {
					t.Fatal(r)
				}
				finished := authoringAwait(t, d, session, "one")
				if !strings.Contains(string(finished.Data), `"cancelled"`) {
					t.Fatal(string(finished.Data))
				}
			}
		})
	}
}

func TestAuthoringRequestBoundsDigestExpiryAndSessionCapacity(t *testing.T) {
	d, _, _ := testDaemon(t)
	f := installAuthoringFixture(t, d)
	session, digest := authoringCreateFixture(t, d, nil)
	for _, p := range []map[string]any{
		{"session": session, "key": "wrong-digest", "prompt": "draft", "context_digest": strings.Repeat("0", 64)},
		{"session": session, "key": "oversized", "prompt": strings.Repeat("x", 16<<10+1), "context_digest": digest},
	} {
		if r := authoringDispatch(d, "authoring.turn", p); r.OK {
			t.Fatal("unsafe request reserved")
		}
	}
	for i := 1; i < authoringSessionCapacity; i++ {
		authoringCreateFixture(t, d, nil)
	}
	if r := authoringDispatch(d, "authoring.create", map[string]any{"project": "demo", "model": "model", "purpose": "ticket_draft", "context_files": []string{}}); r.OK {
		t.Fatal("unbounded sessions")
	}
	d.authoringMu.Lock()
	d.authoringSessions[session].expires = time.Now().Add(-time.Second)
	d.authoringMu.Unlock()
	if r := authoringDispatch(d, "authoring.turn", map[string]any{"session": session, "key": "expired", "prompt": "draft", "context_digest": digest}); r.OK {
		t.Fatal("expired snapshot admitted")
	}
	if f.runs.Load() != 0 {
		t.Fatal("invalid request launched")
	}
}

func TestAuthoringRestartNeverResendsAndQuarantinePreservesTicketReads(t *testing.T) {
	d, paths, cancel := testDaemon(t)
	f := installAuthoringFixture(t, d)
	session, digest := authoringCreateFixture(t, d, nil)
	snapshot := d.authoringSessions[session]
	input := contracts.AuthoringInput{Purpose: "ticket_draft", Prompt: "approved", Context: snapshot.context}
	if _, _, err := d.store.ReserveAuthoringTurn(context.Background(), snapshot.session, "pending", contracts.AuthoringInputDigest(input), d.epoch); err != nil {
		t.Fatal(err)
	}
	auth := d.auth
	cancel()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	f.uncertain = true
	restarted, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "authoring-restart", Operator: auth, AuthoringSupervisor: f, RecoveryAuthorityKey: f.signer.PublicKey()})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if r := authoringDispatch(restarted, "ticket.status", map[string]any{}); !r.OK {
		t.Fatal("authoring quarantine blocked ordinary ticket reads")
	}
	if r := authoringDispatch(restarted, "authoring.turn", map[string]any{"session": session, "key": "pending", "prompt": "approved", "context_digest": digest}); r.OK {
		t.Fatal("restart reused missing snapshot")
	}
	if r := authoringDispatch(restarted, "authoring.create", map[string]any{"project": "demo", "model": "model", "purpose": "ticket_draft"}); r.OK {
		t.Fatal("quarantine admitted capability")
	}
	if f.runs.Load() != 0 {
		t.Fatal("recovery resent inference")
	}
	if r := authoringDispatch(restarted, "authoring.status", map[string]any{"session": session, "key": "pending"}); !r.OK || !strings.Contains(string(r.Data), `"uncertain"`) {
		t.Fatal(r)
	}
}

func TestAuthoringMalformedDurableClaimPreventsStartup(t *testing.T) {
	d, paths, cancel := testDaemon(t)
	f := installAuthoringFixture(t, d)
	session, _ := authoringCreateFixture(t, d, nil)
	snapshot := d.authoringSessions[session]
	input := contracts.AuthoringInput{Purpose: "ticket_draft", Prompt: "approved", Context: snapshot.context}
	if _, _, err := d.store.ReserveAuthoringTurn(context.Background(), snapshot.session, "pending", contracts.AuthoringInputDigest(input), d.epoch); err != nil {
		t.Fatal(err)
	}
	auth := d.auth
	cancel()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	writer, err := sql.Open("sqlite", paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`DROP TRIGGER authoring_turn_identity_immutable`); err != nil {
		writer.Close()
		t.Fatal(err)
	}
	if _, err := writer.Exec(`UPDATE authoring_turns SET claim='{}'`); err != nil {
		writer.Close()
		t.Fatal(err)
	}
	writer.Close()
	restarted, err := Start(context.Background(), Config{Channel: domain.ChannelStable, Paths: paths, DaemonIdentity: "authoring-malformed", Operator: auth, AuthoringSupervisor: f, RecoveryAuthorityKey: f.signer.PublicKey()})
	if err == nil {
		restarted.Close()
		t.Fatal("malformed authoring authority admitted startup")
	}
}

func TestAuthoringSlowCreateCannotBlockKnownTurnCancellation(t *testing.T) {
	d, _, _ := testDaemon(t)
	f := installAuthoringFixture(t, d)
	session, digest := authoringCreateFixture(t, d, nil)
	if r := authoringDispatch(d, "authoring.turn", map[string]any{"session": session, "key": "one", "prompt": "draft", "context_digest": digest}); !r.OK {
		t.Fatal(r)
	}
	select {
	case <-f.entered:
	case <-time.After(time.Second):
		t.Fatal("worker missing")
	}
	f.prepareEntered = make(chan struct{})
	f.prepareRelease = make(chan struct{})
	defer close(f.prepareRelease)
	created := make(chan api.Response, 1)
	go func() {
		created <- authoringDispatch(d, "authoring.create", map[string]any{"project": "demo", "model": "model", "purpose": "ticket_draft"})
	}()
	select {
	case <-f.prepareEntered:
	case <-time.After(time.Second):
		t.Fatal("prepare missing")
	}
	cancelled := make(chan api.Response, 1)
	go func() {
		cancelled <- authoringDispatch(d, "authoring.cancel", map[string]any{"session": session, "key": "one"})
	}()
	select {
	case r := <-cancelled:
		if !r.OK {
			t.Fatal(r)
		}
	case <-time.After(time.Second):
		t.Fatal("prepare blocked independent cancellation")
	}
	if r := authoringAwait(t, d, session, "one"); !strings.Contains(string(r.Data), `"cancelled"`) {
		t.Fatal(r)
	}
}

func TestAuthoringRequestDisconnectDoesNotCancelAndFourTurnsRemainBounded(t *testing.T) {
	d, _, _ := testDaemon(t)
	f := installAuthoringFixture(t, d)
	session, digest := authoringCreateFixture(t, d, nil)
	ctx, cancel := context.WithCancel(context.Background())
	raw, _ := json.Marshal(map[string]any{"channel": d.channel, "session": session, "key": "one", "prompt": "draft", "context_digest": digest})
	r := d.Handle(ctx, transport.Peer{UID: uint32(os.Getuid())}, api.Request{Version: api.Version, RequestID: "disconnect", Method: "authoring.turn", Parameters: raw})
	cancel()
	if !r.OK {
		t.Fatal(r)
	}
	select {
	case <-f.entered:
	case <-time.After(time.Second):
		t.Fatal("worker missing")
	}
	close(f.release)
	if r := authoringAwait(t, d, session, "one"); !strings.Contains(string(r.Data), `"success"`) {
		t.Fatal("request disconnect cancelled turn")
	}
	d.authoringWG.Wait()
	for _, key := range []string{"two", "three", "four"} {
		if r := authoringDispatch(d, "authoring.turn", map[string]any{"session": session, "key": key, "prompt": "draft", "context_digest": digest}); !r.OK {
			t.Fatal(r)
		}
		authoringAwait(t, d, session, key)
		d.authoringWG.Wait()
	}
	if r := authoringDispatch(d, "authoring.turn", map[string]any{"session": session, "key": "five", "prompt": "draft", "context_digest": digest}); r.OK {
		t.Fatal("fifth turn admitted")
	}
	if f.runs.Load() != 4 {
		t.Fatal("wrong inference count")
	}
}

func TestAuthoringUnknownDrainPreservesChannelSlotAndHidesProviderErrors(t *testing.T) {
	d, _, _ := testDaemon(t)
	f := installAuthoringFixture(t, d)
	f.uncertain = true
	close(f.release)
	session, digest := authoringCreateFixture(t, d, nil)
	if r := authoringDispatch(d, "authoring.turn", map[string]any{"session": session, "key": "one", "prompt": "draft", "context_digest": digest}); !r.OK {
		t.Fatal(r)
	}
	r := authoringAwait(t, d, session, "one")
	if !strings.Contains(string(r.Data), `"uncertain"`) || strings.Contains(string(r.Data), "must-not-leak") {
		t.Fatal(r)
	}
	if r := authoringDispatch(d, "authoring.turn", map[string]any{"session": session, "key": "two", "prompt": "draft", "context_digest": digest}); r.OK {
		t.Fatal("undrained turn released slot")
	}
}

func TestAuthoringLateShutdownRetainsThenReleasesOwnedStore(t *testing.T) {
	d, _, _ := testDaemon(t)
	f := installAuthoringFixture(t, d)
	f.ignoreCancel = true
	d.authoringShutdownTimeout = 10 * time.Millisecond
	session, digest := authoringCreateFixture(t, d, nil)
	if r := authoringDispatch(d, "authoring.turn", map[string]any{"session": session, "key": "one", "prompt": "draft", "context_digest": digest}); !r.OK {
		t.Fatal(r)
	}
	select {
	case <-f.entered:
	case <-time.After(time.Second):
		t.Fatal("worker missing")
	}
	if err := d.Close(); err == nil {
		t.Fatal("late drain falsely reported complete")
	}
	if _, err := d.store.Project(context.Background(), d.channel, "demo"); err != nil {
		t.Fatal("store closed before worker joined")
	}
	if err := d.lease.Validate(); err != nil {
		t.Fatal("leader authority released before worker joined")
	}
	close(f.release)
	deadline := time.After(3 * time.Second)
	for {
		if _, err := d.store.Project(context.Background(), d.channel, "demo"); err != nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("late worker completion leaked owned store")
		case <-time.After(time.Millisecond):
		}
	}
	if err := d.Close(); err == nil {
		t.Fatal("cached initial shutdown uncertainty was erased")
	}
}
