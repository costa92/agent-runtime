package agentruntime

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// A checkpoint written before the version existed must still resume. The
// unversioned shape is byte-identical to protocol 1, so an in-flight Run does
// not have to be abandoned to gain a version number — and a rolling deploy is
// exactly when there are in-flight Runs written by the older build.
func TestACheckpointWithNoProtocolReadsAsTheFirstOne(t *testing.T) {
	legacy := json.RawMessage(`{"plan":{"children":[{"key":"a"}]},"children":{"a":"run-a"}}`)

	decoded, err := decodeCheckpoint(legacy)
	if err != nil {
		t.Fatalf("an unversioned checkpoint was refused: %v", err)
	}
	if len(decoded.Plan) == 0 || len(decoded.Children) == 0 {
		t.Fatalf("the unversioned payload was dropped: %+v", decoded)
	}

	hold := loadApprovalHold(json.RawMessage(`{"approval":{"tool":"publish_article"}}`))
	if hold == nil || hold.Tool != "publish_article" {
		t.Fatalf("an unversioned approval hold did not survive: %+v", hold)
	}
}

// A checkpoint from a newer build carries decisions this one cannot honour.
// Resuming on the parts it happens to recognise would execute a plan nobody
// wrote — json.Unmarshal drops unknown keys and zeroes absent fields, so the
// orchestrator would come back with an empty plan and delegate all over again.
func TestACheckpointFromANewerProtocolIsRefusedRatherThanPartlyRead(t *testing.T) {
	newer := json.RawMessage(`{"protocol":99,"plan":{"children":[{"key":"a"}]}}`)

	_, err := decodeCheckpoint(newer)
	if err == nil {
		t.Fatal("a checkpoint from a newer protocol was accepted")
	}
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) || runtimeError.Code != "unsupported_checkpoint_protocol" {
		t.Fatalf("error = %v, want unsupported_checkpoint_protocol", err)
	}
	if kind := run.KindOf(err); kind != run.ErrorInvalid {
		t.Fatalf("kind = %s, want invalid", kind)
	}
}

// What this build writes, this build reads — and it is stamped, so the next one
// can tell what it is looking at.
func TestWhatIsWrittenCarriesTheProtocolAndRoundTrips(t *testing.T) {
	encoded, err := encodeCheckpoint(checkpoint{Plan: json.RawMessage(`{"children":[]}`)})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var stamped struct {
		Protocol uint32 `json:"protocol"`
	}
	if err := json.Unmarshal(encoded, &stamped); err != nil {
		t.Fatal(err)
	}
	if stamped.Protocol != checkpointProtocol {
		t.Fatalf("protocol = %d, want %d", stamped.Protocol, checkpointProtocol)
	}
	if _, err := decodeCheckpoint(encoded); err != nil {
		t.Fatalf("this build refused its own checkpoint: %v", err)
	}
}

func TestInspectPendingApprovalReturnsOnlyAnUndecidedWaitingHold(t *testing.T) {
	checkpoint := encodeApprovalHold(approvalHold{
		Tool:      "publish_article",
		Arguments: json.RawMessage(`{"article_id":7,"account_id":9}`),
	})
	snapshot := run.Snapshot{
		State:             run.StateWaitingApproval,
		PendingApprovalID: "approval-1",
		Checkpoint:        checkpoint,
	}

	pending, ok := InspectPendingApproval(snapshot)
	if !ok {
		t.Fatal("pending approval was not exposed")
	}
	if pending.Tool != "publish_article" || string(pending.Arguments) != `{"article_id":7,"account_id":9}` {
		t.Fatalf("pending = %+v", pending)
	}

	// The inspection result must not alias the persisted checkpoint.
	pending.Arguments[0] = '['
	again, ok := InspectPendingApproval(snapshot)
	if !ok || string(again.Arguments) != `{"article_id":7,"account_id":9}` {
		t.Fatalf("inspection mutated persisted arguments: %+v", again)
	}

	for name, mutate := range map[string]func(*run.Snapshot){
		"not waiting":      func(s *run.Snapshot) { s.State = run.StateRunning },
		"no approval id":   func(s *run.Snapshot) { s.PendingApprovalID = "" },
		"malformed hold":   func(s *run.Snapshot) { s.Checkpoint = json.RawMessage(`{"approval":[]}`) },
		"unsupported hold": func(s *run.Snapshot) { s.Checkpoint = json.RawMessage(`{"protocol":99,"approval":{}}`) },
		"denied approval": func(s *run.Snapshot) {
			s.Checkpoint = encodeApprovalHold(approvalHold{Tool: "publish_article", Denied: true})
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := snapshot
			mutate(&candidate)
			if _, ok := InspectPendingApproval(candidate); ok {
				t.Fatal("non-pending hold was exposed")
			}
		})
	}
}
