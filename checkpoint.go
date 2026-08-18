package agentruntime

import (
	"encoding/json"
	"fmt"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// checkpointProtocol is the version of the Runtime's own resume state.
//
// Snapshot.Checkpoint is the one durable structure the Runtime both writes and
// reads and nothing else understands. Everything else that crosses a process
// boundary is versioned: a Definition pins a protocol, a graph pins a digest,
// the Store's columns are versioned by migrations. The checkpoint had nothing,
// and it is read by whichever deployment happens to resume the Run — which
// during a rolling deploy is routinely not the one that wrote it.
//
// Unversioned, a shape change is silent in the worst direction: json.Unmarshal
// drops keys it does not know and zeroes fields that are absent, so an
// orchestrator resumed against a changed shape does not fail — it comes back
// with an empty plan and no children and starts delegating again.
//
// Bump this when the meaning or the shape of anything under it changes.
const checkpointProtocol = 1

// checkpoint is the envelope every resume state is written in.
//
// The two payloads are the orchestrator's delegation plan and the write parked
// for a human. They shared this field before by not colliding on key names,
// which is an arrangement that holds until someone picks a name twice.
type checkpoint struct {
	// Protocol is absent on everything written before it existed. Zero
	// therefore means protocol 1: the unversioned shape is byte-identical to
	// this one, so an in-flight Run does not have to be abandoned to gain a
	// version number.
	Protocol uint32 `json:"protocol,omitempty"`

	Plan     json.RawMessage `json:"plan,omitempty"`
	Children json.RawMessage `json:"children,omitempty"`
	Approval json.RawMessage `json:"approval,omitempty"`
}

// decodeCheckpoint reads the envelope and refuses one this build cannot read.
//
// Refusing is the point. A newer deployment's checkpoint carries decisions this
// build does not know how to honour, and resuming on the parts it recognises
// would execute a plan nobody wrote. The Run stops instead, and stays resumable
// by the deployment that understands it.
func decodeCheckpoint(raw json.RawMessage) (checkpoint, error) {
	var decoded checkpoint
	if len(raw) == 0 {
		return decoded, nil
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return checkpoint{}, run.NewError("unreadable_checkpoint", run.ErrorInternal, run.RetryNever, err)
	}
	if decoded.Protocol > checkpointProtocol {
		return checkpoint{}, run.NewError("unsupported_checkpoint_protocol",
			run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("checkpoint protocol %d, this build reads %d",
				decoded.Protocol, checkpointProtocol))
	}
	return decoded, nil
}

func encodeCheckpoint(state checkpoint) (json.RawMessage, error) {
	state.Protocol = checkpointProtocol
	encoded, err := json.Marshal(state)
	if err != nil {
		return nil, run.NewError("unencodable_checkpoint", run.ErrorInternal, run.RetryNever, err)
	}
	return encoded, nil
}
