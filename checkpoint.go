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
// drops keys it does not know and zeroes fields that are absent, so a Run
// resumed against a changed shape does not fail — it comes back with a
// half-empty state and carries on as if that were what was written.
//
// Bump this when the meaning or the shape of anything under it changes.
//
// Deliberately NOT bumped when the delegation plan was removed: no deployment
// could ever write those two keys (they were only produced after a Router
// returned RouteDelegate, and no host wired one), so no stored checkpoint can
// contain them and no reader can observe their absence. Bumping would have
// rejected every parked approval checkpoint that does exist.
const checkpointProtocol = 2

// checkpoint is the envelope every resume state is written in.
//
// Approval and applied-write recovery share one versioned resume envelope.
type checkpoint struct {
	// Protocol is mandatory on every non-empty checkpoint. Legacy unversioned
	// rows are upgraded once by the database migration; keeping that conversion
	// here would turn a deployment boundary into permanent dual-protocol logic.
	Protocol uint32 `json:"protocol"`

	Approval json.RawMessage `json:"approval,omitempty"`
	// AppliedWrite is the node whose external write completed before its node
	// result was committed. A worker must not start that node again after a
	// takeover: the agent's in-memory tool loop would issue a new invocation.
	AppliedWrite *appliedWrite `json:"applied_write,omitempty"`
}

type appliedWrite struct {
	Node       string `json:"node"`
	Invocation run.ID `json:"invocation"`
}

func checkpointWithAppliedWrite(raw json.RawMessage, node string, invocation run.ID) (json.RawMessage, error) {
	state, err := decodeCheckpoint(raw)
	if err != nil {
		return nil, err
	}
	state.AppliedWrite = &appliedWrite{Node: node, Invocation: invocation}
	return encodeCheckpoint(state)
}

func checkpointWithoutAppliedWrite(raw json.RawMessage, node string) (json.RawMessage, error) {
	state, err := decodeCheckpoint(raw)
	if err != nil {
		return nil, err
	}
	if state.AppliedWrite == nil || state.AppliedWrite.Node != node {
		return raw, nil
	}
	state.AppliedWrite = nil
	if len(state.Approval) == 0 {
		return nil, nil
	}
	return encodeCheckpoint(state)
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
	if decoded.Protocol != checkpointProtocol {
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
