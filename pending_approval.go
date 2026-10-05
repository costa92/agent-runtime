package agentruntime

import (
	"encoding/json"

	"github.com/costa92/agent-runtime/run"
)

// PendingApproval is the read-only, user-presentable part of the exact tool
// invocation parked in a Run. Runtime checkpoint internals remain private.
type PendingApproval struct {
	Tool      string
	Arguments json.RawMessage
}

// InspectPendingApproval returns the undecided tool invocation for a Run that
// is currently parked on human approval. Returned argument bytes are copied so
// callers cannot mutate persisted runtime state.
func InspectPendingApproval(snapshot run.Snapshot) (PendingApproval, bool) {
	if snapshot.State != run.StateWaitingApproval || snapshot.PendingApprovalID == "" {
		return PendingApproval{}, false
	}
	hold := loadApprovalHold(snapshot.Checkpoint)
	if hold == nil || hold.Denied {
		return PendingApproval{}, false
	}
	return PendingApproval{
		Tool:      hold.Tool,
		Arguments: append(json.RawMessage(nil), hold.Arguments...),
	}, true
}
