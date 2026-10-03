package agentruntime

import "encoding/json"

// approvalHold is the write a human has been asked to allow.
//
// It lives in Snapshot.Checkpoint so a worker that picks the Run up after
// confirm can perform that exact effect instead of asking the model again.
//
// Denied marks a hold the human refused. The Run resumes — refusing one
// effect is not a reason to fail the whole Run — but the denied write is
// reported back to the agent as a refusal and never granted, so the agent
// can answer without the effect instead of re-asking for it.
type approvalHold struct {
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
	Denied    bool            `json:"denied,omitempty"`
}

func loadApprovalHold(raw json.RawMessage) *approvalHold {
	// A checkpoint this build cannot read is not a missing hold. The caller
	// checks the protocol before resuming; here an unreadable envelope simply
	// yields no hold, which is what the absent case already meant.
	envelope, err := decodeCheckpoint(raw)
	if err != nil || len(envelope.Approval) == 0 {
		return nil
	}
	var hold approvalHold
	if json.Unmarshal(envelope.Approval, &hold) != nil || hold.Tool == "" {
		return nil
	}
	return &hold
}

func holdToolName(hold *approvalHold) string {
	if hold == nil {
		return ""
	}
	return hold.Tool
}

func encodeApprovalHold(raw json.RawMessage, hold approvalHold) (json.RawMessage, error) {
	payload, err := json.Marshal(hold)
	if err != nil {
		return nil, err
	}
	state, err := decodeCheckpoint(raw)
	if err != nil {
		return nil, err
	}
	state.Approval = payload
	return encodeCheckpoint(state)
}
