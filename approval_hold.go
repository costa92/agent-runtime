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
	if len(raw) == 0 {
		return nil
	}
	var wrap struct {
		Approval *approvalHold `json:"approval"`
	}
	if json.Unmarshal(raw, &wrap) != nil || wrap.Approval == nil || wrap.Approval.Tool == "" {
		return nil
	}
	return wrap.Approval
}

func holdToolName(hold *approvalHold) string {
	if hold == nil {
		return ""
	}
	return hold.Tool
}

func encodeApprovalHold(hold approvalHold) json.RawMessage {
	encoded, err := json.Marshal(struct {
		Approval approvalHold `json:"approval"`
	}{Approval: hold})
	if err != nil {
		return nil
	}
	return encoded
}
