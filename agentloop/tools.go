package agentloop

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/costa92/agent-runtime/capability"
	"github.com/costa92/agent-runtime/session"
)

// advanceTool handles one declared model call. It never invokes a tool whose
// begin fact is already present: that case needs outcome reconciliation.
func (l *Loop) advanceTool(ctx context.Context, lease session.Lease, p session.Projection, t session.Turn, step session.Step) (bool, error) {
	if l.Tools == nil {
		return false, errors.New("agentloop: tool service unavailable for a declared call")
	}
	for _, declaration := range step.DeclaredTools {
		state, started := step.Tools[declaration.ID]
		if started {
			if state.Result == nil || state.Result.Outcome == string(capability.Unknown) {
				return false, ErrParked
			}
			continue
		}
		allowed := false
		for _, advertised := range step.AllowedTools {
			if advertised == declaration.Name {
				allowed = true
				break
			}
		}
		if !allowed {
			_, err := l.commit(ctx, lease, p, fact{session.StepEnded, session.StepEnd{TurnID: t.ID, StepID: step.ID, Status: "failed", Output: json.RawMessage(`{}`), Warnings: []string{"model declared a tool outside the stage allowance"}}})
			return true, err
		}
		inv := capability.Invocation{
			ID: declaration.ID, Name: declaration.Name, SessionID: p.ID,
			TurnID: t.ID, StepID: step.ID, Principal: p.PrincipalID,
			Input: declaration.Arguments, IdempotencyKey: p.ID + ":" + declaration.ID,
			Fence: lease.Fence,
		}
		for _, approval := range step.Approvals {
			if approval.Request.ToolCallID != declaration.ID {
				continue
			}
			if approval.Decision == nil {
				return false, ErrParked
			}
			if approval.Decision.Decision == "denied" {
				_, err := l.commit(ctx, lease, p, fact{session.StepEnded, session.StepEnd{TurnID: t.ID, StepID: step.ID, Status: "failed", Output: json.RawMessage(`{}`), Warnings: []string{"tool approval denied"}}})
				return true, err
			}
			inv.ApprovalID = approval.Request.ID
		}
		prepared, err := l.Tools.Prepare(ctx, inv)
		if err != nil {
			return false, err
		}
		if prepared.Status() == capability.AwaitingApproval {
			if inv.ApprovalID != "" {
				return false, errors.New("agentloop: approved invocation was parked again")
			}
			approvalID, err := l.id()
			if err != nil {
				return false, err
			}
			admitted := prepared.Invocation()
			if !bytes.Equal(admitted.Input, declaration.Arguments) {
				return false, errors.New("agentloop: tool interceptor changed declared arguments")
			}
			_, err = l.commit(ctx, lease, p, fact{session.ApprovalAsked, session.ApprovalRequest{
				TurnID: t.ID, StepID: step.ID, ID: approvalID, ToolCallID: declaration.ID,
				ToolName: declaration.Name, Arguments: string(admitted.Input),
				IdempotencyKey: admitted.IdempotencyKey, InvocationFingerprint: prepared.ApprovalFingerprint(),
			}})
			if err != nil {
				return false, err
			}
			return false, ErrParked
		}
		admitted := prepared.Invocation()
		if !bytes.Equal(admitted.Input, declaration.Arguments) {
			return false, errors.New("agentloop: tool interceptor changed declared arguments")
		}
		result, err := l.Tools.Execute(ctx, prepared, &toolRecorder{loop: l, lease: lease})
		if err != nil {
			return false, err
		}
		if result.Status == capability.Unknown {
			return false, ErrParked
		}
		return true, nil
	}
	return false, nil
}

type toolRecorder struct {
	loop  *Loop
	lease session.Lease
}

func (r *toolRecorder) Begin(ctx context.Context, inv capability.Invocation, spec capability.Spec, claim capability.BudgetClaim) (capability.BeginReceipt, error) {
	if inv.SessionID != r.lease.SessionID || inv.Fence != r.lease.Fence {
		return capability.BeginReceipt{}, session.ErrLease
	}
	if claim.Dimension != string(session.BudgetToolCalls) || claim.Units != 1 {
		return capability.BeginReceipt{}, errors.New("agentloop: unsupported tool budget claim")
	}
	p, err := r.loop.Store.Inspect(ctx, inv.SessionID)
	if err != nil {
		return capability.BeginReceipt{}, err
	}
	facts := []fact{{session.ToolCalled, session.ToolCall{
		TurnID: inv.TurnID, StepID: inv.StepID, ID: inv.ID, Name: inv.Name,
		Arguments: string(inv.Input), IdempotencyKey: inv.IdempotencyKey,
		ApprovalID: inv.ApprovalID,
	}}, {session.BudgetSettled, session.BudgetSettlement{TurnID: inv.TurnID, StepID: inv.StepID, Category: session.BudgetToolCalls, Units: claim.Units}}}
	if spec.MediaOps > 0 {
		facts = append(facts, fact{session.BudgetSettled, session.BudgetSettlement{TurnID: inv.TurnID, StepID: inv.StepID, Category: session.BudgetMediaOps, Units: spec.MediaOps}})
	}
	if inv.ApprovalID != "" {
		facts[0].Payload = session.ToolCall{
			TurnID: inv.TurnID, StepID: inv.StepID, ID: inv.ID, Name: inv.Name,
			Arguments: string(inv.Input), IdempotencyKey: inv.IdempotencyKey,
			ApprovalID: inv.ApprovalID, InvocationFingerprint: inv.Fingerprint(),
		}
	}
	if _, err := r.loop.commit(ctx, r.lease, p, facts...); err != nil {
		return capability.BeginReceipt{}, err
	}
	return capability.BeginReceipt{Recorded: true, Reserved: true}, nil
}

func (r *toolRecorder) Settle(ctx context.Context, inv capability.Invocation, receipt capability.BeginReceipt, result capability.Result) error {
	if !receipt.Recorded || !receipt.Reserved {
		return capability.ErrBeginUnconfirmed
	}
	if inv.SessionID != r.lease.SessionID || inv.Fence != r.lease.Fence {
		return session.ErrLease
	}
	p, err := r.loop.Store.Inspect(ctx, inv.SessionID)
	if err != nil {
		return err
	}
	code := result.ErrorCode
	if result.Status == capability.Failed && code == "" {
		code = "tool_failed"
	}
	if result.Status == capability.Unknown && code == "" {
		code = "outcome_unknown"
	}
	output := string(result.Output)
	if result.Status == capability.Unknown {
		output = ""
	}
	_, err = r.loop.commit(ctx, r.lease, p, fact{session.ToolSettled, session.ToolResult{
		TurnID: inv.TurnID, StepID: inv.StepID, ID: inv.ID,
		Outcome: string(result.Status), Result: output, Error: code, CostUsages: result.CostUsages,
	}})
	return err
}

// ResolveApproval records the human decision under a newly claimed lease.
// The caller must authenticate the decision maker before invoking this method.
func (l *Loop) ResolveApproval(ctx context.Context, lease session.Lease, approvalID, decision string) error {
	if decision != "approved" && decision != "denied" {
		return errors.New("agentloop: invalid approval decision")
	}
	p, err := l.Store.Inspect(ctx, lease.SessionID)
	if err != nil {
		return err
	}
	if len(p.Turns) == 0 {
		return session.ErrConflict
	}
	t := p.Turns[len(p.Turns)-1]
	if t.Status != "active" || len(t.Steps) == 0 {
		return session.ErrConflict
	}
	s := t.Steps[len(t.Steps)-1]
	if _, ok := s.Approvals[approvalID]; !ok {
		return session.ErrNotFound
	}
	_, err = l.commit(ctx, lease, p, fact{session.ApprovalResolved, session.ApprovalDecision{TurnID: t.ID, StepID: s.ID, ID: approvalID, Decision: decision}})
	return err
}

// ReconcileTool records a verified final outcome for a pending or unknown
// external effect. The caller must verify the provider result before calling.
func (l *Loop) ReconcileTool(ctx context.Context, lease session.Lease, callID string, outcome capability.Outcome, output json.RawMessage, errorCode string) error {
	if outcome != capability.Succeeded && outcome != capability.Failed {
		return errors.New("agentloop: reconciliation requires a known outcome")
	}
	if len(output) > 0 && !json.Valid(output) {
		return errors.New("agentloop: invalid reconciled output")
	}
	p, err := l.Store.Inspect(ctx, lease.SessionID)
	if err != nil {
		return err
	}
	if len(p.Turns) == 0 {
		return session.ErrConflict
	}
	t := p.Turns[len(p.Turns)-1]
	if t.Status != "active" || len(t.Steps) == 0 {
		return session.ErrConflict
	}
	s := t.Steps[len(t.Steps)-1]
	state, ok := s.Tools[callID]
	if !ok || state.Result != nil && state.Result.Outcome != string(capability.Unknown) {
		return session.ErrConflict
	}
	if outcome == capability.Failed && errorCode == "" {
		return errors.New("agentloop: failure code required")
	}
	if outcome == capability.Succeeded && errorCode != "" {
		return errors.New("agentloop: success cannot have failure code")
	}
	_, err = l.commit(ctx, lease, p, fact{session.ToolSettled, session.ToolResult{TurnID: t.ID, StepID: s.ID, ID: callID, Outcome: string(outcome), Result: string(output), Error: errorCode}})
	return err
}

var _ capability.Recorder = (*toolRecorder)(nil)
