package agentruntime

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/memory"
	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

// governedPorts is the effect surface handed to one agent for one node.
//
// It is the only implementation of agent.Ports the Runtime ships, and it is
// created per invocation. An agent that stored it and used it later would be
// spending against a Run that has moved on, under a lease it no longer holds —
// which is why the fence is re-read from the session on every call rather than
// captured when the ports were built.
type governedPorts struct {
	session *session
	node    workflow.Node
}

var _ agent.Ports = (*governedPorts)(nil)

func definitionToolDefs(refs []definition.ToolRef, gateway *tool.Gateway) []llm.ToolDef {
	if gateway == nil || len(refs) == 0 {
		return nil
	}
	out := make([]llm.ToolDef, 0, len(refs))
	for _, ref := range refs {
		spec, ok := gateway.LookupSpec(ref.Key)
		if !ok {
			continue
		}
		out = append(out, llm.ToolDef{
			Name: spec.Name, Description: spec.Description, Parameters: spec.Parameters,
		})
	}
	return out
}

// Model runs one model call inside the full governance path.
//
// Reserve, commit the begin fact, call, settle. The order is the point: a call
// that reserved afterwards has already spent the budget by the time anything
// could refuse it, and a call with no committed begin fact leaves silence
// rather than evidence if the process dies mid-flight.
func (p *governedPorts) Model(ctx context.Context, request llm.Request) (llm.Response, error) {
	s := p.session
	if s.runtime.deps.Models == nil {
		return llm.Response{}, run.NewError("no_model_registry", run.ErrorInternal, run.RetryNever)
	}

	if request.Model.Profile == "" {
		request.Model.Profile = s.declared.Model.Profile
	}
	client, err := s.runtime.deps.Models.Resolve(ctx, request.Model)
	if err != nil {
		return llm.Response{}, err
	}

	capabilities, err := client.Capabilities(ctx, request.Model)
	if err != nil {
		return llm.Response{}, err
	}
	if len(request.Tools) == 0 {
		request.Tools = definitionToolDefs(s.declared.Tools, s.runtime.deps.Tools)
	}
	if len(request.Tools) > 0 && !capabilities.Tools {
		// Asked rather than assumed, and degraded rather than failed: an engine
		// without tool calling can still answer, and dropping the tools is a
		// visible decision instead of a confusing provider error.
		request.Tools = nil
	}
	s.runtime.record(observe.Decision{
		Name: observe.EventModelSelected, RunID: s.snapshot.ID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrProfile, request.Model.Profile),
			observe.Attr(observe.AttrAgent, p.node.Implementation),
			observe.Attr(observe.AttrNode, p.node.ID),
			observe.Attr(observe.AttrCapability, boolText(capabilities.Tools)),
		},
	})

	reserve := run.Limits{LLMCalls: 1, Tokens: request.MaxTokens}
	if err := s.admitEffect(ctx, reserve, ""); err != nil {
		return llm.Response{}, err
	}

	invocationID := s.runtime.deps.IDs.NewID("model")
	if err := s.begin(ctx, invocationID, "", reserve); err != nil {
		return llm.Response{}, err
	}

	response, callErr := client.Complete(ctx, request)

	// Usage is settled from every attempt, including the failed ones. A
	// provider that consumed the prompt and then errored still charged for it,
	// and a ledger counting only successes understates exactly the spend it
	// exists to bound.
	var used run.Limits
	var usage llm.Usage
	for _, attempt := range response.Attempts {
		usage.InputTokens += attempt.Usage.InputTokens
		usage.OutputTokens += attempt.Usage.OutputTokens
		used = used.Add(attempt.Usage.Limits())
	}
	if len(response.Attempts) == 0 {
		used = response.Usage.Limits()
		usage = response.Usage
	}

	outcome := run.OutcomeApplied
	if callErr != nil {
		outcome = run.OutcomeNotApplied
		if run.KindOf(callErr) == run.ErrorUnknown {
			outcome = run.OutcomeUnknown
		}
	}
	if err := s.complete(ctx, invocationID, outcome, used); err != nil {
		return llm.Response{}, err
	}
	// The response handed to the agent carries the settled totals, so an
	// agent's per-profile report and the Runtime's settlement count the same
	// tokens even when the client left Usage empty and reported only attempts.
	response.Usage = usage
	response.Model = request.Model
	return response, callErr
}

// Tool invokes a declared tool through the fixed gateway chain.
func (p *governedPorts) Tool(ctx context.Context, name string, arguments json.RawMessage) (json.RawMessage, error) {
	s := p.session
	if s.runtime.deps.Tools == nil {
		return nil, run.NewError("no_tool_gateway", run.ErrorInternal, run.RetryNever)
	}

	invocationID := s.runtime.deps.IDs.NewID("tool")

	// A denied hold reports the refusal on every request for that tool, rather
	// than asking for approval again. The Run resumed specifically because a
	// human said no; re-asking turns the decision into a loop. The error is a
	// plain denial — not approval_required — so an agent that can answer
	// without the tool does, instead of parking a second time.
	if s.approvalHold != nil && s.approvalHold.Denied && s.approvalHold.Tool == name &&
		s.snapshot.PendingApprovalID == "" && s.snapshot.State == run.StateRunning {
		return nil, run.NewError("approval_denied", run.ErrorDenied, run.RetryNever)
	}

	granted := s.approvalHold != nil && !s.approvalHold.Denied && s.approvalHold.Tool == name &&
		s.snapshot.PendingApprovalID == "" && s.snapshot.State == run.StateRunning
	prepared, err := s.runtime.deps.Tools.Prepare(ctx, tool.InvocationRequest{
		InvocationID:   invocationID,
		Tool:           name,
		Arguments:      arguments,
		Principal:      s.snapshot.Principal,
		IdempotencyKey: string(invocationID),
		Facts: policy.CallFacts{
			AgentName:          p.node.Implementation,
			BudgetRemainingPct: remainingPercent(s.snapshot.Budget),
		},
		Policies: s.policies,
		// Narrowed, never widened: the Run's own restriction can only remove
		// keys the Definition declared.
		Allowlist:       s.snapshot.Restrictions.Narrow(declaredToolKeys(s)),
		DenySideEffects: s.snapshot.Restrictions.DenySideEffects,
		Granted:         granted,
	})
	if err != nil {
		if tool.IsApprovalRequired(err) {
			s.approvalHold = &approvalHold{Tool: name, Arguments: arguments}
		}
		p.recordPolicy(name, err)
		return nil, err
	}
	p.recordExplanation(name, prepared.Explanation)

	if err := s.admitEffect(ctx, prepared.Reserve, name); err != nil {
		return nil, err
	}
	if err := s.begin(ctx, invocationID, prepared.Invocation.IdempotencyKey, prepared.Reserve); err != nil {
		return nil, err
	}

	mutation, callErr := s.runtime.deps.Tools.Execute(ctx, tool.CommittedInvocation{Prepared: prepared})
	if err := s.complete(ctx, invocationID, mutation.Outcome, mutation.Used); err != nil {
		return nil, err
	}
	return mutation.Output, callErr
}

// Recall reads memory under the Definition's declared scope and ceilings.
//
// The ceilings come from the Definition rather than from the caller: an agent
// that could widen its own retrieval budget could put an unbounded prompt in
// front of the model, and that fails as a model error far from the read.
func (p *governedPorts) Recall(ctx context.Context, key, text string) ([]memory.Record, error) {
	s := p.session
	// The declaration is checked before the wiring: whether this Definition may
	// touch this key is a property of what was published, not of what the
	// deployment happens to have installed.
	declared, err := declaredMemory(s, key)
	if err != nil {
		return nil, err
	}
	if s.runtime.deps.Memories == nil {
		return nil, run.NewError("no_memory_gateway", run.ErrorInternal, run.RetryNever)
	}

	result, err := s.runtime.deps.Memories.Retrieve(ctx, s.snapshot.Principal, memory.RetrieveRequest{
		Key: key,
		Query: memory.Query{
			Scope:      s.memoryScope(declared, p.node),
			Text:       text,
			MaxRecords: declared.MaxRecords,
			MaxTokens:  declared.MaxTokens,
		},
	})
	if err != nil {
		return nil, err
	}
	return result.Records, nil
}

// Remember writes memory through the same reserve-commit-settle path as any
// other effect.
func (p *governedPorts) Remember(ctx context.Context, key, ref, text, idempotencyKey string) error {
	s := p.session
	declared, err := declaredMemory(s, key)
	if err != nil {
		return err
	}
	if s.runtime.deps.Memories == nil {
		return run.NewError("no_memory_gateway", run.ErrorInternal, run.RetryNever)
	}
	if !declared.Writable {
		// Declared read-only. Refusing here rather than at the provider keeps
		// "what may this Definition write" answerable from the Definition.
		return run.NewError("memory_not_writable", run.ErrorDenied, run.RetryNever)
	}

	invocationID := s.runtime.deps.IDs.NewID("memory")
	prepared, err := s.runtime.deps.Memories.PrepareWrite(ctx, s.snapshot.Principal, memory.WriteRequest{
		InvocationID:   invocationID,
		Key:            key,
		Scope:          s.memoryScope(declared, p.node),
		Ref:            ref,
		Text:           text,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return err
	}
	if err := s.admitEffect(ctx, prepared.Reserve, ""); err != nil {
		return err
	}
	if err := s.begin(ctx, invocationID, idempotencyKey, prepared.Reserve); err != nil {
		return err
	}

	fact, writeErr := s.runtime.deps.Memories.ExecuteWrite(ctx, memory.CommittedWrite{Prepared: prepared})
	if err := s.commitMemory(ctx, invocationID, key, declared.Namespace, fact); err != nil {
		return err
	}
	return writeErr
}

func (p *governedPorts) recordExplanation(name string, explanation policy.Explanation) {
	decision := observe.Decision{
		Name: observe.EventPolicyEvaluated, RunID: p.session.snapshot.ID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrTool, name),
			observe.Attr(observe.AttrDecision, string(explanation.Decision)),
			observe.Attr(observe.AttrPolicyDigest, explanation.SnapshotDigest),
		},
	}
	if len(explanation.Matched) > 0 {
		decision.Attributes = append(decision.Attributes,
			observe.Attr(observe.AttrPolicyName, explanation.Matched[0].Name),
			observe.Attr(observe.AttrShadow, boolText(explanation.Matched[0].Shadow)))
	}
	p.session.runtime.record(decision)
}

// recordPolicy reports a refusal that happened before an explanation existed —
// an unknown tool, a schema mismatch, an allowlist miss. The refusal is still a
// governance decision and still has to be visible.
func (p *governedPorts) recordPolicy(name string, err error) {
	p.session.runtime.record(observe.Decision{
		Name: observe.EventPolicyEvaluated, RunID: p.session.snapshot.ID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrTool, name),
			observe.Attr(observe.AttrDecision, string(run.KindOf(err))),
		},
	})
}

// admitEffect applies the windowed quota at the gateway, then the budget.
//
// Both, in that order. Quota is the tenant's ceiling and budget is this Run's;
// a Run can be well inside its own envelope and still be the one that exhausts
// the tenant, and only the first check catches that.
func (s *session) admitEffect(ctx context.Context, want run.Limits, toolName string) error {
	decision, err := s.quotas.AdmitEffect(ctx, s.scope, want)
	if err != nil {
		return err
	}
	if !decision.Allowed {
		s.runtime.record(observe.Decision{
			Name: observe.EventQuotaRejected, RunID: s.snapshot.ID,
			Attributes: quotaAttributes(decision, s.scope),
		})
		return run.NewError("quota_exhausted", run.ErrorDenied, run.RetryBackoff)
	}
	if decision.Degraded {
		s.runtime.record(observe.Decision{
			Name: observe.EventQuotaDegraded, RunID: s.snapshot.ID,
			Attributes: quotaAttributes(decision, s.scope),
		})
	}

	if !s.snapshot.Budget.Affords(want) {
		s.runtime.record(observe.Decision{
			Name: observe.EventBudgetRefused, RunID: s.snapshot.ID,
			Attributes: []observe.Attribute{
				observe.Attr(observe.AttrUnit, "tokens"),
				observe.Attr(observe.AttrTool, toolName),
			},
		})
		return run.NewError("budget_exhausted", run.ErrorDenied, run.RetryNever)
	}
	return nil
}

// begin reserves and commits the invocation-begin fact before the effect.
func (s *session) begin(ctx context.Context, id run.ID, idempotencyKey string, reserve run.Limits) error {
	command := run.Command{
		Kind: run.CommandInvokeTool, Reserve: reserve,
		InvocationID: id, IdempotencyKey: idempotencyKey,
	}
	transition, err := run.Reduce(s.snapshot, command)
	if err != nil {
		return err
	}

	committed, err := s.runtime.deps.Store.BeginInvocation(ctx, store.BeginInvocationCommand{
		Fence: s.fence(),
		Invocation: store.InvocationBegin{
			ID:             id,
			IdempotencyKey: idempotencyKey,
			Reservation:    store.BudgetReservation{ID: id, Amount: reserve},
		},
		Commit: store.CommitContext{Transition: transition, Events: transition.Events},
	})
	if err != nil {
		return err
	}
	s.snapshot = committed
	return nil
}

// complete settles the reservation against what was actually used.
func (s *session) complete(ctx context.Context, id run.ID, outcome run.Outcome, used run.Limits) error {
	if outcome != run.OutcomeApplied {
		slog.Warn("session: invocation settled non-applied",
			"run_id", string(s.snapshot.ID), "invocation", string(id), "outcome", string(outcome))
	}
	reserved := s.snapshot.Invocations[id].Reserved

	transition := run.Transition{Next: s.snapshot}
	transition.Next.Revision = s.snapshot.Revision + 1
	transition.Next.Budget = s.snapshot.Budget.Settle(reserved, used, outcome != run.OutcomeUnknown)
	// The outcome is part of the transition, not a side effect of the store's
	// UPDATE. The snapshot is what every later decision reads — most
	// importantly parkUnclassifiedEffects, which parks a Run the moment it
	// sees an in_flight invocation after an approval resumes it. A completed
	// call left as in_flight here reads exactly like a worker that died
	// mid-effect, and the Run pays for that confusion with a second,
	// unresolvable parking.
	existing := transition.Next.Invocations[id]
	existing.Outcome = outcome
	transition.Next.Invocations[id] = existing

	settlement := store.BudgetSettlement{ReservationID: id, Charged: used, Release: true}
	if outcome == run.OutcomeUnknown {
		// An unknown reservation stays unavailable until it is reconciled.
		// Releasing it would let the budget be spent twice if the effect turns
		// out to have happened.
		settlement.Release = false

		parked, err := run.Reduce(s.snapshot, run.Command{
			Kind: run.CommandRecordUnknown, InvocationID: id,
		})
		if err != nil {
			return err
		}
		parked.Next.Budget = transition.Next.Budget
		transition = parked
	}

	committed, err := s.runtime.deps.Store.CompleteInvocation(ctx, store.CompleteInvocationCommand{
		Fence:  s.fence(),
		Result: store.InvocationResult{ID: id, Outcome: outcome},
		Usage:  used,
		Budget: settlement,
		Commit: store.CommitContext{Transition: transition, Events: transition.Events},
	})
	if err != nil {
		return err
	}
	s.snapshot = committed
	return nil
}
