package agentruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/kart-io/wechat-account/agent-runtime/agent"
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

// nodeToolDefs is what this node can still call, described for the model.
//
// It is the node's grant, not the Definition's declaration: offering a tool the
// gateway would refuse invites the model to spend a turn discovering that. Two
// things used to be offered anyway despite the gateway being certain to refuse
// them, and both are removed here rather than left for the model to find out:
//
//   - a key the Run's own Restrictions narrowed away. The gateway builds its
//     allowlist from Restrictions.Narrow(node.Tools); this used the raw grant,
//     so a narrowed Run was offered exactly the tools it had asked not to have.
//   - a tool whose declared per-Run allowance is spent. The refusal in
//     withinCallCeiling still stands — it is the boundary, and it has to hold
//     for calls the model emits in the same round, before a new offer exists —
//     but there is no reason to keep advertising a tool that can only answer
//     with tool_call_ceiling_exhausted.
func (s *session) nodeToolDefs(keys []string) []llm.ToolDef {
	gateway := s.runtime.deps.Tools
	if gateway == nil || len(keys) == 0 {
		return nil
	}
	out := make([]llm.ToolDef, 0, len(keys))
	for _, key := range s.snapshot.Restrictions.Narrow(keys) {
		spec, ok := gateway.LookupSpec(key)
		if !ok {
			continue
		}
		if _, _, exhausted := s.callCeiling(key); exhausted {
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
	if err := s.detachedErr(); err != nil {
		return llm.Response{}, err
	}
	if s.runtime.deps.Models == nil {
		return llm.Response{}, run.NewError("no_model_registry", run.ErrorInternal, run.RetryNever)
	}

	// The published Definition's model policy, filled per field where the Agent
	// left one unset. Caller-wins, the same rule the profile follows: an Agent
	// that computed a ceiling for this particular call must not have it
	// overridden by the Definition's default.
	//
	// Temperature and MaxTokens used to be dropped here — only Profile was
	// filled — so every Agent going through the shared mechanical step, which
	// builds a bare llm.Request, called with both at zero. MaxTokens is not
	// only a provider argument: it is what the token reservation below is sized
	// from, so a zero meant every model call reserved nothing and the token
	// half of the budget never refused anything. Only LLMCalls was holding.
	if request.Temperature == 0 {
		request.Temperature = s.declared.Model.Temperature
	}
	if request.MaxTokens == 0 {
		request.MaxTokens = s.declared.Model.MaxTokens
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
	// NoTools is the caller saying "offer none"; empty is the caller saying
	// nothing, which means the node's grant.
	if !request.NoTools && len(request.Tools) == 0 {
		request.Tools = s.nodeToolDefs(p.node.Tools)
		// Nothing survived the grant, the narrowing and the ceilings. That is a
		// withdrawal and has to be said as one: an empty Tools with NoTools
		// false is "no opinion" to everything downstream, and the host client
		// routes it to the streaming path, which folds the whole conversation
		// into a single user string and drops every tool_call_id. Doing that on
		// the round that produces the final answer is the corruption the
		// routing rule exists to prevent.
		if len(request.Tools) == 0 {
			request.NoTools = true
		}
	}
	if len(request.Tools) > 0 && !capabilities.Tools {
		// Asked rather than assumed, and degraded rather than failed: an engine
		// without tool calling can still answer, and dropping the tools is a
		// visible decision instead of a confusing provider error.
		request.Tools = nil
	}
	if carriesImages(request) && !capabilities.Vision {
		// Refused rather than degraded, unlike tools above. Dropping the tools
		// leaves a question the model can still answer; dropping the images
		// leaves "describe this picture" with no picture, and the model answers
		// confidently about nothing. The Run has not been charged at this
		// point — the reservation happens below.
		return llm.Response{}, llm.ErrCapabilityUnsupported
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
	if err := s.begin(ctx, invocationID, "", "", reserve, false, p.node.ID); err != nil {
		return llm.Response{}, err
	}

	// Timed here rather than inside the client: this is the only place that
	// sees every provider, and a per-client measurement would miss the ones
	// that never got one.
	callStarted := s.runtime.deps.Clock.Now()
	var response llm.Response
	var callErr error
	if capabilities.Streaming && streamable(request) {
		// Forwarded verbatim, in the provider's own chunking. A Runtime that
		// re-cut the stream would be inventing a cadence, and the only honest
		// cadence is the one the engine produced.
		//
		// The sink never fails: an observer is a bystander, and letting one
		// abort a call the Run has already paid for would make observation a
		// failure mode. A host that stops caring stops reading, it does not
		// stop the Run.
		response, callErr = client.Stream(ctx, request, func(chunk llm.Chunk) error {
			if chunk.Content != "" {
				s.runtime.recorder.Chunk(s.snapshot.ID, chunk.Content)
			}
			return nil
		})
	} else {
		response, callErr = client.Complete(ctx, request)
	}
	callElapsed := s.runtime.deps.Clock.Now().Sub(callStarted)

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
	if err := s.complete(ctx, invocationID, outcome, used, p.node.ID); err != nil {
		return llm.Response{}, err
	}
	// The response handed to the agent carries the settled totals, so an
	// agent's per-profile report and the Runtime's settlement count the same
	// tokens even when the client left Usage empty and reported only attempts.
	response.Usage = usage
	// The client's settled engine wins: it knows which provider/model actually
	// served the call, which is what pricing needs. The request's profile is
	// only the fallback for a client that does not report one.
	if response.Model.Profile == "" {
		response.Model = request.Model
	}
	// Recorded after the settled profile is known, and on the error path too:
	// the call that spent the most wall time is often the one that failed, and
	// leaving it out biases the very measurement used to size the ceiling that
	// would have cut it short.
	s.recordCallTiming(response.Model.Profile, callElapsed)
	return response, callErr
}

// Tool invokes a declared tool through the fixed gateway chain.
func (p *governedPorts) Tool(ctx context.Context, name string, arguments json.RawMessage) (json.RawMessage, error) {
	s := p.session
	if err := s.detachedErr(); err != nil {
		return nil, err
	}
	if s.runtime.deps.Tools == nil {
		return nil, run.NewError("no_tool_gateway", run.ErrorInternal, run.RetryNever)
	}

	if err := s.withinCallCeiling(name); err != nil {
		return nil, err
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
		RunID:          s.snapshot.ID,
		InvocationID:   invocationID,
		Tool:           name,
		Arguments:      arguments,
		Principal:      s.snapshot.Principal,
		IdempotencyKey: string(invocationID),
		Facts: policy.CallFacts{
			AgentName: p.node.Implementation,
			// The Run's own declared labels. The gateway adds the tool's on top,
			// so a policy condition on "label" sees both what this Run is and
			// what it is about to call.
			Labels:             s.snapshot.Restrictions.Labels,
			BudgetRemainingPct: remainingPercent(s.snapshot.Budget),
		},
		Policies: s.policies,
		// Narrowed, never widened: the Run's own restriction can only remove
		// keys this node was granted.
		Allowlist:       s.snapshot.Restrictions.Narrow(p.node.Tools),
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
	if host := prepared.Spec.TargetHost; host != "" {
		// The destination policy and quota just judged, recorded as the fact
		// they judged: "which hosts did this Run reach" is asked long after the
		// allowlist that permitted them has been edited.
		s.runtime.record(observe.Decision{
			Name: observe.EventEgressHostResolved, RunID: s.snapshot.ID,
			Attributes: []observe.Attribute{
				observe.Attr(observe.AttrHost, host),
				observe.Attr(observe.AttrTool, name),
			},
		})
	}

	if err := s.admitEffect(ctx, prepared.Reserve, name); err != nil {
		return nil, err
	}
	if err := s.begin(ctx, invocationID, name, prepared.Invocation.IdempotencyKey, prepared.Reserve, granted, p.node.ID); err != nil {
		return nil, err
	}

	mutation, callErr := s.runtime.deps.Tools.Execute(ctx, tool.CommittedInvocation{Prepared: prepared})
	if err := s.complete(ctx, invocationID, mutation.Outcome, mutation.Used, p.node.ID); err != nil {
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
	if err := s.detachedErr(); err != nil {
		return nil, err
	}
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
	if err := s.detachedErr(); err != nil {
		return err
	}
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
	if err := s.begin(ctx, invocationID, "", idempotencyKey, prepared.Reserve, false, p.node.ID); err != nil {
		return err
	}

	fact, writeErr := s.runtime.deps.Memories.ExecuteWrite(ctx, memory.CommittedWrite{Prepared: prepared})
	if err := s.commitMemory(ctx, invocationID, key, declared.Namespace, fact, p.node.ID); err != nil {
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
	if explanation.Deciding != nil {
		// The rule in force, not the first one that fired: shadow entries sort
		// in among the rest and naming one of them would attribute the decision
		// to a policy that by definition did not make it.
		decision.Attributes = append(decision.Attributes,
			observe.Attr(observe.AttrPolicyName, explanation.Deciding.Name))
	}
	// shadow reports a dry run that disagreed with the enforced outcome, not
	// merely that a shadow rule fired: a shadow rule agreeing with the decision
	// predicts nothing about turning it on.
	_, diverged := explanation.ShadowWouldTighten()
	decision.Attributes = append(decision.Attributes,
		observe.Attr(observe.AttrShadow, boolText(diverged)))
	p.session.runtime.record(decision)
}

// recordPolicy reports a refusal that happened before an explanation existed —
// an unknown tool, a schema mismatch, an allowlist miss. The refusal is still a
// governance decision and still has to be visible.
func (p *governedPorts) recordPolicy(name string, err error) {
	p.session.runtime.record(prePolicyDecision(p.session.snapshot.ID, name, err))
}

// prePolicyDecision builds the record for a refusal that no Explanation
// describes.
//
// The code goes on beside the kind because several refusals share a kind: an
// allowlist miss, a missing permission and a blanket side-effect ban are all
// "denied", and they are fixed in three different places. Recording only the
// kind is what made a version whose stored graph granted no tools at all
// indistinguishable, per call, from a version deliberately withholding one.
func prePolicyDecision(runID run.ID, tool string, err error) observe.Decision {
	attributes := []observe.Attribute{
		observe.Attr(observe.AttrTool, tool),
		observe.Attr(observe.AttrDecision, string(run.KindOf(err))),
	}
	// Omitted rather than empty when the error is not the Runtime's: an
	// attribute present and blank claims the reason was looked up and found to
	// be nothing.
	if code := run.CodeOf(err); code != "" {
		attributes = append(attributes, observe.Attr(observe.AttrReason, code))
	}
	return observe.Decision{
		Name: observe.EventPolicyEvaluated, RunID: runID, Attributes: attributes,
	}
}

// exhaustedUnit names the budget dimension that refused this effect.
//
// It used to be the literal "tokens" on every refusal, whichever dimension was
// actually short. An operator sizing max_tool_calls from telemetry would have
// read every one of those refusals as a token problem and moved the wrong
// number — the reading is worse than none, because it points somewhere.
//
// The order matches Affords: the first dimension that would be exceeded is the
// one reported. A refusal on two at once is reported as the first, which is
// enough to act on.
func exhaustedUnit(budget run.Budget, want run.Limits) string {
	committed := budget.Committed().Add(want)
	switch {
	case budget.Envelope.LLMCalls > 0 && committed.LLMCalls > budget.Envelope.LLMCalls:
		return "llm_calls"
	case budget.Envelope.Tokens > 0 && committed.Tokens > budget.Envelope.Tokens:
		return "tokens"
	case budget.Envelope.ToolCalls > 0 && committed.ToolCalls > budget.Envelope.ToolCalls:
		return "tool_calls"
	}
	// Affords said no, so one of the three should have matched. Reporting an
	// empty unit rather than guessing keeps a future fourth dimension from
	// being silently filed under one of these three.
	return ""
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
				observe.Attr(observe.AttrUnit, exhaustedUnit(s.snapshot.Budget, want)),
				observe.Attr(observe.AttrTool, toolName),
			},
		})
		return run.NewError(run.CodeBudgetExhausted, run.ErrorDenied, run.RetryNever)
	}
	return nil
}

// withinCallCeiling refuses a tool the Definition has already been allowed to
// call as often as it declared.
//
// Counted from the Run's own invocation ledger rather than from anything the
// session holds. A session is per claim: a Run that parks for approval, waits
// on a resolution, or is recovered after a crash gets a fresh one, so a counter
// living in memory would silently reset and the ceiling would bound a claim
// rather than a Run. The ledger is append-only and reloaded whole on every
// claim, which is what makes the count survive.
//
// Denied and permanent, like an approval that was refused: an agent that can
// answer from what it already retrieved should do that, rather than treating
// the refusal as something to retry.
func (s *session) withinCallCeiling(name string) error {
	ceiling, used, exhausted := s.callCeiling(name)
	if !exhausted {
		return nil
	}
	return run.NewError("tool_call_ceiling_exhausted", run.ErrorDenied, run.RetryNever,
		fmt.Errorf("definition %s allows %d call(s) to %q per run; %d already recorded",
			s.snapshot.Definition.ID, ceiling, name, used))
}

// callCeiling answers how much of a tool's declared per-Run allowance is left.
//
// One predicate for two jobs: refusing a call that would exceed it, and leaving
// the tool out of what the model is offered next. They have to agree, and the
// only way to be sure of that is for them to be the same code.
func (s *session) callCeiling(name string) (ceiling, used int, exhausted bool) {
	var declaredCeiling *int
	for _, declared := range s.declared.Tools {
		if declared.Key == name {
			declaredCeiling = declared.MaxCalls
			break
		}
	}
	if declaredCeiling == nil {
		return 0, 0, false
	}

	for _, invocation := range s.snapshot.Invocations {
		if invocation.Tool == name {
			used++
		}
	}
	// Every recorded call counts, including the ones that failed or were parked
	// unknown. A ceiling that only counted successes would make a broken
	// backend an unlimited retry loop, which is the shape of the incident this
	// bounds in the first place.
	return *declaredCeiling, used, used >= *declaredCeiling
}

// begin reserves and commits the invocation-begin fact before the effect.
//
// consumesGrant clears the approval hold in this same commit. A grant
// authorises one write; the hold used to survive the call it was granted for,
// with two consequences. The visible one is on the recovery path: an effect
// that happened but whose result was never committed is parked as unknown,
// resolved as applied, and the next advance injects Granted again and repeats a
// non-idempotent write. The boundary is here rather than after the effect
// because this is the last durable write before it — anything later leaves a
// window where the effect has happened and the grant is still standing.
func (s *session) begin(
	ctx context.Context, id run.ID, toolName, idempotencyKey string,
	reserve run.Limits, consumesGrant bool, node string,
) error {
	if err := s.detachedErr(); err != nil {
		return err
	}
	command := run.Command{
		Kind: run.CommandInvokeTool, Reserve: reserve,
		InvocationID: id, Tool: toolName, IdempotencyKey: idempotencyKey,
	}
	transition, err := run.Reduce(s.snapshot, command)
	if err != nil {
		return err
	}
	if consumesGrant {
		transition.Next.Checkpoint = nil
	}
	stampNode(transition.Events, node)

	committed, err := s.runtime.deps.Store.BeginInvocation(ctx, store.BeginInvocationCommand{
		Fence: s.fence(),
		Invocation: store.InvocationBegin{
			ID:             id,
			IdempotencyKey: idempotencyKey,
			Tool:           toolName,
			Reservation:    store.BudgetReservation{ID: id, Amount: reserve},
		},
		Commit: store.CommitContext{Transition: transition, Events: transition.Events},
	})
	if err != nil {
		return err
	}
	s.snapshot = committed
	if consumesGrant {
		s.approvalHold = nil
	}
	return nil
}

// complete settles the reservation against what was actually used.
func (s *session) complete(ctx context.Context, id run.ID, outcome run.Outcome, used run.Limits, node string) error {
	if err := s.detachedErr(); err != nil {
		return err
	}
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
	stampNode(transition.Events, node)

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

// streamable says whether this request can go down the streaming path without
// losing anything.
//
// Tools and tool history used to disqualify a request, because the host client
// folded a streamed conversation into a single system/user pair and every
// tool_call_id in it was lost. That made the rule "has no turn structure" — and
// it silently excluded the endpoint streaming exists for: an Agent that offers
// a tool offers it on every round, so article review streamed nothing at all
// and the reader watched a blank panel until the whole answer landed. The host
// now streams a structured round through the tool-calling wire instead, so the
// structure is no longer a reason to withhold chunks.
//
// Images remain one. They are a structured content part with nowhere to go in a
// folded string, and unlike tools the consequence is not a lost id but a model
// asked to describe a picture it was never sent. The vision path is also the
// one place a different endpoint is dialled, so streaming it would buy a reader
// nothing and risk the one call that cannot degrade.
func streamable(request llm.Request) bool {
	for _, message := range request.Messages {
		if len(message.Images) > 0 {
			return false
		}
	}
	return true
}

// carriesImages reports whether this request asks the model to look at
// something.
func carriesImages(request llm.Request) bool {
	for _, message := range request.Messages {
		if len(message.Images) > 0 {
			return true
		}
	}
	return false
}
