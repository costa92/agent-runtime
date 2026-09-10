package agentruntime

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/quota"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

// session is one claimed Run being advanced by one worker.
//
// Everything an effect needs is assembled here, once, and nothing in it is
// shared between Runs: a session that outlived its lease would be a worker
// acting on a Run it no longer owns.
type session struct {
	runtime *runtime
	// leaseMu guards lease. The renewal loop runs beside the node it keeps the
	// Run alive for, and both sides touch the lease: the loop replaces it on
	// every tick, and the node reads its token on every command it fences.
	leaseMu sync.Mutex
	lease   store.Lease

	// snapMu guards snapshot, which two goroutines reach at once on exactly one
	// path: a node abandoned at its wall-clock cap parks the Run while the
	// handler it gave up on is still running and still holding ports. detached
	// tells that handler to stop, but the check and the write around it are not
	// one step, so a handler already past the check still writes here while the
	// abandonment reads.
	//
	// Only the field needs guarding. What it points at is immutable: every
	// reducer case is copy-on-write, and the settle path — the last write that
	// was not — now copies too, so no goroutine can be reading a map another is
	// writing. Which of two writers wins is not this lock's problem either: both
	// commit under the execution fence, the Store admits one, and abandonNode
	// answers the loser's conflict by re-reading and parking again.
	snapMu   sync.RWMutex
	snapshot run.Snapshot

	// trace is the advance span every effect span of this session hangs
	// under. Set once after the session is assembled; NopTracer hands the
	// pinned context back, so linkage holds for a host without tracing too.
	trace observe.TraceContext
	// nodeTrace is the span of the node currently executing; effect spans
	// parent to it while it is open. Nodes run one at a time, so one field
	// suffices.
	nodeTrace observe.TraceContext

	declared definition.Definition
	graph    *workflow.ExecutionGraph
	policies policy.Snapshot
	quotas   *quota.Enforcer
	scope    quota.Scope
	// approvalHold is the write parked for a human. Loaded from Checkpoint
	// on resume so confirm can perform that effect.
	approvalHold *approvalHold

	// detached is set when a node was abandoned at its wall-clock cap and the
	// handler is still running. Everything that would touch the session on that
	// handler's behalf refuses from then on.
	//
	// The handler's goroutine is left running — that is the accepted cost of
	// never releasing the lease — but it must stop being a second writer of this
	// session. It would otherwise write Invocations into the same map the
	// abandonment is reading, which is a Go runtime fatal rather than a failed
	// commit, and its settle path never consults the state, so a commit that won
	// the race would leave the abandonment holding a stale fence.
	detached atomic.Bool

	// callTimingMu guards callTiming, which accumulates how long each model
	// call took for the node currently executing. The agent runs on one
	// goroutine but may call the model from several, and this is written on
	// every one of them.
	callTimingMu sync.Mutex
	callTiming   []run.ModelUsage
}

// detachedErr refuses anything a detached handler tries to do through this
// session, before it touches the Snapshot.
//
// It is meant to end that handler's loop, not to be retried around. Denied and
// RetryNever say so in the taxonomy every agent already reads: nothing about
// this session will ever succeed again, and a handler that treats it as a
// recoverable tool error and keeps going spins against a closed gateway inside a
// goroutine nobody is watching. That is the one thing worse than the leak this
// refusal exists to contain.
func (s *session) detachedErr() error {
	if s.detached.Load() {
		return run.NewError("session_detached", run.ErrorDenied, run.RetryNever)
	}
	return nil
}

// recordCallTiming notes one provider round trip for the running node.
func (s *session) recordCallTiming(profile string, elapsed time.Duration) {
	s.callTimingMu.Lock()
	defer s.callTimingMu.Unlock()
	s.callTiming = run.MergeCallTiming(s.callTiming, profile, int(elapsed/time.Millisecond))
}

// takeCallTiming returns the timings gathered since the last node committed and
// clears them, so the next node starts from nothing.
func (s *session) takeCallTiming() []run.ModelUsage {
	s.callTimingMu.Lock()
	defer s.callTimingMu.Unlock()
	timing := s.callTiming
	s.callTiming = nil
	return timing
}

// withCallTiming folds the Runtime's measured durations into the agent's
// reported token lines.
//
// A profile the agent did not report still produces a line: a call that failed
// after burning wall time is exactly the one worth seeing, and it is also the
// one least likely to come back with a token count.
func withCallTiming(reported, timing []run.ModelUsage) []run.ModelUsage {
	for _, t := range timing {
		found := false
		for i := range reported {
			if reported[i].Profile == t.Profile {
				reported[i].Calls += t.Calls
				if t.MaxCallMS > reported[i].MaxCallMS {
					reported[i].MaxCallMS = t.MaxCallMS
				}
				found = true
				break
			}
		}
		if !found {
			reported = append(reported, t)
		}
	}
	return reported
}

// AdvanceNext is the Worker entry point.
//
// Workers never query runnable IDs and never touch the Store: they ask for
// work and get an atomically leased claim. A worker that could list runnable
// Runs and then claim them separately would race every other worker in the
// window between the two calls.
func (r *runtime) AdvanceNext(ctx context.Context, request AdvanceNextRequest) (AdvanceResult, bool, error) {
	claimed, err := r.deps.Store.ClaimBatch(ctx, store.ClaimBatchCommand{
		Owner:       r.deps.Owner,
		Limit:       1,
		LeaseFor:    r.deps.LeaseFor,
		RootQuantum: request.RootQuantum,
	})
	if err != nil {
		return AdvanceResult{}, false, err
	}
	if len(claimed) == 0 {
		// An idle queue is not a failure, and a caller that could not tell the
		// two apart would either log an error every poll or retry a real one
		// forever.
		return AdvanceResult{}, false, nil
	}

	result, err := r.advanceClaimed(ctx, claimed[0])
	return result, true, err
}

// Advance claims one Run and drives it until it waits or finishes.
func (r *runtime) Advance(ctx context.Context, id run.ID) (AdvanceResult, error) {
	lease, snapshot, err := r.deps.Store.Claim(ctx, store.ClaimCommand{
		RunID: id, Owner: r.deps.Owner, LeaseFor: r.deps.LeaseFor,
	})
	if err != nil {
		return AdvanceResult{}, err
	}
	return r.advanceClaimed(ctx, store.ClaimedRun{Lease: lease, Snapshot: snapshot})
}

func (r *runtime) advanceClaimed(ctx context.Context, claimed store.ClaimedRun) (result AdvanceResult, err error) {
	// One worker advance is one span, parented to the trace pinned at Start:
	// two advances of the same Run are separated by the database, so no
	// in-memory parent survives between them. A Run claimed in any state but
	// queued is a continuation — after an approval, a takeover, a crash — and
	// gets the resumed kind, so a wait spent on a human is not inside a
	// latency measurement.
	kind := observe.SpanRun
	if claimed.Snapshot.State != run.StateQueued {
		kind = observe.SpanResumed
	}
	ctx, span := r.deps.Tracer.Start(ctx, observe.SpanRequest{
		Kind: kind, Name: "agent.run", RunID: claimed.Snapshot.ID, Parent: claimed.Snapshot.Pins.Trace,
	})
	defer func() { span.End(err) }()

	current, err := r.newSession(ctx, claimed)
	if err != nil {
		r.endUnrunnable(ctx, claimed, err)
		return AdvanceResult{}, err
	}
	current.trace = span.Context()

	if current.state().State == run.StateQueued {
		if err := current.start(ctx); err != nil {
			return AdvanceResult{}, err
		}
	}

	// An effect left in flight by a worker that is gone has to be classified
	// before anything else happens to this Run.
	//
	// The begin fact is committed before the effect is issued, so an invocation
	// still in flight after its owner's lease lapsed is one nobody can classify:
	// the model call may have been issued and charged, the tool may have
	// published. Scheduling the node again is the duplicate side effect the
	// reserve-before-effect order exists to prevent — and it is invisible,
	// because the second attempt succeeds and looks like the only one.
	if err := current.parkUnclassifiedEffects(ctx); err != nil {
		return AdvanceResult{Run: current.state()}, err
	}

	for {
		if current.state().State.Terminal() {
			return AdvanceResult{Run: current.state()}, nil
		}
		if current.state().State.Waiting() {
			return AdvanceResult{Run: current.state(), Waiting: true}, nil
		}

		ready := workflow.ReadyNodes(current.graph, current.state())
		if len(ready) == 0 {
			// Nothing schedulable and nothing waiting: either the budget is
			// gone or every remaining node is blocked behind a failure. Either
			// way the Run is finished, and the scheduler's own accounting says
			// how.
			return current.finish(ctx)
		}
		if err := current.runNode(ctx, ready[0]); err != nil {
			return AdvanceResult{Run: current.state()}, err
		}
	}
}

// endUnrunnable fails a Run whose session could not be assembled, when the
// reason is one no later attempt can change.
//
// Without this, such a Run is immortal. advanceClaimed used to return the
// assembly error and leave the state untouched, so the Run went back on the
// queue, was claimed again, failed identically, and repeated on the lease
// period forever. It was never terminal, so retention never reaped it — that
// cleanup only deletes terminal trees — and the caller waiting on it never got
// an answer, only silence. Six such Runs were found in the local database, one
// of them having already spent six model calls before it stuck.
//
// Only run.RunIsUnrunnable qualifies, and its doc explains why the obvious
// predicate (RetryNever) would have been a fleet-killer rather than a fix.
//
// A queued Run is started first, because the reducer refuses CommandFail from
// queued and that refusal is a real invariant — a queued Run has produced
// nothing. Two commits and two events (run.started, run.failed) is the honest
// rendering of what happened. Cancelling instead would have been one commit and
// a lie: cancelled means a human stopped it, which is exactly the distinction
// the manual cleanup of those six Runs relied on.
//
// Best-effort by construction. Every failure here is swallowed, because the
// caller is about to return the assembly error and that error is the diagnosis.
// Losing the fence to another worker or to a takeover is the expected way this
// gives up: whoever won will reach the same conclusion.
func (r *runtime) endUnrunnable(ctx context.Context, claimed store.ClaimedRun, cause error) {
	if !run.RunIsUnrunnable(cause) {
		return
	}
	// A bare session: commitNode and start read only the Store, the lease token
	// and the snapshot, all of which the claim already carries. Going through
	// them rather than calling the Store directly is what keeps the terminal
	// projection fact and the lease sealing attached to this path — commitNode
	// documents itself as the one place a Run may settle.
	current := &session{runtime: r, lease: claimed.Lease, snapshot: claimed.Snapshot}
	if current.state().State == run.StateQueued {
		if err := current.start(ctx); err != nil {
			return
		}
	}
	transition, err := run.Reduce(current.state(), run.Command{Kind: run.CommandFail})
	if err != nil {
		return
	}
	if _, err := current.commitNode(ctx, transition, "", "", run.Limits{}, nil); err != nil {
		return
	}
	r.record(observe.Decision{
		Name: observe.EventRunUnrunnable, RunID: claimed.Snapshot.ID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrReason, run.CodeOf(cause)),
		},
	})
}

// newSession assembles everything the Run pinned. Every load is by the pinned
// version, never by "current": a Run resumed a day later is judged by the rules
// it started under.
func (r *runtime) newSession(ctx context.Context, claimed store.ClaimedRun) (*session, error) {
	snapshot := claimed.Snapshot

	declared, graphRef, err := r.deps.Definitions.Load(ctx, snapshot.Definition)
	if err != nil {
		return nil, err
	}
	if graphRef != snapshot.Graph {
		// The published graph moved under a Run that pinned the old one. Failing
		// closed is the only safe answer: executing the new shape would silently
		// run something the Run was never authorized or budgeted for.
		return nil, run.NewError("graph_mismatch", run.ErrorInvalid, run.RetryNever)
	}
	graph, err := r.graphs.Get(ctx, snapshot.Graph)
	if err != nil {
		return nil, err
	}

	policies, err := r.deps.Governance.PolicySnapshot(ctx, snapshot.Principal.Tenant, snapshot.Pins.PolicyDigest)
	if err != nil {
		return nil, err
	}
	quotas, err := r.deps.Governance.QuotaSnapshot(ctx, snapshot.Principal.Tenant, snapshot.Pins.QuotaDigest)
	if err != nil {
		return nil, err
	}
	enforcer, err := quota.NewEnforcer(quotas, r.deps.Meter)
	if err != nil {
		return nil, err
	}

	return &session{
		runtime:      r,
		lease:        claimed.Lease,
		snapshot:     snapshot,
		declared:     declared,
		graph:        graph,
		policies:     policies,
		quotas:       enforcer,
		approvalHold: loadApprovalHold(snapshot.Checkpoint),
		scope: quota.Scope{
			Tenant: snapshot.Principal.Tenant, Principal: snapshot.Principal.Subject,
		},
	}, nil
}

// fence is the execution fence for the session's current view.
func (s *session) fence() store.ExecutionFence {
	return store.ExecutionFence{
		RunID:                         s.state().ID,
		ExpectedRevision:              s.state().Revision,
		LeaseToken:                    s.leaseToken(),
		ExpectedRootCancellationEpoch: s.state().RootCancellationEpoch,
	}
}

// start moves a queued Run to running.
func (s *session) start(ctx context.Context) error {
	transition, err := run.Reduce(s.state(), run.Command{Kind: run.CommandStart})
	if err != nil {
		return err
	}
	committed, err := s.commitNode(ctx, transition, "", "", run.Limits{}, nil)
	if err != nil {
		return err
	}
	s.setSnapshot(committed)
	return nil
}

func approvalRequired(err error) bool {
	return tool.IsApprovalRequired(err)
}

// stampNode names the node an event happened inside. Reduce cannot do this: it
// is pure over the Snapshot, which carries no current node. Same shape as the
// ApprovalID stamping in parkForApproval.
func stampNode(events []run.Event, node string) {
	if node == "" {
		return
	}
	for i := range events {
		events[i].NodeID = node
	}
}

func (s *session) parkForApproval(ctx context.Context, node string) error {
	transition, err := run.Reduce(s.state(), run.Command{Kind: run.CommandWaitApproval})
	if err != nil {
		return err
	}
	approvalID := s.runtime.deps.IDs.NewID("approval")
	for i := range transition.Events {
		if transition.Events[i].To == run.StateWaitingApproval {
			transition.Events[i].ApprovalID = approvalID
		}
	}
	stampNode(transition.Events, node)
	transition.Next.PendingApprovalID = approvalID
	if s.approvalHold != nil {
		transition.Next.Checkpoint = encodeApprovalHold(*s.approvalHold)
	}
	s.runtime.record(observe.Decision{
		Name:  observe.EventApprovalRequested,
		RunID: s.state().ID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrApprovalID, string(approvalID)),
			observe.Attr(observe.AttrTool, holdToolName(s.approvalHold)),
			observe.Attr(observe.AttrPolicyName, "require_approval"),
		},
	})
	committed, err := s.runtime.deps.Store.EnterApproval(ctx, store.EnterApprovalCommand{
		Fence:      s.fence(),
		ApprovalID: approvalID,
		Commit: store.CommitContext{
			Transition: transition, Events: transition.Events,
		},
	})
	if err != nil {
		return err
	}
	s.setSnapshot(committed)
	return nil
}

// runNode executes one node and commits its result.
func (s *session) runNode(ctx context.Context, node workflow.Node) (err error) {
	ctx, span := s.runtime.deps.Tracer.Start(ctx, observe.SpanRequest{
		Kind: observe.SpanNode, Name: "agent.node", RunID: s.state().ID, Parent: s.trace,
		NodeID: node.ID, AgentKey: node.Implementation,
	})
	s.nodeTrace = span.Context()
	// A node whose agent failed commits that failure and returns nil; the
	// span still has to say the node failed, or the trace shows a green node
	// above a red model call.
	var executeErr error
	defer func() {
		s.nodeTrace = observe.TraceContext{}
		if err != nil {
			span.End(err)
			return
		}
		span.End(executeErr)
	}()

	factory, err := s.runtime.deps.Agents.Lookup(node.Implementation)
	if err != nil {
		return err
	}
	implementation, err := factory.New(nil)
	if err != nil {
		return err
	}

	s.runtime.record(observe.Decision{
		Name: observe.EventRouterPlanSelected, RunID: s.state().ID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrPlan, s.graph.Ref.Digest),
			observe.Attr(observe.AttrAgent, node.Implementation),
			observe.Attr(observe.AttrNode, node.ID),
		},
	})

	// The renew loop runs for as long as the effect does. A node that calls a
	// slow model can outlive the lease it was claimed under, and a worker whose
	// lease lapsed mid-effect must not be the one that commits the result.
	effectCtx, abandoned, stop := s.renewing(ctx)
	ports := &governedPorts{session: s, node: node}
	built, err := s.request(ctx, node, ports)
	if err != nil {
		stop()
		return err
	}

	// The handler runs beside this goroutine rather than in it, because a
	// handler that ignores its context is exactly the one the cap exists for and
	// waiting on its return here would be waiting forever.
	type execution struct {
		response agent.Response
		err      error
	}
	finished := make(chan execution, 1)
	go func() {
		response, err := implementation.Execute(effectCtx, built)
		finished <- execution{response: response, err: err}
	}()

	var done execution
	select {
	case done = <-finished:
	case <-abandoned:
		// Cancelled at the cap and still running after the grace period. The
		// lease is not released and the Run is not failed: the effect may
		// already have landed, so the only honest state is an unknown outcome.
		stop()
		return s.abandonNode(ctx, node)
	}
	response := done.response
	executeErr = done.err
	stopped := stop()

	if stopped.capped {
		// The handler returned because we cancelled it at the cap. That is the
		// same governance decision as the branch above and has to reach the same
		// place — a well-behaved abandonment is still an abandonment. Reporting
		// it as an error instead left the Run running with renewal already
		// stopped, which is the takeover this cap exists to prevent, and the
		// node re-executed effects that had already completed.
		//
		// Ahead of the approval and ownership branches on purpose: the cap has
		// already fired, and an answer that arrives after it does not get to
		// decide what happens to the Run.
		return s.abandonNode(ctx, node)
	}
	renewErr := stopped.err

	if executeErr != nil && approvalRequired(executeErr) {
		return s.parkForApproval(ctx, node.ID)
	}

	if renewErr != nil {
		// Ownership was lost while the effect was in flight. The result is
		// rejected rather than committed: another worker may already have taken
		// over, and two workers committing one node is the double-write the
		// lease exists to prevent.
		return renewErr
	}

	result := workflow.NodeResult{NodeID: node.ID, Output: response.Output}
	if executeErr != nil {
		result.Failed = true
		s.runtime.deps.Logger.Error(ctx, "agent node failed",
			"run_id", string(s.state().ID),
			"node_id", node.ID,
			"agent", node.Implementation,
			"kind", string(run.KindOf(executeErr)),
			"err", executeErr)
	} else {
		result.OutputRef = string(s.runtime.deps.IDs.NewID("output"))
	}

	progress, err := workflow.ApplyNodeResult(s.graph, s.state(), result, s.runtime.deps.Schemas)
	if err != nil {
		return err
	}
	// The scheduler owns the schema boundary. Commit exactly the normalized
	// value and settled reference it returned; retaining the agent's raw value
	// here would validate one representation and publish another.
	result.Output = progress.Output
	result.OutputRef = progress.Nodes[node.ID].OutputRef
	result.Failed = progress.Nodes[node.ID].Status == run.StateFailed

	command := run.Command{Kind: run.CommandSucceed}
	if progress.Command != nil {
		command = *progress.Command
	}
	transition, err := s.transitionFor(progress, command)
	if err != nil {
		return err
	}

	fact, err := s.nodeFact(node, result, executeErr)
	if err != nil {
		return err
	}

	modelUsage := withCallTiming(response.ModelUsage, s.takeCallTiming())
	committed, err := s.commitNode(ctx, transition, node.ID, result.OutputRef, response.Used, modelUsage, fact)
	if err != nil {
		return err
	}
	s.setSnapshot(committed)
	return nil
}

// nodeFact is what one finished node contributes to the host's transcript.
//
// A succeeded node contributes its output; a failed one contributes a progress
// marker instead. Not both: the outbox key is one sequence per fact and the
// distinction is what the host is reading for — an output fact for a node that
// produced nothing would render as an empty assistant message rather than as a
// step that failed.
func (s *session) nodeFact(
	node workflow.Node, result workflow.NodeResult, executeErr error,
) (store.ProjectionFact, error) {
	if result.Failed {
		// The code travels; the error text does not. It is what lets the host
		// say why the turn stopped instead of only that it did — the failure
		// used to exist solely in this process's log, so a reader saw nothing
		// at all. run.CodeOf yields "" for anything that is not a run.Error,
		// which the host renders as its unattributed fallback.
		return store.NewProjectionFact(store.ProgressPayload{
			NodeID: node.ID, AgentKey: node.Implementation, Failed: true,
			FailureCode: run.CodeOf(executeErr),
		})
	}
	return store.NewProjectionFact(store.AssistantMessagePayload{
		Output: result.Output, OutputRef: result.OutputRef,
		AgentKey: node.Implementation, NodeID: node.ID,
	})
}

// transitionFor folds the graph's node progress into the Run transition.
//
// The two are computed apart — Reduce owns Run state and knows nothing about
// nodes — and committed together, because a node map that landed without its
// transition would describe a graph further along than the Run it belongs to.
func (s *session) transitionFor(progress workflow.Progress, command run.Command) (run.Transition, error) {
	if progress.Command == nil {
		// The graph advanced but the Run's state did not. There is no reducer
		// command for "keep running", so the transition is the node map alone.
		next := s.state()
		next.Revision = s.state().Revision + 1
		next.Nodes = progress.Nodes
		return run.Transition{Next: next}, nil
	}
	transition, err := run.Reduce(s.state(), command)
	if err != nil {
		return run.Transition{}, err
	}
	transition.Next.Nodes = progress.Nodes
	return transition, nil
}

// finish terminates a Run that has nothing left to schedule.
func (s *session) finish(ctx context.Context) (AdvanceResult, error) {
	command := run.Command{Kind: run.CommandPartial}
	if !s.state().Budget.Affords(run.Limits{LLMCalls: 1}) {
		// Out of budget with work outstanding is a partial result, not a
		// failure: what did complete is still the honest answer.
		s.runtime.record(observe.Decision{
			Name: observe.EventBudgetRefused, RunID: s.state().ID,
			Attributes: []observe.Attribute{observe.Attr(observe.AttrUnit, "llm_calls")},
		})
	}
	if len(s.state().Nodes) == 0 {
		command = run.Command{Kind: run.CommandFail}
	}

	transition, err := run.Reduce(s.state(), command)
	if err != nil {
		return AdvanceResult{Run: s.state()}, err
	}
	committed, err := s.commitNode(ctx, transition, "", "", run.Limits{}, nil)
	if err != nil {
		return AdvanceResult{Run: s.state()}, err
	}
	s.setSnapshot(committed)
	return AdvanceResult{Run: committed}, nil
}

// parkUnclassifiedEffects moves the Run to waiting_resolution for the first
// invocation left in flight.
//
// One at a time, because waiting_resolution accepts nothing but a resolution:
// the next claim parks the next one. Resolving is a deliberate act — a human or
// a reconciler establishing whether the effect happened — and it is the only
// thing that releases the reservation, which is what stops the budget being
// spent twice on one call.
func (s *session) parkUnclassifiedEffects(ctx context.Context) error {
	if s.state().State != run.StateRunning {
		return nil
	}
	for id, invocation := range s.state().Invocations {
		if invocation.Outcome != run.OutcomeInFlight {
			continue
		}
		transition, err := run.Reduce(s.state(), run.Command{
			Kind: run.CommandRecordUnknown, InvocationID: id,
		})
		if err != nil {
			return err
		}
		committed, err := s.commitNode(ctx, transition, "", "", run.Limits{}, nil)
		if err != nil {
			return err
		}
		s.setSnapshot(committed)
		return nil
	}
	return nil
}

// abandonNode parks a Run whose node hit the wall-clock cap and did not return.
//
// Unknown, never failed. The handler is still running and reserve-before-effect
// only promises that the begin fact is durable — whether the write landed is
// precisely what nobody knows, and failing the Run tells the layer above it is
// safe to try again. waiting_resolution says the true thing and is the same path
// a tool with an unestablished outcome takes, so the same resolution — a human
// or a reconciler — settles it.
//
// The invocation named is the one left in flight where there is one, so the
// resolver has the effect itself to look at. A node wedged before it issued
// anything gets a fresh id instead: the reducer needs one, and parking under an
// invocation the Run never made would point the resolver at nothing.
//
// It refuses the detached handler first and then insists, because leaving the
// Run running is the one outcome that must not happen. Renewal has already
// stopped by the time this runs, so a Run still running when this returns is a
// Run whose lease lapses under a handler that is still going — the takeover this
// whole mechanism exists to prevent. A settle from the detached handler that won
// the race bumps the revision and leaves this holding a stale fence, so a
// conflict is answered by re-reading the Run and parking it again rather than by
// giving up.
func (s *session) abandonNode(ctx context.Context, node workflow.Node) error {
	s.detached.Store(true)

	const attempts = 3
	for attempt := 0; ; attempt++ {
		if s.state().State != run.StateRunning {
			// The detached handler parked or settled the Run itself. Nothing to
			// park, and nothing another worker can claim.
			return nil
		}
		err := s.parkAbandoned(ctx, node, attempt == 0)
		if err == nil || attempt == attempts-1 || run.KindOf(err) != run.ErrorConflict {
			return err
		}
		refreshed, getErr := s.runtime.deps.Store.Get(ctx, s.state().ID)
		if getErr != nil {
			return getErr
		}
		s.setSnapshot(refreshed)
	}
}

// parkAbandoned is one attempt at recording the abandonment. It announces the
// abandonment on the first attempt only: a retry is the same event, and an
// operator counting them would be counting fence conflicts.
func (s *session) parkAbandoned(ctx context.Context, node workflow.Node, announce bool) error {
	invocationID := run.ID("")
	for id, invocation := range s.state().Invocations {
		if invocation.Outcome == run.OutcomeInFlight {
			invocationID = id
			break
		}
	}
	if invocationID == "" {
		invocationID = s.runtime.deps.IDs.NewID("abandoned")
	}

	transition, err := run.Reduce(s.state(), run.Command{
		Kind: run.CommandRecordUnknown, InvocationID: invocationID,
	})
	if err != nil {
		return err
	}
	// This event is what keeps the cap from being silent. It says the node hit
	// its wall-clock ceiling and the Run was parked as unknown — not that any
	// handler misbehaved. Both wind-down paths reach here: the one whose handler
	// returned the moment we cancelled it, and the one that never returned at
	// all. Only the second leaks a goroutine, which is the accepted cost of
	// never releasing the lease, and this event alone does not tell them apart.
	if announce {
		s.runtime.record(observe.Decision{
			Name: observe.EventNodeAbandoned, RunID: s.state().ID,
			Attributes: []observe.Attribute{
				observe.Attr(observe.AttrNode, node.ID),
				observe.Attr(observe.AttrAgent, node.Implementation),
				observe.Attr(observe.AttrInvocationID, string(invocationID)),
			},
		})
	}
	committed, err := s.commitNode(ctx, transition, node.ID, "", run.Limits{}, nil)
	if err != nil {
		return err
	}
	s.setSnapshot(committed)
	return nil
}

// commitNode seals the lease and submits one typed command.
//
// Sealing first is what keeps a commit and a takeover from both believing they
// own the Run. The protection is the check, not a state change: Seal uses
// Store-authoritative time and refuses an expired lease even when the token and
// revision still match — a worker judging expiry by its own clock hands
// ownership away whenever the two disagree. The lease itself comes back
// unchanged, so the commit below fences on the same token.
// Terminal facts are derived here rather than passed in, so no terminal path
// can forget one: every commit that settles a Run goes through this function,
// and a caller that had to remember would eventually be a caller that did not.
func (s *session) commitNode(
	ctx context.Context, transition run.Transition, node, outputRef string,
	used run.Limits, modelUsage []run.ModelUsage, facts ...store.ProjectionFact,
) (run.Snapshot, error) {
	if _, err := s.runtime.deps.Store.SealForCommit(ctx, store.SealCommand{
		RunID: s.state().ID, LeaseToken: s.leaseToken(),
	}); err != nil {
		return run.Snapshot{}, err
	}
	stampNode(transition.Events, node)

	name := node
	if name == "" {
		name = "run"
	}
	if transition.Next.State.Terminal() {
		userID, _ := strconv.ParseInt(s.state().Principal.Subject, 10, 64)
		terminal, err := store.NewProjectionFact(store.TerminalResultPayload{
			State:        string(transition.Next.State),
			UserID:       userID,
			UsedLLMCalls: int64(transition.Next.Budget.Used.LLMCalls),
			UsedTokens:   int64(transition.Next.Budget.Used.Tokens),
		})
		if err != nil {
			return run.Snapshot{}, err
		}
		facts = append(facts, terminal)
	}

	committed, err := s.runtime.deps.Store.CommitNodeResult(ctx, store.CommitNodeResultCommand{
		Fence:      s.fence(),
		NodeName:   name,
		OutputRef:  outputRef,
		Usage:      used,
		ModelUsage: modelUsage,
		Commit: store.CommitContext{
			Transition: transition, Events: transition.Events, Projections: facts,
		},
	})
	if err != nil {
		return run.Snapshot{}, err
	}

	// No re-claim here. There used to be one, on the belief that sealing
	// retired the token and a further effect in this Advance needed a fresh
	// one. Sealing does neither: it keeps the same token, and a Store grants a
	// claim only over a lease that is absent or lapsed, so this Run — holding
	// one it just renewed — could never be granted anything. Every commit made
	// a round trip that was certain to fail and then dropped the error, which
	// is why nothing said so. The token stays valid, renewal keeps working, and
	// the next command fences on the same lease.
	return committed, nil
}

// request builds what the agent implementation sees.
func (s *session) request(ctx context.Context, node workflow.Node, ports *governedPorts) (agent.Request, error) {
	remaining := s.state().Budget.Remaining()
	nodeInput, err := s.input(ctx, node)
	if err != nil {
		return agent.Request{}, err
	}
	req := agent.Request{
		RunID:     s.state().ID,
		Principal: s.state().Principal,
		Prompt:    s.declared.Prompt,
		Input:     nodeInput,
		Upstreams: s.state().Upstreams,
		Remaining: remaining,
		Ports:     ports,
	}
	if s.approvalHold != nil && !s.approvalHold.Denied && s.state().PendingApprovalID == "" && s.state().State == run.StateRunning {
		// Granted only ever carries a confirmed write. A denied hold resumes
		// the Run so the agent can answer, but the refused effect must not be
		// performed — not even silently, as a granted write.
		req.Granted = &agent.GrantedTool{
			Name: s.approvalHold.Tool, Arguments: s.approvalHold.Arguments,
		}
	}
	return req, nil
}

// input is what one node executes on.
//
// Answered from the node's declared binding, never guessed: a node that reads
// another node's output reads its ref, and every other node reads the Run's own
// input. Returning nothing for the latter is what made a Run's input
// unreachable — the value was accepted at Start, stored nowhere, and the agent
// was handed nil, so an assistant ran every turn without the question.
func (s *session) input(ctx context.Context, node workflow.Node) (json.RawMessage, error) {
	switch node.Input.Source {
	case workflow.SourceNode:
		if len(node.Input.From) != 1 {
			return nil, run.NewError("malformed_binding", run.ErrorInternal, run.RetryNever)
		}
		return s.upstreamOutput(ctx, node.Input.From[0])
	case workflow.SourceNodes:
		// Keyed by producing node, so a step working from several upstreams can
		// tell them apart. A concatenation would leave the consumer guessing
		// which half was the plan and which the research.
		merged := make(map[string]json.RawMessage, len(node.Input.From))
		for _, from := range node.Input.From {
			// An upstream that produced nothing is left out rather than fatal.
			// Only a soft-failed node can reach here without an output — a hard
			// one has already failed the Run — and dropping the whole step
			// because an optional predecessor was skipped would make declaring
			// a second upstream riskier than working without it.
			if state, ok := s.state().Nodes[from]; !ok || state.OutputRef == "" {
				continue
			}
			output, err := s.upstreamOutput(ctx, from)
			if err != nil {
				return nil, err
			}
			merged[from] = output
		}
		if len(merged) == 0 {
			return nil, run.NewError("missing_upstream_output", run.ErrorInvalid, run.RetryNever)
		}
		if node.Input.WithRunInput {
			// Added after the emptiness check, so declaring the flag cannot turn a
			// step whose every upstream was skipped into one that runs on the Run
			// input alone and reports success.
			merged[workflow.RunInputKey] = s.state().Input
		}
		encoded, err := json.Marshal(merged)
		if err != nil {
			return nil, run.NewError("unencodable_input", run.ErrorInternal, run.RetryNever, err)
		}
		return encoded, nil
	default:
		return s.state().Input, nil
	}
}

func (s *session) upstreamOutput(ctx context.Context, from string) (json.RawMessage, error) {
	state, ok := s.state().Nodes[from]
	if !ok || state.OutputRef == "" {
		// The binding names an upstream that produced no ref. Falling back to
		// the Run's input here would hand the node something it did not ask
		// for, and it would look like it worked.
		return nil, run.NewError("missing_upstream_output", run.ErrorInvalid, run.RetryNever)
	}
	// Resolved here rather than passed along as a ref. The Snapshot cannot
	// carry outputs — they are unbounded and it is rewritten on every
	// transition — so the edge names the upstream by ref and the engine follows
	// it. Handing the ref itself to the agent made the pointer the whole brief:
	// the writer answered that it could not see the research, and the assembler
	// failed for want of a body. Nothing on the agent side can follow a ref;
	// Ports has no read for it, by design.
	return s.runtime.deps.Store.NodeOutput(ctx, s.state().ID, state.OutputRef)
}

func (s *session) leaseToken() string {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	return s.lease.Token
}

func (s *session) setLease(lease store.Lease) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	s.lease = lease
}

// state is the session's current view of the Run. See snapMu for who else reads
// it. The Snapshot is copied out, so a caller that holds one across a commit is
// looking at the Run as it was — which is what every fence built from it means.
func (s *session) state() run.Snapshot {
	s.snapMu.RLock()
	defer s.snapMu.RUnlock()
	return s.snapshot
}

func (s *session) setSnapshot(snapshot run.Snapshot) {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	s.snapshot = snapshot
}

// windDown is why the renew loop stopped.
//
// The two reasons are not interchangeable, and conflating them is what let a
// cancelled node fall through to takeover: losing ownership means this worker
// must not commit anything, while hitting the cap means it is the only worker
// that may — it still holds the lease and has to park the Run before it lapses.
type windDown struct {
	// err is set when ownership was lost while the effect was in flight.
	err error
	// capped is set when the node hit its wall-clock cap, whether or not the
	// handler went on to return.
	capped bool
}

// abandonGraceFor is how long a cancelled handler gets to return before its
// node is abandoned.
//
// A twentieth of the cap and never more than 30s: at the 10m default that is
// the 30s wind-down, and a caller that sets a short cap — which in practice
// means a test — gets a proportionally short grace rather than a fixed 30s wait
// it has no way to shorten.
func abandonGraceFor(limit time.Duration) time.Duration {
	grace := limit / 20
	if grace > 30*time.Second {
		grace = 30 * time.Second
	}
	if grace <= 0 {
		grace = time.Millisecond
	}
	return grace
}

// renewing starts a bounded renew loop and returns a context that is cancelled
// the moment ownership is lost, plus a channel closed if the node is abandoned.
//
// One loop per active Advance, refreshing before a third of the TTL has
// elapsed. A third rather than a half so that one missed tick is survivable:
// renewing at the last moment means the first hiccup is a lost lease.
//
// The loop is also where the node's wall-clock cap lives, because it is the only
// thing running beside an effect that has not returned. At the cap it cancels
// the effect and keeps renewing through a grace period — the lease must still be
// alive to park the Run — and only then gives up on the handler.
func (s *session) renewing(ctx context.Context) (context.Context, <-chan struct{}, func() windDown) {
	effectCtx, cancel := context.WithCancel(ctx)
	interval := s.runtime.deps.LeaseFor / 3
	if interval <= 0 {
		interval = time.Millisecond
	}

	var (
		once      sync.Once
		done      = make(chan struct{})
		stopping  = make(chan struct{})
		abandoned = make(chan struct{})
		outcome   windDown
	)
	// Captured before the goroutine starts. The ID never changes, but the
	// Snapshot around it is replaced on every commit, so reading it from here
	// would race the node that is committing.
	runID := s.state().ID
	go func() {
		defer close(done)
		// Renew once up front rather than only on a tick. One Advance walks the
		// whole graph under a single claim, and a node that finishes inside the
		// first interval — which is what a normal model call does — left the
		// deadline pinned at claim time. Node after node the loop started and
		// stopped without ever firing, the lease aged while the work went on,
		// and another poller took the Run over mid-effect the moment the
		// graph's total run time passed LeaseFor. Sealing blocks takeover, not
		// the owner's own renewal, so this is the extension available here.
		if lease, err := s.runtime.deps.Store.Renew(ctx, store.RenewCommand{
			RunID: runID, LeaseToken: s.leaseToken(), LeaseFor: s.runtime.deps.LeaseFor,
		}); err != nil {
			outcome.err = err
			cancel()
			return
		} else {
			s.setLease(lease)
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// A negative cap is the rollback: renewal is unbounded again, exactly as
		// it was before the cap existed.
		var deadline <-chan time.Time
		if limit := s.runtime.deps.MaxNodeDuration; limit > 0 {
			timer := time.NewTimer(limit)
			defer timer.Stop()
			deadline = timer.C
		}
		// Both stay nil until the cap fires: grace is the wind-down window, and
		// effectDone is dropped once we are the ones who cancelled, so our own
		// cancellation does not end the renewal the wind-down still needs.
		var grace <-chan time.Time
		effectDone := effectCtx.Done()

		// Once the cap has fired, every way out of this loop is an abandonment,
		// not only the one that waits out the grace period: a renewal that fails
		// mid-wind-down leaves the same wedged handler, and returning without
		// saying so would leave the node blocked on a handler that never returns
		// — the wedged slot this whole mechanism exists to release.
		winding := false
		defer func() {
			if winding {
				close(abandoned)
			}
		}()
		for {
			select {
			case <-stopping:
				return
			case <-effectDone:
				return
			case <-deadline:
				// The node outran its wall-clock budget. Cancel it and keep the
				// lease alive: whichever way the wind-down ends, this worker is
				// the one that records it.
				outcome.capped = true
				cancel()
				winding = true
				deadline, effectDone = nil, nil
				timer := time.NewTimer(abandonGraceFor(s.runtime.deps.MaxNodeDuration))
				defer timer.Stop()
				grace = timer.C
			case <-grace:
				// Cancelled and still running. Renewal stops here so the lease
				// lapses on its own, and the node's caller parks the Run out of
				// running before it can — a Run that is not running is not one
				// another worker can take over.
				return
			case <-ticker.C:
				lease, err := s.runtime.deps.Store.Renew(ctx, store.RenewCommand{
					RunID: runID, LeaseToken: s.leaseToken(), LeaseFor: s.runtime.deps.LeaseFor,
				})
				if err != nil {
					// A renewal that fails after the cap has fired changes
					// nothing: the node is abandoned either way, and reporting
					// lost ownership instead would send it back down the path
					// that leaves the Run running.
					if !outcome.capped {
						outcome.err = err
					}
					// Cancelling the effect context is what stops the in-flight
					// call. An already-started side effect still follows the
					// Invocation reconciliation rules — it is never retried on
					// the assumption it did not happen.
					cancel()
					return
				}
				s.setLease(lease)
			}
		}
	}()

	return effectCtx, abandoned, func() windDown {
		once.Do(func() { close(stopping) })
		<-done
		cancel()
		return outcome
	}
}
