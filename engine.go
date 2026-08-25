package agentruntime

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"sync"
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
	leaseMu  sync.Mutex
	lease    store.Lease
	snapshot run.Snapshot
	declared definition.Definition
	graph    *workflow.ExecutionGraph
	policies policy.Snapshot
	quotas   *quota.Enforcer
	scope    quota.Scope
	// approvalHold is the write parked for a human. Loaded from Checkpoint
	// on resume so confirm can perform that effect.
	approvalHold *approvalHold
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

func (r *runtime) advanceClaimed(ctx context.Context, claimed store.ClaimedRun) (AdvanceResult, error) {
	current, err := r.newSession(ctx, claimed)
	if err != nil {
		return AdvanceResult{}, err
	}

	if current.snapshot.State == run.StateQueued {
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
		return AdvanceResult{Run: current.snapshot}, err
	}

	for {
		if current.snapshot.State.Terminal() {
			return AdvanceResult{Run: current.snapshot}, nil
		}
		if current.snapshot.State.Waiting() {
			return AdvanceResult{Run: current.snapshot, Waiting: true}, nil
		}

		ready := workflow.ReadyNodes(current.graph, current.snapshot)
		if len(ready) == 0 {
			// Nothing schedulable and nothing waiting: either the budget is
			// gone or every remaining node is blocked behind a failure. Either
			// way the Run is finished, and the scheduler's own accounting says
			// how.
			return current.finish(ctx)
		}
		if err := current.runNode(ctx, ready[0]); err != nil {
			return AdvanceResult{Run: current.snapshot}, err
		}
	}
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
		return nil, run.NewError("graph_mismatch", run.ErrorConflict, run.RetryNever)
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
		RunID:                         s.snapshot.ID,
		ExpectedRevision:              s.snapshot.Revision,
		LeaseToken:                    s.leaseToken(),
		ExpectedRootCancellationEpoch: s.snapshot.RootCancellationEpoch,
	}
}

// start moves a queued Run to running.
func (s *session) start(ctx context.Context) error {
	transition, err := run.Reduce(s.snapshot, run.Command{Kind: run.CommandStart})
	if err != nil {
		return err
	}
	committed, err := s.commitNode(ctx, transition, "", "", run.Limits{}, nil)
	if err != nil {
		return err
	}
	s.snapshot = committed
	return nil
}

func approvalRequired(err error) bool {
	return tool.IsApprovalRequired(err)
}

func (s *session) parkForApproval(ctx context.Context) error {
	transition, err := run.Reduce(s.snapshot, run.Command{Kind: run.CommandWaitApproval})
	if err != nil {
		return err
	}
	approvalID := s.runtime.deps.IDs.NewID("approval")
	for i := range transition.Events {
		if transition.Events[i].To == run.StateWaitingApproval {
			transition.Events[i].ApprovalID = approvalID
		}
	}
	transition.Next.PendingApprovalID = approvalID
	if s.approvalHold != nil {
		transition.Next.Checkpoint = encodeApprovalHold(*s.approvalHold)
	}
	s.runtime.record(observe.Decision{
		Name:  observe.EventApprovalRequested,
		RunID: s.snapshot.ID,
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
	s.snapshot = committed
	return nil
}

// runNode executes one node and commits its result.
func (s *session) runNode(ctx context.Context, node workflow.Node) error {
	factory, err := s.runtime.deps.Agents.Lookup(node.Implementation)
	if err != nil {
		return err
	}
	implementation, err := factory.New(nil)
	if err != nil {
		return err
	}

	s.runtime.record(observe.Decision{
		Name: observe.EventRouterPlanSelected, RunID: s.snapshot.ID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrPlan, s.graph.Ref.Digest),
			observe.Attr(observe.AttrAgent, node.Implementation),
			observe.Attr(observe.AttrNode, node.ID),
		},
	})

	// The renew loop runs for as long as the effect does. A node that calls a
	// slow model can outlive the lease it was claimed under, and a worker whose
	// lease lapsed mid-effect must not be the one that commits the result.
	effectCtx, stop := s.renewing(ctx)
	ports := &governedPorts{session: s, node: node}
	built, err := s.request(ctx, node, ports)
	if err != nil {
		stop()
		return err
	}
	response, executeErr := implementation.Execute(effectCtx, built)
	renewErr := stop()

	if executeErr != nil && approvalRequired(executeErr) {
		return s.parkForApproval(ctx)
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
		slog.Error("agent node failed",
			"run_id", string(s.snapshot.ID),
			"node_id", node.ID,
			"agent", node.Implementation,
			"kind", string(run.KindOf(executeErr)),
			"err", executeErr)
	} else {
		result.OutputRef = string(s.runtime.deps.IDs.NewID("output"))
	}

	progress, err := workflow.ApplyNodeResult(s.graph, s.snapshot, result, s.runtime.deps.Schemas)
	if err != nil {
		return err
	}

	command := run.Command{Kind: run.CommandSucceed}
	if progress.Command != nil {
		command = *progress.Command
	}
	transition, err := s.transitionFor(progress, command)
	if err != nil {
		return err
	}

	fact, err := s.nodeFact(node, result)
	if err != nil {
		return err
	}

	committed, err := s.commitNode(ctx, transition, node.ID, result.OutputRef, response.Used, response.ModelUsage, fact)
	if err != nil {
		return err
	}
	s.snapshot = committed
	return nil
}

// nodeFact is what one finished node contributes to the host's transcript.
//
// A succeeded node contributes its output; a failed one contributes a progress
// marker instead. Not both: the outbox key is one sequence per fact and the
// distinction is what the host is reading for — an output fact for a node that
// produced nothing would render as an empty assistant message rather than as a
// step that failed.
func (s *session) nodeFact(node workflow.Node, result workflow.NodeResult) (store.ProjectionFact, error) {
	if result.Failed {
		return store.NewProjectionFact(store.ProgressPayload{
			NodeID: node.ID, AgentKey: node.Implementation, Failed: true,
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
		next := s.snapshot
		next.Revision = s.snapshot.Revision + 1
		next.Nodes = progress.Nodes
		return run.Transition{Next: next}, nil
	}
	transition, err := run.Reduce(s.snapshot, command)
	if err != nil {
		return run.Transition{}, err
	}
	transition.Next.Nodes = progress.Nodes
	return transition, nil
}

// finish terminates a Run that has nothing left to schedule.
func (s *session) finish(ctx context.Context) (AdvanceResult, error) {
	command := run.Command{Kind: run.CommandPartial}
	if !s.snapshot.Budget.Affords(run.Limits{LLMCalls: 1}) {
		// Out of budget with work outstanding is a partial result, not a
		// failure: what did complete is still the honest answer.
		s.runtime.record(observe.Decision{
			Name: observe.EventBudgetRefused, RunID: s.snapshot.ID,
			Attributes: []observe.Attribute{observe.Attr(observe.AttrUnit, "llm_calls")},
		})
	}
	if len(s.snapshot.Nodes) == 0 {
		command = run.Command{Kind: run.CommandFail}
	}

	transition, err := run.Reduce(s.snapshot, command)
	if err != nil {
		return AdvanceResult{Run: s.snapshot}, err
	}
	committed, err := s.commitNode(ctx, transition, "", "", run.Limits{}, nil)
	if err != nil {
		return AdvanceResult{Run: s.snapshot}, err
	}
	s.snapshot = committed
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
	if s.snapshot.State != run.StateRunning {
		return nil
	}
	for id, invocation := range s.snapshot.Invocations {
		if invocation.Outcome != run.OutcomeInFlight {
			continue
		}
		transition, err := run.Reduce(s.snapshot, run.Command{
			Kind: run.CommandRecordUnknown, InvocationID: id,
		})
		if err != nil {
			return err
		}
		committed, err := s.commitNode(ctx, transition, "", "", run.Limits{}, nil)
		if err != nil {
			return err
		}
		s.snapshot = committed
		return nil
	}
	return nil
}

// commitNode seals the lease and submits one typed command.
//
// Sealing first is what keeps a commit and a takeover from both believing they
// own the Run. Seal uses Store-authoritative time and refuses an expired lease
// even when the token and revision still match — a worker judging expiry by its
// own clock hands ownership away whenever the two disagree.
// Terminal facts are derived here rather than passed in, so no terminal path
// can forget one: every commit that settles a Run goes through this function,
// and a caller that had to remember would eventually be a caller that did not.
func (s *session) commitNode(
	ctx context.Context, transition run.Transition, node, outputRef string,
	used run.Limits, modelUsage []run.ModelUsage, facts ...store.ProjectionFact,
) (run.Snapshot, error) {
	if _, err := s.runtime.deps.Store.SealForCommit(ctx, store.SealCommand{
		RunID: s.snapshot.ID, LeaseToken: s.leaseToken(),
	}); err != nil {
		return run.Snapshot{}, err
	}

	name := node
	if name == "" {
		name = "run"
	}
	if transition.Next.State.Terminal() {
		userID, _ := strconv.ParseInt(s.snapshot.Principal.Subject, 10, 64)
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

	// The seal closed the lease to renewal, so a further effect in this
	// Advance needs a fresh claim. Re-claiming here keeps the loop honest about
	// ownership rather than reusing a token the Store has already retired.
	lease, _, err := s.runtime.deps.Store.Claim(ctx, store.ClaimCommand{
		RunID: s.snapshot.ID, Owner: s.runtime.deps.Owner, LeaseFor: s.runtime.deps.LeaseFor,
	})
	if err == nil {
		s.setLease(lease)
	}
	return committed, nil
}

// request builds what the agent implementation sees.
func (s *session) request(ctx context.Context, node workflow.Node, ports *governedPorts) (agent.Request, error) {
	remaining := s.snapshot.Budget.Envelope
	if !remaining.Zero() {
		committed := s.snapshot.Budget.Committed()
		remaining = run.Limits{
			LLMCalls:  remaining.LLMCalls - committed.LLMCalls,
			Tokens:    remaining.Tokens - committed.Tokens,
			ToolCalls: remaining.ToolCalls - committed.ToolCalls,
		}
	}
	nodeInput, err := s.input(ctx, node)
	if err != nil {
		return agent.Request{}, err
	}
	req := agent.Request{
		RunID:     s.snapshot.ID,
		Principal: s.snapshot.Principal,
		Prompt:    s.declared.Prompt,
		Input:     nodeInput,
		Upstreams: s.snapshot.Upstreams,
		Remaining: remaining,
		Ports:     ports,
	}
	if s.approvalHold != nil && !s.approvalHold.Denied && s.snapshot.PendingApprovalID == "" && s.snapshot.State == run.StateRunning {
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
			if state, ok := s.snapshot.Nodes[from]; !ok || state.OutputRef == "" {
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
			merged[workflow.RunInputKey] = s.snapshot.Input
		}
		encoded, err := json.Marshal(merged)
		if err != nil {
			return nil, run.NewError("unencodable_input", run.ErrorInternal, run.RetryNever, err)
		}
		return encoded, nil
	default:
		return s.snapshot.Input, nil
	}
}

func (s *session) upstreamOutput(ctx context.Context, from string) (json.RawMessage, error) {
	state, ok := s.snapshot.Nodes[from]
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
	return s.runtime.deps.Store.NodeOutput(ctx, s.snapshot.ID, state.OutputRef)
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

// renewing starts a bounded renew loop and returns a context that is cancelled
// the moment ownership is lost.
//
// One loop per active Advance, refreshing before a third of the TTL has
// elapsed. A third rather than a half so that one missed tick is survivable:
// renewing at the last moment means the first hiccup is a lost lease.
func (s *session) renewing(ctx context.Context) (context.Context, func() error) {
	effectCtx, cancel := context.WithCancel(ctx)
	interval := s.runtime.deps.LeaseFor / 3
	if interval <= 0 {
		interval = time.Millisecond
	}

	var (
		once     sync.Once
		done     = make(chan struct{})
		stopping = make(chan struct{})
		failure  error
	)
	// Captured before the goroutine starts. The ID never changes, but the
	// Snapshot around it is replaced on every commit, so reading it from here
	// would race the node that is committing.
	runID := s.snapshot.ID
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
			failure = err
			cancel()
			return
		} else {
			s.setLease(lease)
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopping:
				return
			case <-effectCtx.Done():
				return
			case <-ticker.C:
				lease, err := s.runtime.deps.Store.Renew(ctx, store.RenewCommand{
					RunID: runID, LeaseToken: s.leaseToken(), LeaseFor: s.runtime.deps.LeaseFor,
				})
				if err != nil {
					failure = err
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

	return effectCtx, func() error {
		once.Do(func() { close(stopping) })
		<-done
		cancel()
		return failure
	}
}
