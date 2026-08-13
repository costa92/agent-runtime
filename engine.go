package agentruntime

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/quota"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

// session is one claimed Run being advanced by one worker.
//
// Everything an effect needs is assembled here, once, and nothing in it is
// shared between Runs: a session that outlived its lease would be a worker
// acting on a Run it no longer owns.
type session struct {
	runtime  *runtime
	lease    store.Lease
	snapshot run.Snapshot
	declared definition.Definition
	graph    *workflow.ExecutionGraph
	policies policy.Snapshot
	quotas   *quota.Enforcer
	scope    quota.Scope
}

// AdvanceNext is the Worker entry point.
//
// Workers never query runnable IDs and never touch the Store: they ask for
// work and get an atomically leased claim. A worker that could list runnable
// Runs and then claim them separately would race every other worker in the
// window between the two calls.
func (r *runtime) AdvanceNext(ctx context.Context, request AdvanceNextRequest) (AdvanceResult, bool, error) {
	if request.Limit <= 0 {
		request.Limit = 1
	}
	claimed, err := r.deps.Store.ClaimBatch(ctx, store.ClaimBatchCommand{
		Owner:       r.deps.Owner,
		Limit:       request.Limit,
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

	for {
		if current.snapshot.State.Terminal() {
			return AdvanceResult{Run: current.snapshot}, nil
		}
		// Delegation is checked before the waiting states, because
		// waiting_children is exactly the state a parent resumes from: whether
		// its children have finished is the question this call exists to ask.
		if current.orchestrating() {
			// An orchestrator's work is its children. Running its own graph
			// node as well would give the same Run two ways to produce a
			// result, and nothing downstream could say which one was the
			// answer.
			delegated, err := current.delegate(ctx)
			if err != nil {
				return AdvanceResult{Run: current.snapshot}, err
			}
			if delegated {
				if current.snapshot.State.Terminal() {
					return AdvanceResult{Run: current.snapshot}, nil
				}
				return AdvanceResult{Run: current.snapshot, Waiting: true}, nil
			}
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
		runtime:  r,
		lease:    claimed.Lease,
		snapshot: snapshot,
		declared: declared,
		graph:    graph,
		policies: policies,
		quotas:   enforcer,
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
		LeaseToken:                    s.lease.Token,
		ExpectedRootCancellationEpoch: s.snapshot.RootCancellationEpoch,
	}
}

// start moves a queued Run to running.
func (s *session) start(ctx context.Context) error {
	transition, err := run.Reduce(s.snapshot, run.Command{Kind: run.CommandStart})
	if err != nil {
		return err
	}
	committed, err := s.commitNode(ctx, transition, "", "", run.Limits{})
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
	response, executeErr := implementation.Execute(effectCtx, s.request(node, ports))
	renewErr := stop()

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

	committed, err := s.commitNode(ctx, transition, node.ID, result.OutputRef, response.Used)
	if err != nil {
		return err
	}
	s.snapshot = committed
	return nil
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
	committed, err := s.commitNode(ctx, transition, "", "", run.Limits{})
	if err != nil {
		return AdvanceResult{Run: s.snapshot}, err
	}
	s.snapshot = committed
	return AdvanceResult{Run: committed}, nil
}

// commitNode seals the lease and submits one typed command.
//
// Sealing first is what keeps a commit and a takeover from both believing they
// own the Run. Seal uses Store-authoritative time and refuses an expired lease
// even when the token and revision still match — a worker judging expiry by its
// own clock hands ownership away whenever the two disagree.
func (s *session) commitNode(ctx context.Context, transition run.Transition, node, outputRef string, used run.Limits) (run.Snapshot, error) {
	if _, err := s.runtime.deps.Store.SealForCommit(ctx, store.SealCommand{
		RunID: s.snapshot.ID, LeaseToken: s.lease.Token,
	}); err != nil {
		return run.Snapshot{}, err
	}

	name := node
	if name == "" {
		name = "run"
	}
	committed, err := s.runtime.deps.Store.CommitNodeResult(ctx, store.CommitNodeResultCommand{
		Fence:     s.fence(),
		NodeName:  name,
		OutputRef: outputRef,
		Usage:     used,
		Commit:    store.CommitContext{Transition: transition, Events: transition.Events},
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
		s.lease = lease
	}
	return committed, nil
}

// request builds what the agent implementation sees.
func (s *session) request(node workflow.Node, ports *governedPorts) agent.Request {
	remaining := s.snapshot.Budget.Envelope
	if !remaining.Zero() {
		committed := s.snapshot.Budget.Committed()
		remaining = run.Limits{
			LLMCalls:  remaining.LLMCalls - committed.LLMCalls,
			Tokens:    remaining.Tokens - committed.Tokens,
			ToolCalls: remaining.ToolCalls - committed.ToolCalls,
		}
	}
	return agent.Request{
		RunID:     s.snapshot.ID,
		Principal: s.snapshot.Principal,
		Prompt:    s.declared.Prompt,
		Input:     s.input(node),
		Remaining: remaining,
		Ports:     ports,
	}
}

func (s *session) input(node workflow.Node) json.RawMessage {
	if node.Input.Source == workflow.SourceNode {
		if state, ok := s.snapshot.Nodes[node.Input.From]; ok && state.OutputRef != "" {
			return json.RawMessage(`{"from":"` + state.OutputRef + `"}`)
		}
	}
	return nil
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
	go func() {
		defer close(done)
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
					RunID: s.snapshot.ID, LeaseToken: s.lease.Token, LeaseFor: s.runtime.deps.LeaseFor,
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
				s.lease = lease
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
