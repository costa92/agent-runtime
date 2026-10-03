package agentruntime_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/memory"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
)

// --- fixtures -------------------------------------------------------------

func TestALostLeaseRejectsTheResultInsteadOfCommittingIt(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		// While the effect is in flight, someone else takes the Run.
		<-ctx.Done()
		return agent.Response{}, ctx.Err()
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.LeaseFor = 30 * time.Millisecond
	}))
	started := start(t, h)

	go func() {
		time.Sleep(5 * time.Millisecond)
		// Store-authoritative expiry: wall-clock sleep does not lapse the
		// MemoryStore lease. Advance the clock past the deadline, then take over.
		h.clock.Advance(time.Second)
		_, _, _ = h.store.Claim(context.Background(), store.ClaimCommand{
			RunID: started.ID, Owner: "worker-2", LeaseFor: time.Second,
		})
	}()

	_, err := h.runtime.Advance(t.Context(), started.ID)
	if err == nil {
		t.Fatal("a worker that lost its lease committed anyway")
	}
	if run.KindOf(err) != run.ErrorConflict {
		t.Fatalf("kind=%s want=conflict", run.KindOf(err))
	}
}

// A picture-book render lasts minutes. A 30s lease that is not renewed mid-effect
// is stolen, the invocation is parked as unknown, and the book never lands.

func TestAdvanceRenewsTheLeaseWhileAnEffectIsInFlight(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		time.Sleep(80 * time.Millisecond)
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.LeaseFor = 30 * time.Millisecond
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if h.store.Renews.Load() == 0 {
		t.Fatal("the lease was not renewed while the effect ran")
	}
}

// The renew loop ticks at LeaseFor/3, so a node that finishes sooner than that
// never renews at all — and the sibling test above hides this by sleeping well
// past a tick. Real nodes are fast: a model call answers in seconds against a
// thirty-second lease, and one Advance walks the whole graph under a single
// claim. Node after node the loop started and stopped before its first tick,
// the deadline stayed pinned at claim time, and once the graph's total run time
// passed LeaseFor another poller took the Run over and parked whatever was in
// flight. That is how every article generation ended in waiting_resolution.

func TestTheLeaseIsRenewedEvenWhenTheNodeIsFasterThanATick(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.LeaseFor = time.Minute
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if h.store.Renews.Load() == 0 {
		t.Fatal("a node that outran the renew ticker left the lease pinned at claim time")
	}
}

func TestARunWithAnInFlightEffectIsNotReExecuted(t *testing.T) {
	attempts := 0
	h := newHarness(t, scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		attempts++
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	// The crash is injected at the Store, which is where it happens: the begin
	// fact is committed and nothing settles it, exactly as a killed process
	// leaves it.
	ctx := t.Context()
	lease, snapshot, err := h.store.Claim(ctx, store.ClaimCommand{
		RunID: started.ID, Owner: "dying-worker", LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	running, err := run.Reduce(snapshot, run.Command{Kind: run.CommandStart})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	committed, err := h.store.CommitNodeResult(ctx, store.CommitNodeResultCommand{
		Fence:    store.ExecutionFence{RunID: started.ID, ExpectedRevision: snapshot.Revision, LeaseToken: lease.Token},
		NodeName: "run",
		Commit:   store.CommitContext{Transition: running, Events: running.Events},
	})
	if err != nil {
		t.Fatalf("commit start: %v", err)
	}

	begun, err := run.Reduce(committed, run.Command{
		Kind: run.CommandInvokeModel, InvocationID: "inv-1",
		IdempotencyKey: "key-1", Reserve: run.Limits{LLMCalls: 1, Tokens: 10},
	})
	if err != nil {
		t.Fatalf("reduce begin: %v", err)
	}
	if _, err := h.store.BeginInvocation(ctx, store.BeginInvocationCommand{
		Fence: store.ExecutionFence{RunID: started.ID, ExpectedRevision: committed.Revision, LeaseToken: lease.Token},
		Invocation: store.InvocationBegin{
			ID: "inv-1", IdempotencyKey: "key-1",
			Reservation: store.BudgetReservation{ID: "inv-1", Amount: run.Limits{LLMCalls: 1, Tokens: 10}},
		},
		Commit: store.CommitContext{Transition: begun, Events: begun.Events},
	}); err != nil {
		t.Fatalf("begin: %v", err)
	}

	// The worker is gone. Its lease lapses and the Run is claimable again.
	before := attempts
	h.clock.Advance(time.Hour)
	result, err := h.runtime.Advance(ctx, started.ID)

	if attempts > before {
		t.Fatalf("the node ran again; its effect may already have happened")
	}
	if err == nil && result.Run.State != run.StateWaitingResolution {
		t.Fatalf("state = %s; an effect nobody can classify has to be resolved, "+
			"not stepped over", result.Run.State)
	}
}

// A Run whose effects all settled is not parked.
//
// Without this the guard above would park every Run that ever made a call, and
// the whole runtime would stop at the first model request waiting for a human.

func TestASettledEffectDoesNotParkTheRun(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Model(ctx, llm.Request{
			Messages: []llm.Message{{Role: "user", Content: "hi"}},
		}); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.State == run.StateWaitingResolution {
		t.Fatal("a Run whose call completed is waiting for somebody to classify it")
	}
	if !result.Run.State.Terminal() {
		t.Fatalf("state = %s", result.Run.State)
	}
}

type failNodeCommitOnce struct {
	store.Execution
	failed bool
}

type countingMemoryProvider struct{ writes int }

func (*countingMemoryProvider) Retrieve(context.Context, memory.Query) ([]memory.Record, error) {
	return nil, nil
}

func (p *countingMemoryProvider) Write(context.Context, memory.Scope, string, string) (memory.Mutation, error) {
	p.writes++
	return memory.Mutation{Ref: "saved"}, nil
}

func TestCompletedMemoryWriteIsNotRepeatedAfterNodeCommitCrash(t *testing.T) {
	provider := &countingMemoryProvider{}
	registry := memory.NewRegistry()
	if err := registry.Register("notes", provider); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if err := request.Ports.Remember(ctx, "notes", "ref", "text", "one-write"); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`"done"`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:  run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode: definition.ModeSpecialist, Implementation: "answer",
		Model:    definition.ModelPolicy{Profile: "fast"},
		Memories: []definition.MemoryRef{{Key: "notes", Namespace: "ns", Writable: true, MaxRecords: 8, MaxTokens: 600}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Memories = memory.NewGateway(registry, testkit.AllowAllMemoryAuthorizer())
		deps.Store = &failNodeCommitOnce{Execution: deps.Store}
	}))
	started := start(t, h)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err == nil {
		t.Fatal("node commit was not interrupted")
	}
	if provider.writes != 1 {
		t.Fatalf("memory writes after first execution = %d, want 1", provider.writes)
	}
	h.clock.Advance(time.Minute)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatal(err)
	}
	if provider.writes != 1 {
		t.Fatalf("recovery repeated memory write %d times", provider.writes)
	}
}

func (s *failNodeCommitOnce) CommitNodeResult(ctx context.Context, command store.CommitNodeResultCommand) (run.Snapshot, error) {
	if command.OutputRef != "" && !s.failed {
		s.failed = true
		return run.Snapshot{}, errors.New("injected process loss after tool settlement")
	}
	return s.Execution.CommitNodeResult(ctx, command)
}

func TestCompletedWriteIsNotRepeatedAfterNodeCommitCrash(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"id":"created"}`)
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "create book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"title":"one"}`)); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`"done"`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:  run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode: definition.ModeSpecialist, Implementation: "answer",
		Model: definition.ModelPolicy{Profile: "fast"},
		Tools: []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{policies: policy.Snapshot{Policies: []policy.Policy{{
			Name: "allow-test-write", Scope: policy.ScopeTenant,
			Decision: policy.DecisionAllow,
		}}}}
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
		deps.Store = &failNodeCommitOnce{Execution: deps.Store}
	}))
	started := start(t, h)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err == nil {
		t.Fatal("node commit was not interrupted")
	}
	if got := handler.Calls(); got != 1 {
		t.Fatalf("first execution called write %d times, want 1", got)
	}
	h.clock.Advance(time.Minute)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if got := handler.Calls(); got != 1 {
		t.Fatalf("recovery repeated a completed write %d times", got)
	}
}

func TestReconciledAppliedWriteIsNotExecutedAgain(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"id":"duplicate"}`)
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "create book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"title":"one"}`))
		return agent.Response{Output: json.RawMessage(`"done"`)}, err
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
		Graph: definition.GraphSpec{Nodes: []definition.NodeSpec{
			{Name: "a_after", Agent: "answer", DependsOn: []string{"z_writer"}, Optional: true},
			{Name: "z_writer", Agent: "answer", Tools: []string{"render_picture_book"}},
		}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{policies: policy.Snapshot{Policies: []policy.Policy{{
			Name: "allow-test-write", Scope: policy.ScopeTenant,
			Decision: policy.DecisionAllow,
		}}}}
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
	started := start(t, h)
	ctx := t.Context()
	lease, queued, err := h.store.Claim(ctx, store.ClaimCommand{
		RunID: started.ID, Owner: "dying-worker", LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	transition, err := run.Reduce(queued, run.Command{Kind: run.CommandStart})
	if err != nil {
		t.Fatal(err)
	}
	running, err := h.store.CommitNodeResult(ctx, store.CommitNodeResultCommand{
		Fence:    store.ExecutionFence{RunID: started.ID, ExpectedRevision: queued.Revision, LeaseToken: lease.Token},
		NodeName: "run", Commit: store.CommitContext{Transition: transition, Events: transition.Events},
	})
	if err != nil {
		t.Fatal(err)
	}
	transition, err = run.Reduce(running, run.Command{
		Kind: run.CommandInvokeTool, InvocationID: "tool-lost", NodeID: "z_writer", Tool: "render_picture_book", Write: true,
		IdempotencyKey: "tool-lost", Reserve: run.Limits{ToolCalls: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.BeginInvocation(ctx, store.BeginInvocationCommand{
		Fence: store.ExecutionFence{RunID: started.ID, ExpectedRevision: running.Revision, LeaseToken: lease.Token},
		Invocation: store.InvocationBegin{
			ID: "tool-lost", NodeID: "z_writer", Tool: "render_picture_book", Write: true, IdempotencyKey: "tool-lost",
			Reservation: store.BudgetReservation{ID: "tool-lost", Amount: run.Limits{ToolCalls: 1}},
		},
		Commit: store.CommitContext{Transition: transition, Events: transition.Events},
	}); err != nil {
		t.Fatal(err)
	}
	h.clock.Advance(time.Hour)
	parked, err := h.runtime.Advance(ctx, started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if parked.Run.State != run.StateWaitingResolution {
		t.Fatalf("state = %s, want waiting_resolution", parked.Run.State)
	}
	stillUnknown, err := h.runtime.ResolveInvocation(ctx, agentruntime.InvocationResolution{
		RunID: started.ID, InvocationID: "tool-lost", Outcome: run.OutcomeStillUnknown,
		ResolvedBy: principal(), Reason: "external status pending",
	})
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := h.runtime.ResolveInvocation(ctx, agentruntime.InvocationResolution{
		RunID: started.ID, InvocationID: "tool-lost", Outcome: run.OutcomeStillUnknown,
		ResolvedBy: principal(), Reason: "external status pending",
	})
	if err != nil || repeated.Revision != stillUnknown.Revision {
		t.Fatalf("repeated still_unknown: revision=%d, want %d; error=%v", repeated.Revision, stillUnknown.Revision, err)
	}
	if _, err := h.runtime.ResolveInvocation(ctx, agentruntime.InvocationResolution{
		RunID: started.ID, InvocationID: "tool-lost", Outcome: run.OutcomeApplied,
		ExpectedRevision: parked.Run.Revision, ResolvedBy: principal(), Reason: "stale tab",
	}); run.KindOf(err) != run.ErrorConflict {
		t.Fatalf("stale client revision was accepted: %v", err)
	}
	if _, err := h.runtime.ResolveInvocation(ctx, agentruntime.InvocationResolution{
		RunID: started.ID, InvocationID: "tool-lost", Outcome: run.OutcomeApplied,
		ExpectedRevision: stillUnknown.Revision, ResolvedBy: principal(), Reason: "confirmed external action",
	}); err != nil {
		t.Fatal(err)
	}
	h.clock.Advance(time.Hour)
	if _, err := h.runtime.Advance(ctx, started.ID); err != nil {
		t.Fatal(err)
	}
	if handler.Calls() != 0 {
		t.Fatalf("resolved applied write was repeated %d times", handler.Calls())
	}
}

// A parked approval the client cannot name cannot be answered. The id has to
// live on the Snapshot and on the waiting_approval event, because Inspect
// and the event stream are the only two reads the confirm UI has.
