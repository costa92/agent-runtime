package agentruntime_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
)

// These tests are about the wall-clock cap on one node's execution.
//
// Two handlers appear here, and the difference between them is the whole point.
// wedgedAgent is the one the cap exists for: it ignores ctx entirely, so only
// the Runtime's own wind-down ends the node. It returns eventually rather than
// blocking forever so that a Runtime without the cap fails these assertions
// instead of hanging the suite.
//
// cancellableAgent is the common case, and for a while it was the untested one:
// a handler that returns the moment we cancel it at the cap. Hitting the cap is
// the same governance decision either way, so it must reach the same place. It
// did not — the well-behaved return fell through to the lost-ownership branch,
// which left the Run running with renewal already stopped, and the next worker
// re-executed the node.

// wedgedAgent runs for wedgeFor no matter what the context says.
func wedgedAgent(wedgeFor time.Duration, returned *atomic.Bool) agent.Agent {
	return scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		time.Sleep(wedgeFor)
		returned.Store(true)
		return agent.Response{Output: json.RawMessage(`"too late"`)}, nil
	}}
}

// cancellableAgent returns as soon as its context is cancelled.
func cancellableAgent(returned *atomic.Bool) agent.Agent {
	return scriptedAgent{execute: func(ctx context.Context, _ agent.Request) (agent.Response, error) {
		<-ctx.Done()
		returned.Store(true)
		return agent.Response{}, ctx.Err()
	}}
}

// A handler that honours the cancellation is still a node that hit its cap, and
// the cap's whole promise is that this worker keeps the Run. Returning the
// deadline up the lost-ownership path instead left the Run running with renewal
// stopped: the lease lapsed, another worker claimed it, and the node re-executed
// from scratch — every effect that had already completed, done twice. A node
// that deterministically overruns turned into a claim/lapse/reclaim loop that
// burned a worker slot forever and never once said it had been abandoned.

func TestANodeIsAbandonedEvenWhenItsHandlerHonoursTheCancellation(t *testing.T) {
	var returned atomic.Bool
	h := newHarness(t, cancellableAgent(&returned),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.LeaseFor = 300 * time.Millisecond
			deps.MaxNodeDuration = 150 * time.Millisecond
		}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if !returned.Load() {
		t.Fatal("the handler never saw the cancellation; this test proves nothing about the honoured case")
	}
	if result.Run.State != run.StateWaitingResolution {
		t.Fatalf("state=%s, want waiting_resolution", result.Run.State)
	}
	if len(h.observer.named(observe.EventNodeAbandoned)) != 1 {
		t.Fatal("a node that hit its cap said nothing about it")
	}

	h.clock.Advance(time.Hour)
	if _, _, err := h.store.Claim(context.Background(), store.ClaimCommand{
		RunID: started.ID, Owner: "worker-2", LeaseFor: time.Minute,
	}); err == nil {
		t.Fatal("another worker claimed the Run and will re-execute the node")
	}
}

func TestRenewalStopsAtTheNodeDeadline(t *testing.T) {
	var returned atomic.Bool
	h := newHarness(t, wedgedAgent(2*time.Second, &returned),
		withDeps(func(deps *agentruntime.Dependencies) {
			// A 100ms tick against a 250ms cap: two ticks plus the renewal the
			// loop does up front, and nothing after the cap.
			deps.LeaseFor = 300 * time.Millisecond
			deps.MaxNodeDuration = 250 * time.Millisecond
		}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if returned.Load() {
		t.Fatal("the handler returned on its own; this test proves nothing about the cap")
	}

	atDeadline := h.store.Renews.Load()
	if atDeadline > 4 {
		t.Fatalf("renewals = %d, want no more than 4: the loop renewed past the node cap", atDeadline)
	}
	// The loop is gone, not merely between ticks.
	time.Sleep(400 * time.Millisecond)
	if after := h.store.Renews.Load(); after != atDeadline {
		t.Fatalf("renewals kept climbing after the cap: %d -> %d", atDeadline, after)
	}
}

// The one that matters. Reserve-before-effect makes the begin fact durable
// before the effect is issued, so a handler still running may already have
// published. Releasing the lease here invites a second worker to do it again.

func TestTheLeaseIsNotReleasedForTakeoverAfterTheDeadline(t *testing.T) {
	var returned atomic.Bool
	h := newHarness(t, wedgedAgent(2*time.Second, &returned),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.LeaseFor = 300 * time.Millisecond
			deps.MaxNodeDuration = 150 * time.Millisecond
		}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if returned.Load() {
		t.Fatal("the handler returned on its own; this test proves nothing about the cap")
	}
	if !result.Waiting || result.Run.State != run.StateWaitingResolution {
		t.Fatalf("state=%s waiting=%t, want waiting_resolution", result.Run.State, result.Waiting)
	}

	// Let the lease lapse, which is what abandoning it looks like from another
	// worker's side, and try to take the Run over while the effect is still in
	// flight.
	h.clock.Advance(time.Hour)
	if _, _, err := h.store.Claim(context.Background(), store.ClaimCommand{
		RunID: started.ID, Owner: "worker-2", LeaseFor: time.Minute,
	}); err == nil {
		t.Fatal("another worker claimed a Run whose effect is still running: this is the double write")
	}
}

// settlingBehindTheAbandonment reproduces the interleaving where the detached
// handler's settle lands first.
//
// The abandonment's own commit is what this intercepts: before it goes through,
// a commit is made under the same lease at the revision the session still
// believes in — which is exactly what the leaked handler's settle does, and it
// leaves the abandonment holding a stale fence.
type settlingBehindTheAbandonment struct {
	store.Execution
	raced bool
}

func (s *settlingBehindTheAbandonment) CommitNodeResult(
	ctx context.Context, command store.CommitNodeResultCommand,
) (run.Snapshot, error) {
	if !s.raced && command.NodeName != "run" {
		s.raced = true
		current, err := s.Execution.Get(ctx, command.Fence.RunID)
		if err != nil {
			return run.Snapshot{}, err
		}
		ahead := run.Transition{Next: current}
		ahead.Next.Revision = current.Revision + 1
		if _, err := s.Execution.CommitNodeResult(ctx, store.CommitNodeResultCommand{
			Fence: store.ExecutionFence{
				RunID:            command.Fence.RunID,
				ExpectedRevision: current.Revision,
				LeaseToken:       command.Fence.LeaseToken,
			},
			NodeName: "leaked-settle",
			Commit:   store.CommitContext{Transition: ahead},
		}); err != nil {
			return run.Snapshot{}, err
		}
	}
	return s.Execution.CommitNodeResult(ctx, command)
}

// The settle path never consults the Run's state, so a detached handler's
// settle is accepted on its own terms and bumps the revision. If the
// abandonment took that conflict as its answer, the Run would be left running
// with renewal already stopped — the lease lapses, another worker claims it, and
// the handler is still going.

func TestASettleThatLandsDuringTheWindDownStillLeavesTheRunUnclaimable(t *testing.T) {
	var returned atomic.Bool
	h := newHarness(t, wedgedAgent(2*time.Second, &returned),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.LeaseFor = 300 * time.Millisecond
			deps.MaxNodeDuration = 150 * time.Millisecond
			deps.Store = &settlingBehindTheAbandonment{Execution: deps.Store}
		}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.State == run.StateRunning {
		t.Fatal("the Run was left running after a lost fence race, with renewal already stopped")
	}
	if result.Run.State != run.StateWaitingResolution {
		t.Fatalf("state=%s, want waiting_resolution", result.Run.State)
	}

	h.clock.Advance(time.Hour)
	if _, _, err := h.store.Claim(context.Background(), store.ClaimCommand{
		RunID: started.ID, Owner: "worker-2", LeaseFor: time.Minute,
	}); err == nil {
		t.Fatal("another worker claimed a Run whose effect is still running")
	}
}

// renewFailingAfterCancel makes the Store stop renewing the moment the effect
// is cancelled, which is the moment the wind-down starts.
type renewFailingAfterCancel struct {
	store.Execution
	failing *atomic.Bool
}

func (s renewFailingAfterCancel) Renew(ctx context.Context, command store.RenewCommand) (store.Lease, error) {
	if s.failing.Load() {
		return store.Lease{}, run.NewError("store_unavailable", run.ErrorInternal, run.RetryBackoff)
	}
	return s.Execution.Renew(ctx, command)
}

// The wind-down needs the lease, and the lease needs a Store that answers. When
// it stops answering the node is no less abandoned — and a renew loop that
// simply gave up there would leave the node blocked on the handler forever,
// which is the wedged slot the cap exists to release.

func TestAStoreThatFailsMidWindDownStillAbandonsTheNode(t *testing.T) {
	var failing atomic.Bool
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, _ agent.Request) (agent.Response, error) {
		go func() {
			<-ctx.Done()
			failing.Store(true)
		}()
		time.Sleep(3 * time.Second)
		return agent.Response{Output: json.RawMessage(`"too late"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		// A ~7ms tick against a 60ms grace, so renewal is attempted — and fails —
		// several times inside the wind-down, with room to spare on a loaded box.
		deps.LeaseFor = 20 * time.Millisecond
		deps.MaxNodeDuration = 1200 * time.Millisecond
		deps.Store = renewFailingAfterCancel{Execution: deps.Store, failing: &failing}
	}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.State != run.StateWaitingResolution {
		t.Fatalf("state=%s, want waiting_resolution", result.Run.State)
	}
}

// The rollback. A deployment that finds the cap killing legitimate nodes turns
// it off at config level and gets the unbounded renewal it had before.

func TestANegativeCapRestoresUnboundedRenewal(t *testing.T) {
	var returned atomic.Bool
	h := newHarness(t, wedgedAgent(300*time.Millisecond, &returned),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.LeaseFor = 30 * time.Millisecond
			deps.MaxNodeDuration = -1
		}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if !returned.Load() {
		t.Fatal("the handler was cut short with the cap disabled")
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s, want succeeded", result.Run.State)
	}
}

func TestAnAbandonedEffectLeavesTheRunWaitingResolutionNotFailed(t *testing.T) {
	var returned atomic.Bool
	h := newHarness(t, wedgedAgent(2*time.Second, &returned),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.LeaseFor = 300 * time.Millisecond
			deps.MaxNodeDuration = 150 * time.Millisecond
		}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	// Judging it failed would invite the layer above to retry a non-idempotent
	// write that may already have landed.
	if result.Run.State == run.StateFailed {
		t.Fatal("an abandoned effect was judged failed, which reads as safe to retry")
	}
	if result.Run.State != run.StateWaitingResolution {
		t.Fatalf("state=%s, want waiting_resolution", result.Run.State)
	}

	// A leaked goroutine is the accepted cost of never releasing the lease, and
	// the price of accepting it is that it is never silent.
	abandoned := h.observer.named(observe.EventNodeAbandoned)
	if len(abandoned) != 1 {
		t.Fatalf("%d node.abandoned decisions, want 1", len(abandoned))
	}
	var node string
	for _, attribute := range abandoned[0].Attributes {
		if attribute.Key == observe.AttrNode {
			node = attribute.Value
		}
	}
	if node == "" {
		t.Fatal("the abandonment does not say which node leaked")
	}
}

// The cap hands the Run back while the handler it gave up on is still holding
// ports, so for as long as that handler runs there are two goroutines on this
// session: the abandonment parking the Run, and the handler still spending
// against it. detached tells the handler to stop, but the check and the write
// that follows it are not one step — between them sits a Store round trip — so
// a handler already past the check writes the session's snapshot while the
// abandonment reads it.
//
// slowBeginStore is what makes that window observable rather than a matter of
// scheduling luck: it holds the invocation-begin commit open past the cap, so
// the handler is guaranteed to be inside begin when the abandonment starts.
//
// Which of the two writers wins is settled by the execution fence and is not
// what this test is about. It is about the reads and writes themselves: they
// were unsynchronised, and the field they race on carries the maps every
// recovery scan walks. -race is the assertion — the test's own checks pass
// either way, and the failure this pins arrives in production as a Go runtime
// fatal rather than as a wrong answer.
type slowBeginStore struct {
	store.Execution
	delay time.Duration
}

func (s slowBeginStore) BeginInvocation(
	ctx context.Context, command store.BeginInvocationCommand,
) (run.Snapshot, error) {
	snapshot, err := s.Execution.BeginInvocation(ctx, command)
	time.Sleep(s.delay)
	return snapshot, err
}

func TestAnAbandonedHandlerStillUsingItsPortsDoesNotRaceTheAbandonment(t *testing.T) {
	handlerDone := make(chan struct{})
	h := newHarness(t, scriptedAgent{execute: func(_ context.Context, request agent.Request) (agent.Response, error) {
		defer close(handlerDone)
		// context.Background, deliberately: this is the handler the cap exists
		// for, the one that never notices it was cancelled.
		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			//nolint:errcheck // the refusals are the point; the calls are what race.
			_, _ = request.Ports.Model(context.Background(), llm.Request{MaxTokens: 1})
		}
		return agent.Response{Output: json.RawMessage(`"too late"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{}
		deps.Store = slowBeginStore{Execution: deps.Store, delay: 80 * time.Millisecond}
		deps.LeaseFor = 2 * time.Second
		deps.MaxNodeDuration = 20 * time.Millisecond
	}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.State != run.StateWaitingResolution {
		t.Fatalf("state=%s, want waiting_resolution", result.Run.State)
	}

	// Wait for the handler before returning: a test that leaves it running
	// hands its race to whichever test runs next.
	select {
	case <-handlerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the runaway handler never returned")
	}
}
