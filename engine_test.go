package agentruntime_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
)

// These tests are about the wall-clock cap on one node's execution.
//
// The handler they run is the one the cap exists for: it ignores ctx entirely,
// so cancelling it changes nothing and only the Runtime's own wind-down ends the
// node. It returns eventually rather than blocking forever so that a Runtime
// without the cap fails these assertions instead of hanging the suite.

// wedgedAgent runs for wedgeFor no matter what the context says.
func wedgedAgent(wedgeFor time.Duration, returned *atomic.Bool) agent.Agent {
	return scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		time.Sleep(wedgeFor)
		returned.Store(true)
		return agent.Response{Output: json.RawMessage(`"too late"`)}, nil
	}}
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
		time.Sleep(2 * time.Second)
		return agent.Response{Output: json.RawMessage(`"too late"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		// A 2ms tick against a 7ms grace, so renewal is certain to be attempted
		// — and to fail — inside the wind-down.
		deps.LeaseFor = 6 * time.Millisecond
		deps.MaxNodeDuration = 150 * time.Millisecond
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
