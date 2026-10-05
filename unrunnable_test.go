package agentruntime_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	agentruntime "github.com/costa92/agent-runtime"
	"github.com/costa92/agent-runtime/policy"
	"github.com/costa92/agent-runtime/quota"
	"github.com/costa92/agent-runtime/run"
	"github.com/costa92/agent-runtime/store"
)

// brokenGovernance answers Start normally and then fails every later lookup.
//
// The order matters: a Run has to exist and be pinned before the failure this
// tests can happen at all. Breaking Start instead would test that a Run is
// never created, which is a different and already-working thing.
type brokenGovernance struct {
	err    error
	broken *atomic.Bool
}

func (g brokenGovernance) PolicySnapshot(context.Context, string, string) (policy.Snapshot, error) {
	if g.broken.Load() {
		return policy.Snapshot{}, g.err
	}
	// Digested while healthy: an undigested set would fail at creation for a
	// different reason than the one under test.
	return policy.Snapshot{Digest: "policy-fake"}, nil
}

func (g brokenGovernance) QuotaSnapshot(context.Context, string, string) (quota.Snapshot, error) {
	if g.broken.Load() {
		return quota.Snapshot{}, g.err
	}
	return quota.Snapshot{Digest: "quota-fake"}, nil
}

// A Run whose session can never be assembled is ended, not left on the queue.
//
// Before this, advanceClaimed returned the assembly error and left the state
// alone: the Run went back on the queue, was claimed again, failed identically,
// and repeated every lease period forever. Never terminal meant retention never
// reaped it — that cleanup only deletes terminal trees — and the caller waiting
// on it got silence rather than an answer. Six such Runs were found in the
// local database.
func TestARunThatCanNeverBeAssembledIsEnded(t *testing.T) {
	h := newHarness(t, answering("done"))
	started := start(t, h)

	// The published graph moved under a Run that pinned the old one. No later
	// attempt reads anything different.
	h.source.graphRef.Digest = "some-other-digest"

	if _, err := h.runtime.Advance(t.Context(), started.ID); err == nil {
		t.Fatal("a Run with an unresolvable graph advanced")
	}

	after, err := h.store.Get(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if after.State != run.StateFailed {
		t.Fatalf("state = %s, want failed — the Run is back on the queue to fail "+
			"the same way forever, and nothing will ever reap it", after.State)
	}
	if !after.State.Terminal() {
		t.Fatal("the Run did not reach a terminal state, so retention will never reap it")
	}

	// Started first, then failed. The reducer refuses CommandFail from queued —
	// a queued Run has produced nothing — and that invariant is worth two
	// commits rather than a relaxed state machine. Cancelling instead would be
	// one commit and a lie: cancelled means a human stopped it.
	page, err := h.store.Events(t.Context(), store.EventQuery{RunID: started.ID})
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var states []run.State
	for _, event := range page.Events {
		if event.Kind == run.EventStateChanged {
			states = append(states, event.To)
		}
	}
	if len(states) != 2 || states[0] != run.StateRunning || states[1] != run.StateFailed {
		t.Fatalf("state events = %v, want running then failed", states)
	}
}

// The dangerous direction: a Run that is merely unlucky must be left alone.
//
// Every one of these arrives at exactly the same place as the case above, and
// the naive predicate — RetryOf(err) == RetryNever — would end the Run for all
// of them. Each is something a whole fleet hits at once, so getting this wrong
// does not strand one Run, it destroys every Run in flight.
func TestARunThatIsMerelyUnluckyIsLeftAlone(t *testing.T) {
	for _, testCase := range []struct {
		name string
		err  error
	}{
		{
			// What agent-platform's wrap() produces for an unclassified
			// transport fault. Note the retry hint: RetryNever, identical to a
			// permanent failure.
			name: "the database did not answer",
			err: run.NewError("store.resource_get", run.ErrorInternal, run.RetryNever,
				errors.New("dial tcp: connection refused")),
		},
		{
			// Not a run.Error at all — a raw driver error on its way up through
			// a host adapter. RetryOf reports RetryNever for these by design.
			name: "a raw driver error",
			err:  errors.New("driver: bad connection"),
		},
		{
			// governance.not_published: the publish that fixes it needs no
			// restart, so the Run is one operator action away from running.
			name: "the rule set is not published yet",
			err:  run.NewError("governance.not_published", run.ErrorInternal, run.RetryBackoff),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			broken := &atomic.Bool{}
			h := newHarness(t, answering("done"), withDeps(func(deps *agentruntime.Dependencies) {
				deps.Governance = brokenGovernance{err: testCase.err, broken: broken}
			}))
			started := start(t, h)
			broken.Store(true)

			if _, err := h.runtime.Advance(t.Context(), started.ID); err == nil {
				t.Fatal("a broken governance served a snapshot")
			}

			after, err := h.store.Get(t.Context(), started.ID)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if after.State.Terminal() {
				t.Fatalf("state = %s — a recoverable failure ended the Run; "+
					"in production this shape arrives for every Run at once",
					after.State)
			}
			if after.Revision != started.Revision {
				t.Fatalf("revision moved %d → %d; the Run was written to",
					started.Revision, after.Revision)
			}
		})
	}
}
