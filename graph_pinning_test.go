package agentruntime_test

import (
	"fmt"
	"testing"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/quota"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

// --- fixtures -------------------------------------------------------------

func TestStartPinsTheDefinitionGraphAndRuleSets(t *testing.T) {
	h := newHarness(t, answering("done"), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{
			policies: policy.Snapshot{Digest: "policy-7"},
			quotas:   quota.Snapshot{Digest: "quota-3"},
		}
	}))
	started := start(t, h)

	if started.Graph.Digest == "" {
		t.Fatal("the Run did not pin a graph digest")
	}
	if started.Pins.PolicyDigest != "policy-7" || started.Pins.QuotaDigest != "quota-3" {
		t.Fatalf("pins=%+v", started.Pins)
	}
	if started.Pins.Trace.TraceID == "" {
		t.Error("the Run started no trace; a resumed Run would open an unlinked one")
	}
}

// Only the durable half of an identity may reach storage. Persisting claims
// turns a momentary authorization into one replayed to every later reader.

func TestAdvanceFailsClosedOnAGraphThatMoved(t *testing.T) {
	h := newHarness(t, answering("done"))
	started := start(t, h)

	h.source.graphRef.Digest = "some-other-digest"

	_, err := h.runtime.Advance(t.Context(), started.ID)
	// Invalid, not conflict: reloading never produces the pinned graph again,
	// and a worker meters conflicts as healthy contention.
	if run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("kind=%s want=invalid", run.KindOf(err))
	}
}

func TestTheGraphCacheServesRepeatedRuns(t *testing.T) {
	h := newHarness(t, answering("done"))

	for range 3 {
		started := start(t, h)
		if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
			t.Fatalf("advance: %v", err)
		}
	}
	// One load for the first Start; every later Start and Advance is a hit on
	// the same digest.
	if loads := h.source.loads.Load(); loads != 1 {
		t.Fatalf("graph loads=%d want=1", loads)
	}
}

func TestAnUnpinnedGraphIsRefused(t *testing.T) {
	cache := agentruntime.NewGraphCache(graphLoader{source: &fakeSource{}}, 2)

	if _, err := cache.Get(t.Context(), run.ExecutionGraphRef{ID: "x", Version: 1}); err == nil {
		t.Fatal("a ref with no digest was accepted")
	}
}

func TestTheGraphCacheIsBounded(t *testing.T) {
	source := &fakeSource{}
	cache := agentruntime.NewGraphCache(graphLoader{source: source}, 2)

	for i := range 5 {
		digest := fmt.Sprintf("digest-%d", i)
		ref := run.ExecutionGraphRef{ID: "x", Version: uint64(i + 1), Digest: digest}
		source.graph = &workflow.ExecutionGraph{Ref: ref}
		if _, err := cache.Get(t.Context(), ref); err != nil {
			t.Fatalf("get: %v", err)
		}
	}
	if cache.Len() != 2 {
		t.Fatalf("cache holds %d graphs; the bound is not enforced", cache.Len())
	}
}

// The whole reason the cache is keyed by digest: a hit must be the same graph,
// not merely the same name and version.

func TestACachedGraphIsStillVerified(t *testing.T) {
	source := &fakeSource{}
	cache := agentruntime.NewGraphCache(graphLoader{source: source}, 4)

	ref := run.ExecutionGraphRef{ID: "x", Version: 1, Digest: "digest-1"}
	source.graph = &workflow.ExecutionGraph{Ref: run.ExecutionGraphRef{
		ID: "x", Version: 1, Digest: "a-different-digest",
	}}

	if _, err := cache.Get(t.Context(), ref); run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("kind=%s want=invalid", run.KindOf(err))
	}
}

// Every governance decision must be an event, and every event must be declared.
