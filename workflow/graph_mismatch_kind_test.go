package workflow

import (
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// A pinned graph that no longer exists is not a conflict.
//
// ErrorConflict promises the caller's view was stale, not wrong — reload and
// decide again. Nothing about reloading helps here: the Run pinned one graph,
// the catalog holds another, and every future attempt fails identically. That
// is ErrorInvalid's contract word for word ("the same call will never succeed").
//
// The distinction is not cosmetic. agent-platform/worker meters ErrorConflict as
// "another worker won this claim — expected and healthy in a fleet", so calling
// this a conflict filed a permanent, unadvanceable Run under normal contention:
// no log, no failure counter, and the Run re-claimed forever while the metrics
// said the pool was working.
func TestAPinnedGraphThatMovedIsInvalidNotAConflict(t *testing.T) {
	graph := &ExecutionGraph{Ref: run.ExecutionGraphRef{
		ID: "a", Version: 1, Protocol: 1, Digest: "old",
	}}
	moved := run.ExecutionGraphRef{ID: "a", Version: 1, Protocol: 1, Digest: "new"}

	err := graph.Verify(moved)
	if err == nil {
		t.Fatal("a moved graph verified")
	}
	if kind := run.KindOf(err); kind != run.ErrorInvalid {
		t.Errorf("kind = %q, want %q", kind, run.ErrorInvalid)
	}
}

// The matching ref still verifies, so the test above is about the mismatch and
// not about Verify refusing everything.
func TestTheSameGraphStillVerifies(t *testing.T) {
	ref := run.ExecutionGraphRef{ID: "a", Version: 1, Protocol: 1, Digest: "d"}
	if err := (&ExecutionGraph{Ref: ref}).Verify(ref); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}
