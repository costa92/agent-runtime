package agentruntime

import (
	"container/list"
	"context"
	"sync"

	"github.com/costa92/agent-runtime/run"
	"github.com/costa92/agent-runtime/workflow"
)

// GraphSource loads a graph that was compiled at publish time.
//
// Loading, never compiling. The Runtime does not own a Compiler at execution
// time on purpose: a Run that recompiled its Definition on resume would come
// back as whatever the current deployment's compiler produces, which is exactly
// the drift the pinned digest exists to detect.
type GraphSource interface {
	Load(ctx context.Context, ref run.ExecutionGraphRef) (*workflow.ExecutionGraph, error)
}

// GraphCache is a bounded, digest-keyed, read-only cache.
//
// Digest-keyed rather than ref-keyed because the digest is what makes a hit
// safe: two entries under the same ID and version but different digests are
// different graphs, and a ref-keyed cache would serve one Run the other's
// shape.
type GraphCache struct {
	mu       sync.Mutex
	capacity int
	source   GraphSource
	entries  map[string]*list.Element
	order    *list.List
}

type cacheEntry struct {
	digest string
	graph  *workflow.ExecutionGraph
}

// NewGraphCache bounds the cache. A cache with no ceiling is a leak with a
// hit rate: one graph per published version, held forever.
func NewGraphCache(source GraphSource, capacity int) *GraphCache {
	if capacity <= 0 {
		capacity = 64
	}
	return &GraphCache{
		capacity: capacity,
		source:   source,
		entries:  make(map[string]*list.Element, capacity),
		order:    list.New(),
	}
}

// Get returns the pinned graph, failing closed.
//
// A ref with no digest, a source that cannot produce it, or a graph whose own
// ref does not match what was asked for are all refusals. None of them is
// recoverable by running anyway: the Run would execute a shape nobody pinned.
func (c *GraphCache) Get(ctx context.Context, ref run.ExecutionGraphRef) (*workflow.ExecutionGraph, error) {
	if ref.Digest == "" {
		return nil, run.NewError("unpinned_graph", run.ErrorInvalid, run.RetryNever)
	}

	c.mu.Lock()
	if element, ok := c.entries[ref.Digest]; ok {
		graph := element.Value.(cacheEntry).graph
		if err := graph.Verify(ref); err == nil {
			c.order.MoveToFront(element)
			c.mu.Unlock()
			return graph, nil
		}
		// Same digest, different ref (a new version of an identical graph).
		// Serving the cached value would fail Verify and look like the new
		// version was unpublished. Drop the stale entry and reload.
		c.order.Remove(element)
		delete(c.entries, ref.Digest)
	}
	c.mu.Unlock()

	graph, err := c.source.Load(ctx, ref)
	if err != nil {
		return nil, err
	}
	if graph == nil {
		// Invalid for the same reason as graph_mismatch: a pinned graph the
		// catalog does not hold will not appear on the next attempt.
		return nil, run.NewError("missing_graph", run.ErrorInvalid, run.RetryNever)
	}
	if err := graph.Verify(ref); err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[ref.Digest]; ok {
		cached := element.Value.(cacheEntry).graph
		if err := cached.Verify(ref); err == nil {
			c.order.MoveToFront(element)
			return cached, nil
		}
		c.order.Remove(element)
		delete(c.entries, ref.Digest)
	}
	element := c.order.PushFront(cacheEntry{digest: ref.Digest, graph: graph})
	c.entries[ref.Digest] = element
	for c.order.Len() > c.capacity {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(cacheEntry).digest)
	}
	return graph, nil
}

// Len reports how many graphs are held, for tests and for a startup metric.
func (c *GraphCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
