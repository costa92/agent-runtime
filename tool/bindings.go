package tool

import (
	"context"
	"sync"
)

// BindingLookup resolves a tool that was published after the registry froze.
//
// This is the operator-tier seam: a ToolBinding resource becomes an invocable
// tool without a release. The registry still owns capability *types*; this
// owns capability *instances*.
type BindingLookup interface {
	Lookup(ctx context.Context, name string) (Spec, Handler, bool)
}

// BindingMap is an in-process BindingLookup for tests and offline fixtures.
type BindingMap struct {
	mu      sync.RWMutex
	entries map[string]binding
}

type binding struct {
	spec    Spec
	handler Handler
}

func NewBindingMap() *BindingMap {
	return &BindingMap{entries: map[string]binding{}}
}

// Publish makes a binding invocable. It may be called after the Runtime is
// built — that is the whole point.
func (m *BindingMap) Publish(spec Spec, handler Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[spec.Name] = binding{spec: spec, handler: handler}
}

func (m *BindingMap) Lookup(_ context.Context, name string) (Spec, Handler, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	found, ok := m.entries[name]
	if !ok {
		return Spec{}, nil, false
	}
	return found.spec, found.handler, true
}

func WithBindings(lookup BindingLookup) Option {
	return func(g *Gateway) { g.bindings = lookup }
}
