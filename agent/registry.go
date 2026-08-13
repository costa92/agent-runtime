package agent

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Registry maps stable keys to implementations.
//
// It freezes: registration happens during assembly and is closed before the
// Runtime is built. A registry that could still change at run time would make
// "this key exists" a question with a different answer at publish time than at
// execution time, and a Run would fail halfway through on a definition that
// validated cleanly.
type Registry struct {
	mu        sync.RWMutex
	frozen    bool
	factories map[string]Factory
}

// NewRegistry returns an empty, unfrozen registry.
func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

// Register binds a key to a factory. Empty and duplicate keys are errors rather
// than last-write-wins: a silently replaced agent is a change in what a
// published definition does, made by import order.
func (r *Registry) Register(key string, factory Factory) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return run.NewError("empty_agent_key", run.ErrorInvalid, run.RetryNever)
	}
	if factory == nil {
		return run.NewError("nil_agent_factory", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("agent %q", key))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return run.NewError("registry_frozen", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("agent %q registered after freeze", key))
	}
	if _, exists := r.factories[key]; exists {
		return run.NewError("duplicate_agent_key", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("agent %q already registered", key))
	}
	r.factories[key] = factory
	return nil
}

// Freeze closes the registry. It is idempotent so that an assembly path which
// can be reached twice does not have to track whether it already ran.
func (r *Registry) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
}

// Frozen reports whether registration is closed.
func (r *Registry) Frozen() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.frozen
}

// Lookup returns the factory for a key. An unknown key is an error, never a
// nil factory: fail fast at publish rather than nil-panic mid-Run.
func (r *Registry) Lookup(key string) (Factory, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.factories[key]
	if !ok {
		return nil, run.NewError("unknown_agent_key", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("agent %q is not registered", key))
	}
	return factory, nil
}

// Keys returns every registered key, sorted. Used for diagnostics and for the
// startup log that makes the frozen set visible.
func (r *Registry) Keys() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]string, 0, len(r.factories))
	for key := range r.factories {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
