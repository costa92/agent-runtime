package memory

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/costa92/agent-runtime/run"
)

// Registry maps stable keys to memory providers.
//
// Same shape and same reasoning as the agent registry: registration during
// assembly, frozen before the Runtime is built, duplicates refused. It is a
// separate registry rather than a shared generic one because the two are looked
// up for different reasons at different points, and one registry holding both
// would make "is this key an agent or a memory" a question the caller has to
// ask.
type Registry struct {
	mu        sync.RWMutex
	frozen    bool
	providers map[string]Provider
}

func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]Provider)}
}

// Register binds a key to a provider.
func (r *Registry) Register(key string, provider Provider) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return run.NewError("empty_memory_key", run.ErrorInvalid, run.RetryNever)
	}
	if provider == nil {
		return run.NewError("nil_memory_provider", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("memory %q", key))
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return run.NewError("registry_frozen", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("memory %q registered after freeze", key))
	}
	if _, exists := r.providers[key]; exists {
		return run.NewError("duplicate_memory_key", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("memory %q already registered", key))
	}
	r.providers[key] = provider
	return nil
}

// Freeze closes the registry. Idempotent.
func (r *Registry) Freeze() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
}

func (r *Registry) Frozen() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.frozen
}

// Lookup returns the provider for a key, or an error for an unknown one.
func (r *Registry) Lookup(key string) (Provider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	provider, ok := r.providers[key]
	if !ok {
		return nil, run.NewError("unknown_memory_key", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("memory %q is not registered", key))
	}
	return provider, nil
}

// Keys returns every registered key, sorted.
func (r *Registry) Keys() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]string, 0, len(r.providers))
	for key := range r.providers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
