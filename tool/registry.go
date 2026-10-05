package tool

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/costa92/agent-runtime/run"
)

type entry struct {
	spec    Spec
	handler Handler
}

// Registry binds specs to handlers and freezes before the Runtime is built,
// like the agent and memory registries and for the same reason.
type Registry struct {
	mu      sync.RWMutex
	frozen  bool
	entries map[string]entry
}

func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]entry)}
}

// Register validates the spec and binds it.
func (r *Registry) Register(spec Spec, handler Handler) error {
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return run.NewError("empty_tool_name", run.ErrorInvalid, run.RetryNever)
	}
	if handler == nil {
		return run.NewError("nil_tool_handler", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("tool %q", name))
	}
	if spec.RiskLevel == "" || spec.SideEffect == "" {
		// Governance judges the declaration. An undeclared risk or side effect
		// would have to be defaulted, and either default is wrong for half the
		// tools that forgot to say.
		return run.NewError("undeclared_tool_semantics", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("tool %q must declare risk_level and side_effect", name))
	}
	spec.Name = name

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return run.NewError("registry_frozen", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("tool %q registered after freeze", name))
	}
	if _, exists := r.entries[name]; exists {
		return run.NewError("duplicate_tool_name", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("tool %q already registered", name))
	}
	r.entries[name] = entry{spec: spec, handler: handler}
	return nil
}

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

// Lookup returns a tool's spec. The handler is deliberately not returned:
// nothing outside Gateway.Execute may reach it, which is what makes the gateway
// chain unavoidable rather than merely conventional.
func (r *Registry) Lookup(name string) (Spec, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	found, ok := r.entries[name]
	if !ok {
		return Spec{}, run.NewError("unknown_tool", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("tool %q is not registered", name))
	}
	return found.spec, nil
}

// handlerFor is unexported so only the gateway in this package can invoke a
// handler.
func (r *Registry) handlerFor(name string) (entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	found, ok := r.entries[name]
	if !ok {
		return entry{}, run.NewError("unknown_tool", run.ErrorInvalid, run.RetryNever)
	}
	return found, nil
}

// Specs returns every registered spec, sorted, for the model-visible tool set
// and for diagnostics.
func (r *Registry) Specs() []Spec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	specs := make([]Spec, 0, len(r.entries))
	for _, found := range r.entries {
		specs = append(specs, found.spec)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	return specs
}
