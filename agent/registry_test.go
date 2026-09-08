package agent_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

type stubAgent struct{}

func (stubAgent) Execute(context.Context, agent.Request) (agent.Response, error) {
	return agent.Response{}, nil
}

func stubFactory() agent.Factory {
	return agent.FactoryFunc(func(json.RawMessage) (agent.Agent, error) { return stubAgent{}, nil })
}

func TestRegistryRefusesEmptyAndDuplicateKeys(t *testing.T) {
	registry := agent.NewRegistry()

	if err := registry.Register("content.writer", stubFactory()); err != nil {
		t.Fatalf("first registration: %v", err)
	}

	// Last-write-wins would mean import order silently changes what a published
	// definition does.
	if run.KindOf(registry.Register("content.writer", stubFactory())) != run.ErrorInvalid {
		t.Error("a duplicate key was accepted")
	}
	for _, key := range []string{"", "   "} {
		if run.KindOf(registry.Register(key, stubFactory())) != run.ErrorInvalid {
			t.Errorf("an empty key %q was accepted", key)
		}
	}
	if run.KindOf(registry.Register("content.other", nil)) != run.ErrorInvalid {
		t.Error("a nil factory was accepted")
	}
}

// The registry closes before the Runtime is built. Otherwise "this key exists"
// has one answer at publish time and another at execution time, and a Run fails
// halfway through on a definition that validated cleanly.
func TestRegistryFreezesBeforeUse(t *testing.T) {
	registry := agent.NewRegistry()
	if err := registry.Register("content.writer", stubFactory()); err != nil {
		t.Fatalf("register: %v", err)
	}

	registry.Freeze()
	if !registry.Frozen() {
		t.Fatal("Freeze did not take")
	}
	if run.KindOf(registry.Register("content.late", stubFactory())) != run.ErrorInvalid {
		t.Error("registration after freeze was accepted")
	}

	registry.Freeze() // idempotent: an assembly path reachable twice must not care
	if _, err := registry.Lookup("content.writer"); err != nil {
		t.Errorf("freeze broke lookup: %v", err)
	}
}

// An unknown key is an error, never a nil factory: fail at publish rather than
// nil-panic mid-Run.
func TestLookupOfUnknownKeyIsAnErrorNotNil(t *testing.T) {
	registry := agent.NewRegistry()
	factory, err := registry.Lookup("content.absent")
	if run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("error=%s want=invalid", run.KindOf(err))
	}
	if factory != nil {
		t.Fatal("a factory was returned for an unregistered key")
	}
}

func TestKeysAreSortedForDiagnostics(t *testing.T) {
	registry := agent.NewRegistry()
	for _, key := range []string{"c", "a", "b"} {
		if err := registry.Register(key, stubFactory()); err != nil {
			t.Fatalf("register %s: %v", key, err)
		}
	}
	keys := registry.Keys()
	if len(keys) != 3 || keys[0] != "a" || keys[1] != "b" || keys[2] != "c" {
		t.Fatalf("keys not sorted: %v", keys)
	}
}

// The registry is read concurrently by every Advance once frozen, so it is
// locked rather than being a bare map anyone may read while assembly is still
// writing.
func TestRegistryIsSafeForConcurrentUse(t *testing.T) {
	registry := agent.NewRegistry()
	if err := registry.Register("content.writer", stubFactory()); err != nil {
		t.Fatalf("register: %v", err)
	}
	registry.Freeze()

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := registry.Lookup("content.writer"); err != nil {
				t.Errorf("lookup: %v", err)
			}
			_ = registry.Keys()
			_ = registry.Frozen()
		}()
	}
	wg.Wait()
}

func TestResponseCarriesOptionalHandoffSignal(t *testing.T) {
	resp := agent.Response{
		Output: json.RawMessage(`{"text":"hello"}`),
		Handoff: &agent.HandoffSignal{
			TargetAgentKey: "content.writer",
			ContextPayload: json.RawMessage(`{"summary":"done"}`),
			Reason:         "Need specialized writing",
		},
	}
	if resp.Handoff == nil || resp.Handoff.TargetAgentKey != "content.writer" {
		t.Fatalf("unexpected handoff response: %+v", resp.Handoff)
	}
}
