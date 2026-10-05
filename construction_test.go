package agentruntime_test

import (
	"errors"
	"testing"

	agentruntime "github.com/costa92/agent-runtime"
	"github.com/costa92/agent-runtime/agent"
	"github.com/costa92/agent-runtime/internal/testkit"
	"github.com/costa92/agent-runtime/observe"
	"github.com/costa92/agent-runtime/run"
)

// --- fixtures -------------------------------------------------------------

func TestNewRefusesARegistryMissingABuiltinEvent(t *testing.T) {
	agents := agent.NewRegistry()
	agents.Freeze()
	// A registry with only one of the builtin events declared.
	partial, err := observe.NewEventSpecRegistry(observe.BuiltinEventSpecs()[0])
	if err != nil {
		t.Fatalf("events: %v", err)
	}

	source := &fakeSource{}
	_, err = agentruntime.New(agentruntime.Dependencies{
		Store: testkit.NewMemoryStore(testkit.NewClock()), Definitions: source,
		Graphs: graphLoader{source: source}, Governance: fakeGovernance{},
		Authorization: fakeAuthorizer{}, Meter: fakeMeter{}, Agents: agents,
		Clock: testkit.NewClock(), IDs: &sequentialIDs{}, Events: partial,
	})
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) || runtimeError.Code != "undeclared_event" {
		t.Fatalf("err=%v; a Runtime that can emit an undeclared event must not start", err)
	}
}

func TestNewFailsClosedOnAMissingDependency(t *testing.T) {
	_, err := agentruntime.New(agentruntime.Dependencies{})
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) || runtimeError.Code != "missing_dependency" {
		t.Fatalf("err=%v", err)
	}
}

func TestNewRefusesAnOpenAgentRegistry(t *testing.T) {
	source := &fakeSource{}
	events, _ := observe.NewEventSpecRegistry(observe.BuiltinEventSpecs()...)

	_, err := agentruntime.New(agentruntime.Dependencies{
		Store: testkit.NewMemoryStore(testkit.NewClock()), Definitions: source,
		Graphs: graphLoader{source: source}, Governance: fakeGovernance{},
		Authorization: fakeAuthorizer{}, Meter: fakeMeter{}, Agents: agent.NewRegistry(),
		Clock: testkit.NewClock(), IDs: &sequentialIDs{}, Events: events,
	})
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) || runtimeError.Code != "registry_not_frozen" {
		t.Fatalf("err=%v", err)
	}
}

// An agent implementation reaches effects only through the ports it is handed.
