package agentruntime_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
)

// What the engine enqueues for the host to project.
//
// The outbox is the only supported way a host learns what a Run produced —
// reading Runtime tables directly is the thing it exists to replace — so an
// engine that advanced a Run and enqueued nothing would leave the host with a
// transcript that stops at the question.

func factsFor(t *testing.T, h *harness, id run.ID) []store.ProjectionFact {
	t.Helper()
	var facts []store.ProjectionFact
	for _, fact := range h.store.Projections() {
		if fact.RunID == id {
			facts = append(facts, fact)
		}
	}
	return facts
}

func TestEngineCommitsTheSchedulersNormalizedOutput(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		return agent.Response{Output: json.RawMessage(`{"count":1.0}`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		OutputSchema:   json.RawMessage(`{"type":"object","properties":{"count":{"type":"integer"}}}`),
		Model:          definition.ModelPolicy{Profile: "fast"},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Schemas = schemaProcessor{normalize: func(_, value json.RawMessage) (json.RawMessage, error) {
			if string(value) != `{"count":1.0}` {
				t.Fatalf("scheduler received output %s", value)
			}
			return json.RawMessage(`{"count":1}`), nil
		}}
	}))
	snapshot := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), snapshot.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	facts := factsFor(t, h, snapshot.ID)
	message, err := store.DecodeProjection[store.AssistantMessagePayload](facts[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(message.Output) != `{"count":1}` {
		t.Fatalf("committed output = %s, want normalized literal %s", message.Output, `{"count":1}`)
	}
}

func TestEngineDoesNotCommitAReferenceOrSuccessFactForInvalidOutput(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		return agent.Response{Output: json.RawMessage(`"not an object"`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		OutputSchema:   json.RawMessage(`{"type":"object"}`),
		Model:          definition.ModelPolicy{Profile: "fast"},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Schemas = schemaProcessor{normalize: func(_, _ json.RawMessage) (json.RawMessage, error) {
			return nil, errors.New("output is not an object")
		}}
	}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	for nodeID, state := range result.Run.Nodes {
		if state.OutputRef != "" {
			t.Fatalf("node %s retained invalid output ref %q", nodeID, state.OutputRef)
		}
	}
	for _, fact := range factsFor(t, h, started.ID) {
		if fact.Kind == store.ProjectionAssistantMessage {
			t.Fatal("invalid output was committed as a successful assistant message")
		}
	}
}

func kindsOf(facts []store.ProjectionFact) []store.ProjectionKind {
	kinds := make([]store.ProjectionKind, 0, len(facts))
	for _, fact := range facts {
		kinds = append(kinds, fact.Kind)
	}
	return kinds
}

// A finished Run leaves its output and its outcome in the outbox.
func TestASucceededRunEnqueuesItsOutputAndItsOutcome(t *testing.T) {
	h := newHarness(t, answering("done"))
	snapshot := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), snapshot.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	facts := factsFor(t, h, snapshot.ID)
	if len(facts) < 2 {
		t.Fatalf("facts = %v; a finished Run left nothing to project", kindsOf(facts))
	}

	message, err := store.DecodeProjection[store.AssistantMessagePayload](facts[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(message.Output) != `"done"` {
		t.Fatalf("output = %s; the agent's own result is carried verbatim", message.Output)
	}
	if message.AgentKey != "answer" {
		t.Fatalf("agent = %q; a delegated tree needs to know who spoke", message.AgentKey)
	}

	terminal, err := store.DecodeProjection[store.TerminalResultPayload](facts[len(facts)-1])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !run.State(terminal.State).Terminal() {
		t.Fatalf("state = %q, which is not a settled state", terminal.State)
	}
}

// A failed node contributes a progress marker, not an empty message.
//
// An assistant message with no output renders as the agent having said nothing,
// which is indistinguishable from a step that succeeded quietly.
func TestAFailedNodeEnqueuesProgressRatherThanAnEmptyMessage(t *testing.T) {
	failing := scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		return agent.Response{}, run.NewError("agent_failed", run.ErrorInternal, run.RetryNever)
	}}
	h := newHarness(t, failing)
	snapshot := start(t, h)

	// The error is the Run's, not the test's: an Advance that returns one has
	// still committed whatever it committed.
	_, _ = h.runtime.Advance(t.Context(), snapshot.ID)

	facts := factsFor(t, h, snapshot.ID)
	// Without this the loop below is vacuous: no facts at all would read as
	// "no empty message was projected", which is the wrong reason to pass.
	progress := 0
	for _, fact := range facts {
		if fact.Kind == store.ProjectionProgress {
			progress++
		}
	}
	if progress == 0 {
		t.Fatalf("facts = %v; the failed node contributed nothing", kindsOf(facts))
	}

	for _, fact := range facts {
		if fact.Kind != store.ProjectionAssistantMessage {
			continue
		}
		message, err := store.DecodeProjection[store.AssistantMessagePayload](fact)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(message.Output) == 0 {
			t.Fatal("a failed node was projected as an assistant message with no output")
		}
	}
}

// Every fact carries a sequence the Store assigned, and no two share one.
func TestEveryEnqueuedFactIsSeparatelyAddressable(t *testing.T) {
	h := newHarness(t, answering("done"))
	snapshot := start(t, h)
	if _, err := h.runtime.Advance(t.Context(), snapshot.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	seen := map[uint64]bool{}
	for _, fact := range factsFor(t, h, snapshot.ID) {
		if fact.Sequence == 0 {
			t.Fatalf("%s has no sequence; the projector could not deduplicate it", fact.Kind)
		}
		if seen[fact.Sequence] {
			t.Fatalf("sequence %d was used twice; one of the two facts is lost", fact.Sequence)
		}
		seen[fact.Sequence] = true
	}
}

// A fact is committed with the state that produced it, so a Run that did not
// advance leaves nothing behind.
func TestARefusedCommitLeavesNoFact(t *testing.T) {
	h := newHarness(t, answering("done"))
	snapshot := start(t, h)

	// Advancing a Run nobody claimed cannot commit, so nothing may be
	// projected: a transcript line for a turn that never ran is worse than a
	// missing one, because nothing about it looks wrong.
	if _, err := h.runtime.Advance(t.Context(), snapshot.ID+"-missing"); err == nil {
		t.Fatal("an unknown Run advanced")
	}
	if facts := factsFor(t, h, snapshot.ID+"-missing"); len(facts) != 0 {
		t.Fatalf("%d facts for a Run that never ran", len(facts))
	}
}
