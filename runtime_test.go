package agentruntime_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/quota"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

// This file holds the shared fixtures and the tests about advancing a Run:
// running one to terminal, cancellation, reading without advancing, and what
// AdvanceNext does with an empty queue.
//
// Everything else lives with its subject — construction_test, graph_pinning,
// budget, lease, authorization, approval, dataflow, egress, model_policy. They
// were one 2000-line file with 47 tests across nine unrelated topics, which is
// not a style complaint: the fixtures below are shared by all of them, so a
// reader looking for why an approval parks a Run had to page through quota and
// egress to find it, and the file had already stopped being read that way —
// checkpoint_test, projection_test and delegation_loop_test split off long ago
// and reuse newHarness from here.
//
// Same package, so the split moves nothing: any fixture here is visible to all
// of them, and a new test goes in the file named after what it asserts.

// --- fixtures -------------------------------------------------------------

type harness struct {
	runtime  agentruntime.Runtime
	store    *testkit.MemoryStore
	clock    *testkit.Clock
	observer *recordingObserver
	source   *fakeSource
}

type fakeSource struct {
	declared definition.Definition
	graph    *workflow.ExecutionGraph
	// loads counts graph loads, to tell a cache hit from a miss.
	loads    atomic.Int64
	loadErr  error
	graphRef run.ExecutionGraphRef
}

func (f *fakeSource) Load(_ context.Context, _ run.DefinitionRef) (definition.Definition, run.ExecutionGraphRef, error) {
	return f.declared, f.graphRef, nil
}

func (f *fakeSource) LoadGraph(_ context.Context, _ run.ExecutionGraphRef) (*workflow.ExecutionGraph, error) {
	f.loads.Add(1)
	if f.loadErr != nil {
		return nil, f.loadErr
	}
	return f.graph, nil
}

type graphLoader struct{ source *fakeSource }

func (g graphLoader) Load(ctx context.Context, ref run.ExecutionGraphRef) (*workflow.ExecutionGraph, error) {
	return g.source.LoadGraph(ctx, ref)
}

type fakeGovernance struct {
	policies policy.Snapshot
	quotas   quota.Snapshot
}

func (g fakeGovernance) PolicySnapshot(context.Context, string, string) (policy.Snapshot, error) {
	return g.policies, nil
}

func (g fakeGovernance) QuotaSnapshot(context.Context, string, string) (quota.Snapshot, error) {
	return g.quotas, nil
}

type fakeAuthorizer struct{ err error }

func (a fakeAuthorizer) AuthorizeUse(context.Context, authorization.PrincipalContext, authorization.ResourceRef) error {
	return a.err
}

func (a fakeAuthorizer) AuthorizePublish(context.Context, authorization.PrincipalContext, authorization.ResourceRef) error {
	return a.err
}

type fakeMeter struct{ usage map[quota.Unit]int }

func (m fakeMeter) Observe(_ context.Context, _ quota.Scope, unit quota.Unit, _ time.Duration) (int, error) {
	return m.usage[unit], nil
}

type sequentialIDs struct{ n atomic.Int64 }

func (s *sequentialIDs) NewID(prefix string) run.ID {
	return run.ID(fmt.Sprintf("%s-%d", prefix, s.n.Add(1)))
}

type recordingObserver struct{ decisions []observe.Decision }

func (o *recordingObserver) Decision(decision observe.Decision) {
	o.decisions = append(o.decisions, decision)
}

func (o *recordingObserver) Chunk(run.ID, string) {}

func (o *recordingObserver) named(name string) []observe.Decision {
	var found []observe.Decision
	for _, decision := range o.decisions {
		if decision.Name == name {
			found = append(found, decision)
		}
	}
	return found
}

// scriptedAgent runs whatever the test hands it.

type scriptedAgent struct {
	execute func(ctx context.Context, request agent.Request) (agent.Response, error)
}

func (s scriptedAgent) Execute(ctx context.Context, request agent.Request) (agent.Response, error) {
	return s.execute(ctx, request)
}

func (s scriptedAgent) New(json.RawMessage) (agent.Agent, error) { return s, nil }

func principal() authorization.PrincipalContext {
	return authorization.PrincipalContext{
		Ref: authorization.PrincipalRef{
			Subject: "alice", Tenant: "acme", Kind: authorization.PrincipalUser,
		},
		Claims: map[string]string{"role": "editor"},
	}
}

type harnessOption func(*agentruntime.Dependencies, *fakeSource)

func withDefinition(declared definition.Definition) harnessOption {
	return func(_ *agentruntime.Dependencies, source *fakeSource) { source.declared = declared }
}

func withDeps(mutate func(*agentruntime.Dependencies)) harnessOption {
	return func(deps *agentruntime.Dependencies, _ *fakeSource) { mutate(deps) }
}

func newHarness(t *testing.T, implementation agent.Agent, options ...harnessOption) *harness {
	t.Helper()

	declared := definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
	}

	agents := agent.NewRegistry()
	if err := agents.Register("answer", agent.FactoryFunc(func(json.RawMessage) (agent.Agent, error) {
		return implementation, nil
	})); err != nil {
		t.Fatalf("register: %v", err)
	}
	agents.Freeze()

	events, err := observe.NewEventSpecRegistry(observe.BuiltinEventSpecs()...)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	clock := testkit.NewClock()
	observer := &recordingObserver{}
	source := &fakeSource{declared: declared}

	deps := agentruntime.Dependencies{
		Store:         testkit.NewMemoryStore(clock),
		Definitions:   source,
		Graphs:        graphLoader{source: source},
		Governance:    fakeGovernance{},
		Authorization: fakeAuthorizer{},
		Meter:         fakeMeter{},
		Agents:        agents,
		Clock:         clock,
		IDs:           &sequentialIDs{},
		Events:        events,
		Observer:      observer,
		LeaseFor:      30 * time.Second,
		Owner:         "worker-1",
	}
	for _, option := range options {
		option(&deps, source)
	}

	graph, err := workflow.Compiler{Registries: workflow.Registries{
		Agents:   agents,
		Tools:    frozenTools{},
		Memories: frozenKeys{keys: []string{"notes"}},
		Models:   frozenKeys{keys: []string{"fast"}},
	}}.Compile(source.declared)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	source.graph = graph
	source.graphRef = graph.Ref

	runtime, err := agentruntime.New(deps)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return &harness{
		runtime:  runtime,
		store:    deps.Store.(*testkit.MemoryStore),
		clock:    clock,
		observer: observer,
		source:   source,
	}
}

type frozenKeys struct{ keys []string }

func (frozenKeys) Frozen() bool { return true }

func (f frozenKeys) Keys() []string { return f.keys }

type frozenTools struct{}

func (frozenTools) Frozen() bool { return true }

func (frozenTools) Specs() []tool.Spec {
	return []tool.Spec{{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, {
		// A plain read, so a rule that refuses it is refusing it on the fact
		// the test names rather than on its risk level.
		Name: "search_evidence", Description: "retrieve",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskLow, SideEffect: policy.SideEffectRead,
	}}
}

func answering(output string) agent.Agent {
	return scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		return agent.Response{Output: json.RawMessage(`"` + output + `"`)}, nil
	}}
}

func start(t *testing.T, h *harness) run.Snapshot {
	t.Helper()
	snapshot, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:  principal(),
		Definition: run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Input:      json.RawMessage(`{"q":"x"}`),
		Budget:     run.Limits{LLMCalls: 10, Tokens: 10_000, ToolCalls: 10},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	return snapshot
}

// --- tests ----------------------------------------------------------------

func TestRuntimeRunsASingleAgentToTerminal(t *testing.T) {
	h := newHarness(t, answering("done"))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s want=succeeded", result.Run.State)
	}
	if result.Waiting {
		t.Error("a finished Run was reported as waiting")
	}
}

// A Run captures its rules at creation. What it is judged by must not depend on
// when a worker happens to pick it up.

func TestAModelCallSeesTheDefinitionTools(t *testing.T) {
	registry := tool.NewRegistry()
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, testkit.ToolSucceeding(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()
	gateway := tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())

	var seen []string
	models := capturingModels{onRequest: func(request llm.Request) {
		for _, def := range request.Tools {
			seen = append(seen, def.Name)
		}
	}}
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Model(ctx, llm.Request{Messages: []llm.Message{{Role: llm.RoleUser, Content: "draw"}}}); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book", Required: true}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Tools = gateway
		deps.Models = models
	}))
	started := start(t, h)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if len(seen) == 0 || seen[0] != "render_picture_book" {
		t.Fatalf("model saw tools %v, want render_picture_book", seen)
	}
}

type capturingModels struct {
	onRequest func(llm.Request)
}

func (m capturingModels) Resolve(context.Context, llm.ModelRef) (llm.Client, error) {
	return capturingClient{onRequest: m.onRequest}, nil
}

type capturingClient struct {
	onRequest func(llm.Request)
}

func (capturingClient) Capabilities(context.Context, llm.ModelRef) (llm.Capabilities, error) {
	return llm.Capabilities{Tools: true, Streaming: true}, nil
}

func (c capturingClient) Complete(_ context.Context, request llm.Request) (llm.Response, error) {
	if c.onRequest != nil {
		c.onRequest(request)
	}
	return llm.Response{Attempts: []llm.Attempt{{Usage: llm.Usage{InputTokens: 10, OutputTokens: 5}}}}, nil
}

func (c capturingClient) Stream(ctx context.Context, request llm.Request, _ func(llm.Chunk) error) (llm.Response, error) {
	return c.Complete(ctx, request)
}

func TestCancellationStopsTheTree(t *testing.T) {
	h := newHarness(t, answering("done"))
	started := start(t, h)

	cancelled, err := h.runtime.Cancel(t.Context(), agentruntime.CancelRequest{
		RootID: started.ID, RequestedBy: principal(), Reason: "operator",
	})
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if cancelled.State != run.StateCancelled {
		t.Fatalf("state=%s", cancelled.State)
	}

	// A terminal Run accepts nothing further, whatever arrives late.
	if _, err := h.runtime.Advance(t.Context(), started.ID); err == nil {
		t.Fatal("a cancelled Run was advanced")
	}
}

func TestInspectAndListEventsReadWithoutAdvancing(t *testing.T) {
	h := newHarness(t, answering("done"))
	started := start(t, h)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	snapshot, err := h.runtime.Inspect(t.Context(), principal(), started.ID)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if snapshot.State != run.StateSucceeded {
		t.Fatalf("state=%s", snapshot.State)
	}

	page, err := h.runtime.ListEvents(t.Context(), principal(), store.EventQuery{RunID: started.ID, Limit: 100})
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(page.Events) == 0 {
		t.Fatal("a Run that ran produced no events")
	}
	// Contiguous, so a consumer can detect a gap rather than silently miss one.
	for i, event := range page.Events {
		if event.Sequence != uint64(i+1) {
			t.Fatalf("event %d has sequence %d; the sequence is not contiguous", i, event.Sequence)
		}
	}
}

// A Run is private to the exact durable principal that started it. General
// permission to use agents does not grant access to another user's Run: the
// snapshot, its event stream, cancellation and human decisions can all carry
// user content or change an external effect.

func TestAdvanceNextDistinguishesAnIdleQueueFromAFailure(t *testing.T) {
	h := newHarness(t, answering("done"))

	_, found, err := h.runtime.AdvanceNext(t.Context(), agentruntime.AdvanceNextRequest{})
	if err != nil {
		t.Fatalf("advance next: %v", err)
	}
	if found {
		t.Fatal("work was found in an empty queue")
	}

	start(t, h)
	result, found, err := h.runtime.AdvanceNext(t.Context(), agentruntime.AdvanceNextRequest{})
	if err != nil {
		t.Fatalf("advance next: %v", err)
	}
	if !found {
		t.Fatal("a runnable Run was not claimed")
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s", result.Run.State)
	}
}

// AdvanceNext returns one result, so it must lease exactly one Run. Leasing a
// batch and returning only the first result hides the remaining leases until
// they expire and makes queued work appear to vanish from every other worker.

func TestAdvanceNextLeavesTheNextRunImmediatelyClaimable(t *testing.T) {
	h := newHarness(t, answering("done"))
	start(t, h)
	start(t, h)
	start(t, h)

	if _, found, err := h.runtime.AdvanceNext(t.Context(), agentruntime.AdvanceNextRequest{}); err != nil || !found {
		t.Fatalf("first advance found=%v err=%v", found, err)
	}
	if _, found, err := h.runtime.AdvanceNext(t.Context(), agentruntime.AdvanceNextRequest{}); err != nil || !found {
		t.Fatalf("second Run was hidden behind the first lease: found=%v err=%v", found, err)
	}
}

// --- helpers --------------------------------------------------------------

type scriptedModels struct {
	response llm.Response
	err      error
	onCall   func()
	// onRequest sees the request as the provider receives it — after the
	// governed port has filled whatever the Agent left unset. It is the only
	// vantage point from which "the Definition's model policy reached the
	// provider" is observable.
	onRequest func(llm.Request)
}

func (m scriptedModels) Resolve(context.Context, llm.ModelRef) (llm.Client, error) {
	return scriptedClient{models: m}, nil
}

type scriptedClient struct{ models scriptedModels }

func (c scriptedClient) Capabilities(context.Context, llm.ModelRef) (llm.Capabilities, error) {
	return llm.Capabilities{Tools: true, Streaming: true}, nil
}

func (c scriptedClient) Complete(_ context.Context, request llm.Request) (llm.Response, error) {
	if c.models.onRequest != nil {
		c.models.onRequest(request)
	}
	if c.models.onCall != nil {
		c.models.onCall()
	}
	if c.models.err != nil {
		return c.models.response, c.models.err
	}
	response := c.models.response
	if len(response.Attempts) == 0 {
		response.Attempts = []llm.Attempt{{Usage: llm.Usage{InputTokens: 10, OutputTokens: 5}}}
	}
	return response, nil
}

func (c scriptedClient) Stream(ctx context.Context, request llm.Request, _ func(llm.Chunk) error) (llm.Response, error) {
	return c.Complete(ctx, request)
}

func contains(haystack, needle string) bool { return strings.Contains(haystack, needle) }

// The agent is handed what the Run was started on.
//
// This is the whole point of the input: an implementation that receives nil has
// no question to answer, and an assistant would run every turn on the prompt
// alone. It was accepted at Start, stored nowhere and dropped in silence — the
// Run still succeeded, having answered nothing.

func attributeOf(decision observe.Decision, key string) string {
	for _, attribute := range decision.Attributes {
		if attribute.Key == key {
			return attribute.Value
		}
	}
	return ""
}

// A Run label must reach the policy evaluation, refuse the call, and leave the
// Run able to finish without it.
//
// The gap this closes is specific: a step that calls a tool because the model
// asked for it cannot be governed by a prompt. Telling the model not to search
// is a request, and the only place a request becomes a refusal is the gateway.
