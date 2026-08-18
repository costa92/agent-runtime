package agentruntime_test

import (
	"context"
	"encoding/json"
	"errors"
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

func (frozenKeys) Frozen() bool     { return true }
func (f frozenKeys) Keys() []string { return f.keys }

type frozenTools struct{}

func (frozenTools) Frozen() bool { return true }
func (frozenTools) Specs() []tool.Spec {
	return []tool.Spec{{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
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
func TestStartPinsTheDefinitionGraphAndRuleSets(t *testing.T) {
	h := newHarness(t, answering("done"), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{
			policies: policy.Snapshot{Digest: "policy-7"},
			quotas:   quota.Snapshot{Digest: "quota-3"},
		}
	}))
	started := start(t, h)

	if started.Graph.Digest == "" {
		t.Fatal("the Run did not pin a graph digest")
	}
	if started.Pins.PolicyDigest != "policy-7" || started.Pins.QuotaDigest != "quota-3" {
		t.Fatalf("pins=%+v", started.Pins)
	}
	if started.Pins.Trace.TraceID == "" {
		t.Error("the Run started no trace; a resumed Run would open an unlinked one")
	}
}

// Only the durable half of an identity may reach storage. Persisting claims
// turns a momentary authorization into one replayed to every later reader.
func TestNoRequestScopedClaimReachesTheSnapshot(t *testing.T) {
	h := newHarness(t, answering("done"))
	started := start(t, h)

	encoded, err := json.Marshal(started)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(encoded) == "" {
		t.Fatal("empty snapshot")
	}
	for _, forbidden := range []string{"editor", "Claims", "claims"} {
		if contains(string(encoded), forbidden) {
			t.Fatalf("the snapshot carries request-scoped claims: %s", encoded)
		}
	}
}

func TestAnUnauthorizedDefinitionNeverCreatesARun(t *testing.T) {
	h := newHarness(t, answering("done"), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Authorization = fakeAuthorizer{
			err: run.NewError("forbidden", run.ErrorDenied, run.RetryNever),
		}
	}))

	if _, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:  principal(),
		Definition: run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
	}); run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("kind=%s want=denied", run.KindOf(err))
	}
}

// Refusing at creation is cheap; refusing later leaves a Run nobody can run.
func TestQuotaRejectionAtCreationCreatesNoRun(t *testing.T) {
	h := newHarness(t, answering("done"), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{quotas: quota.Snapshot{
			Digest: "q", Limits: []quota.Limit{{
				Name: "concurrency", Scope: quota.Scope{Tenant: "acme"},
				Unit: quota.UnitConcurrentRuns, Max: 1,
			}},
		}}
		deps.Meter = fakeMeter{usage: map[quota.Unit]int{quota.UnitConcurrentRuns: 1}}
	}))

	_, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:  principal(),
		Definition: run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
	})
	if run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("kind=%s want=denied", run.KindOf(err))
	}
	if len(h.observer.named(observe.EventQuotaRejected)) != 1 {
		t.Error("the rejection produced no decision event")
	}
}

// A Run must come back as the shape it started as. A deployment whose published
// graph moved cannot be allowed to resume it as something else.
func TestAdvanceFailsClosedOnAGraphThatMoved(t *testing.T) {
	h := newHarness(t, answering("done"))
	started := start(t, h)

	h.source.graphRef.Digest = "some-other-digest"

	_, err := h.runtime.Advance(t.Context(), started.ID)
	if run.KindOf(err) != run.ErrorConflict {
		t.Fatalf("kind=%s want=conflict", run.KindOf(err))
	}
}

func TestTheGraphCacheServesRepeatedRuns(t *testing.T) {
	h := newHarness(t, answering("done"))

	for range 3 {
		started := start(t, h)
		if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
			t.Fatalf("advance: %v", err)
		}
	}
	// One load for the first Start; every later Start and Advance is a hit on
	// the same digest.
	if loads := h.source.loads.Load(); loads != 1 {
		t.Fatalf("graph loads=%d want=1", loads)
	}
}

func TestAnUnpinnedGraphIsRefused(t *testing.T) {
	cache := agentruntime.NewGraphCache(graphLoader{source: &fakeSource{}}, 2)

	if _, err := cache.Get(t.Context(), run.ExecutionGraphRef{ID: "x", Version: 1}); err == nil {
		t.Fatal("a ref with no digest was accepted")
	}
}

func TestTheGraphCacheIsBounded(t *testing.T) {
	source := &fakeSource{}
	cache := agentruntime.NewGraphCache(graphLoader{source: source}, 2)

	for i := range 5 {
		digest := fmt.Sprintf("digest-%d", i)
		ref := run.ExecutionGraphRef{ID: "x", Version: uint64(i + 1), Digest: digest}
		source.graph = &workflow.ExecutionGraph{Ref: ref}
		if _, err := cache.Get(t.Context(), ref); err != nil {
			t.Fatalf("get: %v", err)
		}
	}
	if cache.Len() != 2 {
		t.Fatalf("cache holds %d graphs; the bound is not enforced", cache.Len())
	}
}

// The whole reason the cache is keyed by digest: a hit must be the same graph,
// not merely the same name and version.
func TestACachedGraphIsStillVerified(t *testing.T) {
	source := &fakeSource{}
	cache := agentruntime.NewGraphCache(graphLoader{source: source}, 4)

	ref := run.ExecutionGraphRef{ID: "x", Version: 1, Digest: "digest-1"}
	source.graph = &workflow.ExecutionGraph{Ref: run.ExecutionGraphRef{
		ID: "x", Version: 1, Digest: "a-different-digest",
	}}

	if _, err := cache.Get(t.Context(), ref); run.KindOf(err) != run.ErrorConflict {
		t.Fatalf("kind=%s want=conflict", run.KindOf(err))
	}
}

// Every governance decision must be an event, and every event must be declared.
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
func TestAgentsReceiveGovernedPortsAndTheRunsBudget(t *testing.T) {
	var seen agent.Request
	h := newHarness(t, scriptedAgent{execute: func(_ context.Context, request agent.Request) (agent.Response, error) {
		seen = request
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if seen.Ports == nil {
		t.Fatal("the agent got no ports; it could only reach effects ungoverned or not at all")
	}
	if seen.Prompt != "be brief" {
		t.Errorf("prompt=%q; the published Definition's prompt did not reach the agent", seen.Prompt)
	}
	if seen.Remaining.Tokens != 10_000 {
		t.Errorf("remaining=%+v; an agent that cannot see the ceiling discovers it by hitting it", seen.Remaining)
	}
	if seen.Principal.Subject != "alice" {
		t.Errorf("principal=%+v", seen.Principal)
	}
}

// A model call must reserve before it is issued and settle after. Reserving
// afterwards means the budget is already spent by the time anything could
// refuse.
func TestAModelCallIsReservedBeforeItIsIssuedAndSettledAfter(t *testing.T) {
	var reservedDuringCall bool
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Model(ctx, llm.Request{MaxTokens: 100}); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{onCall: func() { reservedDuringCall = true }}
	}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if !reservedDuringCall {
		t.Fatal("the model was never called")
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s", result.Run.State)
	}
	if result.Run.Budget.Used.Tokens == 0 {
		t.Error("the call settled no usage; a ledger that counts nothing bounds nothing")
	}
	if len(h.observer.named(observe.EventModelSelected)) == 0 {
		t.Error("model selection produced no decision event")
	}
}

// Every attempt reports usage, including the failed ones: a provider that
// consumed the prompt and then errored still charged for it.
func TestAFailedModelCallStillSettlesItsUsage(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		_, err := request.Ports.Model(ctx, llm.Request{MaxTokens: 100})
		if err == nil {
			t.Error("the scripted failure did not surface")
		}
		return agent.Response{Output: json.RawMessage(`"partial"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{
			err: run.NewError("provider_down", run.ErrorRetryable, run.RetryBackoff),
			response: llm.Response{Attempts: []llm.Attempt{
				{Usage: llm.Usage{InputTokens: 40}, Failed: true},
			}},
		}
	}))
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.Budget.Used.Tokens != 40 {
		t.Fatalf("used=%+v; a failed attempt's spend is still real", result.Run.Budget.Used)
	}
}

// Production assistant envelopes copy MaxLLMCalls/MaxToolCalls and leave
// Tokens at 0. After the first model call settles its real usage, the next
// effect must still be admitted — otherwise a tool-calling turn fails with
// no assistant reply, which is what the user saw as a silent failed run.
func TestAnUncappedTokenEnvelopeStillAdmitsTheNextEffect(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Model(ctx, llm.Request{}); err != nil {
			return agent.Response{}, err
		}
		if _, err := request.Ports.Model(ctx, llm.Request{}); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{response: llm.Response{
			Attempts: []llm.Attempt{{Usage: llm.Usage{InputTokens: 2000, OutputTokens: 147}}},
		}}
	}))
	started, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:  principal(),
		Definition: run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Input:      json.RawMessage(`{"q":"x"}`),
		Budget:     run.Limits{LLMCalls: 10, ToolCalls: 20},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s; the follow-up model call against an uncapped token component failed", result.Run.State)
	}
	if result.Run.Budget.Used.LLMCalls != 2 {
		t.Fatalf("used llm=%d; the second model call did not settle", result.Run.Budget.Used.LLMCalls)
	}
}

// A budget that cannot pay must refuse before the call, not after.
func TestAnEffectOverTheBudgetIsRefusedBeforeTheProviderIsCalled(t *testing.T) {
	var called bool
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Model(ctx, llm.Request{MaxTokens: 1_000_000}); err == nil {
			t.Error("an unaffordable call was allowed")
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{onCall: func() { called = true }}
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if called {
		t.Fatal("the provider was reached despite the refusal")
	}
	if len(h.observer.named(observe.EventBudgetRefused)) == 0 {
		t.Error("the budget refusal produced no decision event")
	}
}

// A memory key the Definition never declared is refused even when the provider
// exists: the Definition is what the Run was published to do.
func TestAnUndeclaredMemoryKeyIsRefused(t *testing.T) {
	var refusal error
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		_, refusal = request.Ports.Recall(ctx, "notes", "anything")
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if run.KindOf(refusal) != run.ErrorDenied {
		t.Fatalf("kind=%s want=denied", run.KindOf(refusal))
	}
}

// A worker whose lease lapsed mid-effect must not commit the result: another
// worker may already have taken over.
func TestALostLeaseRejectsTheResultInsteadOfCommittingIt(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		// While the effect is in flight, someone else takes the Run.
		<-ctx.Done()
		return agent.Response{}, ctx.Err()
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.LeaseFor = 30 * time.Millisecond
	}))
	started := start(t, h)

	go func() {
		time.Sleep(5 * time.Millisecond)
		// Store-authoritative expiry: wall-clock sleep does not lapse the
		// MemoryStore lease. Advance the clock past the deadline, then take over.
		h.clock.Advance(time.Second)
		_, _, _ = h.store.Claim(context.Background(), store.ClaimCommand{
			RunID: started.ID, Owner: "worker-2", LeaseFor: time.Second,
		})
	}()

	_, err := h.runtime.Advance(t.Context(), started.ID)
	if err == nil {
		t.Fatal("a worker that lost its lease committed anyway")
	}
	if run.KindOf(err) != run.ErrorConflict {
		t.Fatalf("kind=%s want=conflict", run.KindOf(err))
	}
}

// A picture-book render lasts minutes. A 30s lease that is not renewed mid-effect
// is stolen, the invocation is parked as unknown, and the book never lands.
func TestAdvanceRenewsTheLeaseWhileAnEffectIsInFlight(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		time.Sleep(80 * time.Millisecond)
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.LeaseFor = 30 * time.Millisecond
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if h.store.Renews.Load() == 0 {
		t.Fatal("the lease was not renewed while the effect ran")
	}
}

// The renew loop ticks at LeaseFor/3, so a node that finishes sooner than that
// never renews at all — and the sibling test above hides this by sleeping well
// past a tick. Real nodes are fast: a model call answers in seconds against a
// thirty-second lease, and one Advance walks the whole graph under a single
// claim. Node after node the loop started and stopped before its first tick,
// the deadline stayed pinned at claim time, and once the graph's total run time
// passed LeaseFor another poller took the Run over and parked whatever was in
// flight. That is how every article generation ended in waiting_resolution.
func TestTheLeaseIsRenewedEvenWhenTheNodeIsFasterThanATick(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.LeaseFor = time.Minute
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if h.store.Renews.Load() == 0 {
		t.Fatal("a node that outran the renew ticker left the lease pinned at claim time")
	}
}

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

	snapshot, err := h.runtime.Inspect(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if snapshot.State != run.StateSucceeded {
		t.Fatalf("state=%s", snapshot.State)
	}

	page, err := h.runtime.ListEvents(t.Context(), store.EventQuery{RunID: started.ID, Limit: 100})
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

// An idle queue is not a failure. A caller that could not tell them apart would
// log an error on every poll.
func TestAdvanceNextDistinguishesAnIdleQueueFromAFailure(t *testing.T) {
	h := newHarness(t, answering("done"))

	_, found, err := h.runtime.AdvanceNext(t.Context(), agentruntime.AdvanceNextRequest{Limit: 1})
	if err != nil {
		t.Fatalf("advance next: %v", err)
	}
	if found {
		t.Fatal("work was found in an empty queue")
	}

	start(t, h)
	result, found, err := h.runtime.AdvanceNext(t.Context(), agentruntime.AdvanceNextRequest{Limit: 1})
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

// --- helpers --------------------------------------------------------------

type scriptedModels struct {
	response llm.Response
	err      error
	onCall   func()
}

func (m scriptedModels) Resolve(context.Context, llm.ModelRef) (llm.Client, error) {
	return scriptedClient{models: m}, nil
}

type scriptedClient struct{ models scriptedModels }

func (c scriptedClient) Capabilities(context.Context, llm.ModelRef) (llm.Capabilities, error) {
	return llm.Capabilities{Tools: true, Streaming: true}, nil
}

func (c scriptedClient) Complete(context.Context, llm.Request) (llm.Response, error) {
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
func TestTheAgentReceivesTheRunsInput(t *testing.T) {
	var seen agent.Request
	h := newHarness(t, scriptedAgent{execute: func(_ context.Context, request agent.Request) (agent.Response, error) {
		seen = request
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if string(seen.Input) != `{"q":"x"}` {
		t.Fatalf("input = %s; the agent executed without what it was asked", seen.Input)
	}
}

// A Run whose effect was left in flight is not simply run again.
//
// A worker that dies between committing the invocation-begin fact and settling
// it leaves an effect nobody can classify: the model call may have been issued
// and charged, the tool may have published. The next worker to claim the Run
// cannot schedule the node again — re-executing is the duplicate side effect the
// whole reserve-before-effect order exists to prevent, and refusing it is what
// the legacy delegation worker did by failing closed on a child that had already
// emitted events.
func TestARunWithAnInFlightEffectIsNotReExecuted(t *testing.T) {
	attempts := 0
	h := newHarness(t, scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		attempts++
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	// The crash is injected at the Store, which is where it happens: the begin
	// fact is committed and nothing settles it, exactly as a killed process
	// leaves it.
	ctx := t.Context()
	lease, snapshot, err := h.store.Claim(ctx, store.ClaimCommand{
		RunID: started.ID, Owner: "dying-worker", LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	running, err := run.Reduce(snapshot, run.Command{Kind: run.CommandStart})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	committed, err := h.store.CommitNodeResult(ctx, store.CommitNodeResultCommand{
		Fence:    store.ExecutionFence{RunID: started.ID, ExpectedRevision: snapshot.Revision, LeaseToken: lease.Token},
		NodeName: "run",
		Commit:   store.CommitContext{Transition: running, Events: running.Events},
	})
	if err != nil {
		t.Fatalf("commit start: %v", err)
	}

	begun, err := run.Reduce(committed, run.Command{
		Kind: run.CommandInvokeModel, InvocationID: "inv-1",
		IdempotencyKey: "key-1", Reserve: run.Limits{LLMCalls: 1, Tokens: 10},
	})
	if err != nil {
		t.Fatalf("reduce begin: %v", err)
	}
	if _, err := h.store.BeginInvocation(ctx, store.BeginInvocationCommand{
		Fence: store.ExecutionFence{RunID: started.ID, ExpectedRevision: committed.Revision, LeaseToken: lease.Token},
		Invocation: store.InvocationBegin{
			ID: "inv-1", IdempotencyKey: "key-1",
			Reservation: store.BudgetReservation{ID: "inv-1", Amount: run.Limits{LLMCalls: 1, Tokens: 10}},
		},
		Commit: store.CommitContext{Transition: begun, Events: begun.Events},
	}); err != nil {
		t.Fatalf("begin: %v", err)
	}

	// The worker is gone. Its lease lapses and the Run is claimable again.
	before := attempts
	h.clock.Advance(time.Hour)
	result, err := h.runtime.Advance(ctx, started.ID)

	if attempts > before {
		t.Fatalf("the node ran again; its effect may already have happened")
	}
	if err == nil && result.Run.State != run.StateWaitingResolution {
		t.Fatalf("state = %s; an effect nobody can classify has to be resolved, "+
			"not stepped over", result.Run.State)
	}
}

// A Run whose effects all settled is not parked.
//
// Without this the guard above would park every Run that ever made a call, and
// the whole runtime would stop at the first model request waiting for a human.
func TestASettledEffectDoesNotParkTheRun(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if _, err := request.Ports.Model(ctx, llm.Request{
			Messages: []llm.Message{{Role: "user", Content: "hi"}},
		}); err != nil {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.State == run.StateWaitingResolution {
		t.Fatal("a Run whose call completed is waiting for somebody to classify it")
	}
	if !result.Run.State.Terminal() {
		t.Fatalf("state = %s", result.Run.State)
	}
}

// A parked approval the client cannot name cannot be answered. The id has to
// live on the Snapshot and on the waiting_approval event, because Inspect
// and the event stream are the only two reads the confirm UI has.
func TestARequiredApprovalIsNamedOnTheParkedRun(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		return agent.Response{}, tool.ApprovalRequired
	}})
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if result.Run.State != run.StateWaitingApproval {
		t.Fatalf("state = %s, want waiting_approval", result.Run.State)
	}
	inspected, err := h.runtime.Inspect(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if inspected.PendingApprovalID == "" {
		t.Fatal("Inspect has no pending_approval_id; confirm has nothing to POST")
	}

	page, err := h.store.Events(t.Context(), store.EventQuery{RunID: started.ID, Limit: 20})
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var parked run.Event
	for _, event := range page.Events {
		if event.To == run.StateWaitingApproval {
			parked = event
			break
		}
	}
	if parked.ApprovalID == "" {
		t.Fatal("the waiting_approval event has no approval_id")
	}
	if parked.ApprovalID != inspected.PendingApprovalID {
		t.Fatalf("event approval %q != snapshot %q", parked.ApprovalID, inspected.PendingApprovalID)
	}
}

// Denying a write must not kill the Run: an assistant whose tool request was
// refused still has to answer. The denied effect is reported back as a plain
// refusal — granted never carries it into execution — so the agent can tell
// the model and let it answer without the tool.
func TestDenyingAWriteLetsTheRunContinueWithoutTheEffect(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"book_id":"b1","title":"兔","render_marker":"<!--picture-book b1-->"}`)
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	// Round 1 asks for the write and parks. Round 2 (after the denial) asks
	// again, is refused, and answers in text instead — exactly what the
	// assistant agent does with a refused tool.
	rounds := 0
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		rounds++
		if request.Granted != nil {
			t.Fatal("granted was injected for a denied approval; the refused write must never execute")
		}
		_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"兔"}`))
		if err == nil {
			t.Fatal("the denied tool executed")
		}
		if tool.IsApprovalRequired(err) {
			return agent.Response{}, err
		}
		return agent.Response{Output: json.RawMessage(`{"content":"绘本没有生成，我直接回答你"}`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{policies: policy.Snapshot{
			Policies: []policy.Policy{{
				Name:  "writes-need-a-human",
				Scope: policy.ScopeTenant,
				Conditions: []policy.Condition{{
					Fact: policy.FactToolSideEffect, Operator: policy.OpEquals,
					Values: []string{string(policy.SideEffectWrite)},
				}},
				Decision: policy.DecisionRequireApproval,
			}},
		}}
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
	started := start(t, h)

	parked, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if parked.Run.State != run.StateWaitingApproval {
		t.Fatalf("state=%s want=waiting_approval", parked.Run.State)
	}

	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: parked.Run.PendingApprovalID,
		Approved: false, DecidedBy: principal(),
	}); err != nil {
		t.Fatalf("deny: %v", err)
	}
	h.clock.Advance(time.Minute)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance after deny: %v", err)
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s; a denied approval failed the whole Run instead of letting the agent answer", result.Run.State)
	}
	if rounds != 2 {
		t.Fatalf("rounds=%d want=2 (ask, then answer after the refusal)", rounds)
	}
	if handler.Calls() != 0 {
		t.Fatalf("write calls=%d; the denied effect ran anyway", handler.Calls())
	}
}

// Confirming a write must perform that write. Re-running the node from
// scratch asks the model again, which parks again, and the user sees no
// follow-up after 确认执行.
func TestConfirmingAWritePerformsTheWrite(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"book_id":"b1","title":"兔","render_marker":"<!--picture-book b1-->"}`)
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if request.Granted != nil {
			out, err := request.Ports.Tool(ctx, request.Granted.Name, request.Granted.Arguments)
			if err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: out}, nil
		}
		_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"兔"}`))
		return agent.Response{}, err
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{policies: policy.Snapshot{
			Policies: []policy.Policy{{
				Name:  "writes-need-a-human",
				Scope: policy.ScopeTenant,
				Conditions: []policy.Condition{{
					Fact: policy.FactToolSideEffect, Operator: policy.OpEquals,
					Values: []string{string(policy.SideEffectWrite)},
				}},
				Decision: policy.DecisionRequireApproval,
			}},
		}}
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
	started := start(t, h)

	parked, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if parked.Run.State != run.StateWaitingApproval {
		t.Fatalf("state=%s want=waiting_approval", parked.Run.State)
	}
	if handler.Calls() != 0 {
		t.Fatal("the write ran before a human approved it")
	}

	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: parked.Run.PendingApprovalID,
		Approved: true, DecidedBy: principal(),
	}); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	// The worker that parked still holds the lease. Production waits it out;
	// the test clock is Store-authoritative.
	h.clock.Advance(time.Minute)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance after confirm: %v", err)
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s; confirming a write did not finish the turn", result.Run.State)
	}
	if handler.Calls() != 1 {
		t.Fatalf("write calls=%d; the approved effect did not run", handler.Calls())
	}
}

// Confirming must not park the resumed Run over a model call it already
// finished. The real assistant shape is model → tool: by the time the write
// parks for approval, the model call has completed. Left as in_flight in the
// snapshot — complete() used to settle the budget without carrying the
// outcome — that call reads to parkUnclassifiedEffects as a worker that died
// mid-effect, and the Run parks itself in waiting_resolution the moment the
// approval is confirmed. The user sees 确认执行 and then 正在核对 forever.
func TestConfirmingAWriteDoesNotParkTheRunOverItsFinishedModelCall(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"book_id":"b1","title":"兔","render_marker":"<!--picture-book b1-->"}`)
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if request.Granted != nil {
			out, err := request.Ports.Tool(ctx, request.Granted.Name, request.Granted.Arguments)
			if err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: out}, nil
		}
		// One finished model call first, then the write that needs a human:
		// the exact shape that left an in_flight model invocation behind when
		// the Run parked for approval.
		if _, err := request.Ports.Model(ctx, llm.Request{}); err != nil {
			return agent.Response{}, err
		}
		_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"兔"}`))
		return agent.Response{}, err
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{response: llm.Response{
			Attempts: []llm.Attempt{{Usage: llm.Usage{InputTokens: 100}}},
		}}
		deps.Governance = fakeGovernance{policies: policy.Snapshot{
			Policies: []policy.Policy{{
				Name:  "writes-need-a-human",
				Scope: policy.ScopeTenant,
				Conditions: []policy.Condition{{
					Fact: policy.FactToolSideEffect, Operator: policy.OpEquals,
					Values: []string{string(policy.SideEffectWrite)},
				}},
				Decision: policy.DecisionRequireApproval,
			}},
		}}
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
	started := start(t, h)

	parked, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if parked.Run.State != run.StateWaitingApproval {
		t.Fatalf("state=%s want=waiting_approval", parked.Run.State)
	}

	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: parked.Run.PendingApprovalID,
		Approved: true, DecidedBy: principal(),
	}); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	h.clock.Advance(time.Minute)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance after confirm: %v", err)
	}
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s; the resumed Run parked over the model call it already finished", result.Run.State)
	}
	if handler.Calls() != 1 {
		t.Fatalf("write calls=%d; the approved effect did not run", handler.Calls())
	}
	for id, invocation := range result.Run.Invocations {
		if invocation.Outcome == run.OutcomeInFlight {
			t.Fatalf("invocation %s is still in_flight at the terminal snapshot", id)
		}
	}
}

// The per-profile consumption detail the agent reports must reach the Store
// verbatim: it is the host's only input for pricing v2 spend, and the engine
// is the only path that carries it out of the agent.
func TestNodeModelUsageReachesTheStore(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		var modelUsage []run.ModelUsage
		for range 2 {
			response, err := request.Ports.Model(ctx, llm.Request{MaxTokens: 100})
			if err != nil {
				return agent.Response{}, err
			}
			modelUsage = run.MergeModelUsage(modelUsage,
				response.Model.Profile, response.Usage.InputTokens, response.Usage.OutputTokens)
		}
		return agent.Response{Output: json.RawMessage(`"ok"`), ModelUsage: modelUsage}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{}
	}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	usage := h.store.NodeModelUsage(started.ID)
	if len(usage) != 1 {
		t.Fatalf("model usage lines = %+v, want one line for the pinned profile", usage)
	}
	if usage[0].Profile != "fast" {
		t.Fatalf("profile = %q, want the pinned profile %q", usage[0].Profile, "fast")
	}
	if usage[0].InputTokens != 20 || usage[0].OutputTokens != 10 {
		t.Fatalf("usage = %+v, want input 20 output 10 (two scripted calls)", usage[0])
	}
}

// A node that reports no model detail must commit nothing: an empty profile
// line would reach the host as a zero-priced slice and read like free spend.
func TestEmptyModelUsageCommitsNoRows(t *testing.T) {
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}})
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if usage := h.store.NodeModelUsage(started.ID); len(usage) != 0 {
		t.Fatalf("usage = %+v, want none", usage)
	}
}

// A grant authorises one write, not a standing permission for that tool.
//
// The hold lives in the checkpoint and nothing removed it once the granted
// call had run, so it stayed in place for the rest of the Run. The damage
// shows on the recovery path — effect performed, result not committed, the
// call parked as unknown, resolved as applied, and the next advance injects
// Granted again and repeats a non-idempotent write (a second picture book was
// generated this way in production) — but the mechanism is simply that the
// hold outlives the call it was granted for, which a second request for the
// same tool exposes directly.
func TestAGrantCoversExactlyOneExecution(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"book_id":"b1","title":"兔"}`)
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	var secondCallErr error
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if request.Granted != nil {
			out, err := request.Ports.Tool(ctx, request.Granted.Name, request.Granted.Arguments)
			if err != nil {
				return agent.Response{}, err
			}
			// The approved write is done. Asking for another one is a new
			// request for a human to decide, not a continuation of the old
			// grant — the same question the resume path asks after an
			// interrupted effect is resolved.
			_, secondCallErr = request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"猫"}`))
			return agent.Response{Output: out}, nil
		}
		_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"兔"}`))
		return agent.Response{}, err
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{policies: policy.Snapshot{
			Policies: []policy.Policy{{
				Name:  "writes-need-a-human",
				Scope: policy.ScopeTenant,
				Conditions: []policy.Condition{{
					Fact: policy.FactToolSideEffect, Operator: policy.OpEquals,
					Values: []string{string(policy.SideEffectWrite)},
				}},
				Decision: policy.DecisionRequireApproval,
			}},
		}}
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
	started := start(t, h)

	parked, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if parked.Run.State != run.StateWaitingApproval {
		t.Fatalf("state=%s want=waiting_approval", parked.Run.State)
	}

	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: parked.Run.PendingApprovalID,
		Approved: true, DecidedBy: principal(),
	}); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	h.clock.Advance(time.Minute)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance after confirm: %v", err)
	}

	if handler.Calls() != 1 {
		t.Fatalf("write calls=%d, want 1; the grant carried into a second effect", handler.Calls())
	}
	if secondCallErr == nil {
		t.Fatal("the second write was allowed to run under the first grant")
	}
	if !tool.IsApprovalRequired(secondCallErr) {
		t.Fatalf("second write refused with %v; a new write must ask a human again", secondCallErr)
	}
}
