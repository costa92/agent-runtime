package agentruntime_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

// --- delegation fixtures --------------------------------------------------

type scriptedRouter struct {
	decision workflow.RouteDecision
	err      error
	// seen records what the router was asked, to prove the Runtime supplies the
	// published routing policy rather than the router inventing its own.
	seen *workflow.RouteRequest
}

func (r *scriptedRouter) Route(_ context.Context, request workflow.RouteRequest) (workflow.RouteDecision, error) {
	if r.seen != nil {
		*r.seen = request
	}
	return r.decision, r.err
}

type fixedCandidates struct{ candidates []workflow.Candidate }

func (c fixedCandidates) Candidates(context.Context, workflow.RouteRequest) ([]workflow.Candidate, error) {
	return c.candidates, nil
}

type countingSynthesis struct {
	calls    int
	children []workflow.ChildResult
}

func (s *countingSynthesis) Synthesize(_ context.Context, request workflow.SynthesisRequest) (json.RawMessage, error) {
	s.calls++
	s.children = append([]workflow.ChildResult(nil), request.Children...)
	return json.RawMessage(`"combined"`), nil
}

func childSpec(key string, dependsOn ...string) workflow.ChildSpec {
	return workflow.ChildSpec{
		Key:        key,
		Definition: run.DefinitionRef{ID: key, Version: 1, Protocol: 1},
		Graph:      run.ExecutionGraphRef{ID: key, Version: 1, Protocol: 1, Digest: "digest-" + key},
		DependsOn:  dependsOn,
		Budget:     run.Limits{LLMCalls: 1, Tokens: 100, ToolCalls: 1},
	}
}

func delegatePlan(children ...workflow.ChildSpec) workflow.RouteDecision {
	return workflow.RouteDecision{
		Kind: workflow.RouteDelegate,
		Plan: workflow.DelegationPlan{Children: children},
	}
}

func candidatesOf(children ...workflow.ChildSpec) []workflow.Candidate {
	candidates := make([]workflow.Candidate, 0, len(children))
	for _, child := range children {
		candidates = append(candidates, workflow.Candidate{
			Key: child.Key, Definition: child.Definition, Enabled: true, Authorized: true,
		})
	}
	return candidates
}

// orchestrator returns a harness whose Definition delegates.
type orchestratorOption func(*definition.RoutingPolicy, *agentruntime.Dependencies)

func withSynthesis(strategy workflow.SynthesisStrategy) orchestratorOption {
	return func(_ *definition.RoutingPolicy, deps *agentruntime.Dependencies) {
		deps.Synthesis = strategy
	}
}

func withRouting(mutate func(*definition.RoutingPolicy)) orchestratorOption {
	return func(routing *definition.RoutingPolicy, _ *agentruntime.Dependencies) { mutate(routing) }
}

func orchestrator(t *testing.T, router *scriptedRouter, children []workflow.ChildSpec, options ...orchestratorOption) *harness {
	t.Helper()
	routing := definition.RoutingPolicy{}
	var extra []func(*agentruntime.Dependencies)
	for _, apply := range options {
		var deps agentruntime.Dependencies
		apply(&routing, &deps)
		if deps.Synthesis != nil {
			strategy := deps.Synthesis
			extra = append(extra, func(d *agentruntime.Dependencies) { d.Synthesis = strategy })
		}
	}
	return newHarness(
		t, answering("unused"),
		withDefinition(definition.Definition{
			Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Mode:           definition.ModeOrchestrator,
			Implementation: "answer",
			Routing:        routing,
		}),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Router = router
			deps.Candidates = fixedCandidates{candidates: candidatesOf(children...)}
			for _, apply := range extra {
				apply(deps)
			}
		}),
	)
}

// --- tests ----------------------------------------------------------------

// An orchestrator's work is its children. It parks rather than finishing, and
// the children exist before it does so.
func TestDelegationCreatesChildrenAndParksTheParent(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research"), childSpec("survey")}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children)
	started := start(t, h)

	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if !result.Waiting || result.Run.State != run.StateWaitingChildren {
		t.Fatalf("state=%s waiting=%v want waiting_children", result.Run.State, result.Waiting)
	}
	if created := h.store.Children(started.ID); len(created) != 2 {
		t.Fatalf("children=%d want=2", len(created))
	}
}

// The tree must be walkable after a crash without inferring structure from
// timing, and each child's slice must be charged against the root the moment it
// is granted.
func TestChildrenCarryDurableLinksAndRootReservations(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research")}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children)
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	links := h.store.Links()
	if len(links) != 1 || links[0].ParentID != started.ID || links[0].NodeName != "research" {
		t.Fatalf("links=%+v", links)
	}
	if _, ok := h.store.Reservation(links[0].ChildID); !ok {
		t.Fatal("the child's slice was never reserved against the root")
	}

	parent, err := h.runtime.Inspect(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(parent.Budget.Slices) != 1 {
		t.Fatalf("slices=%+v; the grant is not committed against the root", parent.Budget.Slices)
	}
}

// A child acts as the same principal as its root. Delegation widens capability,
// never identity.
func TestChildrenInheritTheRootPrincipalAndRootID(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research")}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children)
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	created := h.store.Children(started.ID)
	if len(created) != 1 {
		t.Fatalf("children=%d", len(created))
	}
	if created[0].Principal != started.Principal {
		t.Errorf("child principal=%+v want=%+v", created[0].Principal, started.Principal)
	}
	if created[0].RootID != started.ID || created[0].ParentID != started.ID {
		t.Errorf("child is not rooted: %+v", created[0])
	}
	if created[0].Graph.Digest == "" {
		t.Error("the child pinned no graph; recovery could bring it back a different shape")
	}
}

// direct and clarify are answers this Run gives itself. Only delegation creates
// children.
func TestDirectAndClarifyCreateNoChildren(t *testing.T) {
	for name, decision := range map[string]workflow.RouteDecision{
		"direct":  {Kind: workflow.RouteDirect},
		"clarify": {Kind: workflow.RouteClarify, Question: "which account?"},
	} {
		t.Run(name, func(t *testing.T) {
			h := orchestrator(t, &scriptedRouter{decision: decision}, nil)
			started := start(t, h)

			if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
				t.Fatalf("advance: %v", err)
			}
			if created := h.store.Children(started.ID); len(created) != 0 {
				t.Fatalf("children=%d; only delegation creates children", len(created))
			}
		})
	}
}

// The router is product code and may be an LLM. The limits it is bounded by are
// published data, and checking them here is what makes them limits rather than
// suggestions.
func TestTheRuntimeValidatesWhateverTheRouterReturns(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research"), childSpec("survey"), childSpec("draft")}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children,
		withRouting(func(routing *definition.RoutingPolicy) { routing.MaxDelegations = 2 }))
	started := start(t, h)

	_, err := h.runtime.Advance(t.Context(), started.ID)
	if run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("kind=%s want=denied", run.KindOf(err))
	}
	if created := h.store.Children(started.ID); len(created) != 0 {
		t.Fatalf("children=%d; the refusal came after creation", len(created))
	}
}

// A candidate nobody authorized must not be reachable by delegating to it —
// that is precisely how a Run would reach capability it was never granted.
func TestAnUnauthorizedCandidateIsRefused(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research")}
	h := newHarness(
		t, answering("unused"),
		withDefinition(definition.Definition{
			Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Mode:           definition.ModeOrchestrator,
			Implementation: "answer",
		}),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Router = &scriptedRouter{decision: delegatePlan(children...)}
			deps.Candidates = fixedCandidates{candidates: []workflow.Candidate{{
				Key: "research", Definition: children[0].Definition,
				Enabled: true, Authorized: false,
			}}}
		}),
	)
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("kind=%s want=denied", run.KindOf(err))
	}
}

// The Runtime supplies the published routing policy; the router does not bring
// its own.
func TestTheRouterIsGivenThePublishedRoutingPolicy(t *testing.T) {
	var seen workflow.RouteRequest
	children := []workflow.ChildSpec{childSpec("research")}
	router := &scriptedRouter{decision: delegatePlan(children...), seen: &seen}
	h := orchestrator(t, router, children,
		withRouting(func(routing *definition.RoutingPolicy) { routing.MaxDelegations = 5 }))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if seen.Routing.MaxDelegations != 5 {
		t.Fatalf("routing=%+v; the published policy did not reach the router", seen.Routing)
	}
	if seen.Parent.ID != started.ID {
		t.Errorf("the router was not told which Run it is routing")
	}
}

// A parent parks on its children and may be resumed hours later by a different
// worker in a different process, which has no memory of the plan it made.
func TestThePlanSurvivesAcrossAdvanceCalls(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research"), childSpec("write", "research")}
	router := &scriptedRouter{decision: delegatePlan(children...)}
	h := orchestrator(t, router, children)
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	// Only the dependency-free child exists so far.
	if created := h.store.Children(started.ID); len(created) != 1 {
		t.Fatalf("children=%d want=1; the second generation is blocked", len(created))
	}

	parked, err := h.runtime.Inspect(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if len(parked.Checkpoint) == 0 {
		t.Fatal("the plan was not made durable; a resuming worker could not continue it")
	}

	// The router is removed: whatever resumes the parent must work from the
	// durable plan alone.
	router.decision = workflow.RouteDecision{}
	router.err = run.NewError("router_must_not_be_asked_again", run.ErrorInternal, run.RetryNever)

	finishChild(t, h, started.ID, "research")
	resume(t, h, started.ID)

	if created := h.store.Children(started.ID); len(created) != 2 {
		t.Fatalf("children=%d want=2; the second generation never ran", len(created))
	}
}

// Some children produced output and some did not: partial is the honest answer.
func TestARootSynthesizesOnceWhenEveryChildIsTerminal(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research"), childSpec("survey")}
	synthesis := &countingSynthesis{}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children,
		withSynthesis(synthesis))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	finishChild(t, h, started.ID, "research")
	failChild(t, h, started.ID, "survey")

	result := resume(t, h, started.ID)
	if result.Run.State != run.StatePartial {
		t.Fatalf("state=%s want=partial", result.Run.State)
	}
	if synthesis.calls != 1 {
		t.Fatalf("synthesis calls=%d want=1", synthesis.calls)
	}
}

// A second root result would contradict what every downstream consumer was
// already told.
func TestASecondSynthesisIsRefused(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research")}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children)
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	finishChild(t, h, started.ID, "research")

	result := resume(t, h, started.ID)
	if result.Run.State != run.StateSucceeded {
		t.Fatalf("state=%s want=succeeded", result.Run.State)
	}
	// A terminal root accepts nothing further, whatever arrives late.
	if _, err := h.runtime.Advance(t.Context(), started.ID); err == nil {
		t.Fatal("a synthesized root was advanced again")
	}
}

// The parent stays parked while any child is still running rather than polling
// itself to a conclusion.
func TestTheParentStaysParkedWhileAChildRuns(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research"), childSpec("survey")}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children)
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	finishChild(t, h, started.ID, "research")

	result := resume(t, h, started.ID)
	if !result.Waiting || result.Run.State != run.StateWaitingChildren {
		t.Fatalf("state=%s waiting=%v; one child is still running", result.Run.State, result.Waiting)
	}
}

// Cancelling the root must reach every descendant without waiting for
// row-by-row state updates.
func TestCancellingTheRootFencesItsChildren(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research")}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children)
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	created := h.store.Children(started.ID)
	if len(created) != 1 {
		t.Fatalf("children=%d", len(created))
	}

	if _, err := h.runtime.Cancel(t.Context(), agentruntime.CancelRequest{
		RootID: started.ID, RequestedBy: principal(), Reason: "operator",
	}); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// The child cannot be advanced after the fence, whatever its own row says.
	if _, err := h.runtime.Advance(t.Context(), created[0].ID); err == nil {
		t.Fatal("a child of a cancelled root was advanced")
	}
}

// --- child drivers --------------------------------------------------------
//
// Children are driven through the Store rather than through Advance: these
// tests are about the parent's coordination, and running a real child would
// need its own published Definition and graph, which is Task 10's fixture work.

func settleChild(t *testing.T, h *harness, parentID run.ID, key string, terminal run.CommandKind) {
	t.Helper()

	var childID run.ID
	for _, link := range h.store.Links() {
		if link.ParentID == parentID && link.NodeName == key {
			childID = link.ChildID
		}
	}
	if childID == "" {
		t.Fatalf("no child %q under %s", key, parentID)
	}

	ctx := t.Context()
	lease, snapshot, err := h.store.Claim(ctx, store.ClaimCommand{
		RunID: childID, Owner: "child-driver", LeaseFor: time.Minute,
	})
	if err != nil {
		t.Fatalf("claim child: %v", err)
	}
	started, err := run.Reduce(snapshot, run.Command{Kind: run.CommandStart})
	if err != nil {
		t.Fatalf("start child: %v", err)
	}
	afterStart, err := h.store.CommitNodeResult(ctx, store.CommitNodeResultCommand{
		Fence: store.ExecutionFence{
			RunID: childID, ExpectedRevision: snapshot.Revision, LeaseToken: lease.Token,
		},
		NodeName: "run",
		Commit:   store.CommitContext{Transition: started, Events: started.Events},
	})
	if err != nil {
		t.Fatalf("commit child start: %v", err)
	}

	finished, err := run.Reduce(afterStart, run.Command{Kind: terminal})
	if err != nil {
		t.Fatalf("finish child: %v", err)
	}
	// A succeeding child commits an output ref, exactly as runNode does. Without
	// one the parent cannot tell "produced nothing" from "produced something we
	// dropped on the way".
	// A succeeding child's node map carries where its output was stored, which
	// is what ApplyNodeResult writes on the production path. Built here by hand
	// because this helper drives the Store directly rather than running a node.
	outputRef := ""
	nodes := map[string]run.NodeState{"run": {Status: finished.Next.State}}
	if terminal == run.CommandSucceed {
		outputRef = "out-" + string(childID)
		nodes["run"] = run.NodeState{Status: finished.Next.State, OutputRef: outputRef}
	}
	finished.Next.Nodes = nodes
	if _, err := h.store.CommitNodeResult(ctx, store.CommitNodeResultCommand{
		Fence: store.ExecutionFence{
			RunID: childID, ExpectedRevision: afterStart.Revision, LeaseToken: lease.Token,
		},
		NodeName:  "run",
		OutputRef: outputRef,
		Commit:    store.CommitContext{Transition: finished, Events: finished.Events},
	}); err != nil {
		t.Fatalf("commit child terminal: %v", err)
	}
}

func finishChild(t *testing.T, h *harness, parentID run.ID, key string) {
	t.Helper()
	settleChild(t, h, parentID, key, run.CommandSucceed)
}

func failChild(t *testing.T, h *harness, parentID run.ID, key string) {
	t.Helper()
	settleChild(t, h, parentID, key, run.CommandFail)
}

// resume advances a parked parent, which is how it learns its children moved.
//
// The clock is advanced past the lease first. A parent parked on its children
// still holds the lease its last worker took, and it is the expiry that makes
// the Run claimable again — which is exactly how a parked Run is picked up in
// production, by a different worker after the first one moved on.
func resume(t *testing.T, h *harness, id run.ID) agentruntime.AdvanceResult {
	t.Helper()
	h.clock.Advance(time.Hour)
	result, err := h.runtime.Advance(t.Context(), id)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	return result
}

// The root synthesises from its children's outputs, not from their states alone.
//
// ChildResult carries an OutputRef and synthesis is the only consumer of it, so
// a root handed empty refs combines nothing while reporting success — the whole
// delegation produces an answer assembled from no answers.
func TestSynthesisReceivesEachChildsOutput(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research"), childSpec("survey")}
	synthesis := &countingSynthesis{}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children,
		withSynthesis(synthesis))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	finishChild(t, h, started.ID, "research")
	finishChild(t, h, started.ID, "survey")
	resume(t, h, started.ID)

	if len(synthesis.children) != 2 {
		t.Fatalf("synthesis saw %d children", len(synthesis.children))
	}
	for _, child := range synthesis.children {
		if child.OutputRef == "" {
			t.Errorf("%s reached synthesis with no output ref; the root would "+
				"combine nothing and still report success", child.Key)
		}
	}
}

// A failed child has no output, and that is not the same as a lost one.
func TestAFailedChildReachesSynthesisWithNoOutput(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research"), childSpec("survey")}
	synthesis := &countingSynthesis{}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children,
		withSynthesis(synthesis))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	finishChild(t, h, started.ID, "research")
	failChild(t, h, started.ID, "survey")
	resume(t, h, started.ID)

	byKey := map[string]workflow.ChildResult{}
	for _, child := range synthesis.children {
		byKey[child.Key] = child
	}
	if byKey["research"].OutputRef == "" {
		t.Error("the child that succeeded reached synthesis with no output")
	}
	if byKey["survey"].OutputRef != "" {
		t.Errorf("a failed child carried an output ref %q", byKey["survey"].OutputRef)
	}
}

// A child graph with outputs on more than one node has no declared answer.
//
// Picking one by map order would make the root synthesise from whichever the
// runtime happened to iterate first — a different answer on each takeover, and
// no way to tell which one a user was shown.
func TestAChildWithSeveralOutputsIsRefusedRatherThanPickedFrom(t *testing.T) {
	children := []workflow.ChildSpec{childSpec("research")}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children,
		withSynthesis(&countingSynthesis{}))
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	finishChild(t, h, started.ID, "research")

	// Give the finished child a second node that also produced something.
	child := h.store.Children(started.ID)[0]
	nodes := map[string]run.NodeState{}
	for name, node := range child.Nodes {
		nodes[name] = node
	}
	nodes["extra"] = run.NodeState{Status: run.StateSucceeded, OutputRef: "out-extra"}
	h.store.OverwriteNodes(child.ID, nodes)

	h.clock.Advance(time.Hour)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err == nil {
		t.Fatal("a root synthesized from a child with no declared answer")
	}
}

// A child executes on the task it was delegated.
//
// ChildSpec.Input was carried through the plan and dropped at the Store
// boundary, so every child ran on its Definition's prompt alone — the
// orchestrator's decomposition reached nothing that could act on it.
func TestAChildCarriesTheTaskItWasDelegated(t *testing.T) {
	spec := childSpec("research")
	spec.Input = json.RawMessage(`{"task":"survey the literature"}`)
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(spec)}, []workflow.ChildSpec{spec})
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	created := h.store.Children(started.ID)
	if len(created) != 1 {
		t.Fatalf("children=%d", len(created))
	}
	if string(created[0].Input) != string(spec.Input) {
		t.Fatalf("child input = %s, want %s", created[0].Input, spec.Input)
	}
}

// A dependent child is told where its upstream stored its output.
//
// Without this the second stage runs on a task that says "use the result of the
// first" with no result attached — and it answers anyway. The failure is
// invisible: both children succeed, the root synthesises, and only the content
// is wrong.
func TestADependentChildIsGivenItsUpstreamsOutput(t *testing.T) {
	first := childSpec("research")
	second := childSpec("survey", "research")
	children := []workflow.ChildSpec{first, second}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children)
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	finishChild(t, h, started.ID, "research")
	resume(t, h, started.ID)

	created := h.store.Children(started.ID)
	if len(created) != 2 {
		t.Fatalf("children=%d; the second generation never ran", len(created))
	}

	var dependent run.Snapshot
	for _, child := range created {
		if len(child.Upstreams) > 0 {
			dependent = child
		}
	}
	if dependent.ID == "" {
		t.Fatal("no child was told where its upstream stored anything")
	}
	if ref := dependent.Upstreams["research"]; ref == "" {
		t.Fatalf("upstreams=%v; the dependency it declared is missing", dependent.Upstreams)
	}
}

// A child sees what it declared a dependency on, and nothing else.
//
// The same rule the graph applies to node bindings: handing over the whole
// tree's refs would let a child read a sibling it never declared, which is a
// dependency nobody reviewed and an ordering nothing enforces.
func TestAChildSeesOnlyTheUpstreamsItDeclared(t *testing.T) {
	children := []workflow.ChildSpec{
		childSpec("research"), childSpec("interviews"), childSpec("survey", "research"),
	}
	h := orchestrator(t, &scriptedRouter{decision: delegatePlan(children...)}, children)
	started := start(t, h)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	finishChild(t, h, started.ID, "research")
	finishChild(t, h, started.ID, "interviews")
	resume(t, h, started.ID)

	for _, child := range h.store.Children(started.ID) {
		if _, leaked := child.Upstreams["interviews"]; leaked {
			t.Fatalf("a child was handed %q, which it never declared", "interviews")
		}
	}
}
