package workflow_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

func childRef(name string) run.DefinitionRef {
	return run.DefinitionRef{ID: name, Version: 1, Protocol: 1}
}

func child(key string, dependsOn ...string) workflow.ChildSpec {
	return workflow.ChildSpec{
		Key:        key,
		Definition: childRef(key),
		Graph:      run.ExecutionGraphRef{ID: key, Version: 1, Digest: "digest-" + key},
		DependsOn:  dependsOn,
		Budget:     run.Limits{LLMCalls: 1, Tokens: 100, ToolCalls: 1},
	}
}

func candidatesFor(plan workflow.DelegationPlan) []workflow.Candidate {
	candidates := make([]workflow.Candidate, 0, len(plan.Children))
	for _, spec := range plan.Children {
		candidates = append(candidates, workflow.Candidate{
			Key: spec.Key, Definition: spec.Definition, Enabled: true, Authorized: true,
		})
	}
	return candidates
}

func rootBudget() run.Budget {
	return run.Budget{Envelope: run.Limits{LLMCalls: 100, Tokens: 100_000, ToolCalls: 100}}
}

func researchPlan() workflow.DelegationPlan {
	return workflow.DelegationPlan{Children: []workflow.ChildSpec{
		child("research"),
		child("write", "research"),
	}}
}

func validate(t *testing.T, plan workflow.DelegationPlan, mutate ...func(*definition.RoutingPolicy)) error {
	t.Helper()
	routing := definition.RoutingPolicy{}
	for _, apply := range mutate {
		apply(&routing)
	}
	return plan.Validate(routing, 0, candidatesFor(plan), rootBudget())
}

func validationCode(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		t.Fatal("the plan validated, want a refusal")
	}
	return codeOf(t, err)
}

func TestAValidPlanIsAccepted(t *testing.T) {
	if err := validate(t, researchPlan()); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

// The three route kinds are distinct decisions with distinct audit meanings.
// "Delegate to nobody" and "answer directly" must not look the same.
func TestRouteKindsAreDistinct(t *testing.T) {
	kinds := map[workflow.RouteKind]bool{
		workflow.RouteDirect: true, workflow.RouteClarify: true, workflow.RouteDelegate: true,
	}
	if len(kinds) != 3 {
		t.Fatal("the route kinds are not distinct")
	}
}

// A Definition can exist, be switched off, and be one this principal may never
// use. Collapsing those makes "why was this not chosen" unanswerable.
func TestUnknownDisabledAndUnauthorizedCandidatesAreDistinguished(t *testing.T) {
	plan := workflow.DelegationPlan{Children: []workflow.ChildSpec{child("research")}}

	unknown := plan.Validate(definition.RoutingPolicy{}, 0, nil, rootBudget())
	if got := validationCode(t, unknown); got != "unknown_candidate" {
		t.Errorf("code=%q want=unknown_candidate", got)
	}

	disabled := plan.Validate(definition.RoutingPolicy{}, 0, []workflow.Candidate{{
		Key: "research", Definition: childRef("research"), Enabled: false, Authorized: true,
	}}, rootBudget())
	if got := validationCode(t, disabled); got != "disabled_candidate" {
		t.Errorf("code=%q want=disabled_candidate", got)
	}

	unauthorized := plan.Validate(definition.RoutingPolicy{}, 0, []workflow.Candidate{{
		Key: "research", Definition: childRef("research"), Enabled: true, Authorized: false,
	}}, rootBudget())
	if got := validationCode(t, unauthorized); got != "unauthorized_candidate" {
		t.Errorf("code=%q want=unauthorized_candidate", got)
	}
}

// A plan naming a candidate but pointing at a different version would delegate
// to something nobody authorized.
func TestAPlanCannotSubstituteADifferentDefinition(t *testing.T) {
	plan := workflow.DelegationPlan{Children: []workflow.ChildSpec{child("research")}}
	plan.Children[0].Definition = run.DefinitionRef{ID: "research", Version: 9, Protocol: 1}

	err := plan.Validate(definition.RoutingPolicy{}, 0, []workflow.Candidate{{
		Key: "research", Definition: childRef("research"), Enabled: true, Authorized: true,
	}}, rootBudget())
	if got := validationCode(t, err); got != "candidate_mismatch" {
		t.Fatalf("code=%q", got)
	}
}

func TestRoutingLimitsAreEnforcedBeforeAnyChildExists(t *testing.T) {
	wide := workflow.DelegationPlan{Children: []workflow.ChildSpec{
		child("research"), child("write"), child("review"),
	}}

	delegations := validate(t, wide, func(r *definition.RoutingPolicy) { r.MaxDelegations = 2 })
	if got := validationCode(t, delegations); got != "too_many_delegations" {
		t.Errorf("code=%q want=too_many_delegations", got)
	}

	// All three are dependency-free, so all three would run at once.
	concurrency := validate(t, wide, func(r *definition.RoutingPolicy) { r.MaxConcurrency = 2 })
	if got := validationCode(t, concurrency); got != "max_concurrency_exceeded" {
		t.Errorf("code=%q want=max_concurrency_exceeded", got)
	}

	depth := researchPlan().Validate(
		definition.RoutingPolicy{MaxDepth: 1}, 1, candidatesFor(researchPlan()), rootBudget(),
	)
	if got := validationCode(t, depth); got != "max_depth_exceeded" {
		t.Errorf("code=%q want=max_depth_exceeded", got)
	}
}

// A concurrency limit that only counted the first wave would be satisfied by a
// plan whose second wave is ten times wider.
func TestConcurrencyCountsTheWidestWaveNotTheFirst(t *testing.T) {
	plan := workflow.DelegationPlan{Children: []workflow.ChildSpec{
		child("a"),
		child("b", "a"), child("c", "a"), child("d", "a"),
	}}

	err := validate(t, plan, func(r *definition.RoutingPolicy) { r.MaxConcurrency = 2 })
	if got := validationCode(t, err); got != "max_concurrency_exceeded" {
		t.Fatalf("code=%q; the second wave is three wide", got)
	}
}

func TestCyclesAndUnknownDependenciesAreRefused(t *testing.T) {
	cyclic := workflow.DelegationPlan{Children: []workflow.ChildSpec{
		child("a", "b"), child("b", "a"),
	}}
	if got := validationCode(t, validate(t, cyclic)); got != "delegation_cycle" {
		t.Errorf("code=%q want=delegation_cycle", got)
	}

	dangling := workflow.DelegationPlan{Children: []workflow.ChildSpec{child("a", "ghost")}}
	if got := validationCode(t, validate(t, dangling)); got != "unknown_child_dependency" {
		t.Errorf("code=%q want=unknown_child_dependency", got)
	}
}

func TestDuplicateAndUnnamedChildrenAreRefused(t *testing.T) {
	duplicate := workflow.DelegationPlan{Children: []workflow.ChildSpec{child("a"), child("a")}}
	if got := validationCode(t, validate(t, duplicate)); got != "duplicate_child_key" {
		t.Errorf("code=%q", got)
	}

	unnamed := workflow.DelegationPlan{Children: []workflow.ChildSpec{{}}}
	if got := validationCode(t, validate(t, unnamed)); got != "unnamed_child" {
		t.Errorf("code=%q", got)
	}
}

// A child with no pinned graph could come back a different shape after a crash,
// which is exactly what pinning exists to prevent — and a child is the hardest
// place to notice it.
func TestAnUnpinnedChildGraphIsRefused(t *testing.T) {
	plan := workflow.DelegationPlan{Children: []workflow.ChildSpec{child("research")}}
	plan.Children[0].Graph.Digest = ""

	if got := validationCode(t, validate(t, plan)); got != "unpinned_child_graph" {
		t.Fatalf("code=%q", got)
	}
}

// Slices are granted in parallel and each looks affordable alone. Only the root
// sees the sum, which is why the check is against the root envelope and never
// against a parent's remaining share.
func TestFiftyConcurrentChildrenAreCheckedAgainstTheRootEnvelope(t *testing.T) {
	var children []workflow.ChildSpec
	for i := range 50 {
		spec := child(string(rune('a'+i%26)) + itoa(i))
		spec.Budget = run.Limits{LLMCalls: 1, Tokens: 1_000, ToolCalls: 1}
		children = append(children, spec)
	}
	plan := workflow.DelegationPlan{Children: children}

	// 50 × 1,000 tokens against a 40,000-token envelope: each child is trivially
	// affordable and the plan is not.
	tight := run.Budget{Envelope: run.Limits{LLMCalls: 100, Tokens: 40_000, ToolCalls: 100}}
	err := plan.Validate(definition.RoutingPolicy{}, 0, candidatesFor(plan), tight)
	if got := validationCode(t, err); got != "budget_exhausted" {
		t.Fatalf("code=%q; the sum is what overruns", got)
	}

	roomy := run.Budget{Envelope: run.Limits{LLMCalls: 100, Tokens: 60_000, ToolCalls: 100}}
	if err := plan.Validate(definition.RoutingPolicy{}, 0, candidatesFor(plan), roomy); err != nil {
		t.Fatalf("a plan that fits was refused: %v", err)
	}
}

// A child created before its dependency finished has nothing to read, and from
// the outside it is indistinguishable from one that is merely slow.
func TestChildrenAreCreatedOneGenerationAtATime(t *testing.T) {
	plan := researchPlan()

	first := plan.Generation(nil)
	if len(first) != 1 || first[0].Key != "research" {
		t.Fatalf("generation=%+v want=[research]", keysOf(first))
	}

	blocked := plan.Generation(map[string]run.State{"research": run.StateRunning})
	if len(blocked) != 0 {
		t.Fatalf("generation=%v; the dependency has not succeeded", keysOf(blocked))
	}

	second := plan.Generation(map[string]run.State{"research": run.StateSucceeded})
	if len(second) != 1 || second[0].Key != "write" {
		t.Fatalf("generation=%v want=[write]", keysOf(second))
	}
}

func TestAFailedDependencyNeverUnblocksItsDependents(t *testing.T) {
	plan := researchPlan()

	generation := plan.Generation(map[string]run.State{"research": run.StateFailed})
	if len(generation) != 0 {
		t.Fatalf("generation=%v; a failed dependency produced no output", keysOf(generation))
	}
}

// Children, links and reservations are produced together because they are
// committed together: children without links leave a tree nothing can walk
// after a crash, and children without reservations are spend nobody charged for.
func TestCommandsProduceChildrenLinksAndReservationsTogether(t *testing.T) {
	plan := researchPlan()
	generation := plan.Generation(nil)

	children, links, reservations := plan.Commands("root-1", "root-1", testPrincipal(), generation, nil,
		func(key string) run.ID { return run.ID("child-" + key) })

	if len(children) != 1 || len(links) != 1 || len(reservations) != 1 {
		t.Fatalf("children=%d links=%d reservations=%d", len(children), len(links), len(reservations))
	}
	if children[0].RootID != "root-1" || children[0].ParentID != "root-1" {
		t.Errorf("child is not linked to its root: %+v", children[0])
	}
	if links[0].NodeName != "research" {
		t.Errorf("the link does not name the plan key: %+v", links[0])
	}
	if reservations[0].ID != children[0].ID {
		t.Errorf("the reservation is not keyed to the child it pays for")
	}
	if err := children[0].Validate(); err != nil {
		t.Errorf("the generated command does not validate: %v", err)
	}
}

// All-or-nothing. A half-created generation leaves the parent waiting on
// children that do not exist, and nothing in the tree can tell that apart from
// children that have not started.
func TestDelegationCreatesWholeGraphAtomically(t *testing.T) {
	clock := testkit.NewClock()
	memory := testkit.NewMemoryStore(clock, testkit.FailChildCreateAt(1))

	root, err := memory.Create(context.Background(), store.CreateCommand{
		ID:         "root-1",
		Definition: childRef("orchestrator"),
		Graph:      run.ExecutionGraphRef{ID: "orchestrator", Version: 1, Digest: "digest-root"},
		Principal:  testPrincipal(),
		Budget:     rootBudget(),
	})
	if err != nil {
		t.Fatalf("create root: %v", err)
	}
	lease, snapshot, err := memory.Claim(context.Background(), store.ClaimCommand{
		RunID: root.ID, Owner: "worker-1", LeaseFor: 30_000_000_000,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	started, err := run.Reduce(snapshot, run.Command{Kind: run.CommandStart})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	plan := workflow.DelegationPlan{Children: []workflow.ChildSpec{child("research"), child("survey")}}
	children, links, reservations := plan.Commands(root.ID, root.ID, testPrincipal(), plan.Generation(nil), nil,
		func(key string) run.ID { return run.ID("child-" + key) })

	_, err = memory.CreateChildren(context.Background(), store.CreateChildrenCommand{
		Fence: store.ExecutionFence{
			RunID: root.ID, ExpectedRevision: snapshot.Revision, LeaseToken: lease.Token,
		},
		Children:     children,
		Links:        links,
		Reservations: reservations,
		Commit:       store.CommitContext{Transition: started, Events: started.Events},
	})
	if err == nil {
		t.Fatal("the injected child failure did not surface")
	}
	if created := memory.Children(root.ID); len(created) != 0 {
		t.Fatalf("children=%d; a partial generation survived the failure", len(created))
	}
}

// Some children produced output and some did not. Reporting success hides a
// missing branch; reporting failure discards work that was done.
func TestOutcomeReportsPartialWhenSomeChildrenSucceeded(t *testing.T) {
	plan := workflow.DelegationPlan{Children: []workflow.ChildSpec{child("a"), child("b")}}

	outcome := workflow.Outcome(plan, []workflow.ChildResult{
		{Key: "a", State: run.StateSucceeded},
		{Key: "b", State: run.StateFailed},
	})
	if !outcome.Complete || outcome.Command.Kind != run.CommandPartial {
		t.Fatalf("outcome=%+v want=partial", outcome)
	}
}

func TestOutcomeWaitsWhileAnyChildIsStillRunning(t *testing.T) {
	plan := workflow.DelegationPlan{Children: []workflow.ChildSpec{child("a"), child("b")}}

	outcome := workflow.Outcome(plan, []workflow.ChildResult{
		{Key: "a", State: run.StateSucceeded},
		{Key: "b", State: run.StateRunning},
	})
	if outcome.Complete {
		t.Fatalf("outcome=%+v; one child is still running", outcome)
	}
}

func TestOutcomeSucceedsAndFailsAtTheExtremes(t *testing.T) {
	plan := workflow.DelegationPlan{Children: []workflow.ChildSpec{child("a"), child("b")}}

	all := workflow.Outcome(plan, []workflow.ChildResult{
		{Key: "a", State: run.StateSucceeded}, {Key: "b", State: run.StateSucceeded},
	})
	if all.Command.Kind != run.CommandSucceed {
		t.Errorf("command=%s want=succeed", all.Command.Kind)
	}

	none := workflow.Outcome(plan, []workflow.ChildResult{
		{Key: "a", State: run.StateFailed}, {Key: "b", State: run.StateCancelled},
	})
	if none.Command.Kind != run.CommandFail {
		t.Errorf("command=%s want=fail", none.Command.Kind)
	}
}

// Children stranded behind a failure will never run. Counting them as
// outstanding would park the root forever on work nothing can schedule.
func TestChildrenStrandedBehindAFailureDoNotStallTheRoot(t *testing.T) {
	plan := workflow.DelegationPlan{Children: []workflow.ChildSpec{
		child("a"), child("b", "a"),
	}}

	outcome := workflow.Outcome(plan, []workflow.ChildResult{{Key: "a", State: run.StateFailed}})
	if !outcome.Complete || outcome.Command.Kind != run.CommandFail {
		t.Fatalf("outcome=%+v; b can never run", outcome)
	}
}

// Two runs of the same tree must synthesize the same way. Ordering by
// completion would make the root's answer depend on which child finished first.
func TestOutcomeIsIndependentOfCompletionOrder(t *testing.T) {
	plan := workflow.DelegationPlan{Children: []workflow.ChildSpec{child("a"), child("b")}}
	forward := []workflow.ChildResult{
		{Key: "a", State: run.StateSucceeded}, {Key: "b", State: run.StateFailed},
	}
	reversed := []workflow.ChildResult{forward[1], forward[0]}

	first, second := workflow.Outcome(plan, forward), workflow.Outcome(plan, reversed)
	if first.Complete != second.Complete || first.Command.Kind != second.Command.Kind {
		t.Fatalf("the root's answer depends on which child finished first: %+v vs %+v", first, second)
	}
}

func keysOf(specs []workflow.ChildSpec) []string {
	keys := make([]string, 0, len(specs))
	for _, spec := range specs {
		keys = append(keys, spec.Key)
	}
	return keys
}

func itoa(n int) string { return strconv.Itoa(n) }

func testPrincipal() authorization.PrincipalRef {
	return authorization.PrincipalRef{
		Subject: "alice", Tenant: "acme", Kind: authorization.PrincipalUser,
	}
}

func codeOf(t *testing.T, err error) string {
	t.Helper()
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) {
		t.Fatalf("not a run.Error: %v", err)
	}
	return runtimeError.Code
}
