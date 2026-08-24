package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
)

// RouteKind is what an orchestrator decided to do.
//
// Three outcomes, declared as a closed set. "Delegate to nobody" and "answer
// directly" are different decisions with different audit meanings, and a router
// that returned an empty plan for both would make them indistinguishable.
type RouteKind string

const (
	// RouteDirect: the orchestrator answers itself.
	RouteDirect RouteKind = "direct"
	// RouteClarify: it cannot proceed without asking. This is a terminal answer
	// for this turn, not a failure — a Run that guessed instead would produce a
	// confidently wrong result.
	RouteClarify  RouteKind = "clarify"
	RouteDelegate RouteKind = "delegate"
)

// Candidate is a Definition the router may delegate to.
//
// Enabled and Authorized are separate: a Definition can exist, be switched off
// for the deployment, and also be one this principal could never use. Collapsing
// them would make "why was this not chosen" unanswerable.
type Candidate struct {
	Key        string
	Definition run.DefinitionRef
	Enabled    bool
	Authorized bool
}

// RouteRequest is what a router decides from.
type RouteRequest struct {
	Parent  run.Snapshot
	Routing definition.RoutingPolicy
	// Depth is how deep the parent already is. The root is zero.
	Depth      int
	Candidates []Candidate
	Input      json.RawMessage
}

// RouteDecision is the answer.
type RouteDecision struct {
	Kind RouteKind
	Plan DelegationPlan
	// Question is set for RouteClarify. It is the router's own text; the
	// Runtime does not compose it.
	Question string
}

// Router chooses between answering, asking, and delegating.
//
// It is a port, not an implementation: how an orchestrator picks its children
// is product judgement — an LLM, a rules table, a hard-coded pipeline — and the
// Runtime's job is to bound and record the decision, not to make it.
type Router interface {
	Route(ctx context.Context, request RouteRequest) (RouteDecision, error)
}

// ChildSpec is one delegated child.
type ChildSpec struct {
	// Key names the child within the plan. It is the handle dependencies and
	// synthesis refer to, so it must be stable and unique within one plan.
	Key        string
	Definition run.DefinitionRef
	Graph      run.ExecutionGraphRef
	DependsOn  []string
	// Budget is the slice requested from the root envelope.
	Budget run.Limits
	Input  json.RawMessage
}

// DelegationPlan is a whole child graph.
type DelegationPlan struct {
	Children []ChildSpec
}

// Validate refuses a plan before any child exists.
//
// Everything here is checked against the root and against the published routing
// policy, never against the parent's own remaining slice: slices are handed out
// in parallel, each looks affordable alone, and only the root sees the sum.
func (p DelegationPlan) Validate(routing definition.RoutingPolicy, depth int, candidates []Candidate, budget run.Budget) error {
	if len(p.Children) == 0 {
		return run.NewError("empty_plan", run.ErrorInvalid, run.RetryNever)
	}
	if routing.MaxDelegations > 0 && len(p.Children) > routing.MaxDelegations {
		return run.NewError("too_many_delegations", run.ErrorDenied, run.RetryNever,
			fmt.Errorf("%d children against a limit of %d", len(p.Children), routing.MaxDelegations))
	}
	if routing.MaxDepth > 0 && depth+1 > routing.MaxDepth {
		// Checked before creation, because a tree that discovers its depth limit
		// halfway down has already spent the budget getting there.
		return run.NewError("max_depth_exceeded", run.ErrorDenied, run.RetryNever,
			fmt.Errorf("depth %d against a limit of %d", depth+1, routing.MaxDepth))
	}

	usable := map[string]Candidate{}
	for _, candidate := range candidates {
		usable[candidate.Key] = candidate
	}

	keys := map[string]bool{}
	var total run.Limits
	for _, child := range p.Children {
		if child.Key == "" {
			return run.NewError("unnamed_child", run.ErrorInvalid, run.RetryNever)
		}
		if keys[child.Key] {
			return run.NewError("duplicate_child_key", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("child %q declared twice", child.Key))
		}
		keys[child.Key] = true

		candidate, known := usable[child.Key]
		switch {
		case !known:
			return run.NewError("unknown_candidate", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("child %q is not a candidate", child.Key))
		case !candidate.Enabled:
			return run.NewError("disabled_candidate", run.ErrorDenied, run.RetryNever,
				fmt.Errorf("child %q is disabled", child.Key))
		case !candidate.Authorized:
			// Authorized per candidate and at plan time, not once for the whole
			// orchestrator: delegating is how a Run reaches capability it was
			// not itself granted.
			return run.NewError("unauthorized_candidate", run.ErrorDenied, run.RetryNever,
				fmt.Errorf("child %q is not authorized", child.Key))
		}
		if candidate.Definition != child.Definition {
			return run.NewError("candidate_mismatch", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("child %q does not name the candidate's definition", child.Key))
		}
		if child.Graph.Digest == "" {
			return run.NewError("unpinned_child_graph", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("child %q", child.Key))
		}
		if child.Budget.LLMCalls < 0 || child.Budget.Tokens < 0 || child.Budget.ToolCalls < 0 {
			return run.NewError("negative_child_budget", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("child %q", child.Key))
		}
		total = total.Add(child.Budget)
	}

	for _, child := range p.Children {
		for _, dependency := range child.DependsOn {
			if !keys[dependency] {
				return run.NewError("unknown_child_dependency", run.ErrorInvalid, run.RetryNever,
					fmt.Errorf("child %q depends on undeclared %q", child.Key, dependency))
			}
		}
	}
	if err := p.checkAcyclic(); err != nil {
		return err
	}
	if routing.MaxConcurrency > 0 {
		if width := p.widestWave(); width > routing.MaxConcurrency {
			return run.NewError("max_concurrency_exceeded", run.ErrorDenied, run.RetryNever,
				fmt.Errorf("%d children would run at once against a limit of %d",
					width, routing.MaxConcurrency))
		}
	}
	if !budget.Affords(total) {
		return run.NewError("budget_exhausted", run.ErrorDenied, run.RetryNever,
			fmt.Errorf("the plan asks for %+v against the root envelope", total))
	}
	return nil
}

// checkAcyclic refuses a child graph that can never finish.
func (p DelegationPlan) checkAcyclic() error {
	remaining := map[string]int{}
	dependents := map[string][]string{}
	for _, child := range p.Children {
		remaining[child.Key] = len(child.DependsOn)
		for _, dependency := range child.DependsOn {
			dependents[dependency] = append(dependents[dependency], child.Key)
		}
	}

	var ready []string
	for key, count := range remaining {
		if count == 0 {
			ready = append(ready, key)
		}
	}
	settled := 0
	for len(ready) > 0 {
		key := ready[len(ready)-1]
		ready = ready[:len(ready)-1]
		settled++
		for _, dependent := range dependents[key] {
			remaining[dependent]--
			if remaining[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
	}
	if settled != len(p.Children) {
		var cyclic []string
		for key, count := range remaining {
			if count > 0 {
				cyclic = append(cyclic, key)
			}
		}
		sort.Strings(cyclic)
		return run.NewError("delegation_cycle", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("children %v can never become ready", cyclic))
	}
	return nil
}

// widestWave is the largest number of children that could run at once.
//
// Computed from the dependency structure rather than from how many the caller
// happens to start: a concurrency limit that only counted the first wave would
// be satisfied by a plan whose second wave is ten times wider.
func (p DelegationPlan) widestWave() int {
	depth := map[string]int{}
	var levelOf func(key string, seen map[string]bool) int
	byKey := map[string]ChildSpec{}
	for _, child := range p.Children {
		byKey[child.Key] = child
	}
	levelOf = func(key string, seen map[string]bool) int {
		if level, ok := depth[key]; ok {
			return level
		}
		if seen[key] {
			return 0
		}
		seen[key] = true
		level := 0
		for _, dependency := range byKey[key].DependsOn {
			if candidate := levelOf(dependency, seen) + 1; candidate > level {
				level = candidate
			}
		}
		depth[key] = level
		return level
	}

	counts := map[int]int{}
	widest := 0
	for _, child := range p.Children {
		level := levelOf(child.Key, map[string]bool{})
		counts[level]++
		if counts[level] > widest {
			widest = counts[level]
		}
	}
	return widest
}

// Generation returns the children that may be created now.
//
// Children are created a generation at a time because CreateChildren is
// all-or-nothing: a child whose dependency has not finished has nothing to read,
// and creating it early would leave a Run parked on an input that does not exist
// yet — indistinguishable, from the outside, from one that is simply slow.
func (p DelegationPlan) Generation(settled map[string]run.State) []ChildSpec {
	var ready []ChildSpec
	for _, child := range p.Children {
		if _, started := settled[child.Key]; started {
			continue
		}
		runnable := true
		for _, dependency := range child.DependsOn {
			if settled[dependency] != run.StateSucceeded {
				runnable = false
				break
			}
		}
		if runnable {
			ready = append(ready, child)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].Key < ready[j].Key })
	return ready
}

// Commands turns one generation into the typed create command's parts.
//
// All three are produced together and committed together: children without
// their links leave a tree that cannot be walked after a crash, and children
// without their reservations are spend nobody charged for.
// Commands turns one generation into the commands that create it.
//
// outputs is where every settled child's output was stored, by plan key. A
// child's own upstreams are selected from it rather than the whole map being
// handed over: a child can read what it declared a dependency on, and nothing
// else, which is the same rule the graph applies to node bindings.
// pins are the parent's. A child inherits them rather than being created
// against whatever the rules happen to be at the moment it is spawned: an
// empty PolicyDigest reaches Governance as "give me the current set", so a
// publish landing mid-flight would judge the children under rules the parent
// pinned itself against — the one thing pinning exists to prevent.
func (p DelegationPlan) Commands(rootID, parentID run.ID, principal authorization.PrincipalRef, pins run.Pins, generation []ChildSpec, outputs map[string]string, ids func(key string) run.ID) ([]store.CreateCommand, []store.LinkFact, []store.BudgetReservation) {
	children := make([]store.CreateCommand, 0, len(generation))
	links := make([]store.LinkFact, 0, len(generation))
	reservations := make([]store.BudgetReservation, 0, len(generation))

	for _, child := range generation {
		id := ids(child.Key)
		children = append(children, store.CreateCommand{
			ID:         id,
			Definition: child.Definition,
			Graph:      child.Graph,
			RootID:     rootID,
			ParentID:   parentID,
			// A child acts as the same principal as its root. Delegation
			// widens capability, never identity: a child running as somebody
			// else would be a privilege escalation the tree performs on itself.
			Principal: principal,
			// The rule-set versions come across unchanged. The trace does not:
			// the child keeps the tree's TraceID and its sampling decision, but
			// carries its own SpanID, which is the same convention the root is
			// created with. Reusing the parent's SpanID would make the two
			// indistinguishable once read back, and the parent link is already
			// expressed by parent_id.
			Pins: run.Pins{
				PolicyDigest: pins.PolicyDigest,
				QuotaDigest:  pins.QuotaDigest,
				Trace: run.TraceContext{
					TraceID: pins.Trace.TraceID,
					SpanID:  string(id),
					Sampled: pins.Trace.Sampled,
				},
			},
			Budget: run.Budget{Envelope: child.Budget},
			// The child's task. Dropped here, the child would execute with no
			// idea what it was delegated to do.
			Input:     child.Input,
			Upstreams: upstreamsFor(child, outputs),
		})
		links = append(links, store.LinkFact{
			ParentID: parentID, ChildID: id, NodeName: child.Key,
		})
		if !child.Budget.Zero() {
			reservations = append(reservations, store.BudgetReservation{
				ID: id, Amount: child.Budget,
			})
		}
	}
	return children, links, reservations
}

// ChildResult is one finished child, as synthesis sees it.
type ChildResult struct {
	Key   string
	State run.State
	// OutputRef points at the stored output. The root never holds child outputs
	// in state: they are unbounded and the Snapshot is written on every
	// transition.
	OutputRef string
}

// SynthesisRequest is what the root combines.
type SynthesisRequest struct {
	Root run.Snapshot
	// Children are in plan order, so two runs of the same tree synthesize the
	// same way. Ordering by completion time would make the root's answer depend
	// on which child happened to finish first.
	Children []ChildResult
}

// SynthesisStrategy produces the root's combined answer.
//
// A port, because combining results is product judgement. What the Runtime owns
// is that there is exactly one such answer per root and that it is committed
// once.
type SynthesisStrategy interface {
	Synthesize(ctx context.Context, request SynthesisRequest) (json.RawMessage, error)
}

// SynthesisOutcome is what a finished child set means for the root.
type SynthesisOutcome struct {
	Command  run.Command
	Complete bool
}

// Outcome classifies a finished child set.
//
// Partial is a real answer, not a rounded one: some children produced output
// and some did not, and a root that reported success would hide a missing
// branch while a root that reported failure would discard work that was done.
func Outcome(plan DelegationPlan, results []ChildResult) SynthesisOutcome {
	byKey := make(map[string]run.State, len(results))
	for _, result := range results {
		byKey[result.Key] = result.State
	}

	var succeeded, settled int
	for _, child := range plan.Children {
		state, known := byKey[child.Key]
		if !known {
			if reachableChild(plan, byKey, child) {
				return SynthesisOutcome{}
			}
			// Stranded behind a failed dependency: it will never run, and that
			// is settled rather than pending.
			settled++
			continue
		}
		if !state.Terminal() {
			return SynthesisOutcome{}
		}
		settled++
		if state == run.StateSucceeded {
			succeeded++
		}
	}
	if settled != len(plan.Children) {
		return SynthesisOutcome{}
	}

	switch {
	case succeeded == len(plan.Children):
		return SynthesisOutcome{Command: run.Command{Kind: run.CommandSucceed}, Complete: true}
	case succeeded == 0:
		return SynthesisOutcome{Command: run.Command{Kind: run.CommandFail}, Complete: true}
	default:
		return SynthesisOutcome{Command: run.Command{Kind: run.CommandPartial}, Complete: true}
	}
}

func reachableChild(plan DelegationPlan, settled map[string]run.State, child ChildSpec) bool {
	byKey := map[string]ChildSpec{}
	for _, candidate := range plan.Children {
		byKey[candidate.Key] = candidate
	}
	for _, dependency := range child.DependsOn {
		state, known := settled[dependency]
		if !known {
			if !reachableChild(plan, settled, byKey[dependency]) {
				return false
			}
			continue
		}
		if state == run.StateFailed || state == run.StateCancelled {
			return false
		}
	}
	return true
}

// upstreamsFor selects the refs one child may see.
//
// A dependency that settled without an output is left out rather than mapped to
// an empty string: "it produced nothing" and "it produced something I cannot
// find" are different facts, and a child handed an empty ref would fetch
// nothing and carry on as if it had.
func upstreamsFor(child ChildSpec, outputs map[string]string) map[string]string {
	if len(child.DependsOn) == 0 || len(outputs) == 0 {
		return nil
	}
	upstreams := make(map[string]string, len(child.DependsOn))
	for _, key := range child.DependsOn {
		if ref := outputs[key]; ref != "" {
			upstreams[key] = ref
		}
	}
	if len(upstreams) == 0 {
		return nil
	}
	return upstreams
}
