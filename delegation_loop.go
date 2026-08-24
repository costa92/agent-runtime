package agentruntime

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

// CandidateSource lists what an orchestrator may delegate to.
//
// The host answers, because which Definitions exist, which are switched on, and
// which this principal may use are all its facts. The Runtime's job is to bound
// and record the choice, not to make it.
type CandidateSource interface {
	Candidates(ctx context.Context, request workflow.RouteRequest) ([]workflow.Candidate, error)
}

// delegationState is the orchestrator's resume state.
//
// It lives in the Snapshot's Checkpoint, which the Runtime carries and does not
// interpret elsewhere. It has to be durable: a parent parks on its children and
// may be resumed hours later by a different worker in a different process,
// which has no memory of the plan it made.
type delegationState struct {
	Plan     workflow.DelegationPlan `json:"plan"`
	Children map[string]run.ID       `json:"children"`
}

// delegate routes an orchestrator and creates the next child generation.
//
// Returns true when the parent parked on children, which is the point the
// Advance loop stops: the parent has nothing further to do until something
// outside it moves.
func (s *session) delegate(ctx context.Context) (bool, error) {
	state, err := s.delegationState()
	if err != nil {
		return false, err
	}

	if len(state.Plan.Children) == 0 {
		decision, err := s.route(ctx)
		if err != nil {
			return false, err
		}
		if decision.Kind != workflow.RouteDelegate {
			// direct and clarify are both answers this Run gives itself. Only
			// delegation creates children.
			return false, nil
		}
		state.Plan = decision.Plan
		state.Children = map[string]run.ID{}
	}

	settled, outputs, err := s.childStates(ctx, state)
	if err != nil {
		return false, err
	}

	generation := state.Plan.Generation(settled)
	if len(generation) > 0 {
		if err := s.createChildren(ctx, state, generation, outputs); err != nil {
			return false, err
		}
		return true, nil
	}

	results := make([]workflow.ChildResult, 0, len(settled))
	for key, childState := range settled {
		results = append(results, workflow.ChildResult{
			Key: key, State: childState, OutputRef: outputs[key],
		})
	}
	outcome := workflow.Outcome(state.Plan, results)
	if !outcome.Complete {
		// Children are still running. The parent stays parked rather than
		// polling: whoever finishes last is what resumes it.
		return true, s.park(ctx, run.CommandWaitChildren)
	}
	return true, s.synthesize(ctx, state, results, outcome)
}

func (s *session) delegationState() (*delegationState, error) {
	state := &delegationState{Children: map[string]run.ID{}}
	envelope, err := decodeCheckpoint(s.snapshot.Checkpoint)
	if err != nil {
		return nil, err
	}
	if len(envelope.Plan) > 0 {
		if err := json.Unmarshal(envelope.Plan, &state.Plan); err != nil {
			return nil, run.NewError("unreadable_checkpoint", run.ErrorInternal, run.RetryNever, err)
		}
	}
	if len(envelope.Children) > 0 {
		if err := json.Unmarshal(envelope.Children, &state.Children); err != nil {
			return nil, run.NewError("unreadable_checkpoint", run.ErrorInternal, run.RetryNever, err)
		}
	}
	if state.Children == nil {
		state.Children = map[string]run.ID{}
	}
	return state, nil
}

// route asks the host's Router and validates whatever it returns.
//
// Validation is the Runtime's, not the router's. A router is product code and
// may be an LLM; the limits it is bounded by are published data, and checking
// them here is what makes them limits rather than suggestions.
func (s *session) route(ctx context.Context) (workflow.RouteDecision, error) {
	if s.runtime.deps.Router == nil {
		return workflow.RouteDecision{Kind: workflow.RouteDirect}, nil
	}

	request := workflow.RouteRequest{
		Parent:  s.snapshot,
		Routing: s.declared.Routing,
		Depth:   s.depth(),
	}
	if s.runtime.deps.Candidates != nil {
		candidates, err := s.runtime.deps.Candidates.Candidates(ctx, request)
		if err != nil {
			return workflow.RouteDecision{}, err
		}
		request.Candidates = candidates
	}

	decision, err := s.runtime.deps.Router.Route(ctx, request)
	if err != nil {
		return workflow.RouteDecision{}, err
	}
	s.runtime.record(observe.Decision{
		Name: observe.EventRouterPlanSelected, RunID: s.snapshot.ID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrPlan, string(decision.Kind)),
			observe.Attr(observe.AttrAgent, s.declared.Implementation),
		},
	})

	if decision.Kind == workflow.RouteDelegate {
		if err := decision.Plan.Validate(
			s.declared.Routing, s.depth(), request.Candidates, s.snapshot.Budget,
		); err != nil {
			return workflow.RouteDecision{}, err
		}
	}
	return decision, nil
}

// depth is how deep this Run already sits. A root is zero.
func (s *session) depth() int {
	if s.snapshot.RootID == "" {
		return 0
	}
	// The tree's shape is durable in the Links, but depth is only needed as a
	// bound, and the parent's own presence is what a child's depth is measured
	// from. One level is what this Run can establish without walking the tree
	// on every route decision.
	return 1
}

// childStates reads every created child's current state.
func (s *session) childStates(
	ctx context.Context, state *delegationState,
) (map[string]run.State, map[string]string, error) {
	settled := make(map[string]run.State, len(state.Children))
	outputs := make(map[string]string, len(state.Children))
	for key, id := range state.Children {
		child, err := s.runtime.deps.Store.Get(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		settled[key] = child.State
		ref, err := childOutputRef(child)
		if err != nil {
			return nil, nil, err
		}
		outputs[key] = ref
	}
	return settled, outputs, nil
}

// childOutputRef is what a finished child contributes to its parent.
//
// A Run does not record "its" output — nodes do — so the child's answer is the
// ref of the node that produced one. Exactly one node may have produced it: a
// child graph with several outputs has no declared answer, and picking one by
// map order would make the root synthesise from whichever the runtime happened
// to iterate first, differently on each takeover.
//
// Refusing is deliberately louder than returning nothing. An empty ref reaches
// synthesis as a child that ran and produced nothing, and the root then answers
// from an empty set without anything reporting a problem.
func childOutputRef(child run.Snapshot) (string, error) {
	found := ""
	for name, node := range child.Nodes {
		if node.OutputRef == "" {
			continue
		}
		if found != "" {
			return "", run.NewError("ambiguous_child_output", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("child %s has outputs on more than one node (%s and %s) "+
					"and no declared answer", child.ID, found, name))
		}
		found = node.OutputRef
	}
	return found, nil
}

// createChildren commits one whole generation, its links and its reservations
// in a single typed command.
func (s *session) createChildren(
	ctx context.Context, state *delegationState,
	generation []workflow.ChildSpec, outputs map[string]string,
) error {
	rootID := s.snapshot.RootID
	if rootID == "" {
		rootID = s.snapshot.ID
	}

	children, links, reservations := state.Plan.Commands(
		rootID, s.snapshot.ID, s.snapshot.Principal, s.snapshot.Pins, generation, outputs,
		func(key string) run.ID { return s.runtime.deps.IDs.NewID("child") },
	)
	for i, child := range children {
		state.Children[generation[i].Key] = child.ID
	}

	var slices map[run.ID]run.Limits
	for _, reservation := range reservations {
		if slices == nil {
			slices = map[run.ID]run.Limits{}
		}
		slices[reservation.ID] = reservation.Amount
	}

	// A parent creating a later generation is parked on the one before it, so
	// it resumes first: granting children is something only a running Run does,
	// and skipping the resume would leave the state machine describing a Run
	// that acted while waiting.
	from := s.snapshot
	var events []run.Event
	if from.State.Waiting() {
		resumed, err := run.Reduce(from, run.Command{Kind: run.CommandResume})
		if err != nil {
			return err
		}
		from = resumed.Next
		events = resumed.Events
	}

	// The grant is checked against the root envelope by the reducer, then the
	// parent parks. Every transition is folded into the one command so the
	// children, their links, their reservations and the parent's new state
	// cannot land separately.
	granted, err := run.Reduce(from, run.Command{Kind: run.CommandCreateChildren, Slices: slices})
	if err != nil {
		return err
	}
	granted.Events = append(events, granted.Events...)
	parked, err := run.Reduce(granted.Next, run.Command{Kind: run.CommandWaitChildren})
	if err != nil {
		return err
	}
	parked.Events = append(granted.Events, parked.Events...)

	encoded, err := encodeDelegationState(state)
	if err != nil {
		return err
	}
	parked.Next.Checkpoint = encoded

	// Sealed first, like every other commit: a commit and a takeover must not
	// both believe they own the Run.
	if _, err := s.runtime.deps.Store.SealForCommit(ctx, store.SealCommand{
		RunID: s.snapshot.ID, LeaseToken: s.lease.Token,
	}); err != nil {
		return err
	}
	committed, err := s.runtime.deps.Store.CreateChildren(ctx, store.CreateChildrenCommand{
		Fence:        s.fence(),
		Children:     children,
		Links:        links,
		Reservations: reservations,
		Commit:       store.CommitContext{Transition: parked, Events: parked.Events},
	})
	if err != nil {
		return err
	}
	s.snapshot = committed
	return nil
}

// park moves the parent into a waiting state.
func (s *session) park(ctx context.Context, kind run.CommandKind) error {
	if s.snapshot.State.Waiting() {
		return nil
	}
	transition, err := run.Reduce(s.snapshot, run.Command{Kind: kind})
	if err != nil {
		return err
	}
	committed, err := s.commitNode(ctx, transition, "", "", run.Limits{}, nil)
	if err != nil {
		return err
	}
	s.snapshot = committed
	return nil
}

// synthesize commits the root's single combined answer.
//
// The synthesis Invocation ID is derived from the Run rather than generated, so
// a takeover that reaches this point produces the same identity and the Store's
// at-most-once rule recognises it. A generated ID would make a second synthesis
// look like a first one.
func (s *session) synthesize(ctx context.Context, state *delegationState, results []workflow.ChildResult, outcome workflow.SynthesisOutcome) error {
	ordered := make([]workflow.ChildResult, 0, len(state.Plan.Children))
	byKey := make(map[string]workflow.ChildResult, len(results))
	for _, result := range results {
		byKey[result.Key] = result
	}
	for _, child := range state.Plan.Children {
		if result, ok := byKey[child.Key]; ok {
			ordered = append(ordered, result)
		}
	}

	outputRef := "synthesis-" + string(s.snapshot.ID)
	if s.runtime.deps.Synthesis != nil {
		if _, err := s.runtime.deps.Synthesis.Synthesize(ctx, workflow.SynthesisRequest{
			Root: s.snapshot, Children: ordered,
		}); err != nil {
			return err
		}
	}

	transition, err := run.Reduce(s.snapshot, outcome.Command)
	if err != nil {
		return err
	}

	if _, err := s.runtime.deps.Store.SealForCommit(ctx, store.SealCommand{
		RunID: s.snapshot.ID, LeaseToken: s.lease.Token,
	}); err != nil {
		return err
	}
	committed, err := s.runtime.deps.Store.CommitSynthesis(ctx, store.CommitSynthesisCommand{
		Fence:     s.fence(),
		OutputRef: outputRef,
		Commit:    store.CommitContext{Transition: transition, Events: transition.Events},
	})
	if err != nil {
		return err
	}
	s.snapshot = committed
	return nil
}

// orchestrating reports whether this Run delegates rather than doing the work.
//
// Declared, not inferred from whether a plan exists: "it had no children this
// time" is not the same fact as "it is not allowed to have any".
func (s *session) orchestrating() bool {
	return s.declared.Mode == definition.ModeOrchestrator
}

// encodeDelegationState writes the plan and the children it produced into the
// versioned checkpoint envelope.
func encodeDelegationState(state *delegationState) (json.RawMessage, error) {
	plan, err := json.Marshal(state.Plan)
	if err != nil {
		return nil, run.NewError("unencodable_checkpoint", run.ErrorInternal, run.RetryNever, err)
	}
	children, err := json.Marshal(state.Children)
	if err != nil {
		return nil, run.NewError("unencodable_checkpoint", run.ErrorInternal, run.RetryNever, err)
	}
	return encodeCheckpoint(checkpoint{Plan: plan, Children: children})
}
