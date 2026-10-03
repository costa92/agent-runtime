// Package replay checks a Run's recorded history against the current reducer.
//
// What this is for: every Run that has ever executed left a contiguous sequence
// of state changes behind, and each one is a walk the state machine allowed at
// the time it happened. Fed back through Reduce, they become the regression
// suite nobody wrote — a commit that narrows a transition is caught by the
// production history that used it, rather than by whoever remembers that path
// exists.
//
// What this is not: a durable-execution replay. Engines that replay
// deterministically journal their inputs; this Runtime's event record holds its
// decisions. budget_reserved does not name the invocation it charged for and
// invocation_resolved does not carry the resolver's answer, so the command
// stream cannot be reconstructed from events and re-executing one is not on
// offer. Verify checks the trajectory, which is the part the record does
// support, and it does so without re-issuing a single effect.
//
// The legal successors are probed out of Reduce rather than tabulated here. A
// table would be a second statement of the machine, and the failure it would
// produce on the day it drifted is "the table is wrong", which is the opposite
// of the thing this package exists to detect.
package replay

import (
	"fmt"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// probeInvocation is the invocation id the successor probe installs, so that
// the commands which require one (record_unknown, resolve_invocation) are
// exercised rather than refused for a reason unrelated to the state.
const (
	probeInvocation run.ID = "replay-probe"
	probeInFlight   run.ID = "replay-in-flight"
)

// Result is what a verified history amounts to.
type Result struct {
	// From is the state the history starts in, taken from the first recorded
	// transition rather than assumed to be queued: retention prunes old events,
	// so the earliest event on hand is often not a Run's first.
	From run.State
	// Final is the state the walk ends in.
	Final run.State
	// Transitions counts the state changes walked, not the events read.
	Transitions int
	// Events counts every event read, including those that record no
	// transition.
	Events int
}

// ReplayStepResult captures the deterministic state snapshot resulting from applying a single command.
type ReplayStepResult struct {
	StepIndex  int
	Command    run.Command
	Snapshot   run.Snapshot
	Transition run.Transition
}

// ReplayCommands deterministically steps through a sequence of commands from an initial Snapshot.
func ReplayCommands(initial run.Snapshot, commands []run.Command) ([]ReplayStepResult, error) {
	current := initial
	history := make([]ReplayStepResult, 0, len(commands))

	for i, cmd := range commands {
		transition, err := run.Reduce(current, cmd)
		if err != nil {
			return history, fmt.Errorf("replay error at step %d (%s): %w", i, cmd.Kind, err)
		}
		history = append(history, ReplayStepResult{
			StepIndex:  i,
			Command:    cmd,
			Snapshot:   transition.Next,
			Transition: transition,
		})
		current = transition.Next
	}
	return history, nil
}

// Verify walks one Run's events and reports the trajectory they describe.
//
// Events must be in sequence order. Two properties are checked: the sequence is
// contiguous — a gap means a consumer keyed on sequence silently missed
// something, which is the whole reason the numbers are contiguous — and every
// recorded transition is one the current reducer can still produce from the
// state the Run was actually in.
func Verify(events []run.Event) (Result, error) {
	var result Result
	result.Events = len(events)

	current := run.State("")
	previousSequence := uint64(0)

	for index, event := range events {
		if index > 0 && event.Sequence != previousSequence+1 {
			return result, fmt.Errorf("replay: sequence gap after %d: next event is %d; "+
				"a Run's events are contiguous, so a gap is a lost event and not a sparse log",
				previousSequence, event.Sequence)
		}
		previousSequence = event.Sequence

		if event.Kind != run.EventStateChanged {
			continue
		}

		if current == "" {
			current = event.From
			result.From = event.From
		}
		if event.From != current {
			return result, fmt.Errorf("replay: event %d starts from %q but the Run was in %q",
				event.Sequence, event.From, current)
		}
		if !reachable(current, event.To) {
			return result, fmt.Errorf("replay: event %d records %q → %q, which the reducer no longer "+
				"produces; this history executed, so either the transition was removed by mistake or "+
				"the removal needs a migration for the Runs that took it",
				event.Sequence, event.From, event.To)
		}

		current = event.To
		result.Transitions++
	}

	result.Final = current
	return result, nil
}

// reachable reports whether some command makes the machine record this
// transition, by asking the reducer rather than by consulting a list.
//
// The test is that a command emits a matching state event, not merely that it
// leaves the snapshot in that state. Most commands do not change state at all —
// reserving budget, resolving an invocation the same way twice — and reading
// the resulting state would make every state its own legal successor. The
// recorded events being checked are exactly the ones the reducer emits, so
// comparing against those is both stricter and the more direct statement of
// the property.
func reachable(from, to run.State) bool {
	for _, command := range probeCommands() {
		transition, err := run.Reduce(probeSnapshot(from), command)
		if err != nil {
			continue
		}
		for _, event := range transition.Events {
			if event.Kind == run.EventStateChanged && event.From == from && event.To == to {
				return true
			}
		}
	}
	return false
}

// probeSnapshot is a Run in the given state with every precondition a command
// might need already satisfied, so that a refusal means the state forbids the
// command and never that the fixture was short a field.
func probeSnapshot(state run.State) run.Snapshot {
	return run.Snapshot{
		ID:    "replay-probe",
		State: state,
		// A zero component is unlimited, so the zero Budget affords everything
		// and no probe is refused for a reason the trajectory does not describe.
		Invocations: map[run.ID]run.Invocation{
			probeInvocation: {ID: probeInvocation, Outcome: run.OutcomeUnknown},
			probeInFlight:   {ID: probeInFlight, Outcome: run.OutcomeInFlight},
		},
	}
}

// probeCommands is one command per kind, filled so each is accepted wherever
// its state allows it. A test in this package parses the CommandKind constants
// and fails when one is missing here: a kind left out is a transition the probe
// silently calls illegal.
func probeCommands() []run.Command {
	return []run.Command{
		{Kind: run.CommandStart},
		{Kind: run.CommandResume},
		{Kind: run.CommandWaitApproval},
		{Kind: run.CommandAdvanceNodes, Nodes: map[string]run.NodeState{"probe": {Status: run.StateSucceeded}}},
		{Kind: run.CommandSettleInvocation, InvocationID: probeInvocation, Outcome: run.OutcomeApplied},
		{Kind: run.CommandInvokeModel, InvocationID: probeInvocation},
		{Kind: run.CommandInvokeTool, InvocationID: probeInvocation},
		{Kind: run.CommandWriteMemory, InvocationID: probeInvocation},
		{Kind: run.CommandRecordUnknown, InvocationID: probeInFlight},
		{Kind: run.CommandResolveInvocation, InvocationID: probeInvocation, Outcome: run.OutcomeApplied},
		{Kind: run.CommandSucceed},
		{Kind: run.CommandPartial},
		{Kind: run.CommandFail},
		{Kind: run.CommandCancel},
		{Kind: run.CommandFenceCancellation, CancelEpoch: 1},
	}
}
