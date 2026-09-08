package run

// State is the Run lifecycle. One aggregate and one state machine serve every
// execution shape — single agent, DAG — because a second execution path is a
// second set of invariants, and the two drift.
//
//	queued  → running
//	running → waiting_approval | waiting_resolution
//	waiting_* → running
//	queued | running | waiting_* → succeeded | partial | failed | cancelled
//
// Terminal states are irreversible.
type State string

const (
	StateQueued  State = "queued"
	StateRunning State = "running"

	// StateWaitingApproval: a governed effect needs a human decision.
	StateWaitingApproval State = "waiting_approval"
	// StateWaitingResolution: an Invocation's outcome is unknown — the side
	// effect may have happened. Only a resolution leaves this state, because a
	// retry from here could duplicate the effect. That is also why the Runtime
	// has no waiting_retry: a failure it can establish is safe to repeat is
	// retried within the node, and one it cannot establish is this state.
	StateWaitingResolution State = "waiting_resolution"

	StateSucceeded State = "succeeded"
	// StatePartial: some nodes produced output and some did not, and that is
	// the honest terminal answer rather than a rounded-off success or failure.
	StatePartial   State = "partial"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

// States returns every state, in lifecycle order.
//
// Declared as a list so that a consumer outside this module — a transport, a
// browser — can be checked against the machine rather than against somebody's
// memory of it. A test in this package parses the constants and fails if one is
// missing here, so the list cannot fall behind the type.
func States() []State {
	return []State{
		StateQueued, StateRunning,
		StateWaitingApproval, StateWaitingResolution,
		StateSucceeded, StatePartial, StateFailed, StateCancelled,
	}
}

// Terminal reports whether the Run has finished. A terminal Run accepts no
// command at all.
func (s State) Terminal() bool {
	switch s {
	case StateSucceeded, StatePartial, StateFailed, StateCancelled:
		return true
	default:
		return false
	}
}

// Waiting reports whether the Run is parked on something outside itself.
func (s State) Waiting() bool {
	switch s {
	case StateWaitingApproval, StateWaitingResolution:
		return true
	default:
		return false
	}
}
