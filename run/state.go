package run

// State is the Run lifecycle. One aggregate and one state machine serve every
// execution shape — single agent, DAG, delegation tree — because a second
// execution path is a second set of invariants, and the two drift.
//
//	queued  → running
//	running → waiting_approval | waiting_children | waiting_retry | waiting_resolution
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
	// StateWaitingChildren: delegated children are still running.
	StateWaitingChildren State = "waiting_children"
	// StateWaitingRetry: a failure the Runtime established is safe to repeat.
	StateWaitingRetry State = "waiting_retry"
	// StateWaitingResolution: an Invocation's outcome is unknown — the side
	// effect may have happened. Only a resolution leaves this state; a retry
	// from here could duplicate the effect, which is why it is a state of its
	// own rather than a flavour of waiting_retry.
	StateWaitingResolution State = "waiting_resolution"

	StateSucceeded State = "succeeded"
	// StatePartial: some nodes produced output and some did not, and that is
	// the honest terminal answer rather than a rounded-off success or failure.
	StatePartial   State = "partial"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
)

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
	case StateWaitingApproval, StateWaitingChildren, StateWaitingRetry, StateWaitingResolution:
		return true
	default:
		return false
	}
}
