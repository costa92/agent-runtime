package store

import (
	"encoding/json"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// ProjectionKind is the closed set of durable facts a host may project.
//
// Closed and typed, rather than a product DTO or a generic mutation bag,
// because the outbox is the seam between the Runtime and the host's own tables.
// An open payload there would let a product write arbitrary state through the
// Runtime's transaction and call it a projection.
type ProjectionKind string

const (
	ProjectionUserTurn         ProjectionKind = "user_turn"
	ProjectionAssistantMessage ProjectionKind = "assistant_message"
	ProjectionPendingApproval  ProjectionKind = "pending_approval"
	ProjectionProgress         ProjectionKind = "progress"
	ProjectionTerminalResult   ProjectionKind = "terminal_result"
)

func (k ProjectionKind) Valid() bool {
	switch k {
	case ProjectionUserTurn, ProjectionAssistantMessage, ProjectionPendingApproval,
		ProjectionProgress, ProjectionTerminalResult:
		return true
	default:
		return false
	}
}

// ProjectionFact is one durable outbox entry, committed in the same transaction
// as the Run state that produced it.
//
// That atomicity is the whole point. The alternative — commit the Run, then
// write the transcript — loses the transcript on any crash in between, and the
// user sees a Run that advanced with no record of why. Product services must
// never perform that two-step.
type ProjectionFact struct {
	Kind ProjectionKind
	// RunID and Sequence make a fact idempotent: a projector that replays the
	// outbox writes each fact at most once, keyed by this pair, so a redelivery
	// is free rather than duplicated.
	RunID    run.ID
	Sequence uint64
	Payload  json.RawMessage
}

// Validate rejects a fact the projector could not apply idempotently.
func (f ProjectionFact) Validate() error {
	if !f.Kind.Valid() {
		return run.NewError("unknown_projection_kind", run.ErrorInvalid, run.RetryNever)
	}
	if f.RunID == "" {
		return run.NewError("missing_run_id", run.ErrorInvalid, run.RetryNever)
	}
	if f.Sequence == 0 {
		// Sequence zero would make two facts for the same Run collide on the
		// idempotency key, so the second would be silently dropped.
		return run.NewError("missing_sequence", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}
