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
	//
	// Both are assigned by the Store at commit, and are zero on the way in.
	// RunID because the Store already knows which Run it is committing — from
	// the fence, or from the Create it is performing — and a producer that
	// supplied it could file a fact under a Run that is not the one being
	// written, which no later reader could detect.
	//
	// The producer cannot assign it: it is per-Run monotonic, so knowing the
	// next value means reading the outbox, and the Store is already holding the
	// Run's row lock when it writes. A producer guessing — reusing the event
	// sequence, say — collides with itself the moment one commit carries two
	// facts, and the second is dropped by the outbox's own conflict clause
	// rather than reported.
	RunID    run.ID
	Sequence uint64
	Payload  json.RawMessage
}

// Validate rejects a fact the projector could not apply idempotently.
//
// Neither RunID nor Sequence is checked: neither is the producer's to set. A
// Store that failed to stamp them is caught by the conformance suite, which is
// where a Store bug belongs — refusing here would refuse every legitimate
// enqueue instead.
func (f ProjectionFact) Validate() error {
	if !f.Kind.Valid() {
		return run.NewError("unknown_projection_kind", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// The payload of each Kind.
//
// Typed here rather than left to whoever writes the fact, because the outbox
// has two ends built at different times: the engine enqueues, the host applies,
// and a payload agreed only in prose drifts silently — the projector keeps
// reading a field the producer stopped writing and records an empty transcript
// line rather than failing.
//
// None of them carries a session: the Runtime has no session concept, and a
// field only the host could fill would be written as empty by the only code
// that enqueues it. The fact's RunID is the correlation key, and the host
// already knows which session it started that Run for.
//
// None of them carries Run state either: a projection is a fact about something
// that happened, and a projector that re-derived state from it would be a
// second reader of a Run's status with no fence over it.

// UserTurnPayload is one accepted user message.
//
// Recorded on acceptance rather than on completion, so a turn that fails still
// has the message that caused it — a transcript missing the question but
// showing the error is the shape that makes an incident unreadable.
//
// Produced by the host, not by the engine: CreateCommand carries neither a
// commit context nor the user's text, so this is the one Kind the Runtime
// cannot enqueue for itself.
type UserTurnPayload struct {
	// SessionID is carried here and nowhere else. The other four Kinds are
	// engine-produced and cannot know it; this one is enqueued by the host with
	// the Run's own creation, which is the moment — and the only moment — the
	// binding between a Run and a session exists in one place. Every later fact
	// for that Run is resolved through this one.
	SessionID int64 `json:"session_id"`
	// UserID is the session owner, carried so the projector can scope
	// session-state writes (like the first-turn fallback title) through the
	// host's per-user data scope — the projection context has no user claims.
	UserID int64  `json:"user_id,omitempty"`
	Text   string `json:"text"`
}

// AssistantMessagePayload is one agent output.
//
// Output is the agent's own opaque result, carried verbatim. The engine does
// not know it is a message — an Agent's output is json.RawMessage by design —
// so rendering it is the host's job. A field called Text here would be the
// engine asserting a shape it never had.
type AssistantMessagePayload struct {
	Output json.RawMessage `json:"output"`
	// OutputRef is the ref the node result was committed under, so a projection
	// and the Run's own node map can be lined up after the fact.
	OutputRef string `json:"output_ref,omitempty"`
	// AgentKey and NodeID name which Agent spoke. A delegated tree produces
	// output from several, and a transcript attributing all of it to the root
	// would be wrong in exactly the case somebody is reading it to understand.
	AgentKey string `json:"agent_key"`
	NodeID   string `json:"node_id"`
}

// PendingApprovalPayload is one decision waiting on a human.
//
// Action and Detail are what the person is being asked to approve. They are
// carried rather than looked up, because the approval must be answerable from
// the projection alone — a projector that had to read Runtime tables to render
// the prompt would be reaching across the seam this outbox exists to be.
//
// No engine path parks a Run for approval yet, so nothing enqueues this today.
type PendingApprovalPayload struct {
	ApprovalID string `json:"approval_id"`
	Action     string `json:"action"`
	Detail     string `json:"detail,omitempty"`
}

// ProgressPayload marks one node finishing.
//
// Advisory by construction: it has no ordering guarantee beyond the fact's own
// Sequence and no meaning to any decision. Anything a decision depends on is a
// Run event, not a progress line.
//
// It carries identity rather than prose. The engine has no sentence to write
// that the host could not write better, and a message composed here would be
// wording the host cannot change without a Runtime release.
type ProgressPayload struct {
	NodeID   string `json:"node_id"`
	AgentKey string `json:"agent_key"`
	Failed   bool   `json:"failed,omitempty"`
}

// TerminalResultPayload is the settled outcome of a Run.
//
// State is the terminal Run state as its own string rather than a bool, because
// succeeded, partial, failed and cancelled are four different things to show a
// user, and collapsing them to "done" loses the only distinction that matters
// after the fact.
//
// It carries no error text: a Snapshot has none, and the reason a Run failed is
// already in its events, which the host can read by RunID. A field the engine
// could only ever fill with an empty string would read, to a projector, as "it
// failed for no reason".
type TerminalResultPayload struct {
	State string `json:"state"`
	// UserID is the run's principal, carried so the projector can scope
	// session-state writes through the host's per-user data scope — the
	// projection context has no user claims.
	UserID int64 `json:"user_id,omitempty"`
	// UsedLLMCalls and UsedTokens are the settled budget at the terminal
	// commit, carried so the host can fold spend into its session counters.
	UsedLLMCalls int64 `json:"used_llm_calls,omitempty"`
	UsedTokens   int64 `json:"used_tokens,omitempty"`
}

// ProjectionPayload is what every payload above implements.
//
// The Kind comes from the payload rather than being passed alongside it, so a
// fact whose Kind and payload disagree cannot be constructed at all. Validating
// the pair instead would leave the mismatch possible and only caught at the
// projector, one deploy and one queue away from the code that made it.
type ProjectionPayload interface {
	ProjectionKind() ProjectionKind
}

func (UserTurnPayload) ProjectionKind() ProjectionKind { return ProjectionUserTurn }
func (AssistantMessagePayload) ProjectionKind() ProjectionKind {
	return ProjectionAssistantMessage
}
func (PendingApprovalPayload) ProjectionKind() ProjectionKind { return ProjectionPendingApproval }
func (ProgressPayload) ProjectionKind() ProjectionKind        { return ProjectionProgress }
func (TerminalResultPayload) ProjectionKind() ProjectionKind  { return ProjectionTerminalResult }

// NewProjectionFact is the only supported way to enqueue a fact.
func NewProjectionFact(payload ProjectionPayload) (ProjectionFact, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return ProjectionFact{}, run.NewError("unencodable_projection", run.ErrorInvalid, run.RetryNever, err)
	}
	fact := ProjectionFact{
		Kind: payload.ProjectionKind(), Payload: encoded,
	}
	if err := fact.Validate(); err != nil {
		return ProjectionFact{}, err
	}
	return fact, nil
}

// DecodeProjection reads a fact back into its payload type.
//
// It refuses a fact of a different Kind rather than decoding what it can: JSON
// into a mismatched struct succeeds with every field left at its zero value,
// which reaches the host's tables as an empty transcript line rather than as an
// error anybody sees.
func DecodeProjection[P ProjectionPayload](fact ProjectionFact) (P, error) {
	var payload P
	if fact.Kind != payload.ProjectionKind() {
		return payload, run.NewError("projection_kind_mismatch", run.ErrorInvalid, run.RetryNever)
	}
	if err := json.Unmarshal(fact.Payload, &payload); err != nil {
		return payload, run.NewError("undecodable_projection", run.ErrorInvalid, run.RetryNever, err)
	}
	return payload, nil
}
