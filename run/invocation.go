package run

// Outcome is what happened to one external call.
//
// It is a property of the Invocation, never of the Run: a Run holding an
// unresolved Invocation is in a well-defined state — waiting for someone to say
// what happened — and calling the Run itself "unknown" would throw that away.
type Outcome string

const (
	// OutcomeInFlight: persisted before leaving the Store, so a crash in the
	// window that follows leaves evidence that the call was attempted.
	OutcomeInFlight Outcome = "in_flight"
	OutcomeApplied  Outcome = "applied"
	// OutcomeNotApplied: established not to have taken effect. Safe to retry.
	OutcomeNotApplied Outcome = "not_applied"
	// OutcomeUnknown: the process died in the window where the effect may have
	// happened and the result was not committed. Requires reconciliation.
	OutcomeUnknown Outcome = "unknown"
)

// Resolvable reports whether an outcome is an answer a resolver may give.
// in_flight is not: it is what the Runtime writes on the way out, not a
// decision anybody makes.
func (o Outcome) Resolvable() bool {
	switch o {
	case OutcomeApplied, OutcomeNotApplied, OutcomeStillUnknown:
		return true
	default:
		return false
	}
}

// OutcomeStillUnknown is a resolver's answer, not a stored state: "I looked and
// I still cannot tell." It is distinct from OutcomeUnknown so that a Run which
// has been investigated is distinguishable from one nobody has looked at, and
// it leaves the Run parked.
const OutcomeStillUnknown Outcome = "still_unknown"

// Invocation is one external call: the durable record written before the call
// leaves the Store, so that a crash mid-flight leaves a fact behind rather than
// silence.
type Invocation struct {
	ID ID `json:"id"`
	// Tool is the tool key this Invocation called, empty for model and memory
	// calls. It is what makes the ledger answerable to "how many times has this
	// Run called X" — a question no in-memory counter can answer honestly,
	// because a session is per claim and a Run that parks for approval or is
	// recovered after a crash gets a fresh one.
	Tool string `json:"tool,omitempty"`
	// IdempotencyKey lets a tool that declares idempotency be replayed safely.
	// Its absence is what forces an unknown result into reconciliation.
	IdempotencyKey string  `json:"idempotency_key,omitempty"`
	Outcome        Outcome `json:"outcome"`
	// Reserved is what was charged against the budget for this call, held so
	// that settling the Invocation can release exactly what it took.
	Reserved Limits `json:"reserved,omitzero"`
}
