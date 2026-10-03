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

// OutcomeStillUnknown is a resolver's answer and a stored state: "I looked and
// I still cannot tell." It is distinct from OutcomeUnknown so that a Run which
// has been investigated is distinguishable from one nobody has looked at, and
// it leaves the Run parked.
const OutcomeStillUnknown Outcome = "still_unknown"

// Invocation is one external call: the durable record written before the call
// leaves the Store, so that a crash mid-flight leaves a fact behind rather than
// silence.
type Invocation struct {
	ID ID `json:"id"`
	// NodeID is the graph node that issued this call. Recovery uses the durable
	// identity instead of guessing from graph order before deciding whether a
	// completed write may be run again.
	NodeID string `json:"node_id,omitempty"`
	// Tool is the tool key this Invocation called, empty for model and memory
	// calls. It is what makes the ledger answerable to "how many times has this
	// Run called X" — a question no in-memory counter can answer honestly,
	// because a session is per claim and a Run that parks for approval or is
	// recovered after a crash gets a fresh one.
	Tool string `json:"tool,omitempty"`
	// Write preserves the effect's side-effect classification at invocation time,
	// including writable memory calls.
	Write bool `json:"write,omitempty"`
	// RequestDigest identifies the exact final model request without retaining
	// its potentially sensitive messages in the invocation ledger.
	RequestDigest string `json:"request_digest,omitempty"`
	// IdempotencyKey lets a tool that declares idempotency be replayed safely.
	// Its absence is what forces an unknown result into reconciliation.
	IdempotencyKey string  `json:"idempotency_key,omitempty"`
	Outcome        Outcome `json:"outcome"`
	// Reserved is what was charged against the budget for this call, held so
	// that settling the Invocation can release exactly what it took.
	Reserved Limits `json:"reserved,omitzero"`
	// Charged is the cumulative known usage already included in Budget.Used.
	// An unknown outcome retains its reservation, but its reported usage is
	// charged immediately; reconciliation must only charge the remaining amount.
	Charged Limits `json:"charged,omitzero"`
}

// RemainingCharge is the most reconciliation can add after an unknown result.
// Known usage above the reservation remains charged; confirmation cannot
// erase an amount the provider already reported spending.
func (i Invocation) RemainingCharge() Limits {
	remaining := Limits{
		LLMCalls:  max(0, i.Reserved.LLMCalls-i.Charged.LLMCalls),
		Tokens:    max(0, i.Reserved.Tokens-i.Charged.Tokens),
		ToolCalls: max(0, i.Reserved.ToolCalls-i.Charged.ToolCalls),
	}
	for _, name := range i.Reserved.UnitNames() {
		remaining = remaining.WithUnit(name, max(0, i.Reserved.Unit(name)-i.Charged.Unit(name)))
	}
	return remaining
}
