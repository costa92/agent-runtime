package run

// Limits is a spend in the four units the Runtime meters. It is used for the
// envelope, for what has been spent and for what is reserved — one type,
// because a limit and a usage that are shaped differently cannot be compared
// without a conversion nobody will keep correct.
type Limits struct {
	LLMCalls  int `json:"llm_calls"`
	Tokens    int `json:"tokens"`
	ToolCalls int `json:"tool_calls"`
	// MediaOps counts billable media generations — one per image, video or
	// audio clip actually produced. It is separate from ToolCalls because one
	// tool call may produce nine images, and a cap that cannot tell those
	// apart is off by that factor.
	MediaOps int `json:"media_ops"`
}

// Add returns the componentwise sum.
func (l Limits) Add(other Limits) Limits {
	return Limits{
		LLMCalls:  l.LLMCalls + other.LLMCalls,
		Tokens:    l.Tokens + other.Tokens,
		ToolCalls: l.ToolCalls + other.ToolCalls,
		MediaOps:  l.MediaOps + other.MediaOps,
	}
}

// Zero reports whether nothing is being asked for.
func (l Limits) Zero() bool { return l == Limits{} }

// ExceededBy reports whether other exceeds l in any component. Any single
// component over the line is over the line: a Run that stayed inside its token
// budget by making a thousand tool calls has not stayed inside its budget.
func (l Limits) ExceededBy(other Limits) bool {
	return other.LLMCalls > l.LLMCalls || other.Tokens > l.Tokens ||
		other.ToolCalls > l.ToolCalls || other.MediaOps > l.MediaOps
}

// Budget is the root envelope and everything charged against it.
//
// Reserved is separate from Used on purpose. An effect is charged before it is
// issued and settled after it returns, so between those two moments the spend
// is real but not yet measured. Without the reservation, concurrent children
// each see an affordable balance and collectively overrun the envelope — the
// classic double-spend, and it is why the check is against the root rather
// than against a parent's remaining slice.
type Budget struct {
	Envelope Limits `json:"envelope"`
	Used     Limits `json:"used"`
	Reserved Limits `json:"reserved"`
}

// Committed is everything already spoken for: spent or reserved.
func (b Budget) Committed() Limits {
	return b.Used.Add(b.Reserved)
}

// Affords reports whether want fits in what the envelope has left.
//
// A zero component means unlimited rather than "nothing allowed". Hosts
// publish LLM and tool ceilings without a token ceiling; treating Tokens:0
// as a hard cap of zero refuses every effect after the first settled usage.
// A Runtime embedded with no budget configured has to run; a Runtime that
// refuses every call until someone sets three numbers is broken out of the
// box, and the mistake is loud either way — an unbounded Run shows up in
// the ledger.
func (b Budget) Affords(want Limits) bool {
	if b.Envelope.Zero() {
		return true
	}
	committed := b.Committed().Add(want)
	return !exceedsCapped(b.Envelope.LLMCalls, committed.LLMCalls) &&
		!exceedsCapped(b.Envelope.Tokens, committed.Tokens) &&
		!exceedsCapped(b.Envelope.ToolCalls, committed.ToolCalls) &&
		!exceedsCapped(b.Envelope.MediaOps, committed.MediaOps)
}

func exceedsCapped(ceiling, used int) bool {
	return ceiling > 0 && used > ceiling
}

// Settle closes a reservation: the charge moves into Used, and the unspent
// remainder is released only when the outcome is known.
//
// An unknown reservation stays held. Releasing it would let the budget be spent
// twice if the effect turns out to have happened, and "we could not tell" is
// not the same answer as "it did not happen".
func (b Budget) Settle(reserved, charged Limits, release bool) Budget {
	next := b
	next.Used = b.Used.Add(charged)
	if !release {
		return next
	}
	next.Reserved = Limits{
		LLMCalls:  max(0, b.Reserved.LLMCalls-reserved.LLMCalls),
		Tokens:    max(0, b.Reserved.Tokens-reserved.Tokens),
		ToolCalls: max(0, b.Reserved.ToolCalls-reserved.ToolCalls),
		MediaOps:  max(0, b.Reserved.MediaOps-reserved.MediaOps),
	}
	return next
}

// reserve returns a copy with want moved into Reserved.
func (b Budget) reserve(want Limits) Budget {
	next := b
	next.Reserved = b.Reserved.Add(want)
	return next
}
