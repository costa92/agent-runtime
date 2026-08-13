package run

import "maps"

// Limits is a spend in the three units the Runtime meters. It is used for the
// envelope, for what has been spent, for what is reserved, and for a child's
// slice — one type, because a limit and a usage that are shaped differently
// cannot be compared without a conversion nobody will keep correct.
type Limits struct {
	LLMCalls  int `json:"llm_calls"`
	Tokens    int `json:"tokens"`
	ToolCalls int `json:"tool_calls"`
}

// Add returns the componentwise sum.
func (l Limits) Add(other Limits) Limits {
	return Limits{
		LLMCalls:  l.LLMCalls + other.LLMCalls,
		Tokens:    l.Tokens + other.Tokens,
		ToolCalls: l.ToolCalls + other.ToolCalls,
	}
}

// Zero reports whether nothing is being asked for.
func (l Limits) Zero() bool { return l == Limits{} }

// ExceededBy reports whether other exceeds l in any component. Any single
// component over the line is over the line: a Run that stayed inside its token
// budget by making a thousand tool calls has not stayed inside its budget.
func (l Limits) ExceededBy(other Limits) bool {
	return other.LLMCalls > l.LLMCalls || other.Tokens > l.Tokens || other.ToolCalls > l.ToolCalls
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
	// Slices are the envelopes handed to child Runs. They are committed
	// against the root the moment they are granted, not when the child spends
	// them, for the same reason Reserved exists.
	Slices map[ID]Limits `json:"slices,omitempty"`
}

// Committed is everything already spoken for: spent, reserved, or handed to a
// child.
func (b Budget) Committed() Limits {
	total := b.Used.Add(b.Reserved)
	for _, slice := range b.Slices {
		total = total.Add(slice)
	}
	return total
}

// Affords reports whether want fits in what the envelope has left.
//
// A zero envelope means unlimited rather than "nothing allowed". A Runtime
// embedded with no budget configured has to run; a Runtime that refuses every
// call until someone sets three numbers is broken out of the box, and the
// mistake is loud either way — an unbounded Run shows up in the ledger.
func (b Budget) Affords(want Limits) bool {
	if b.Envelope.Zero() {
		return true
	}
	return !b.Envelope.ExceededBy(b.Committed().Add(want))
}

// reserve returns a copy with want moved into Reserved.
func (b Budget) reserve(want Limits) Budget {
	next := b
	next.Reserved = b.Reserved.Add(want)
	next.Slices = copySlices(b.Slices)
	return next
}

// grantSlices returns a copy with the given child envelopes recorded.
func (b Budget) grantSlices(slices map[ID]Limits) Budget {
	next := b
	next.Slices = copySlices(b.Slices)
	if next.Slices == nil {
		next.Slices = make(map[ID]Limits, len(slices))
	}
	maps.Copy(next.Slices, slices)
	return next
}

func copySlices(slices map[ID]Limits) map[ID]Limits {
	if slices == nil {
		return nil
	}
	out := make(map[ID]Limits, len(slices))
	maps.Copy(out, slices)
	return out
}
