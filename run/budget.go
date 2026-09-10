package run

import (
	"encoding/json"
	"maps"
	"slices"
)

// Limits is a spend in the units the Runtime meters. It is used for the
// envelope, for what has been spent and for what is reserved — one type,
// because a limit and a usage that are shaped differently cannot be compared
// without a conversion nobody will keep correct.
//
// Three units are the Runtime's own: every Run makes model calls, spends
// tokens and calls tools. Everything else is the host's — how many images a
// tool produced, how many pages a render queued — and lives in Units under the
// host's own name, so that a new billable thing is a declaration in the host
// rather than a field here.
type Limits struct {
	LLMCalls  int `json:"llm_calls"`
	Tokens    int `json:"tokens"`
	ToolCalls int `json:"tool_calls"`
	// Units are host-declared consumption units, keyed by the quota unit name
	// they are charged to. A missing key is zero. Never nil-checked by
	// callers: every method here treats nil and empty alike.
	Units map[string]int `json:"units,omitempty"`
}

// Unit returns one host-declared unit's count, zero when absent.
func (l Limits) Unit(name string) int { return l.Units[name] }

// WithUnit returns a copy with one host-declared unit set. Zero removes the
// key, so a Limits that says nothing about a unit stays Zero.
func (l Limits) WithUnit(name string, count int) Limits {
	next := l
	next.Units = maps.Clone(l.Units)
	if count == 0 {
		delete(next.Units, name)
		if len(next.Units) == 0 {
			next.Units = nil
		}
		return next
	}
	if next.Units == nil {
		next.Units = map[string]int{}
	}
	next.Units[name] = count
	return next
}

// UnitNames lists the declared units in a stable order, so that anything
// iterating them — a ledger charge, a refusal reason — is deterministic.
func (l Limits) UnitNames() []string {
	names := slices.Collect(maps.Keys(l.Units))
	slices.Sort(names)
	return names
}

// Add returns the componentwise sum.
func (l Limits) Add(other Limits) Limits {
	sum := Limits{
		LLMCalls:  l.LLMCalls + other.LLMCalls,
		Tokens:    l.Tokens + other.Tokens,
		ToolCalls: l.ToolCalls + other.ToolCalls,
	}
	for _, name := range l.UnitNames() {
		sum = sum.WithUnit(name, l.Units[name]+other.Units[name])
	}
	for _, name := range other.UnitNames() {
		if _, seen := l.Units[name]; !seen {
			sum = sum.WithUnit(name, other.Units[name])
		}
	}
	return sum
}

// Zero reports whether nothing is being asked for.
func (l Limits) Zero() bool {
	if l.LLMCalls != 0 || l.Tokens != 0 || l.ToolCalls != 0 {
		return false
	}
	for _, count := range l.Units {
		if count != 0 {
			return false
		}
	}
	return true
}

// Equal reports componentwise equality. Limits carries a map, so it is not
// comparable with ==; a unit absent on one side and zero on the other is equal.
func (l Limits) Equal(other Limits) bool {
	if l.LLMCalls != other.LLMCalls || l.Tokens != other.Tokens || l.ToolCalls != other.ToolCalls {
		return false
	}
	for _, name := range l.UnitNames() {
		if l.Units[name] != other.Units[name] {
			return false
		}
	}
	for _, name := range other.UnitNames() {
		if l.Units[name] != other.Units[name] {
			return false
		}
	}
	return true
}

// ExceededBy reports whether other exceeds l in any component. Any single
// component over the line is over the line: a Run that stayed inside its token
// budget by making a thousand tool calls has not stayed inside its budget.
func (l Limits) ExceededBy(other Limits) bool {
	if other.LLMCalls > l.LLMCalls || other.Tokens > l.Tokens || other.ToolCalls > l.ToolCalls {
		return true
	}
	for _, name := range other.UnitNames() {
		if other.Units[name] > l.Units[name] {
			return true
		}
	}
	return false
}

// legacyLimits is the wire shape before host units were generic: media_ops
// was a named field. Rows written then still carry it, and a Run in flight
// across the upgrade must not lose its count.
type legacyLimits struct {
	LLMCalls  int            `json:"llm_calls"`
	Tokens    int            `json:"tokens"`
	ToolCalls int            `json:"tool_calls"`
	Units     map[string]int `json:"units,omitempty"`
	MediaOps  int            `json:"media_ops,omitempty"`
}

func (l *Limits) UnmarshalJSON(data []byte) error {
	var legacy legacyLimits
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}
	*l = Limits{LLMCalls: legacy.LLMCalls, Tokens: legacy.Tokens, ToolCalls: legacy.ToolCalls, Units: legacy.Units}
	if legacy.MediaOps != 0 {
		*l = l.WithUnit("media_ops", l.Unit("media_ops")+legacy.MediaOps)
	}
	return nil
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
	return b.ExhaustedUnit(want) == ""
}

// ExhaustedUnit names the first budget dimension want would push over the
// envelope, or "" when it fits. Order is the Runtime's three units, then the
// host's in name order, so a refusal on two at once is reported the same way
// every time.
func (b Budget) ExhaustedUnit(want Limits) string {
	if b.Envelope.Zero() {
		return ""
	}
	committed := b.Committed().Add(want)
	switch {
	case exceedsCapped(b.Envelope.LLMCalls, committed.LLMCalls):
		return "llm_calls"
	case exceedsCapped(b.Envelope.Tokens, committed.Tokens):
		return "tokens"
	case exceedsCapped(b.Envelope.ToolCalls, committed.ToolCalls):
		return "tool_calls"
	}
	for _, name := range b.Envelope.UnitNames() {
		if exceedsCapped(b.Envelope.Units[name], committed.Units[name]) {
			return name
		}
	}
	return ""
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
	remaining := Limits{
		LLMCalls:  max(0, b.Reserved.LLMCalls-reserved.LLMCalls),
		Tokens:    max(0, b.Reserved.Tokens-reserved.Tokens),
		ToolCalls: max(0, b.Reserved.ToolCalls-reserved.ToolCalls),
	}
	for _, name := range b.Reserved.UnitNames() {
		remaining = remaining.WithUnit(name, max(0, b.Reserved.Units[name]-reserved.Units[name]))
	}
	next.Reserved = remaining
	return next
}

// reserve returns a copy with want moved into Reserved.
func (b Budget) reserve(want Limits) Budget {
	next := b
	next.Reserved = b.Reserved.Add(want)
	return next
}

// Remaining is what is left of the envelope, in the same terms Affords reads it.
//
// Per unit, not per Budget: a zero ceiling means that unit is uncapped, and
// Affords already skips it, so Remaining reports zero for it too — an uncapped
// unit has no remainder to state. Anything else would have to invent a number.
//
// Clamped at zero. The subtraction can go negative wherever settlement charged
// more than was reserved, and a negative remainder read as a quantity says the
// implementation may make minus three calls.
//
// This exists because the caller that needed it wrote the subtraction inline,
// guarded the whole struct on Zero rather than each unit, and left the host
// units out. An agent asking how many images it could still generate was told
// none, whatever the envelope said, and one asking for tokens under an envelope
// that capped only calls was told a negative number.
func (b Budget) Remaining() Limits {
	committed := b.Committed()
	remaining := Limits{
		LLMCalls:  remainingUnit(b.Envelope.LLMCalls, committed.LLMCalls),
		Tokens:    remainingUnit(b.Envelope.Tokens, committed.Tokens),
		ToolCalls: remainingUnit(b.Envelope.ToolCalls, committed.ToolCalls),
	}
	for _, name := range b.Envelope.UnitNames() {
		remaining = remaining.WithUnit(name, remainingUnit(b.Envelope.Units[name], committed.Units[name]))
	}
	return remaining
}

func remainingUnit(ceiling, used int) int {
	if ceiling <= 0 {
		return 0
	}
	return max(0, ceiling-used)
}
