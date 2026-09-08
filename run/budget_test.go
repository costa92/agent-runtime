package run

import "testing"

// A published assistant version copies MaxLLMCalls and MaxToolCalls and
// leaves Tokens at 0. That is "not configured", not "zero tokens allowed":
// the first model call reserves Tokens:0, settles real usage, and the next
// effect — a tool, or the follow-up model turn — must still be admitted.
// Treating the zero component as a hard cap is why a tool-calling turn
// produced no assistant reply: the model applied, then budget_exhausted.
func TestAZeroTokenComponentDoesNotRefuseTheNextEffect(t *testing.T) {
	budget := Budget{
		Envelope: Limits{LLMCalls: 10, ToolCalls: 20},
		Used:     Limits{LLMCalls: 1, Tokens: 2147},
	}

	if !budget.Affords(Limits{LLMCalls: 1}) {
		t.Fatal("a second model call was refused after the first settled tokens against an uncapped component")
	}
	if !budget.Affords(Limits{ToolCalls: 1}) {
		t.Fatal("a tool call was refused after the first model settled tokens against an uncapped component")
	}
}

func TestAConfiguredTokenCeilingStillBinds(t *testing.T) {
	budget := Budget{
		Envelope: Limits{LLMCalls: 10, Tokens: 8000, ToolCalls: 20},
		Used:     Limits{LLMCalls: 1, Tokens: 7900},
	}
	if budget.Affords(Limits{Tokens: 200}) {
		t.Fatal("a configured token ceiling let the next reservation through")
	}
	if !budget.Affords(Limits{LLMCalls: 1}) {
		t.Fatal("an in-budget LLM reservation was refused")
	}
}

func TestMediaOpsIsCarriedThroughEveryLimitsOperation(t *testing.T) {
	// A unit the arithmetic forgets is a unit that silently never binds: the
	// envelope would afford any number of images because the component it
	// compares is always zero.
	a := Limits{MediaOps: 3}
	b := Limits{MediaOps: 4}
	if got := a.Add(b).MediaOps; got != 7 {
		t.Fatalf("Add lost MediaOps: got %d, want 7", got)
	}
	if (Limits{MediaOps: 1}).Zero() {
		t.Fatal("Zero reported true for a non-zero MediaOps")
	}
	if !(Limits{MediaOps: 2}).ExceededBy(Limits{MediaOps: 3}) {
		t.Fatal("ExceededBy ignored MediaOps")
	}
	envelope := Budget{Envelope: Limits{MediaOps: 5}, Used: Limits{MediaOps: 4}}
	if envelope.Affords(Limits{MediaOps: 2}) {
		t.Fatal("Affords let a Run exceed its MediaOps envelope")
	}
	settled := Budget{Reserved: Limits{MediaOps: 3}}.
		Settle(Limits{MediaOps: 3}, Limits{MediaOps: 2}, true)
	if settled.Used.MediaOps != 2 || settled.Reserved.MediaOps != 0 {
		t.Fatalf("Settle mishandled MediaOps: used=%d reserved=%d",
			settled.Used.MediaOps, settled.Reserved.MediaOps)
	}
}

func TestRemainingReportsEveryCappedUnit(t *testing.T) {
	budget := Budget{
		Envelope: Limits{LLMCalls: 10, Tokens: 10000, ToolCalls: 5, MediaOps: 7},
		Used:     Limits{LLMCalls: 3, Tokens: 2500, ToolCalls: 1, MediaOps: 2},
	}

	got := budget.Remaining()

	want := Limits{LLMCalls: 7, Tokens: 7500, ToolCalls: 4, MediaOps: 5}
	if got != want {
		t.Fatalf("remaining=%+v want=%+v", got, want)
	}
}

// Zero is "uncapped" everywhere else in this file — Affords skips a zero
// ceiling — so a remainder computed against one is not a small number, it is
// not a number. Subtracting from it reports a negative allowance to an agent
// that asked how much room it had.
func TestRemainingIsZeroForAnUncappedUnitRatherThanNegative(t *testing.T) {
	budget := Budget{
		Envelope: Limits{LLMCalls: 10},
		Used:     Limits{LLMCalls: 1, Tokens: 3500},
	}

	if got := budget.Remaining().Tokens; got != 0 {
		t.Fatalf("tokens=%d want=0 for an uncapped unit", got)
	}
}

func TestRemainingIsClampedWhenSettlementOverranTheCeiling(t *testing.T) {
	budget := Budget{
		Envelope: Limits{Tokens: 100},
		Used:     Limits{Tokens: 250},
	}

	if got := budget.Remaining().Tokens; got != 0 {
		t.Fatalf("tokens=%d want=0", got)
	}
}
