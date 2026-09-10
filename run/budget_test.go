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

func TestAHostUnitIsCarriedThroughEveryLimitsOperation(t *testing.T) {
	// A unit the arithmetic forgets is a unit that silently never binds: the
	// envelope would afford any number of images because the component it
	// compares is always zero.
	a := Limits{}.WithUnit("images", 3)
	b := Limits{}.WithUnit("images", 4)
	if got := a.Add(b).Unit("images"); got != 7 {
		t.Fatalf("Add lost the host unit: got %d, want 7", got)
	}
	if (Limits{}.WithUnit("images", 1)).Zero() {
		t.Fatal("Zero reported true for a non-zero host unit")
	}
	if !(Limits{}.WithUnit("images", 2)).ExceededBy(Limits{}.WithUnit("images", 3)) {
		t.Fatal("ExceededBy ignored the host unit")
	}
	envelope := Budget{Envelope: Limits{}.WithUnit("images", 5), Used: Limits{}.WithUnit("images", 4)}
	if envelope.Affords(Limits{}.WithUnit("images", 2)) {
		t.Fatal("Affords let a Run exceed its host-unit envelope")
	}
	if got := envelope.ExhaustedUnit(Limits{}.WithUnit("images", 2)); got != "images" {
		t.Fatalf("ExhaustedUnit = %q, want the host unit by its own name", got)
	}
	settled := Budget{Reserved: Limits{}.WithUnit("images", 3)}.
		Settle(Limits{}.WithUnit("images", 3), Limits{}.WithUnit("images", 2), true)
	if settled.Used.Unit("images") != 2 || settled.Reserved.Unit("images") != 0 {
		t.Fatalf("Settle mishandled the host unit: used=%d reserved=%d",
			settled.Used.Unit("images"), settled.Reserved.Unit("images"))
	}
	if !(Limits{}.WithUnit("images", 0)).Equal(Limits{}) {
		t.Fatal("a zero unit is not the same as no unit")
	}
}

func TestRemainingReportsEveryCappedUnit(t *testing.T) {
	budget := Budget{
		Envelope: Limits{LLMCalls: 10, Tokens: 10000, ToolCalls: 5}.WithUnit("images", 7),
		Used:     Limits{LLMCalls: 3, Tokens: 2500, ToolCalls: 1}.WithUnit("images", 2),
	}

	got := budget.Remaining()

	want := Limits{LLMCalls: 7, Tokens: 7500, ToolCalls: 4}.WithUnit("images", 5)
	if !got.Equal(want) {
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
