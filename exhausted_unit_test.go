package agentruntime

import (
	"testing"

	"github.com/costa92/agent-runtime/run"
)

// Every budget dimension must be able to name itself on a refusal. An
// unreported dimension is worse than silence when an operator is sizing the
// envelope from telemetry: a host unit's refusals would have read as "no
// dimension" and pointed at nothing. Host units report under their own name.
func TestEveryBudgetDimensionNamesItselfWhenItRefuses(t *testing.T) {
	for _, tc := range []struct {
		want     string
		envelope run.Limits
		asking   run.Limits
	}{
		{"llm_calls", run.Limits{LLMCalls: 1}, run.Limits{LLMCalls: 2}},
		{"tokens", run.Limits{Tokens: 10}, run.Limits{Tokens: 11}},
		{"tool_calls", run.Limits{ToolCalls: 1}, run.Limits{ToolCalls: 2}},
		{"images", run.Limits{}.WithUnit("images", 8), run.Limits{}.WithUnit("images", 9)},
	} {
		t.Run(tc.want, func(t *testing.T) {
			budget := run.Budget{Envelope: tc.envelope}
			if budget.Affords(tc.asking) {
				t.Fatalf("the envelope %+v afforded %+v; the case tests nothing", tc.envelope, tc.asking)
			}
			if got := exhaustedUnit(budget, tc.asking); got != tc.want {
				t.Fatalf("exhaustedUnit = %q, want %q", got, tc.want)
			}
		})
	}
}
