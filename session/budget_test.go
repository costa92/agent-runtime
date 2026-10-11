package session

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestCheckBudgetSettlesActualTokensBeyondLimitThenBlocksNextModelCall(t *testing.T) {
	p := Projection{Turns: []Turn{{Steps: []Step{
		{Budget: map[BudgetCategory]uint64{BudgetInputTokens: 5}},
		{Budget: map[BudgetCategory]uint64{BudgetOutputTokens: 6}},
	}}}}
	event := Event{Kind: BudgetSettled, Payload: json.RawMessage(`{"turn_id":"t","step_id":"s","category":"output_tokens","units":6}`)}
	policy := BudgetPolicy{SessionTokensMax: 10}
	if _, err := CheckBudget(p, []Event{event}, policy, time.Now()); err != nil {
		t.Fatalf("actual token settlement was rejected: %v", err)
	}
	admission := Event{Kind: BudgetSettled, Payload: json.RawMessage(`{"turn_id":"t","step_id":"s","category":"model_calls","units":1}`)}
	if _, err := CheckBudget(p, []Event{admission}, policy, time.Now()); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("next model call crossed settled token limit: %v", err)
	}
	policy.SessionTokensMax = 12
	policy.WindowLimits = []WindowLimit{{Name: "hourly_tokens", Unit: "tokens", Max: 10, Window: time.Hour}}
	charges, err := CheckBudget(p, []Event{event}, policy, time.Date(2026, 10, 11, 3, 27, 0, 0, time.UTC))
	if err != nil || len(charges) != 1 || charges[0].Amount != 6 || charges[0].WindowStart.Hour() != 3 {
		t.Fatalf("window charges: %+v %v", charges, err)
	}
	probes, err := CheckBudget(p, []Event{admission}, policy, time.Date(2026, 10, 11, 3, 27, 0, 0, time.UTC))
	if err != nil || len(probes) != 1 || !probes[0].Admission || probes[0].Amount != 0 {
		t.Fatalf("model admission lacks token window probe: %+v %v", probes, err)
	}
	// Admission and supplier settlement may share one Append. Admission checks
	// usage before this batch while the actual spend is always recorded.
	before := Projection{Turns: []Turn{{Steps: []Step{{Budget: map[BudgetCategory]uint64{
		BudgetModelCalls: 1, BudgetInputTokens: 3, BudgetOutputTokens: 4,
	}}}}}}
	policy.SessionTokensMax = 5
	if _, err := CheckBudget(before, []Event{admission,
		{Kind: BudgetSettled, Payload: json.RawMessage(`{"turn_id":"t","step_id":"s","category":"input_tokens","units":3}`)},
		{Kind: BudgetSettled, Payload: json.RawMessage(`{"turn_id":"t","step_id":"s","category":"output_tokens","units":4}`)},
	}, policy, time.Now()); err != nil {
		t.Fatalf("same-batch supplier settlement rejected: %v", err)
	}
}
