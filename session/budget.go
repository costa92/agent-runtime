package session

import (
	"encoding/json"
	"errors"
	"math"
	"time"
)

var ErrBudgetExceeded = errors.New("session budget exceeded")

// BudgetPolicy is the pinned execution envelope and the currently published
// principal allowance. Stores apply it in the event append transaction.
type BudgetPolicy struct {
	SessionLimits    map[BudgetCategory]uint64
	SessionTokensMax uint64
	WindowLimits     []WindowLimit
}

// WindowLimit counts one principal's usage in fixed UTC time windows.
type WindowLimit struct {
	Name   string
	Unit   string
	Max    uint64
	Window time.Duration
}

type WindowCharge struct {
	Name        string
	Unit        string
	Max         uint64
	WindowStart time.Time
	Amount      uint64
	Admission   bool
}

// CheckBudget returns charges only for new budget facts. The caller holds the
// Session lock; a persistent store must reserve returned charges before commit.
func CheckBudget(after Projection, events []Event, policy BudgetPolicy, now time.Time) ([]WindowCharge, error) {
	newUsage := map[BudgetCategory]uint64{}
	for _, event := range events {
		if event.Kind != BudgetSettled {
			continue
		}
		var settlement BudgetSettlement
		if err := json.Unmarshal(event.Payload, &settlement); err != nil {
			return nil, ErrInvalidInput
		}
		if settlement.Units == 0 || !validBudgetCategory(settlement.Category) || math.MaxUint64-newUsage[settlement.Category] < settlement.Units {
			return nil, ErrInvalidInput
		}
		newUsage[settlement.Category] += settlement.Units
	}
	if len(newUsage) == 0 {
		return nil, nil
	}
	used := map[BudgetCategory]uint64{}
	for _, turn := range after.Turns {
		for _, step := range turn.Steps {
			for category, units := range step.Budget {
				if math.MaxUint64-used[category] < units {
					return nil, ErrInvalidInput
				}
				used[category] += units
			}
		}
	}
	modelAdmission := newUsage[BudgetModelCalls] > 0
	for category, max := range policy.SessionLimits {
		if !validBudgetCategory(category) {
			return nil, ErrInvalidInput
		}
		if category == BudgetInputTokens || category == BudgetOutputTokens {
			if used[category] < newUsage[category] {
				return nil, ErrInvalidInput
			}
			if modelAdmission && used[category]-newUsage[category] >= max {
				return nil, ErrBudgetExceeded
			}
			continue
		}
		if used[category] > max {
			return nil, ErrBudgetExceeded
		}
	}
	if policy.SessionTokensMax > 0 && modelAdmission {
		if used[BudgetInputTokens] < newUsage[BudgetInputTokens] || used[BudgetOutputTokens] < newUsage[BudgetOutputTokens] {
			return nil, ErrInvalidInput
		}
		input, output := used[BudgetInputTokens]-newUsage[BudgetInputTokens], used[BudgetOutputTokens]-newUsage[BudgetOutputTokens]
		if math.MaxUint64-input < output || input+output >= policy.SessionTokensMax {
			return nil, ErrBudgetExceeded
		}
	}
	charges := make([]WindowCharge, 0, len(policy.WindowLimits))
	seen := map[string]bool{}
	for _, limit := range policy.WindowLimits {
		if limit.Name == "" || limit.Window <= 0 || seen[limit.Name] {
			return nil, ErrInvalidInput
		}
		seen[limit.Name] = true
		var delta uint64
		switch limit.Unit {
		case "tokens":
			input, output := newUsage[BudgetInputTokens], newUsage[BudgetOutputTokens]
			if math.MaxUint64-input < output {
				return nil, ErrInvalidInput
			}
			delta = input + output
		case "tool_calls":
			delta = newUsage[BudgetToolCalls]
		case "model_calls":
			delta = newUsage[BudgetModelCalls]
		case "media_ops":
			delta = newUsage[BudgetMediaOps]
		default:
			return nil, ErrInvalidInput
		}
		if limit.Unit == "tokens" && modelAdmission {
			if limit.Max == 0 || limit.Max > math.MaxInt64 {
				return nil, ErrBudgetExceeded
			}
			charges = append(charges, WindowCharge{Name: limit.Name, Unit: limit.Unit, Max: limit.Max, WindowStart: now.UTC().Truncate(limit.Window), Admission: true})
		}
		if delta == 0 {
			continue
		}
		if delta > math.MaxInt64 || limit.Max > math.MaxInt64 || (limit.Unit != "tokens" && delta > limit.Max) {
			return nil, ErrBudgetExceeded
		}
		charges = append(charges, WindowCharge{Name: limit.Name, Unit: limit.Unit, Max: limit.Max, WindowStart: now.UTC().Truncate(limit.Window), Amount: delta})
	}
	return charges, nil
}
