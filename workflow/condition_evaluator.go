package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// ConditionEvaluator safely evaluates condition expressions against a node's JSON output
// without risk of code injection, infinite loops, or external dependencies.
type ConditionEvaluator struct {
	cache map[string]parsedCondition
	mu    sync.RWMutex
}

type parsedCondition struct {
	field string
	op    string
	val   any
}

// NewConditionEvaluator constructs a condition evaluator.
func NewConditionEvaluator() *ConditionEvaluator {
	return &ConditionEvaluator{
		cache: make(map[string]parsedCondition),
	}
}

// Evaluate checks if the condition matches the node's JSON output.
// Supported syntax formats:
// - "node.field < 80" / "node.field <= 80"
// - "node.field > 80" / "node.field >= 80"
// - "node.field == true" / "node.field != true"
// - "node.field == 'text'" / "node.field != 'text'"
func (e *ConditionEvaluator) Evaluate(ctx context.Context, condition string, nodeOutput json.RawMessage) (bool, error) {
	cond := strings.TrimSpace(condition)
	if cond == "" {
		return false, nil
	}

	parsed, err := e.getOrParse(cond)
	if err != nil {
		return false, err
	}

	if len(nodeOutput) == 0 {
		return false, nil
	}

	var data map[string]any
	if err := json.Unmarshal(nodeOutput, &data); err != nil {
		return false, fmt.Errorf("invalid json output: %w", err)
	}

	actualVal, ok := getNestedField(data, parsed.field)
	if !ok {
		return false, nil
	}

	return compareValues(actualVal, parsed.op, parsed.val)
}

func (e *ConditionEvaluator) getOrParse(condition string) (parsedCondition, error) {
	e.mu.RLock()
	p, ok := e.cache[condition]
	e.mu.RUnlock()
	if ok {
		return p, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if p, ok := e.cache[condition]; ok {
		return p, nil
	}

	parsed, err := parseConditionString(condition)
	if err != nil {
		return parsedCondition{}, err
	}
	e.cache[condition] = parsed
	return parsed, nil
}

func parseConditionString(condition string) (parsedCondition, error) {
	ops := []string{"<=", ">=", "==", "!=", "<", ">"}
	var chosenOp string
	var parts []string
	for _, op := range ops {
		if strings.Contains(condition, op) {
			chosenOp = op
			parts = strings.SplitN(condition, op, 2)
			break
		}
	}
	if chosenOp == "" || len(parts) != 2 {
		return parsedCondition{}, fmt.Errorf("unsupported condition expression: %q", condition)
	}

	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(parts[1])

	left = strings.TrimPrefix(left, "node.")

	var targetVal any
	if num, err := strconv.ParseFloat(right, 64); err == nil {
		targetVal = num
	} else if b, err := strconv.ParseBool(right); err == nil {
		targetVal = b
	} else if (strings.HasPrefix(right, "'") && strings.HasSuffix(right, "'")) ||
		(strings.HasPrefix(right, "\"") && strings.HasSuffix(right, "\"")) {
		targetVal = right[1 : len(right)-1]
	} else {
		targetVal = right
	}

	return parsedCondition{
		field: left,
		op:    chosenOp,
		val:   targetVal,
	}, nil
}

func getNestedField(data map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	var current any = data
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		val, exists := m[part]
		if !exists {
			return nil, false
		}
		current = val
	}
	return current, true
}

func compareValues(actual any, op string, expected any) (bool, error) {
	switch exp := expected.(type) {
	case float64:
		var actFloat float64
		switch v := actual.(type) {
		case float64:
			actFloat = v
		case int:
			actFloat = float64(v)
		case int64:
			actFloat = float64(v)
		default:
			return false, fmt.Errorf("type mismatch: expected number, got %T", actual)
		}
		switch op {
		case "<":
			return actFloat < exp, nil
		case "<=":
			return actFloat <= exp, nil
		case ">":
			return actFloat > exp, nil
		case ">=":
			return actFloat >= exp, nil
		case "==":
			return actFloat == exp, nil
		case "!=":
			return actFloat != exp, nil
		}

	case bool:
		actBool, ok := actual.(bool)
		if !ok {
			return false, fmt.Errorf("type mismatch: expected bool, got %T", actual)
		}
		switch op {
		case "==":
			return actBool == exp, nil
		case "!=":
			return actBool != exp, nil
		default:
			return false, fmt.Errorf("operator %s not supported for booleans", op)
		}

	case string:
		actStr, ok := actual.(string)
		if !ok {
			return false, fmt.Errorf("type mismatch: expected string, got %T", actual)
		}
		switch op {
		case "==":
			return actStr == exp, nil
		case "!=":
			return actStr != exp, nil
		default:
			return false, fmt.Errorf("operator %s not supported for strings", op)
		}
	}

	return false, fmt.Errorf("unsupported comparison between %T and %T", actual, expected)
}
