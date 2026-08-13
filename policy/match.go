package policy

import (
	"slices"
	"strconv"
	"strings"
)

// RiskLevel is a tool's declared blast radius. It is declared on the tool
// rather than inferred, because the inference would have to be made by the
// thing being judged.
type RiskLevel string

const (
	RiskLow    RiskLevel = "low"
	RiskMedium RiskLevel = "medium"
	RiskHigh   RiskLevel = "high"
)

// SideEffect is what a call does to the world.
type SideEffect string

const (
	// SideEffectNone: pure computation.
	SideEffectNone SideEffect = "none"
	// SideEffectRead: observes external state, changes nothing.
	SideEffectRead SideEffect = "read"
	// SideEffectWrite: changes state somewhere the Runtime cannot roll back.
	SideEffectWrite SideEffect = "write"
)

// CallFacts is everything a condition may see.
//
// A struct rather than a map, so that the set of knowable facts is fixed at
// compile time. A map would make "what can a policy read" a runtime question,
// and the honest answer would become "whatever the caller happened to put in".
// Nothing here comes from the Run's business payload.
type CallFacts struct {
	ToolName           string
	ToolRiskLevel      RiskLevel
	ToolSideEffect     SideEffect
	ToolTargetHost     string
	ToolPermissions    []string
	AgentName          string
	DefinitionName     string
	MemoryNamespace    string
	ModelName          string
	PrincipalKind      string
	PrincipalTenant    string
	Labels             []string
	BudgetRemainingPct int
	TimeWindow         string
}

// values returns the fact's value(s) for comparison. Multi-valued facts —
// permissions, labels — compare as sets; single-valued ones as one string.
func (f CallFacts) values(fact Fact) []string {
	switch fact {
	case FactToolName:
		return []string{f.ToolName}
	case FactToolRiskLevel:
		return []string{string(f.ToolRiskLevel)}
	case FactToolSideEffect:
		return []string{string(f.ToolSideEffect)}
	case FactToolTargetHost:
		return []string{f.ToolTargetHost}
	case FactToolPermissions:
		return f.ToolPermissions
	case FactAgentName:
		return []string{f.AgentName}
	case FactDefinitionName:
		return []string{f.DefinitionName}
	case FactMemoryNamespace:
		return []string{f.MemoryNamespace}
	case FactModelName:
		return []string{f.ModelName}
	case FactPrincipalKind:
		return []string{f.PrincipalKind}
	case FactPrincipalTenant:
		return []string{f.PrincipalTenant}
	case FactLabel:
		return f.Labels
	case FactBudgetRemaining:
		return []string{strconv.Itoa(f.BudgetRemainingPct)}
	case FactTimeWindow:
		return []string{f.TimeWindow}
	default:
		return nil
	}
}

// Matches reports whether the condition holds.
//
// Every operator is total and side-effect free: no regular expressions, no
// interpolation, no lookups. The comparison a reviewer reads is the comparison
// that runs.
func (c Condition) Matches(facts CallFacts) bool {
	actual := facts.values(c.Fact)

	switch c.Operator {
	case OpEquals:
		return len(actual) == 1 && actual[0] == c.Values[0]
	case OpNotEquals:
		return !(len(actual) == 1 && actual[0] == c.Values[0])
	case OpIn:
		return slices.ContainsFunc(actual, func(value string) bool {
			return slices.Contains(c.Values, value)
		})
	case OpNotIn:
		return !slices.ContainsFunc(actual, func(value string) bool {
			return slices.Contains(c.Values, value)
		})
	case OpGreaterThan, OpLessThan:
		return compareNumeric(c.Operator, actual, c.Values[0])
	default:
		// An unknown operator never matches. Validation rejects one at publish
		// time; if one reaches here anyway, refusing to match is the safe
		// reading — it cannot turn a deny into an allow.
		return false
	}
}

func compareNumeric(operator Operator, actual []string, want string) bool {
	if len(actual) != 1 {
		return false
	}
	left, err := strconv.Atoi(strings.TrimSpace(actual[0]))
	if err != nil {
		return false
	}
	right, err := strconv.Atoi(strings.TrimSpace(want))
	if err != nil {
		return false
	}
	if operator == OpGreaterThan {
		return left > right
	}
	return left < right
}

// Matches reports whether every condition holds. Conditions are conjunctive, so
// a policy with none matches everything in its scope — which is how a
// scope-wide rule is written.
func (p Policy) Matches(facts CallFacts) bool {
	for _, condition := range p.Conditions {
		if !condition.Matches(facts) {
			return false
		}
	}
	return true
}
