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

// Matches reports whether the call is in the policy's scope and every condition
// holds. Conditions are conjunctive, so a policy with none matches everything in
// its scope — which is how a scope-wide rule is written.
//
// The scope half of that sentence used to be a lie. ScopeName was validated at
// publish, ordered by, and reported in explanations, but never compared against
// anything: a rule written as scope=tool / scope_name=publish_article / deny
// with no conditions read as "deny that one tool" and behaved as "deny every
// tool call in the deployment". The failure direction was the dangerous one —
// more refusal than the author asked for, not less.
//
// The seeded rule that uses ScopeName survived only because it repeats its
// scope_name as a tool.name condition; that duplication was load-bearing and
// nothing said so.
func (p Policy) Matches(facts CallFacts) bool {
	if !p.inScope(facts) {
		return false
	}
	for _, condition := range p.Conditions {
		if !condition.Matches(facts) {
			return false
		}
	}
	return true
}

// inScope compares ScopeName against the fact its scope names.
//
// An empty ScopeName means the whole scope, which is how every tenant-wide rule
// is written. A scope whose fact this Runtime never populates cannot get here
// with a ScopeName set — Validate refuses that at publish, rather than letting
// it through to match nothing for a reason no operator could see.
func (p Policy) inScope(facts CallFacts) bool {
	if p.ScopeName == "" {
		return true
	}
	fact, ok := scopeFact(p.Scope)
	if !ok {
		return false
	}
	values := facts.values(fact)
	return len(values) == 1 && values[0] == p.ScopeName
}
