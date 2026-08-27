// Package policy is governance expressed as data.
//
// Changing a rule must not require a release — that is the premise the whole
// governance story rests on. So a Policy is a published resource evaluated
// inside the fixed gateway chain, not a Go function somebody registers.
package policy

import (
	"fmt"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Scope is what a Policy applies to.
type Scope string

const (
	ScopeTenant     Scope = "tenant"
	ScopeDefinition Scope = "definition"
	ScopeAgent      Scope = "agent"
	ScopeTool       Scope = "tool"
	ScopeMemory     Scope = "memory"
	ScopeModel      Scope = "model"
)

// Specificity orders scopes from broadest to narrowest. Two policies that
// reach the same decision level are ordered by this, then by name, so that
// evaluation is deterministic rather than map-order dependent.
func (s Scope) Specificity() int {
	switch s {
	case ScopeTenant:
		return 0
	case ScopeDefinition:
		return 1
	case ScopeAgent:
		return 2
	case ScopeModel:
		return 3
	case ScopeMemory:
		return 4
	case ScopeTool:
		return 5
	default:
		return -1
	}
}

func (s Scope) Valid() bool { return s.Specificity() >= 0 }

// scopeFact names the fact a scope's ScopeName is compared against.
//
// Only the scopes whose fact the Runtime actually populates are here. Tool,
// principal and agent facts are filled on every governed call — the Gateway
// fills the first two and the tool loop the third — while DefinitionName,
// ModelName and MemoryNamespace are declared on CallFacts and never set by any
// live path. A ScopeName on one of those could only ever match nothing, so
// Validate refuses it instead: a rule that publishes cleanly and silently never
// applies is worse than one that will not publish.
//
// Adding a scope here is therefore not a policy change but a claim that
// something now fills its fact, and TestEveryScopeWithANameHasAFactSomethingFills
// is what checks the claim.
func scopeFact(s Scope) (Fact, bool) {
	switch s {
	case ScopeTenant:
		return FactPrincipalTenant, true
	case ScopeAgent:
		return FactAgentName, true
	case ScopeTool:
		return FactToolName, true
	default:
		return "", false
	}
}

// Fact is the closed set of things a condition may test.
//
// Closed, because the alternative is an expression language, and an expression
// language over the Run's payload is a rule engine with unbounded reach: it can
// read business content, it can be made Turing-complete by accident, and no
// reviewer can tell what a given rule will do. Every fact here is declared at
// publish time and knowable without I/O.
type Fact string

const (
	FactToolName        Fact = "tool.name"
	FactToolRiskLevel   Fact = "tool.risk_level"
	FactToolSideEffect  Fact = "tool.side_effect"
	FactToolTargetHost  Fact = "tool.target_host"
	FactToolPermissions Fact = "tool.required_permissions"
	FactAgentName       Fact = "agent.name"
	FactPrincipalKind   Fact = "principal.kind"
	FactPrincipalTenant Fact = "principal.tenant"
	FactLabel           Fact = "label"
	FactBudgetRemaining Fact = "budget.remaining"
)

// Facts returns every declared fact, in a stable order.
func Facts() []Fact {
	return []Fact{
		FactToolName, FactToolRiskLevel, FactToolSideEffect, FactToolTargetHost,
		FactToolPermissions, FactAgentName, FactPrincipalKind, FactPrincipalTenant,
		FactLabel, FactBudgetRemaining,
	}
}

func (f Fact) Valid() bool {
	for _, known := range Facts() {
		if f == known {
			return true
		}
	}
	return false
}

// Operator is the comparison a condition performs. All are side-effect free and
// total; none can loop.
type Operator string

const (
	OpEquals      Operator = "eq"
	OpNotEquals   Operator = "ne"
	OpIn          Operator = "in"
	OpNotIn       Operator = "not_in"
	OpGreaterThan Operator = "gt"
	OpLessThan    Operator = "lt"
)

func (o Operator) Valid() bool {
	switch o {
	case OpEquals, OpNotEquals, OpIn, OpNotIn, OpGreaterThan, OpLessThan:
		return true
	default:
		return false
	}
}

// Decision is what a matching Policy concludes.
type Decision string

const (
	DecisionAllow             Decision = "allow"
	DecisionDeny              Decision = "deny"
	DecisionRequireApproval   Decision = "require_approval"
	DecisionCapBudget         Decision = "cap_budget"
	DecisionRequireReconciler Decision = "require_reconciler"
)

// Decisions returns every decision, in precedence order: deny first, then
// require_approval, then the rest.
//
// Precedence is a property of the decision rather than of the evaluation code
// so that "deny wins" cannot be quietly changed by reordering a loop.
func Decisions() []Decision {
	return []Decision{
		DecisionDeny, DecisionRequireApproval, DecisionRequireReconciler,
		DecisionCapBudget, DecisionAllow,
	}
}

// Precedence returns the ranking used when several policies match. Lower wins.
func (d Decision) Precedence() int {
	for i, known := range Decisions() {
		if d == known {
			return i
		}
	}
	return -1
}

func (d Decision) Valid() bool { return d.Precedence() >= 0 }

// Condition is one side-effect-free comparison against one declared fact.
type Condition struct {
	Fact     Fact     `json:"fact"`
	Operator Operator `json:"operator"`
	Values   []string `json:"values"`
}

// Policy is scope, conditions and a decision. Conditions are conjunctive:
// several rules that must all hold, which keeps a policy readable without
// precedence rules of its own.
type Policy struct {
	Name       string      `json:"name"`
	Scope      Scope       `json:"scope"`
	ScopeName  string      `json:"scope_name,omitempty"`
	Conditions []Condition `json:"conditions"`
	Decision   Decision    `json:"decision"`
	// Shadow evaluates the policy and reports what it would have decided
	// without enforcing it. That is what makes it safe to publish a rule and
	// watch it before it starts refusing things.
	Shadow bool `json:"shadow,omitempty"`
}

// Validate rejects a policy the evaluator could not run deterministically.
func (p Policy) Validate() error {
	if p.Name == "" {
		return run.NewError("missing_name", run.ErrorInvalid, run.RetryNever)
	}
	if !p.Scope.Valid() {
		return run.NewError("unknown_scope", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("scope %q", p.Scope))
	}
	if !p.Decision.Valid() {
		return run.NewError("unknown_decision", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("decision %q", p.Decision))
	}
	if p.ScopeName != "" {
		if _, ok := scopeFact(p.Scope); !ok {
			return run.NewError("unevaluatable_scope_name", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("scope %q carries no fact this Runtime fills, so scope_name %q "+
					"could only ever match nothing", p.Scope, p.ScopeName))
		}
	}
	for _, condition := range p.Conditions {
		if !condition.Fact.Valid() {
			// This is the branch that catches a condition reaching into the
			// Run's business payload: "input.title" is simply not a declared
			// fact, and there is no fallback that would evaluate it.
			return run.NewError("undeclared_fact", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("fact %q is not one of the declared facts", condition.Fact))
		}
		if !condition.Operator.Valid() {
			return run.NewError("unknown_operator", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("operator %q", condition.Operator))
		}
		if len(condition.Values) == 0 {
			return run.NewError("missing_values", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("condition on %q has no values", condition.Fact))
		}
	}
	return nil
}
