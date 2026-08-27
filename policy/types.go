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
//
// Only scopes this Runtime can actually evaluate exist. There used to be three
// more — definition, model and memory — and none of them had an evaluation
// point: Evaluate has one production caller, the tool gateway, so a model call
// and a memory read never reach a policy at all. A rule scoped to one of them
// published cleanly, appeared in every listing, was pinned into every Run's
// policy_digest, and governed nothing.
//
// Deleting them rather than wiring them up, because wiring them up was the
// larger mistake. It would have meant re-declaring the model.name and
// memory.namespace facts that were removed for being unfillable, and then
// leaving every tool fact empty on those paths — where a tenant-wide rule like
// "tool.side_effect != write" would not fail to apply but would apply wrongly,
// matching every model call. Both paths already have their own admission
// (undeclared_memory_key, memory_not_writable, the Definition's ModelPolicy and
// the models registry), so a policy layer there would be a second gatekeeper
// that can disagree with the first, not a missing one.
//
// Re-adding a scope is a claim that something fills its fact — scopeFact is
// where the claim is written down and TestEveryScopeIsEvaluatable is what
// refuses one made without evidence.
type Scope string

const (
	ScopeTenant Scope = "tenant"
	ScopeAgent  Scope = "agent"
	ScopeTool   Scope = "tool"
)

// Scopes returns every declared scope, broadest first.
//
// An enumeration rather than a switch on its own, matching Facts and Decisions,
// so that "what scopes exist" is one list a test can walk. A new constant that
// is not added here is not a scope: Valid rejects it and no rule using it
// publishes, which is the failure direction that says something rather than the
// one that goes quiet.
func Scopes() []Scope {
	return []Scope{ScopeTenant, ScopeAgent, ScopeTool}
}

// Specificity orders scopes from broadest to narrowest. Two policies that
// reach the same decision level are ordered by this, then by name, so that
// evaluation is deterministic rather than map-order dependent.
//
// Only the relative order carries meaning, so closing the gaps the deleted
// scopes left behind changes nothing about how any published rule sorts.
func (s Scope) Specificity() int {
	for i, known := range Scopes() {
		if s == known {
			return i
		}
	}
	return -1
}

func (s Scope) Valid() bool { return s.Specificity() >= 0 }

// scopeFact names the fact a scope's ScopeName is compared against.
//
// Every Scope has an entry, and that is now an invariant rather than a
// coincidence: a scope with no fact is one whose ScopeName could only ever
// match nothing, which is exactly the state the deleted three were in. Adding a
// scope is therefore not a naming decision but a claim that something fills its
// fact on every governed call — the Gateway fills tenant and tool from the Run's
// principal and the resolved spec, the tool loop fills agent from the node.
//
// The bool is kept for a Scope that came off the wire as an unknown string. It
// fails closed: an unrecognised scope matches nothing rather than everything.
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
	// There is no separate "this scope cannot be evaluated" rejection any more.
	// It existed to catch a ScopeName on definition, model or memory; now that
	// every declared Scope has a fact, the unknown_scope check above is the only
	// way to reach an unevaluatable one.
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
