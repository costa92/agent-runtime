package policy

import (
	"strings"
	"testing"
)

// A scoped rule does not apply outside its scope.
//
// This is the case that was broken, and the direction it broke in is why it
// matters. ScopeName was validated at publish, ordered by, and reported in
// explanations — and never compared against anything. A rule an operator wrote
// as "deny this one tool" denied every tool call in the deployment, and nothing
// in the audit trail would have looked wrong: the explanation names the rule
// that fired, and the rule that fired really was that one.
func TestAScopedRuleDoesNotApplyOutsideItsScope(t *testing.T) {
	rule := Policy{
		Name: "no-publishing", Scope: ScopeTool, ScopeName: "publish_article",
		Decision: DecisionDeny,
	}

	if rule.Matches(CallFacts{ToolName: "read_article"}) {
		t.Fatal("a rule scoped to publish_article matched read_article; " +
			"a condition-less scoped rule is denying the whole deployment")
	}
}

func TestAScopedRuleStillAppliesInsideItsScope(t *testing.T) {
	rule := Policy{
		Name: "no-publishing", Scope: ScopeTool, ScopeName: "publish_article",
		Decision: DecisionDeny,
	}

	if !rule.Matches(CallFacts{ToolName: "publish_article"}) {
		t.Fatal("a rule scoped to publish_article did not match publish_article; " +
			"the scope filter is refusing the calls it exists to catch")
	}
}

// An empty ScopeName means the whole scope. Every tenant-wide rule is written
// that way, including four of the five seeded ones, so reading empty as "match
// nothing" would silently disarm the shipped rule set.
func TestARuleWithNoScopeNameCoversItsWholeScope(t *testing.T) {
	rule := Policy{Name: "writes-need-a-human", Scope: ScopeTenant, Decision: DecisionRequireApproval}

	if !rule.Matches(CallFacts{ToolName: "anything", PrincipalTenant: "acme"}) {
		t.Fatal("a scope-wide rule stopped matching")
	}
}

// Scoping narrows; it never widens. A rule whose conditions do not hold stays
// out even inside its scope.
func TestScopeDoesNotOverrideConditions(t *testing.T) {
	rule := Policy{
		Name: "no-high-risk-publishing", Scope: ScopeTool, ScopeName: "publish_article",
		Conditions: []Condition{{
			Fact: FactToolRiskLevel, Operator: OpEquals, Values: []string{string(RiskHigh)},
		}},
		Decision: DecisionDeny,
	}

	if rule.Matches(CallFacts{ToolName: "publish_article", ToolRiskLevel: RiskLow}) {
		t.Fatal("the scope match overrode a condition that did not hold")
	}
}

// A ScopeName the evaluator could never compare is refused at publication.
//
// DefinitionName, ModelName and MemoryNamespace are declared on CallFacts and
// filled by no live path, so a rule scoped to one of them would publish
// cleanly, appear in every listing, and apply to nothing. Refusing is the
// honest answer: the alternative is a rule that looks enforced and is not,
// which is the whole failure class this file exists to close.
func TestAScopeNameTheRuntimeCannotEvaluateIsRefusedAtPublication(t *testing.T) {
	for _, scope := range []Scope{ScopeModel, ScopeDefinition, ScopeMemory} {
		rule := Policy{
			Name: "pin-it", Scope: scope, ScopeName: "something",
			Decision: DecisionDeny,
		}
		err := rule.Validate()
		if err == nil {
			t.Errorf("scope %q accepted a scope_name it can never evaluate", scope)
			continue
		}
		if !strings.Contains(err.Error(), "unevaluatable_scope_name") {
			t.Errorf("scope %q refused for the wrong reason: %v", scope, err)
		}
	}
}

// The same scopes must still publish without a name: "every model call" is a
// meaningful rule even though "this one model" is not evaluatable yet.
func TestThoseScopesStillPublishWithoutAName(t *testing.T) {
	for _, scope := range []Scope{ScopeModel, ScopeDefinition, ScopeMemory} {
		rule := Policy{Name: "broad", Scope: scope, Decision: DecisionDeny}
		if err := rule.Validate(); err != nil {
			t.Errorf("scope %q without a name was refused: %v", scope, err)
		}
	}
}

// scopeFact is a claim about what the Runtime fills, not a preference.
//
// Adding an entry says "something now populates this fact on every governed
// call". If that is not true, the rules it unlocks match nothing — the state
// this change exists to make unrepresentable. Removing one silently disarms
// every published rule that used it. Either way the edit should be deliberate,
// so it has to come here too.
func TestEveryScopeWithANameHasAFactSomethingFills(t *testing.T) {
	filled := map[Scope]Fact{
		ScopeTenant: FactPrincipalTenant, // Gateway, from the Run's principal
		ScopeAgent:  FactAgentName,       // tool loop, from the node
		ScopeTool:   FactToolName,        // Gateway, from the resolved spec
	}

	for _, scope := range []Scope{
		ScopeTenant, ScopeDefinition, ScopeAgent, ScopeModel, ScopeMemory, ScopeTool,
	} {
		fact, ok := scopeFact(scope)
		want, wantOK := filled[scope]
		if ok != wantOK {
			t.Errorf("scope %q evaluatable=%v, want %v — if a fact really is filled "+
				"now, name what fills it here; if one stopped being filled, every "+
				"published rule scoped to it just went quiet", scope, ok, wantOK)
			continue
		}
		if ok && fact != want {
			t.Errorf("scope %q compares against %q, want %q", scope, fact, want)
		}
	}
}
