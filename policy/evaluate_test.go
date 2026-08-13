package policy_test

import (
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

func publishFacts() policy.CallFacts {
	return policy.CallFacts{
		ToolName:       "publish",
		ToolRiskLevel:  policy.RiskHigh,
		ToolSideEffect: policy.SideEffectWrite,
		AgentName:      "writer",
	}
}

func matchPublish(name string, scope policy.Scope, decision policy.Decision) policy.Policy {
	return policy.Policy{
		Name: name, Scope: scope, Decision: decision,
		Conditions: []policy.Condition{{
			Fact: policy.FactToolName, Operator: policy.OpEquals, Values: []string{"publish"},
		}},
	}
}

// deny wins over everything, and require_approval over the rest. Taken from the
// Decision type so reordering a loop cannot change it.
func TestDenyBeatsRequireApprovalBeatsAllow(t *testing.T) {
	snapshot := policy.Snapshot{Policies: []policy.Policy{
		matchPublish("c-allow", policy.ScopeTool, policy.DecisionAllow),
		matchPublish("a-deny", policy.ScopeTenant, policy.DecisionDeny),
		matchPublish("b-approve", policy.ScopeTool, policy.DecisionRequireApproval),
	}}

	explanation, err := policy.Evaluate(snapshot, publishFacts())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if explanation.Decision != policy.DecisionDeny {
		t.Fatalf("decision=%s want=deny", explanation.Decision)
	}
	// A tenant-wide deny beats a tool-scoped allow: specificity breaks ties
	// within a decision level, it does not outrank the level itself.
	if len(explanation.Matched) != 3 {
		t.Fatalf("matched %d policies, want all 3 reported", len(explanation.Matched))
	}
}

// Two rules at the same level need a total order, or the outcome depends on
// which one the map happened to yield first.
func TestTiesBreakOnSpecificityThenName(t *testing.T) {
	snapshot := policy.Snapshot{Policies: []policy.Policy{
		matchPublish("z-tenant", policy.ScopeTenant, policy.DecisionRequireApproval),
		matchPublish("a-tool", policy.ScopeTool, policy.DecisionRequireApproval),
	}}

	explanation, err := policy.Evaluate(snapshot, publishFacts())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if explanation.Matched[0].Name != "a-tool" {
		t.Fatalf("the more specific rule did not sort first: %+v", explanation.Matched)
	}

	sameScope := policy.Snapshot{Policies: []policy.Policy{
		matchPublish("b", policy.ScopeTool, policy.DecisionAllow),
		matchPublish("a", policy.ScopeTool, policy.DecisionAllow),
	}}
	explanation, err = policy.Evaluate(sameScope, publishFacts())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if explanation.Matched[0].Name != "a" {
		t.Fatalf("equally specific rules did not sort by name: %+v", explanation.Matched)
	}
}

// Same inputs, same decision, same explanation. Anything else and an operator
// cannot reproduce a refusal they are being asked about.
func TestEvaluationIsDeterministic(t *testing.T) {
	snapshot := policy.Snapshot{Digest: "snap-1", Policies: []policy.Policy{
		matchPublish("a", policy.ScopeTool, policy.DecisionRequireApproval),
		matchPublish("b", policy.ScopeTenant, policy.DecisionRequireApproval),
		matchPublish("c", policy.ScopeAgent, policy.DecisionAllow),
	}}

	first, err := policy.Evaluate(snapshot, publishFacts())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	for range 20 {
		again, err := policy.Evaluate(snapshot, publishFacts())
		if err != nil {
			t.Fatalf("evaluate: %v", err)
		}
		if again.Decision != first.Decision || len(again.Matched) != len(first.Matched) {
			t.Fatal("evaluation is not deterministic")
		}
		for i := range again.Matched {
			if again.Matched[i] != first.Matched[i] {
				t.Fatalf("explanation differs between runs:\n%+v\n%+v", first.Matched, again.Matched)
			}
		}
	}
	if first.SnapshotDigest != "snap-1" {
		t.Error("the explanation does not name the rule set it came from")
	}
}

// The safe default differs by tool, so it is configured rather than guessed:
// guessing either way is wrong half the time.
func TestDefaultDeniesHighRiskAndAllowsReadOnly(t *testing.T) {
	empty := policy.Snapshot{}

	high, err := policy.Evaluate(empty, publishFacts())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if high.Decision != policy.DecisionDeny || !high.FromDefault {
		t.Fatalf("high-risk default=%s want=deny", high.Decision)
	}

	readOnly, err := policy.Evaluate(empty, policy.CallFacts{
		ToolName: "search", ToolRiskLevel: policy.RiskLow, ToolSideEffect: policy.SideEffectRead,
	})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if readOnly.Decision != policy.DecisionAllow || !readOnly.FromDefault {
		t.Fatalf("read-only default=%s want=allow", readOnly.Decision)
	}
}

// A strategy exists for conditions the declared facts cannot express. It may
// narrow the outcome and never widen it: a compiled-in loosening would be
// invisible in the published policy set an operator reads.
func TestStrategyMayTightenButNeverLoosen(t *testing.T) {
	allowSnapshot := policy.Snapshot{
		Default: policy.DefaultRule{HighRisk: policy.DecisionAllow},
	}

	tightened, err := policy.Evaluate(allowSnapshot, publishFacts(), tightener{to: policy.DecisionDeny})
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if tightened.Decision != policy.DecisionDeny {
		t.Fatalf("decision=%s want=deny", tightened.Decision)
	}
	if tightened.TightenedBy != "test-strategy" {
		t.Error("the explanation does not name the strategy that narrowed it")
	}

	denySnapshot := policy.Snapshot{Policies: []policy.Policy{
		matchPublish("no-publish", policy.ScopeTool, policy.DecisionDeny),
	}}
	_, err = policy.Evaluate(denySnapshot, publishFacts(), tightener{to: policy.DecisionAllow})
	if run.KindOf(err) != run.ErrorInternal {
		t.Fatalf("loosening error=%s want=internal", run.KindOf(err))
	}
}

// A snapshot is a value. A publish that lands mid-Run cannot reach one already
// in flight, so the same call cannot be allowed at step three and denied at
// step four.
func TestSnapshotIsStableAcrossAPublish(t *testing.T) {
	held := policy.Snapshot{
		Digest:  "snap-1",
		Default: policy.DefaultRule{HighRisk: policy.DecisionAllow},
	}

	before, err := policy.Evaluate(held, publishFacts())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	// An operator publishes a deny. The Run holds its own snapshot.
	_ = policy.Snapshot{Digest: "snap-2", Policies: []policy.Policy{
		matchPublish("no-publish", policy.ScopeTool, policy.DecisionDeny),
	}}

	after, err := policy.Evaluate(held, publishFacts())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if before.Decision != after.Decision || after.SnapshotDigest != "snap-1" {
		t.Fatal("the Run's snapshot drifted mid-flight")
	}
}

func TestConditionsAreConjunctive(t *testing.T) {
	both := policy.Policy{
		Name: "high-risk-writer", Scope: policy.ScopeTool, Decision: policy.DecisionDeny,
		Conditions: []policy.Condition{
			{Fact: policy.FactToolRiskLevel, Operator: policy.OpEquals, Values: []string{"high"}},
			{Fact: policy.FactAgentName, Operator: policy.OpEquals, Values: []string{"reviewer"}},
		},
	}
	snapshot := policy.Snapshot{
		Policies: []policy.Policy{both},
		Default:  policy.DefaultRule{HighRisk: policy.DecisionAllow},
	}

	// Risk matches, agent does not: the policy must not fire.
	explanation, err := policy.Evaluate(snapshot, publishFacts())
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if explanation.Decision != policy.DecisionAllow || len(explanation.Matched) != 0 {
		t.Fatalf("a partially matching policy fired: %+v", explanation)
	}
}

func TestOperatorsCompareDeclaredFacts(t *testing.T) {
	facts := publishFacts()
	facts.ToolPermissions = []string{"publish:write", "media:read"}
	facts.BudgetRemainingPct = 10

	cases := map[string]struct {
		condition policy.Condition
		want      bool
	}{
		"in on a multi-valued fact": {
			policy.Condition{Fact: policy.FactToolPermissions, Operator: policy.OpIn, Values: []string{"publish:write"}}, true,
		},
		"not_in on a multi-valued fact": {
			policy.Condition{Fact: policy.FactToolPermissions, Operator: policy.OpNotIn, Values: []string{"admin"}}, true,
		},
		"less_than on budget headroom": {
			policy.Condition{Fact: policy.FactBudgetRemaining, Operator: policy.OpLessThan, Values: []string{"20"}}, true,
		},
		"greater_than on budget headroom": {
			policy.Condition{Fact: policy.FactBudgetRemaining, Operator: policy.OpGreaterThan, Values: []string{"20"}}, false,
		},
		"not_equals": {
			policy.Condition{Fact: policy.FactAgentName, Operator: policy.OpNotEquals, Values: []string{"reviewer"}}, true,
		},
	}
	for name, tc := range cases {
		if got := tc.condition.Matches(facts); got != tc.want {
			t.Errorf("%s: matched=%v want=%v", name, got, tc.want)
		}
	}
}

type tightener struct{ to policy.Decision }

func (tightener) Name() string { return "test-strategy" }

func (s tightener) Tighten(policy.Decision, policy.CallFacts) policy.Decision { return s.to }
