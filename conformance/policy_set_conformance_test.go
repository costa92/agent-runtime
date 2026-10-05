package conformance_test

import (
	"testing"

	"github.com/costa92/agent-runtime/conformance"
	"github.com/costa92/agent-runtime/policy"
)

// 一致性套件本身要被跑过，否则「写了套件」和「套件能用」是两件事。
//
// store/core_conformance_test.go 用内存 Store 把 store 的套件挂起来，这里对
// conformance.PolicySet 做同一件事：拿一个有代表性的策略集当被测数据，既覆盖
// 套件自带的三项结构检查（每条策略可发布、名字不重复、默认规则是否显式声明），
// 也覆盖用例驱动的决策断言。套件里任何一项检查退化成空转，这个测试会一起垮。
func referencePolicySet() policy.Snapshot {
	return policy.Snapshot{
		Digest: "conformance-reference",
		Default: policy.DefaultRule{
			HighRisk: policy.DecisionDeny,
			ReadOnly: policy.DecisionAllow,
		},
		Policies: []policy.Policy{{
			// 高风险写操作要人批，而不是直接拒——这是产品要的形状。
			Name:     "high-risk-writes-need-approval",
			Scope:    policy.ScopeTool,
			Decision: policy.DecisionRequireApproval,
			Conditions: []policy.Condition{{
				Fact:     policy.FactToolRiskLevel,
				Operator: policy.OpEquals,
				Values:   []string{string(policy.RiskHigh)},
			}},
		}, {
			// 被吊销的租户压过上面那条：deny 的级别高于 require_approval。
			Name:     "revoked-tenant-denied",
			Scope:    policy.ScopeTenant,
			Decision: policy.DecisionDeny,
			Conditions: []policy.Condition{{
				Fact:     policy.FactPrincipalTenant,
				Operator: policy.OpEquals,
				Values:   []string{"revoked"},
			}},
		}, {
			// shadow 策略只观察不生效，发布新规则前要能这么试。
			Name:     "observe-only-search-deny",
			Scope:    policy.ScopeTool,
			Decision: policy.DecisionDeny,
			Shadow:   true,
			Conditions: []policy.Condition{{
				Fact:     policy.FactToolName,
				Operator: policy.OpEquals,
				Values:   []string{"search"},
			}},
		}},
	}
}

func TestReferencePolicySetConformance(t *testing.T) {
	conformance.PolicySet(t, referencePolicySet(), []conformance.PolicyCase{{
		Name: "ReadOnlySearchFallsToTheDefault",
		Facts: policy.CallFacts{
			ToolName:        "search",
			ToolRiskLevel:   policy.RiskLow,
			ToolSideEffect:  policy.SideEffectRead,
			AgentName:       "researcher",
			PrincipalTenant: "acme",
		},
		Want: policy.DecisionAllow,
	}, {
		Name: "HighRiskPublishNeedsApproval",
		Facts: policy.CallFacts{
			ToolName:        "publish",
			ToolRiskLevel:   policy.RiskHigh,
			ToolSideEffect:  policy.SideEffectWrite,
			AgentName:       "writer",
			PrincipalTenant: "acme",
		},
		Want: policy.DecisionRequireApproval,
	}, {
		Name: "RevokedTenantIsDeniedEvenWhereApprovalWouldApply",
		Facts: policy.CallFacts{
			ToolName:        "publish",
			ToolRiskLevel:   policy.RiskHigh,
			ToolSideEffect:  policy.SideEffectWrite,
			AgentName:       "writer",
			PrincipalTenant: "revoked",
		},
		Want: policy.DecisionDeny,
	}, {
		Name: "UnmatchedWriteFallsToTheHighRiskDefault",
		Facts: policy.CallFacts{
			ToolName:        "upload_media",
			ToolRiskLevel:   policy.RiskLow,
			ToolSideEffect:  policy.SideEffectWrite,
			AgentName:       "writer",
			PrincipalTenant: "acme",
		},
		Want: policy.DecisionDeny,
	}})
}
