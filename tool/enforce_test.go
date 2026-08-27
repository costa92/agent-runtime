package tool

import (
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Every decision the policy package can produce is named in the gateway.
//
// The bug this closes was not a wrong branch but a missing one: the gateway
// tested for deny and require_approval and let everything else fall through to
// the allow path. cap_budget and require_reconciler therefore published, matched,
// and were written into the audit explanation while the call proceeded — which
// reads as governance to anyone who does not grep the gateway.
//
// Naming them did not change what they do; that needs a publish, not a deploy
// (TD-058). What it changes is the next one: a decision added to Decisions()
// without a case fails here instead of silently joining the allow side.
func TestEveryDecisionIsNamedInTheGateway(t *testing.T) {
	spec := Spec{Name: "search_evidence"}

	for _, decision := range policy.Decisions() {
		err := enforce(decision, spec)
		if run.CodeOf(err) == "tool.unknown_policy_decision" {
			t.Errorf("decision %q reached the fail-closed default; give it a case in "+
				"enforce saying what the gateway does about it, even if the answer "+
				"is \"nothing yet\"", decision)
		}
	}
}

// The two that are enforced, and the two that are not — pinned as they are
// rather than as they should be, so that implementing one is a visible edit
// here rather than a silent behaviour change.
func TestWhichDecisionsTheGatewayActuallyEnforces(t *testing.T) {
	for _, testCase := range []struct {
		decision policy.Decision
		refuses  bool
	}{
		{policy.DecisionDeny, true},
		{policy.DecisionAllow, false},
		// Refused later, at the approval stage, not here.
		{policy.DecisionRequireApproval, false},
		// No enforcement point at all. When TD-058 removes these, this table
		// loses two rows and TestEveryDecisionIsNamedInTheGateway keeps holding.
		{policy.DecisionCapBudget, false},
		{policy.DecisionRequireReconciler, false},
	} {
		err := enforce(testCase.decision, Spec{Name: "search_evidence"})
		if got := err != nil; got != testCase.refuses {
			t.Errorf("decision %q refuses=%v, want %v (err=%v)",
				testCase.decision, got, testCase.refuses, err)
		}
	}
}

// A decision that is not a decision fails closed.
//
// Reachable in one way: a Policy document published by a newer binary and read
// by an older one. Proceeding would mean the older binary silently ignoring a
// rule the operator believes is in force.
func TestAnUnrecognisedDecisionIsRefused(t *testing.T) {
	err := enforce(policy.Decision("quarantine"), Spec{Name: "search_evidence"})
	if run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("unrecognised decision = %v, want a denial", err)
	}
}
