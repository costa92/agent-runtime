package conformance

import (
	"fmt"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"testing"
)

// PolicyCase is one expectation about a deployment's policy set.
type PolicyCase struct {
	Name  string
	Facts policy.CallFacts
	Want  policy.Decision
}

// PolicySet checks a deployment's own published policy set.
//
// Governance is data, so it is tested like data. Without this, "what does our
// policy set actually decide" is answerable only by running the system and
// watching — and the interesting cases are the ones nobody wants to run: the
// high-risk write, the revoked tenant, the tool nobody meant to allow.
//
// It also checks the set is internally sound: every policy validates, and no
// two share a name. Duplicate names are worth failing on because the tie-break
// falls back to the name, so two policies called the same thing resolve in an
// order nothing defines.
func PolicySet(t *testing.T, snapshot policy.Snapshot, cases []PolicyCase) {
	t.Helper()

	t.Run("EveryPolicyValidates", func(t *testing.T) {
		for _, policy := range snapshot.Policies {
			if err := policy.Validate(); err != nil {
				t.Errorf("policy %q is not publishable: %v", policy.Name, err)
			}
		}
	})

	t.Run("PolicyNamesAreUnique", func(t *testing.T) {
		seen := map[string]bool{}
		for _, policy := range snapshot.Policies {
			if seen[policy.Name] {
				t.Errorf("policy %q is declared twice; ties would resolve in no defined order", policy.Name)
			}
			seen[policy.Name] = true
		}
	})

	t.Run("DefaultIsDeclared", func(t *testing.T) {
		// A zero policy.DefaultRule is deny-for-high-risk and allow-for-read-only,
		// which is a reasonable default but not one anyone chose. Saying so out
		// loud is the point of the check.
		if snapshot.Default == (policy.DefaultRule{}) {
			t.Log("the policy set relies on the implicit default: deny high-risk, allow read-only")
		}
	})

	for _, testCase := range cases {
		t.Run(testCase.Name, func(t *testing.T) {
			explanation, err := policy.Evaluate(snapshot, testCase.Facts)
			if err != nil {
				t.Fatalf("evaluate: %v", err)
			}
			if explanation.Decision != testCase.Want {
				t.Fatalf("decision=%s want=%s\n%s", explanation.Decision, testCase.Want, describe(explanation))
			}
		})
	}
}

func describe(explanation policy.Explanation) string {
	if explanation.FromDefault {
		return fmt.Sprintf("no policy matched; the default applied (snapshot %s)", explanation.SnapshotDigest)
	}
	description := fmt.Sprintf("matched (snapshot %s):", explanation.SnapshotDigest)
	for _, match := range explanation.Matched {
		note := ""
		switch {
		case match.Shadow:
			note = " [shadow]"
		case explanation.Deciding != nil && explanation.Deciding.Name == match.Name:
			// Which rule decided is the first thing a failing case needs, and
			// the list order does not show it: the entries above it may have
			// been shadows.
			note = " [deciding]"
		}
		description += fmt.Sprintf("\n  %s (%s) → %s%s", match.Name, match.Scope, match.Decision, note)
	}
	if explanation.TightenedBy != "" {
		description += "\n  tightened by " + explanation.TightenedBy
	}
	return description
}
