package policy

import "testing"

// Retiring a decision takes away writing it, not reading it.
//
// The two are separate because Valid() is consulted by the document decoder,
// which fails a document whole. Deleting these two would therefore make every
// historical version that used one unreadable — and take its legitimate deny
// rules down with it. The split buys the same "cannot be written again" for one
// predicate.
func TestARetiredDecisionIsStillReadableAndNoLongerPublishable(t *testing.T) {
	for _, retired := range RetiredDecisions() {
		if !retired.Valid() {
			t.Errorf("decision %q is no longer readable; a published version using "+
				"it now fails to decode, taking its other rules with it", retired)
		}
		if retired.Publishable() {
			t.Errorf("decision %q can still be written into a new rule", retired)
		}
	}

	for _, live := range []Decision{DecisionDeny, DecisionRequireApproval, DecisionAllow} {
		if !live.Publishable() {
			t.Errorf("decision %q is enforced but cannot be published", live)
		}
	}
}

// Unknown values are untouched by the split. Retirement tolerates two named
// values; it does not open the set.
func TestAnUnknownDecisionIsNeitherReadableNorPublishable(t *testing.T) {
	unknown := Decision("reconcile_later")
	if unknown.Valid() || unknown.Publishable() || unknown.Retired() {
		t.Fatalf("an unrecognised decision was accepted somewhere")
	}
}

// Every decision lands on exactly one side. A sixth added without deciding
// whether it is enforced would otherwise sit in neither list and be publishable
// by default — which is how these two got here.
func TestEveryDecisionIsEitherPublishableOrRetired(t *testing.T) {
	publishable := 0
	for _, decision := range Decisions() {
		if decision.Publishable() {
			publishable++
			continue
		}
		if !decision.Retired() {
			t.Errorf("decision %q is neither publishable nor retired", decision)
		}
	}
	if publishable+len(RetiredDecisions()) != len(Decisions()) {
		t.Fatalf("publishable=%d retired=%d total=%d",
			publishable, len(RetiredDecisions()), len(Decisions()))
	}
}
