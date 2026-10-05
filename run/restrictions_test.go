package run_test

import (
	"slices"
	"testing"

	"github.com/costa92/agent-runtime/run"
)

// A restriction can only remove.
//
// The direction is the whole point. A Run-scoped list that could add would be a
// privilege escalation performed by whoever started the Run, and the published
// policy that was supposed to decide would never see it — the tool would simply
// be in the allowlist by the time anything looked.
func TestARestrictionCannotGrantAToolTheDefinitionNeverDeclared(t *testing.T) {
	declared := []string{"search"}
	restricted := run.Restrictions{ToolNarrowing: []string{"search", "publish"}}

	allowed := restricted.Narrow(declared)
	if slices.Contains(allowed, "publish") {
		t.Fatalf("allowed = %v; a restriction added a tool", allowed)
	}
	if !slices.Contains(allowed, "search") {
		t.Fatalf("allowed = %v; the declared tool was lost", allowed)
	}
}

// An empty restriction means "no narrowing", not "nothing allowed".
//
// The other reading would silently disarm every Run that did not ask to be
// restricted — every Run in production today.
func TestAnEmptyRestrictionLeavesTheDeclaredToolsAlone(t *testing.T) {
	declared := []string{"search", "publish"}
	allowed := run.Restrictions{}.Narrow(declared)

	if len(allowed) != len(declared) {
		t.Fatalf("allowed = %v, want %v", allowed, declared)
	}
}

// A restriction naming nothing the Definition declared allows nothing, rather
// than falling back to everything.
func TestADisjointRestrictionAllowsNothing(t *testing.T) {
	allowed := run.Restrictions{ToolNarrowing: []string{"publish"}}.Narrow([]string{"search"})
	if len(allowed) != 0 {
		t.Fatalf("allowed = %v", allowed)
	}
}
