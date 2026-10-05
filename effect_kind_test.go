package agentruntime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"testing"

	"github.com/costa92/agent-runtime/run"
)

// The three governed effects must stay distinguishable at the point they are
// issued.
//
// begin() used to hard-code CommandInvokeTool for all three, so every model
// call and every memory write produced an EffectToolCall. Nothing consumes
// Transition.Effects today, which is exactly why the regression was invisible:
// the only other thing separating a model call from a memory write in the
// ledger is that both leave `tool` empty, which separates them from a tool call
// and not from each other. A test that reads the call sites is the cheapest
// thing that fails when someone collapses them again.
func TestBeginIssuesOneCommandKindPerGovernedEffect(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "governed_ports.go", nil, 0)
	if err != nil {
		t.Fatalf("parse governed_ports.go: %v", err)
	}

	var kinds []string
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, isCall := node.(*ast.CallExpr)
		if !isCall {
			return true
		}
		selector, isSelector := call.Fun.(*ast.SelectorExpr)
		if !isSelector || selector.Sel.Name != "begin" {
			return true
		}
		if len(call.Args) < 2 {
			t.Fatalf("s.begin call with %d args; the kind argument is missing", len(call.Args))
		}
		kindArg, isSelectorArg := call.Args[1].(*ast.SelectorExpr)
		if !isSelectorArg {
			t.Fatalf("s.begin second argument is %T, not a run.Command* constant", call.Args[1])
		}
		kinds = append(kinds, kindArg.Sel.Name)
		return true
	})

	sort.Strings(kinds)
	want := []string{"CommandInvokeModel", "CommandInvokeTool", "CommandWriteMemory"}
	if len(kinds) != len(want) {
		t.Fatalf("found %d s.begin call sites (%v), want %d — one per governed effect", len(kinds), kinds, len(want))
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("s.begin call sites pass %v, want %v; collapsing two of them back onto one "+
				"command kind is what made EffectModelCall and EffectMemoryWrite unreachable", kinds, want)
		}
	}
}

// The mapping the call sites depend on. If reserveEffect stopped distinguishing
// these, the test above would still pass and mean nothing.
func TestReserveEffectMapsEachCommandToItsOwnEffect(t *testing.T) {
	base := run.Snapshot{
		ID:     "run-effect-kinds",
		State:  run.StateRunning,
		Budget: run.Budget{},
	}

	for command, want := range map[run.CommandKind]run.EffectKind{
		run.CommandInvokeModel: run.EffectModelCall,
		run.CommandInvokeTool:  run.EffectToolCall,
		run.CommandWriteMemory: run.EffectMemoryWrite,
	} {
		transition, err := run.Reduce(base, run.Command{
			Kind:         command,
			InvocationID: "inv-1",
		})
		if err != nil {
			t.Fatalf("%s: %v", command, err)
		}
		if len(transition.Effects) != 1 {
			t.Fatalf("%s produced %d effects, want 1", command, len(transition.Effects))
		}
		if got := transition.Effects[0].Kind; got != want {
			t.Fatalf("%s produced effect %q, want %q", command, got, want)
		}
	}
}
