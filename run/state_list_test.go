package run

import (
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"testing"
)

// Every declared State constant must be in States().
//
// Parsing this package's own source is the only way to enumerate constants —
// reflection cannot — and the alternative is a hand-kept list, which is the
// second copy States() exists to make checkable.
func TestEveryStateIsListed(t *testing.T) {
	declared := declaredStates(t)
	listed := States()

	for _, state := range declared {
		if !slices.Contains(listed, state) {
			t.Errorf("State %q is declared but missing from States(); a consumer "+
				"checked against the list would not know the machine can reach it", state)
		}
	}
	for _, state := range listed {
		if !slices.Contains(declared, state) {
			t.Errorf("States() has %q, which is not a declared State constant", state)
		}
	}
	if len(listed) != len(declared) {
		t.Errorf("States() has %d entries for %d constants; one is repeated",
			len(listed), len(declared))
	}
}

// Every state is terminal, waiting, or neither — and never two of those.
func TestNoStateIsBothTerminalAndWaiting(t *testing.T) {
	for _, state := range States() {
		if state.Terminal() && state.Waiting() {
			t.Errorf("%q is both terminal and waiting; a worker cannot decide "+
				"whether to park it or stop", state)
		}
	}
}

func declaredStates(t *testing.T) []State {
	t.Helper()

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "state.go", nil, 0)
	if err != nil {
		t.Fatalf("parse state.go: %v", err)
	}

	var declared []State
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}
		typeName, ok := spec.Type.(*ast.Ident)
		if !ok || typeName.Name != "State" {
			return true
		}
		for _, value := range spec.Values {
			literal, ok := value.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				continue
			}
			declared = append(declared, State(literal.Value[1:len(literal.Value)-1]))
		}
		return true
	})

	if len(declared) == 0 {
		t.Fatal("no State constants were found; the walk is broken, not the list")
	}
	return declared
}
