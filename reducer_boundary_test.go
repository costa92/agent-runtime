package agentruntime_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// Runtime orchestration may assemble commands but only run.Reduce may write
// transitions. Store adapters are outside this directory and own persistence.
func TestOrchestrationCannotWriteTransitionsOutsideReducer(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch found := node.(type) {
			case *ast.CompositeLit:
				selector, ok := found.Type.(*ast.SelectorExpr)
				if ok && selector.Sel.Name == "Transition" && len(found.Elts) > 0 {
					t.Errorf("%s: transition literal bypasses run.Reduce", fileSet.Position(found.Pos()))
				}
			case *ast.AssignStmt:
				for _, lhs := range found.Lhs {
					if writesTransitionNext(lhs) {
						t.Errorf("%s: Transition.Next write bypasses run.Reduce", fileSet.Position(lhs.Pos()))
					}
				}
			}
			return true
		})
	}
}

func writesTransitionNext(expr ast.Expr) bool {
	for {
		switch current := expr.(type) {
		case *ast.SelectorExpr:
			if current.Sel.Name == "Next" {
				return true
			}
			expr = current.X
		case *ast.IndexExpr:
			expr = current.X
		default:
			return false
		}
	}
}
