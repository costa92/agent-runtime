package agentruntime

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Tool governance and consumption metering must stay portable off this engine.
//
// The nine-stage tool chain, the policy evaluation, the authorization check and
// the quota enforcer are the parts of this Runtime that have no equivalent in
// the frameworks it would otherwise be replaced by: LangGraph, the OpenAI and
// Claude agent SDKs and eino ship neither a risk-classified tool gateway nor a
// reserve-settle ledger. The single-writer state machine, by contrast, is a
// thing durable-execution engines already do better than a single team can.
//
// So the split matters: if the day comes to move the advance loop onto someone
// else's durable substrate, these packages should come across whole. They do
// today — none of them reach this engine's persistence or scheduling, and the
// only run symbols they touch are values and error classes. Nothing was
// enforcing either half, which is the gap these tests close. They assert a
// property that already holds; their job is to fail on the commit that stops it
// holding.
var governancePackages = []string{"./tool", "./quota", "./policy", "./authorization"}

// runStateMachineSymbols are the parts of package run that only mean something
// inside the advance loop: the snapshot, the commands that mutate it, the
// reducer, and what the reducer emits. Budget is here rather than with Limits
// because Limits is an amount and Budget is reduced state — the envelope with
// reservations already subtracted.
var runStateMachineSymbols = map[string]bool{
	"Snapshot":    true,
	"State":       true,
	"Command":     true,
	"CommandKind": true,
	"Reduce":      true,
	"Transition":  true,
	"Budget":      true,
	"Event":       true,
	"EventKind":   true,
	"Effect":      true,
	"EffectKind":  true,
}

// Governance must not bind to how this engine persists or schedules.
//
// The root package is deliberately not in this list: a governance package
// importing it would be an import cycle, so the compiler already refuses it and
// a test asserting it would pass no matter what anyone wrote.
//
// store and workflow are the two that the compiler does allow and that would
// hurt. store is this engine's persistence contract — seventeen fenced methods
// shaped around its revision, lease and epoch. workflow is its graph scheduler.
// A Gateway that reached either would only run on a substrate that reproduced
// them, which is exactly the portability being protected.
var enginePrivatePackages = []string{
	"github.com/kart-io/wechat-account/agent-runtime/store",
	"github.com/kart-io/wechat-account/agent-runtime/workflow",
}

func TestGovernancePackagesDoNotBindToPersistenceOrScheduling(t *testing.T) {
	for _, pkg := range governancePackages {
		command := exec.Command("go", "list", "-deps", "-json", pkg)
		output, err := command.Output()
		if err != nil {
			t.Fatalf("go list %s: %v", pkg, err)
		}

		decoder := json.NewDecoder(strings.NewReader(string(output)))
		seen := 0
		for decoder.More() {
			var described struct{ ImportPath string }
			if err := decoder.Decode(&described); err != nil {
				t.Fatalf("decode go list output for %s: %v", pkg, err)
			}
			seen++
			for _, forbidden := range enginePrivatePackages {
				if described.ImportPath == forbidden {
					t.Fatalf("%s depends on %s; governance that knows this engine's persistence "+
						"or scheduling cannot be lifted onto another durable substrate whole", pkg, forbidden)
				}
			}
		}
		if seen == 0 {
			t.Fatalf("go list returned nothing for %s; the check is broken, not the boundary", pkg)
		}
	}
}

// Depending on package run is fine — governance speaks its error and amount
// vocabulary. Reaching for the reduced state inside it is not.
func TestGovernancePackagesUseOnlyRunValueTypes(t *testing.T) {
	type offence struct{ file, symbol string }
	var offences []offence

	for _, pkg := range governancePackages {
		directory := strings.TrimPrefix(pkg, "./")
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatalf("read %s: %v", directory, err)
		}

		fileSet := token.NewFileSet()
		read := 0
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || filepath.Ext(name) != ".go" {
				continue
			}
			// Test files included: a fixture that builds a Snapshot is the same
			// coupling as production code doing it, and is how the dependency
			// usually arrives first.
			path := filepath.Join(directory, name)
			parsed, err := parser.ParseFile(fileSet, path, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			read++

			ast.Inspect(parsed, func(node ast.Node) bool {
				selector, isSelector := node.(*ast.SelectorExpr)
				if !isSelector {
					return true
				}
				packageIdent, isIdent := selector.X.(*ast.Ident)
				if !isIdent || packageIdent.Name != "run" {
					return true
				}
				if runStateMachineSymbols[selector.Sel.Name] {
					offences = append(offences, offence{path, "run." + selector.Sel.Name})
				}
				return true
			})
		}
		if read == 0 {
			t.Fatalf("no Go files read in %s; the scan is broken, not the boundary", directory)
		}
	}

	if len(offences) > 0 {
		rendered := make([]string, 0, len(offences))
		for _, item := range offences {
			rendered = append(rendered, item.file+" uses "+item.symbol)
		}
		sort.Strings(rendered)
		t.Fatalf("governance reaches into the Run state machine: %v; these packages may speak run's "+
			"errors and amounts, but a Gateway that reads a Snapshot is a Gateway that only works "+
			"inside this engine", rendered)
	}
}
