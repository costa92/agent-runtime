package replay

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/costa92/agent-runtime/run"
)

func stateChange(sequence uint64, from, to run.State) run.Event {
	return run.Event{Sequence: sequence, Kind: run.EventStateChanged, RunID: "r", From: from, To: to}
}

func TestVerifyWalksARecordedTrajectory(t *testing.T) {
	events := []run.Event{
		stateChange(1, run.StateQueued, run.StateRunning),
		{Sequence: 2, Kind: run.EventBudgetReserved, RunID: "r"},
		stateChange(3, run.StateRunning, run.StateWaitingApproval),
		stateChange(4, run.StateWaitingApproval, run.StateRunning),
		stateChange(5, run.StateRunning, run.StateWaitingResolution),
		stateChange(6, run.StateWaitingResolution, run.StateRunning),
		stateChange(7, run.StateRunning, run.StateSucceeded),
	}

	result, err := Verify(events)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if result.From != run.StateQueued || result.Final != run.StateSucceeded {
		t.Fatalf("walked %q → %q, want queued → succeeded", result.From, result.Final)
	}
	if result.Transitions != 6 || result.Events != 7 {
		t.Fatalf("counted %d transitions over %d events, want 6 over 7", result.Transitions, result.Events)
	}
}

// Retention prunes old events, so a history that starts mid-flight is ordinary
// and must not be mistaken for one that skipped its start.
func TestVerifyStartsFromTheFirstRecordedTransition(t *testing.T) {
	result, err := Verify([]run.Event{
		stateChange(412, run.StateWaitingApproval, run.StateRunning),
		stateChange(413, run.StateRunning, run.StateFailed),
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if result.From != run.StateWaitingApproval {
		t.Fatalf("started from %q, want waiting_approval", result.From)
	}
}

func TestVerifyRejectsASequenceGap(t *testing.T) {
	_, err := Verify([]run.Event{
		stateChange(1, run.StateQueued, run.StateRunning),
		stateChange(3, run.StateRunning, run.StateSucceeded),
	})
	if err == nil || !strings.Contains(err.Error(), "sequence gap") {
		t.Fatalf("err = %v, want a sequence gap", err)
	}
}

func TestVerifyRejectsATransitionThatDoesNotContinueTheWalk(t *testing.T) {
	_, err := Verify([]run.Event{
		stateChange(1, run.StateQueued, run.StateRunning),
		stateChange(2, run.StateWaitingApproval, run.StateRunning),
	})
	if err == nil || !strings.Contains(err.Error(), "was in") {
		t.Fatalf("err = %v, want a mismatched from-state", err)
	}
}

// The property the package exists for: a history the reducer can no longer
// produce is a finding, not a pass.
func TestVerifyRejectsATransitionTheReducerCannotProduce(t *testing.T) {
	_, err := Verify([]run.Event{
		stateChange(1, run.StateWaitingApproval, run.StateWaitingResolution),
	})
	if err == nil || !strings.Contains(err.Error(), "no longer") {
		t.Fatalf("err = %v, want an unreachable transition", err)
	}
}

func TestVerifyAcceptsAnEmptyHistory(t *testing.T) {
	result, err := Verify(nil)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if result.Transitions != 0 || result.Final != "" {
		t.Fatalf("result = %+v, want a zero walk", result)
	}
}

// A kind missing from the probe makes every transition only that kind produces
// look illegal, which would turn this package from a regression detector into a
// source of false findings.
func TestProbeCoversEveryCommandKind(t *testing.T) {
	declared := commandKindConstants(t)
	if len(declared) == 0 {
		t.Fatal("no CommandKind constants parsed; the check is broken, not the probe")
	}

	probed := make(map[run.CommandKind]bool)
	for _, command := range probeCommands() {
		if probed[command.Kind] {
			t.Fatalf("command kind %q probed twice", command.Kind)
		}
		probed[command.Kind] = true
	}

	for _, kind := range declared {
		if !probed[kind] {
			t.Errorf("command kind %q is declared but never probed", kind)
		}
	}
	for kind := range probed {
		if !slices.Contains(declared, kind) {
			t.Errorf("probe issues %q, which is not a declared command kind", kind)
		}
	}
}

// commandKindConstants reads the constants out of the source rather than a Go
// list, because package run publishes no enumeration of them and a hand-kept
// copy here would drift in exactly the direction this test guards.
//
// The whole package is swept rather than one file: moving the constants is a
// refactor nobody would think to update a test for, and the sweep costs
// nothing. The caller's "parsed nothing" check catches the case where they
// leave the package entirely.
func commandKindConstants(t *testing.T) []run.CommandKind {
	t.Helper()

	sources, err := filepath.Glob("../run/*.go")
	if err != nil {
		t.Fatalf("glob run package: %v", err)
	}

	var kinds []run.CommandKind
	fileSet := token.NewFileSet()
	for _, source := range sources {
		parsed, err := parser.ParseFile(fileSet, source, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", source, err)
		}
		kinds = append(kinds, commandKindsIn(parsed)...)
	}
	return kinds
}

func commandKindsIn(parsed *ast.File) []run.CommandKind {
	var kinds []run.CommandKind
	ast.Inspect(parsed, func(node ast.Node) bool {
		spec, isValue := node.(*ast.ValueSpec)
		if !isValue || spec.Type == nil {
			return true
		}
		named, isIdent := spec.Type.(*ast.Ident)
		if !isIdent || named.Name != "CommandKind" {
			return true
		}
		for _, value := range spec.Values {
			literal, isLiteral := value.(*ast.BasicLit)
			if !isLiteral {
				continue
			}
			kinds = append(kinds, run.CommandKind(strings.Trim(literal.Value, `"`)))
		}
		return true
	})
	return kinds
}

// A self-transition is not a transition. Most commands leave the state alone —
// reserving budget, resolving an invocation the same way twice — so a check
// that read the resulting state would make every state its own legal successor
// and would have passed the cancelled → cancelled records this package was
// written to catch, had cancelled not happened to be terminal.
func TestVerifyRejectsARecordedSelfTransition(t *testing.T) {
	_, err := Verify([]run.Event{stateChange(1, run.StateRunning, run.StateRunning)})
	if err == nil || !strings.Contains(err.Error(), "no longer") {
		t.Fatalf("err = %v, want running → running refused", err)
	}
}

func TestReplayCommandsDeterminism(t *testing.T) {
	initial := run.Snapshot{
		ID:    "run-replay-1",
		State: run.StateQueued,
		Budget: run.Budget{
			Envelope: run.Limits{LLMCalls: 10, ToolCalls: 10},
		},
	}

	commands := []run.Command{
		{Kind: run.CommandStart},
		{Kind: run.CommandInvokeTool, InvocationID: "inv-1", Reserve: run.Limits{ToolCalls: 1}},
		{Kind: run.CommandWaitApproval},
		{Kind: run.CommandResume},
		{Kind: run.CommandSucceed},
	}

	history, err := ReplayCommands(initial, commands)
	if err != nil {
		t.Fatalf("ReplayCommands failed: %v", err)
	}

	if len(history) != 5 {
		t.Fatalf("expected 5 replay steps, got %d", len(history))
	}

	if history[0].Snapshot.State != run.StateRunning {
		t.Errorf("step 0 state = %s, want running", history[0].Snapshot.State)
	}
	if history[2].Snapshot.State != run.StateWaitingApproval {
		t.Errorf("step 2 state = %s, want waiting_approval", history[2].Snapshot.State)
	}
	if history[3].Snapshot.State != run.StateRunning {
		t.Errorf("step 3 state = %s, want running", history[3].Snapshot.State)
	}
	if history[4].Snapshot.State != run.StateSucceeded {
		t.Errorf("step 4 state = %s, want succeeded", history[4].Snapshot.State)
	}
}
