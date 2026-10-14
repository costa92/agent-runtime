package session

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestReplayUnexecutedDeclarationIsDistinctFromExternalResult(t *testing.T) {
	log := append(base(),
		event(5, ModelRequested, toolRequest("x")),
		event(6, AttemptStarted, AttemptStart{TurnID: "t1", StepID: "step1", ID: "attempt"}),
		event(7, AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "step1", ID: "attempt", Outcome: "succeeded", Message: Message{Role: "assistant", ToolCalls: []ModelToolCall{{ID: "call", Name: "x", Arguments: json.RawMessage(`{}`)}}}}))
	skip := event(8, ToolSkipped, ToolSkip{TurnID: "t1", StepID: "step1", ID: "call", Reason: "approval denied"})
	p, err := Replay(append(log, skip))
	if err != nil {
		t.Fatal(err)
	}
	step := p.Turns[0].Steps[0]
	if len(step.Tools) != 0 || len(step.SkippedTools) != 1 || len(p.History) != 3 || p.History[2].CallID != "call" || p.History[2].Role != "tool" || p.History[2].Error != "approval denied" {
		t.Fatalf("skip invented an invocation or omitted refusal: %+v", p)
	}
	for _, bad := range []Event{
		event(9, ToolSkipped, ToolSkip{TurnID: "t1", StepID: "step1", ID: "call", Reason: "again"}),
		event(9, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "call", Name: "x", Arguments: "{}", IdempotencyKey: "effect"}),
		event(9, ToolSettled, ToolResult{TurnID: "t1", StepID: "step1", ID: "call", Outcome: "succeeded", Result: "invented"}),
		event(9, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "completed", Output: json.RawMessage(`{}`)}),
	} {
		if _, err := Replay(append(append(append([]Event{}, log...), skip), bad)); !errors.Is(err, ErrInvalidLog) {
			t.Fatalf("invalid conversion accepted for %s: %v", bad.Kind, err)
		}
	}
	if _, err := Replay(append(log, event(8, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "failed", Output: json.RawMessage(`{}`)}))); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("failed step dropped unresolved declaration: %v", err)
	}
	settled := append(append(append([]Event{}, log...), skip), event(9, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "failed", Output: json.RawMessage(`{}`)}))
	if _, err := Replay(settled); err != nil {
		t.Fatalf("explicit unexecuted fact must allow failed step: %v", err)
	}
	started := append(append([]Event{}, log...), event(8, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "call", Name: "x", Arguments: "{}", IdempotencyKey: "effect"}))
	skip.Sequence = 9
	if _, err := Replay(append(started, skip)); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("started effect converted to unexecuted: %v", err)
	}
}

func TestReplayRejectsUnmigratedProtocolVersion(t *testing.T) {
	log := base()
	log[0].Version = 1
	if _, err := Replay(log); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("unmigrated protocol accepted: %v", err)
	}
}
