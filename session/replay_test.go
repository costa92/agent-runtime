package session

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func event(seq uint64, kind Kind, payload any) Event {
	b, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return Event{Version: ProtocolVersion, SessionID: "s1", Sequence: seq, Kind: kind, Payload: b}
}

func base() []Event {
	return []Event{
		event(1, SessionStarted, SessionStart{TenantID: "tenant", PrincipalID: "person", ConfigDigest: "cfg", PluginDigest: "plugins"}),
		event(2, TurnStarted, TurnStart{ID: "t1"}),
		event(3, MessageAccepted, AcceptedMessage{TurnID: "t1", Message: Message{Role: "user", Content: "hello"}}),
		event(4, StepStarted, StepStart{TurnID: "t1", ID: "step1", StageKey: "writer", Kind: StageModel, Input: json.RawMessage(`{}`), AllowedTools: []string{"lookup", "x", "inspect", "tool"}}),
	}
}

func toolRequest(name string) ModelRequest {
	return ModelRequest{TurnID: "t1", StepID: "step1", ConfigDigest: "cfg", Profile: "profile", HistoryThrough: 4, History: []Message{{Role: "user", Content: "hello"}}, Tools: []ModelToolDef{{Name: name, Parameters: json.RawMessage(`{}`)}}}
}

func TestSessionParkAndResume(t *testing.T) {
	start := event(1, SessionStarted, SessionStart{TenantID: "tenant", PrincipalID: "person", ConfigDigest: "cfg", PluginDigest: "plugins"})
	park := event(2, SessionParked, SessionPark{Reason: "missing pinned profile"})
	resume := event(3, SessionResumed, SessionResume{Reason: "profile restored"})
	p, err := Replay([]Event{start, park})
	if err != nil || p.ParkedReason != "missing pinned profile" {
		t.Fatalf("park projection: %+v %v", p, err)
	}
	if _, err := Replay([]Event{start, park, event(3, TurnStarted, TurnStart{ID: "t"})}); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("ordinary event while parked: %v", err)
	}
	if _, err := Replay([]Event{start, resume}); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("resume unparked: %v", err)
	}
	if _, err := Replay([]Event{start, event(2, SessionParked, SessionPark{})}); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("empty park reason: %v", err)
	}
	if _, err := Replay([]Event{start, park, event(3, SessionResumed, SessionResume{})}); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("empty resume reason: %v", err)
	}
	p, err = Replay([]Event{start, park, resume, event(4, TurnStarted, TurnStart{ID: "t"})})
	if err != nil || p.ParkedReason != "" || p.Turns[0].ID != "t" {
		t.Fatalf("resume projection: %+v %v", p, err)
	}
}

func TestReasoningContentInModelHistory(t *testing.T) {
	events := append(base(),
		event(5, ModelRequested, toolRequest("x")),
		event(6, AttemptStarted, AttemptStart{TurnID: "t1", StepID: "step1", ID: "a"}),
		event(7, AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "step1", ID: "a", Outcome: "succeeded", Message: Message{Role: "assistant", ReasoningContent: "reasoned", ToolCalls: []ModelToolCall{{ID: "c", Name: "x", Arguments: json.RawMessage(`null`)}}}}),
	)
	p, err := Replay(events)
	if err != nil || len(p.History) != 2 || p.History[1].ReasoningContent != "reasoned" {
		t.Fatalf("reasoning history: %+v %v", p, err)
	}
	if _, err := Replay(append(events, event(8, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "completed", Output: json.RawMessage(`{}`)}))); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("unsettled tool call: %v", err)
	}
	bad := base()
	bad[2] = event(3, MessageAccepted, AcceptedMessage{TurnID: "t1", Message: Message{Role: "user", ReasoningContent: "secret"}})
	if _, err := Replay(bad); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("user reasoning accepted: %v", err)
	}
	bad = base()
	bad[2] = event(3, MessageAccepted, AcceptedMessage{TurnID: "t1", Message: Message{Role: "tool", ReasoningContent: "secret"}})
	if _, err := Replay(bad); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("tool reasoning accepted: %v", err)
	}
	reasoningOnly := append(base(),
		event(5, ModelRequested, ModelRequest{TurnID: "t1", StepID: "step1", ConfigDigest: "cfg", Profile: "profile", HistoryThrough: 4, History: []Message{{Role: "user", Content: "hello"}}}),
		event(6, AttemptStarted, AttemptStart{TurnID: "t1", StepID: "step1", ID: "reasoning"}),
		event(7, AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "step1", ID: "reasoning", Outcome: "succeeded", Message: Message{Role: "assistant", ReasoningContent: "thought"}}),
		event(8, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "completed", Output: json.RawMessage(`{}`)}),
	)
	if _, err := Replay(reasoningOnly); err != nil {
		t.Fatalf("reasoning-only model output: %v", err)
	}
}

func TestReplayModelAndTool(t *testing.T) {
	events := append(base(),
		event(5, ModelRequested, toolRequest("lookup")),
		event(6, AttemptStarted, AttemptStart{TurnID: "t1", StepID: "step1", ID: "a1"}),
		event(7, AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "step1", ID: "a1", Outcome: "succeeded", Message: Message{Role: "assistant", Content: "checking", ToolCalls: []ModelToolCall{{ID: "call1", Name: "lookup", Arguments: json.RawMessage(`{}`)}}}}),
		event(8, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "call1", Name: "lookup", Arguments: "{}", IdempotencyKey: "idem1"}),
		event(9, ToolSettled, ToolResult{TurnID: "t1", StepID: "step1", ID: "call1", Outcome: "succeeded", Result: "found"}),
		event(10, BudgetSettled, BudgetSettlement{TurnID: "t1", StepID: "step1", Category: BudgetInputTokens, Units: 3}),
		event(11, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "completed", Output: json.RawMessage(`{}`)}),
		event(12, TurnEnded, TurnEnd{TurnID: "t1", Status: "completed"}),
	)
	p, err := Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	if p.Sequence != 12 || p.Turns[0].Status != "completed" || p.Turns[0].Steps[0].Budget[BudgetInputTokens] != 3 || len(p.Turns[0].Messages) != 2 || p.Turns[0].Messages[1].Content != "checking" {
		t.Fatalf("bad projection: %+v", p)
	}
	history, err := ModelHistory(events)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(history, p.History) || len(history) != 3 || len(history[1].ToolCalls) != 1 || history[2].Role != "tool" {
		t.Fatalf("bad history: %+v", history)
	}
	for i := 1; i <= len(events); i++ {
		if _, err := Replay(events[:i]); err != nil {
			t.Fatalf("prefix %d: %v", i, err)
		}
	}
	b, _ := json.Marshal(events)
	var roundtrip []Event
	if err := json.Unmarshal(b, &roundtrip); err != nil {
		t.Fatal(err)
	}
	again, err := Replay(roundtrip)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p, again) {
		t.Fatalf("projection changed after serialization")
	}
}

func TestInterruptedAttemptAndUnknownToolResult(t *testing.T) {
	events := append(base(),
		event(5, ModelRequested, toolRequest("x")),
		event(6, AttemptStarted, AttemptStart{TurnID: "t1", StepID: "step1", ID: "a1"}),
	)
	p, err := Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	if p.Turns[0].Steps[0].ActiveAttempt != "a1" {
		t.Fatal("lost in-flight attempt")
	}
	bad := append(append([]Event{}, events...), event(7, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "failed", Output: json.RawMessage(`{}`)}))
	if _, err := Replay(bad); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("expected invalid interruption, got %v", err)
	}
	events = append(events, event(7, AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "step1", ID: "a1", Outcome: "failed", Error: "timeout"}), event(8, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "failed", Output: json.RawMessage(`{}`)}))
	if _, err := Replay(events); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidOrderAndDuplicateSettlement(t *testing.T) {
	tests := map[string][]Event{
		"sequence gap":            func() []Event { x := base(); x[2].Sequence = 4; return x }(),
		"request prefix mismatch": append(base(), event(5, ModelRequested, ModelRequest{TurnID: "t1", StepID: "step1", ConfigDigest: "cfg", Profile: "profile", HistoryThrough: 3})),
		"turn before step end":    append(base(), event(5, TurnEnded, TurnEnd{TurnID: "t1", Status: "completed"})),
		"tool before model":       append(base(), event(5, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "c", Name: "x", IdempotencyKey: "k"})),
	}
	for name, events := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Replay(events); !errors.Is(err, ErrInvalidLog) {
				t.Fatalf("got %v", err)
			}
		})
	}
	events := append(base(),
		event(5, ModelRequested, toolRequest("x")),
		event(6, AttemptStarted, AttemptStart{TurnID: "t1", StepID: "step1", ID: "a"}),
		event(7, AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "step1", ID: "a", Outcome: "succeeded", Message: Message{Role: "assistant", ToolCalls: []ModelToolCall{{ID: "c", Name: "x", Arguments: json.RawMessage(`null`)}}}}),
		event(8, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "c", Name: "x", Arguments: "null", IdempotencyKey: "k"}),
		event(9, ToolSettled, ToolResult{TurnID: "t1", StepID: "step1", ID: "c", Outcome: "succeeded", Result: "ok"}),
		event(10, ToolSettled, ToolResult{TurnID: "t1", StepID: "step1", ID: "c", Outcome: "succeeded", Result: "again"}),
	)
	if _, err := Replay(events); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("duplicate settlement accepted: %v", err)
	}
}

func TestDeterministicTurnAndHistorySeed(t *testing.T) {
	events := []Event{
		event(1, SessionStarted, SessionStart{TenantID: "tenant", PrincipalID: "person", ConfigDigest: "cfg", PluginDigest: "plugins"}),
		event(2, HistorySeeded, HistorySeed{Messages: []Message{{Role: "user", Content: "past"}, {Role: "assistant", Content: "answer"}}}),
		event(3, TurnStarted, TurnStart{ID: "t1"}),
		event(4, MessageAccepted, AcceptedMessage{TurnID: "t1", Message: Message{Role: "system", Content: "generate"}}),
		event(5, StepStarted, StepStart{TurnID: "t1", ID: "step1", StageKey: "compute", Kind: StageCompute, Input: json.RawMessage(`{}`)}),
		event(6, MessageAccepted, AcceptedMessage{TurnID: "t1", StepID: "step1", Message: Message{Role: "assistant", Content: "done"}}),
		event(7, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "completed", Output: json.RawMessage(`{}`)}),
		event(8, TurnEnded, TurnEnd{TurnID: "t1", Status: "completed"}),
	}
	p, err := Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.History) != 4 || !p.Turns[0].Steps[0].Deterministic {
		t.Fatalf("unexpected projection: %+v", p)
	}
	if _, err := Replay(append(events, event(9, HistorySeeded, HistorySeed{Messages: []Message{{Role: "user", Content: "bad"}}}))); !errors.Is(err, ErrInvalidLog) {
		t.Fatal("late seed accepted")
	}
}

func TestHistorySeedToolTranscript(t *testing.T) {
	declaration := Message{Role: "assistant", ToolCalls: []ModelToolCall{
		{ID: "call-1", Name: "lookup", Arguments: json.RawMessage(`{"id":1}`)},
		{ID: "call-2", Name: "lookup", Arguments: json.RawMessage(`{"id":2}`)},
	}}
	valid := []Message{
		{Role: "user", Content: "find two"}, declaration,
		{Role: "tool", CallID: "call-2", Content: "second"},
		{Role: "tool", CallID: "call-1", Error: "not found"},
		{Role: "assistant", Content: "done"},
	}
	seed := func(messages []Message) []Event {
		return []Event{
			event(1, SessionStarted, SessionStart{TenantID: "tenant", PrincipalID: "person", ConfigDigest: "cfg", PluginDigest: "plugins"}),
			event(2, HistorySeeded, HistorySeed{Messages: messages}),
		}
	}
	p, err := Replay(seed(valid))
	if err != nil || !reflect.DeepEqual(p.History, valid) {
		t.Fatalf("tool transcript lost: %+v, %v", p.History, err)
	}
	for name, messages := range map[string][]Message{
		"unknown result":        {{Role: "tool", CallID: "unknown", Content: "value"}},
		"missing result":        {{Role: "user", Content: "find"}, declaration},
		"interleaved user":      {{Role: "user", Content: "find"}, declaration, {Role: "user", Content: "again"}},
		"duplicate result":      {{Role: "assistant", ToolCalls: []ModelToolCall{{ID: "call", Name: "lookup", Arguments: json.RawMessage(`{}`)}}}, {Role: "tool", CallID: "call", Content: "one"}, {Role: "tool", CallID: "call", Content: "two"}},
		"duplicate declaration": {{Role: "assistant", ToolCalls: []ModelToolCall{{ID: "call", Name: "lookup", Arguments: json.RawMessage(`{}`)}}}, {Role: "tool", CallID: "call", Content: "one"}, {Role: "assistant", ToolCalls: []ModelToolCall{{ID: "call", Name: "lookup", Arguments: json.RawMessage(`{}`)}}}, {Role: "tool", CallID: "call", Content: "two"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Replay(seed(messages)); !errors.Is(err, ErrInvalidLog) {
				t.Fatalf("invalid tool history accepted: %v", err)
			}
		})
	}
	if validHistorySeed([]Message{{Role: "assistant", ToolCalls: []ModelToolCall{{ID: "call", Name: "lookup", Arguments: json.RawMessage(`{`)}}}, {Role: "tool", CallID: "call", Content: "value"}}) {
		t.Fatal("invalid tool arguments accepted")
	}
}

func TestTerminalAndPendingSettlement(t *testing.T) {
	compute := base()
	compute[3] = event(4, StepStarted, StepStart{TurnID: "t1", ID: "step1", StageKey: "compute", Kind: StageCompute, Input: json.RawMessage(`{}`)})
	end := append(compute,
		event(5, MessageAccepted, AcceptedMessage{TurnID: "t1", StepID: "step1", Message: Message{Role: "assistant", Content: "done"}}),
		event(6, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "completed", Output: json.RawMessage(`{}`)}),
		event(7, TurnEnded, TurnEnd{TurnID: "t1", Status: "completed"}),
	)
	if _, err := Replay(end); err != nil {
		t.Fatal(err)
	}
	if _, err := Replay(append(end, event(8, StepStarted, StepStart{TurnID: "t1", ID: "step2", StageKey: "writer", Kind: StageModel, Input: json.RawMessage(`{}`)}))); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("continued terminal turn: %v", err)
	}

	model := append(base(),
		event(5, ModelRequested, toolRequest("x")),
		event(6, AttemptStarted, AttemptStart{TurnID: "t1", StepID: "step1", ID: "a"}),
		event(7, AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "step1", ID: "a", Outcome: "succeeded", Message: Message{Role: "assistant", ToolCalls: []ModelToolCall{{ID: "c", Name: "x", Arguments: json.RawMessage(`null`)}}}}),
		event(8, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "c", Name: "x", Arguments: "null", IdempotencyKey: "k"}),
	)
	p, err := Replay(model)
	if err != nil {
		t.Fatal(err)
	}
	if p.Turns[0].Steps[0].Tools["c"].Result != nil {
		t.Fatal("pending call lost")
	}
	if _, err := Replay(append(model, event(9, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "completed", Output: json.RawMessage(`{}`)}))); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("closed with pending call: %v", err)
	}
}

func TestApprovalBlocksToolUntilGranted(t *testing.T) {
	events := append(base(),
		event(5, ModelRequested, toolRequest("x")),
		event(6, AttemptStarted, AttemptStart{TurnID: "t1", StepID: "step1", ID: "a"}),
		event(7, AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "step1", ID: "a", Outcome: "succeeded", Message: Message{Role: "assistant", Content: "checking", ToolCalls: []ModelToolCall{{ID: "c", Name: "x", Arguments: json.RawMessage(`null`)}}}}),
		event(8, ApprovalAsked, ApprovalRequest{TurnID: "t1", StepID: "step1", ID: "approve", ToolCallID: "c", ToolName: "x", Arguments: "null", IdempotencyKey: "k", InvocationFingerprint: "fingerprint"}),
	)
	call := event(9, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "c", Name: "x", Arguments: "null", IdempotencyKey: "k", ApprovalID: "approve", InvocationFingerprint: "fingerprint"})
	if _, err := Replay(append(events, call)); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("pending approval permitted tool: %v", err)
	}
	events = append(events, event(9, ApprovalResolved, ApprovalDecision{TurnID: "t1", StepID: "step1", ID: "approve", Decision: "approved"}))
	call.Sequence = 10
	if _, err := Replay(append(events, call)); err != nil {
		t.Fatal(err)
	}
}

func TestImageHistoryAndDeclaredToolIdentity(t *testing.T) {
	events := []Event{
		event(1, SessionStarted, SessionStart{TenantID: "tenant", PrincipalID: "person", ConfigDigest: "cfg", PluginDigest: "plugins"}),
		event(2, TurnStarted, TurnStart{ID: "t1"}),
		event(3, MessageAccepted, AcceptedMessage{TurnID: "t1", Message: Message{Role: "user", Images: []ImageRef{{URL: "https://example.test/image.png"}}}}),
		event(4, StepStarted, StepStart{TurnID: "t1", ID: "step1", StageKey: "writer", Kind: StageModel, Input: json.RawMessage(`{}`), AllowedTools: []string{"inspect"}}),
		event(5, ModelRequested, ModelRequest{TurnID: "t1", StepID: "step1", ConfigDigest: "cfg", Profile: "profile", HistoryThrough: 4, History: []Message{{Role: "user", Images: []ImageRef{{URL: "https://example.test/image.png"}}}}, Tools: []ModelToolDef{{Name: "inspect", Parameters: json.RawMessage(`{}`)}}}),
		event(6, AttemptStarted, AttemptStart{TurnID: "t1", StepID: "step1", ID: "a"}),
		event(7, AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "step1", ID: "a", Outcome: "succeeded", Message: Message{Role: "assistant", ToolCalls: []ModelToolCall{{ID: "call", Name: "inspect", Arguments: json.RawMessage(`{"detail":true}`)}}}}),
	}
	p, err := Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.History) != 2 || len(p.History[0].Images) != 1 || len(p.History[1].ToolCalls) != 1 {
		t.Fatalf("lost multimodal history: %+v", p.History)
	}
	bad := append(events, event(8, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "call", Name: "inspect", Arguments: `{"detail":false}`, IdempotencyKey: "idem"}))
	if _, err := Replay(bad); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("mismatched declaration accepted: %v", err)
	}
	good := append(events, event(8, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "call", Name: "inspect", Arguments: `{"detail":true}`, IdempotencyKey: "idem"}))
	if _, err := Replay(good); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownToolOutcomeRequiresReconciliation(t *testing.T) {
	events := append(base(),
		event(5, ModelRequested, toolRequest("tool")),
		event(6, AttemptStarted, AttemptStart{TurnID: "t1", StepID: "step1", ID: "a"}),
		event(7, AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "step1", ID: "a", Outcome: "succeeded", Message: Message{Role: "assistant", ToolCalls: []ModelToolCall{{ID: "c", Name: "tool", Arguments: json.RawMessage(`null`)}}}}),
		event(8, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "c", Name: "tool", Arguments: "null", IdempotencyKey: "idem"}),
		event(9, ToolSettled, ToolResult{TurnID: "t1", StepID: "step1", ID: "c", Outcome: "unknown", Error: "transport lost"}),
	)
	p, err := Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.History) != 2 || p.Turns[0].Steps[0].Tools["c"].Result.Outcome != "unknown" {
		t.Fatalf("unknown was made model-visible: %+v", p)
	}
	if _, err := Replay(append(events, event(10, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "failed", Output: json.RawMessage(`{}`)}))); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("unknown effect closed: %v", err)
	}
	resolved := append(events, event(10, ToolSettled, ToolResult{TurnID: "t1", StepID: "step1", ID: "c", Outcome: "succeeded", Result: "done"}))
	p, err = Replay(resolved)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.History) != 3 || p.History[2].Content != "done" {
		t.Fatalf("reconciliation missing from history: %+v", p.History)
	}
	if _, err := Replay(append(resolved, event(11, ToolSettled, ToolResult{TurnID: "t1", StepID: "step1", ID: "c", Outcome: "succeeded", Result: "twice"}))); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("duplicate settled: %v", err)
	}
}

func TestCancelledSessionKeepsUnknownEffectReconcileable(t *testing.T) {
	events := append(base(),
		event(5, ModelRequested, toolRequest("tool")),
		event(6, AttemptStarted, AttemptStart{TurnID: "t1", StepID: "step1", ID: "a"}),
		event(7, AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "step1", ID: "a", Outcome: "succeeded", Message: Message{Role: "assistant", ToolCalls: []ModelToolCall{{ID: "c", Name: "tool", Arguments: json.RawMessage(`null`)}}}}),
		event(8, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "c", Name: "tool", Arguments: "null", IdempotencyKey: "idem"}),
		event(9, ToolSettled, ToolResult{TurnID: "t1", StepID: "step1", ID: "c", Outcome: "unknown", Error: "transport lost"}),
		event(10, SessionCancelled, struct{}{}),
	)
	p, err := Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != "cancelled" || p.Turns[0].Status != "active" || p.Turns[0].Steps[0].Status != "active" {
		t.Fatalf("cancel discarded unresolved effect state: %+v", p)
	}
	resolved := append(events, event(11, ToolSettled, ToolResult{TurnID: "t1", StepID: "step1", ID: "c", Outcome: "succeeded", Result: "verified"}))
	p, err = Replay(resolved)
	if err != nil {
		t.Fatalf("reconcile after cancellation: %v", err)
	}
	if p.Status != "cancelled" || p.Turns[0].Steps[0].Tools["c"].Result.Outcome != "succeeded" {
		t.Fatalf("cancelled session lost reconciled result: %+v", p)
	}
	if _, err = Replay(append(resolved, event(12, SessionResumed, SessionResume{Reason: "must not resume"}))); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("cancelled session resumed after reconciliation: %v", err)
	}
}

func TestCompleteModelRequestAndApprovalBinding(t *testing.T) {
	events := append(base(), event(5, ModelRequested, ModelRequest{TurnID: "t1", StepID: "step1", ConfigDigest: "cfg", Profile: "vision-profile", HistoryThrough: 4, History: []Message{{Role: "user", Content: "hello"}}, Tools: []ModelToolDef{{Name: "tool", Description: "Do work", Parameters: json.RawMessage(`{"type":"object"}`)}}, Temperature: 0.4, MaxTokens: 1024, Reasoning: Reasoning{Enabled: true, Effort: "medium", MaxTokens: 256}}))
	p, err := Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	r := p.Turns[0].Steps[0].Request
	if r.Profile != "vision-profile" || r.MaxTokens != 1024 || len(r.Tools) != 1 || r.Reasoning.Effort != "medium" {
		t.Fatalf("model input not pinned: %+v", r)
	}
	invalid := append(base(), event(5, ModelRequested, ModelRequest{TurnID: "t1", StepID: "step1", ConfigDigest: "cfg", Profile: "profile", HistoryThrough: 4, History: []Message{{Role: "user", Content: "hello"}}, NoTools: true, Tools: []ModelToolDef{{Name: "x", Parameters: json.RawMessage(`{}`)}}}))
	if _, err := Replay(invalid); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("conflicting tool options accepted: %v", err)
	}
}

func TestStageFactsAndExactModelStageMessages(t *testing.T) {
	baseLog := base()
	baseLog[3] = event(4, StepStarted, StepStart{TurnID: "t1", ID: "step1", StageKey: "draft", Kind: StageModel, Input: json.RawMessage(`{"topic":"hello"}`), Optional: true})
	stagePrompt := Message{Role: "system", Content: "Write a concise draft"}
	request := ModelRequest{TurnID: "t1", StepID: "step1", ConfigDigest: "cfg", Profile: "profile", Preamble: []Message{{Role: "system", Content: "Pinned instructions"}}, HistoryThrough: 4, History: []Message{{Role: "user", Content: "hello"}}, StageMessages: []Message{stagePrompt}}
	events := append(baseLog, event(5, ModelRequested, request))
	p, err := Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	step := p.Turns[0].Steps[0]
	if step.StageKey != "draft" || step.Kind != StageModel || !step.Optional || string(step.Input) != `{"topic":"hello"}` {
		t.Fatalf("stage metadata missing: %+v", step)
	}
	wire, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(wire, &document); err != nil {
		t.Fatal(err)
	}
	if document["tenant_id"] != "tenant" || document["TenantID"] != nil {
		t.Fatalf("projection JSON field names: %s", wire)
	}
	turnWire := document["turns"].([]any)[0].(map[string]any)
	stepWire := turnWire["steps"].([]any)[0].(map[string]any)
	_, toolsPresent := stepWire["allowed_tools"]
	if stepWire["stage_key"] != "draft" || !toolsPresent || stepWire["StageKey"] != nil {
		t.Fatalf("step JSON field names: %s", wire)
	}
	if len(p.History) != 1 || !reflect.DeepEqual(step.Request.Messages(), []Message{{Role: "system", Content: "Pinned instructions"}, {Role: "user", Content: "hello"}, stagePrompt}) {
		t.Fatalf("stage prompt invisible or corrupted: %+v", p)
	}
	badRequest := request
	badEvent := event(5, ModelRequested, badRequest)
	var forged map[string]any
	if err := json.Unmarshal(badEvent.Payload, &forged); err != nil {
		t.Fatal(err)
	}
	forged["history"] = append(append([]Message(nil), request.History...), stagePrompt)
	badEvent.Payload, _ = json.Marshal(forged)
	if _, err := Replay(append(baseLog, badEvent)); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("stage prompt smuggled into base history: %v", err)
	}
	badRequest = request
	badRequest.Preamble = []Message{{Role: "user", Content: "not a system prompt"}}
	if _, err := Replay(append(baseLog, event(5, ModelRequested, badRequest))); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("invalid preamble accepted: %v", err)
	}
	badRequest = request
	badRequest.StageMessages = []Message{{Role: "tool", Content: "hidden"}}
	if _, err := Replay(append(baseLog, event(5, ModelRequested, badRequest))); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("invalid stage prompt accepted: %v", err)
	}
	badRequest = request
	badRequest.Tools = []ModelToolDef{{Name: "unallowed", Parameters: json.RawMessage(`{}`)}}
	if _, err := Replay(append(baseLog, event(5, ModelRequested, badRequest))); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("unallowed model tool accepted: %v", err)
	}
}

func TestStageStartAndEndValidation(t *testing.T) {
	for name, start := range map[string]StepStart{
		"missing key":        {TurnID: "t1", ID: "step1", Kind: StageCompute, Input: json.RawMessage(`{}`)},
		"invalid kind":       {TurnID: "t1", ID: "step1", StageKey: "compute", Kind: "other", Input: json.RawMessage(`{}`)},
		"noncanonical input": {TurnID: "t1", ID: "step1", StageKey: "compute", Kind: StageCompute, Input: json.RawMessage(`{"z":1,"a":2}`)},
		"missing input":      {TurnID: "t1", ID: "step1", StageKey: "compute", Kind: StageCompute},
		"duplicate tools":    {TurnID: "t1", ID: "step1", StageKey: "tool", Kind: StageTool, Input: json.RawMessage(`{}`), AllowedTools: []string{"publish", "publish"}},
		"compute tools":      {TurnID: "t1", ID: "step1", StageKey: "compute", Kind: StageCompute, Input: json.RawMessage(`{}`), AllowedTools: []string{"publish"}},
	} {
		t.Run(name, func(t *testing.T) {
			log := base()
			log[3] = event(4, StepStarted, start)
			if _, err := Replay(log); !errors.Is(err, ErrInvalidLog) {
				t.Fatalf("invalid stage accepted: %v", err)
			}
		})
	}
	log := base()
	log[3] = event(4, StepStarted, StepStart{TurnID: "t1", ID: "step1", StageKey: "compute", Kind: StageCompute, Input: json.RawMessage(`{"a":2,"z":1}`)})
	log = append(log, event(5, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "completed", Output: json.RawMessage(`{"value":42}`), Warnings: []string{"partial source"}}))
	p, err := Replay(log)
	if err != nil {
		t.Fatal(err)
	}
	step := p.Turns[0].Steps[0]
	if step.Status != "completed" || string(step.Output) != `{"value":42}` || !reflect.DeepEqual(step.Warnings, []string{"partial source"}) {
		t.Fatalf("compute projection: %+v", step)
	}
	bad := append(log[:4:4], event(5, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "failed", Output: json.RawMessage(`{}`), Warnings: []string{""}}))
	if _, err := Replay(bad); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("empty warning accepted: %v", err)
	}
	bad = append(log[:4:4], event(5, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "failed", Output: json.RawMessage(`{"z":1,"a":2}`)}))
	if _, err := Replay(bad); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("noncanonical output accepted: %v", err)
	}
}

func TestDeterministicToolStageAndComputeIsolation(t *testing.T) {
	toolLog := base()
	toolLog[3] = event(4, StepStarted, StepStart{TurnID: "t1", ID: "step1", StageKey: "publish", Kind: StageTool, Input: json.RawMessage(`{"name":"publish"}`), AllowedTools: []string{"publish"}})
	toolLog = append(toolLog,
		event(5, ToolCalled, ToolCall{TurnID: "t1", StepID: "step1", ID: "call", Name: "publish", Arguments: `{"text":"hello"}`, IdempotencyKey: "idem"}),
		event(6, ToolSettled, ToolResult{TurnID: "t1", StepID: "step1", ID: "call", Outcome: "succeeded", Result: `{"id":"remote"}`}),
		event(7, StepEnded, StepEnd{TurnID: "t1", StepID: "step1", Status: "completed", Output: json.RawMessage(`{"id":"remote"}`)}),
	)
	p, err := Replay(toolLog)
	if err != nil {
		t.Fatal(err)
	}
	if p.Turns[0].Steps[0].Kind != StageTool || len(p.Turns[0].Steps[0].Tools) != 1 {
		t.Fatalf("tool stage projection: %+v", p)
	}
	compute := append([]Event(nil), toolLog[:4]...)
	compute[3] = event(4, StepStarted, StepStart{TurnID: "t1", ID: "step1", StageKey: "compute", Kind: StageCompute, Input: json.RawMessage(`{}`)})
	compute = append(compute, toolLog[4])
	if _, err := Replay(compute); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("compute invoked external tool: %v", err)
	}
	model := append([]Event(nil), toolLog[:4]...)
	model = append(model, event(5, ModelRequested, ModelRequest{TurnID: "t1", StepID: "step1", ConfigDigest: "cfg", Profile: "profile", HistoryThrough: 4, History: []Message{{Role: "user", Content: "hello"}}}))
	if _, err := Replay(model); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("tool stage requested model: %v", err)
	}
	unauthorized := append([]Event(nil), toolLog[:4]...)
	unauthorized[3] = event(4, StepStarted, StepStart{TurnID: "t1", ID: "step1", StageKey: "publish", Kind: StageTool, Input: json.RawMessage(`{}`), AllowedTools: []string{"other"}})
	unauthorized = append(unauthorized, toolLog[4])
	if _, err := Replay(unauthorized); !errors.Is(err, ErrInvalidLog) {
		t.Fatalf("unallowed direct tool call accepted: %v", err)
	}
}
