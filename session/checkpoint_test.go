package session

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func checkpointTranscript() []Event {
	var events []Event
	add := func(kind Kind, payload any) {
		events = append(events, event(uint64(len(events)+1), kind, payload))
	}
	add(SessionStarted, SessionStart{TenantID: "tenant", PrincipalID: "person", ConfigDigest: "cfg", PluginDigest: "plugins"})
	add(HistorySeeded, HistorySeed{Messages: []Message{{Role: "user", Content: "seed"}, {Role: "assistant", Content: "seed answer"}}})
	add(TurnStarted, TurnStart{ID: "t1"})
	add(MessageAccepted, AcceptedMessage{TurnID: "t1", Message: Message{Role: "user", Content: "publish"}})
	add(StepStarted, StepStart{TurnID: "t1", ID: "s1", StageKey: "writer", Kind: StageModel, Input: json.RawMessage(`{}`), AllowedTools: []string{"publish"}})
	add(ModelRequested, ModelRequest{TurnID: "t1", StepID: "s1", ConfigDigest: "cfg", Profile: "model", HistoryThrough: uint64(len(events)), Tools: []ModelToolDef{{Name: "publish", Parameters: json.RawMessage(`{}`)}}})
	add(AttemptStarted, AttemptStart{TurnID: "t1", StepID: "s1", ID: "a1"})
	add(AttemptFinished, AttemptFinish{TurnID: "t1", StepID: "s1", ID: "a1", Outcome: "succeeded", Message: Message{Role: "assistant", Content: "publishing", ToolCalls: []ModelToolCall{{ID: "c1", Name: "publish", Arguments: json.RawMessage(`{}`)}, {ID: "c2", Name: "publish", Arguments: json.RawMessage(`{}`)}}}})
	add(ApprovalAsked, ApprovalRequest{TurnID: "t1", StepID: "s1", ID: "approval", ToolCallID: "c1", ToolName: "publish", Arguments: "{}", IdempotencyKey: "key1", InvocationFingerprint: "fingerprint"})
	add(ApprovalResolved, ApprovalDecision{TurnID: "t1", StepID: "s1", ID: "approval", Decision: "approved"})
	add(ToolCalled, ToolCall{TurnID: "t1", StepID: "s1", ID: "c1", Name: "publish", Arguments: "{}", IdempotencyKey: "key1", ApprovalID: "approval", InvocationFingerprint: "fingerprint"})
	add(ToolSettled, ToolResult{TurnID: "t1", StepID: "s1", ID: "c1", Outcome: "unknown", Error: "timeout"})
	add(ToolSettled, ToolResult{TurnID: "t1", StepID: "s1", ID: "c1", Outcome: "succeeded", Result: "verified"})
	add(ToolSkipped, ToolSkip{TurnID: "t1", StepID: "s1", ID: "c2", Reason: "budget"})
	add(BudgetSettled, BudgetSettlement{TurnID: "t1", StepID: "s1", Category: BudgetToolCalls, Units: 1})
	add(StepEnded, StepEnd{TurnID: "t1", StepID: "s1", Status: "failed", Output: json.RawMessage(`{}`)})
	add(TurnEnded, TurnEnd{TurnID: "t1", Status: "failed"})
	add(TurnStarted, TurnStart{ID: "t2"})
	add(MessageAccepted, AcceptedMessage{TurnID: "t2", Message: Message{Role: "user", Content: "route"}})
	add(StepStarted, StepStart{TurnID: "t2", ID: "s2", StageKey: "router", Kind: StageModel, Input: json.RawMessage(`{}`), Internal: true})
	add(ModelRequested, ModelRequest{TurnID: "t2", StepID: "s2", ConfigDigest: "cfg", Profile: "model", HistoryThrough: uint64(len(events))})
	add(AttemptStarted, AttemptStart{TurnID: "t2", StepID: "s2", ID: "a2"})
	add(AttemptFinished, AttemptFinish{TurnID: "t2", StepID: "s2", ID: "a2", Outcome: "failed", Error: "interrupted"})
	add(AttemptStarted, AttemptStart{TurnID: "t2", StepID: "s2", ID: "a3"})
	add(AttemptFinished, AttemptFinish{TurnID: "t2", StepID: "s2", ID: "a3", Outcome: "succeeded", Message: Message{Role: "assistant", Content: "internal decision"}})
	add(StepEnded, StepEnd{TurnID: "t2", StepID: "s2", Status: "completed", Output: json.RawMessage(`{}`)})
	add(TurnEnded, TurnEnd{TurnID: "t2", Status: "completed"})
	add(SessionParked, SessionPark{Reason: "profile missing"})
	add(SessionResumed, SessionResume{Reason: "profile restored"})
	add(SessionCancelled, struct{}{})
	return events
}

func TestCheckpointEveryInterruptionBoundary(t *testing.T) {
	events := checkpointTranscript()
	want, err := Replay(events)
	if err != nil {
		t.Fatal(err)
	}
	for cut := 1; cut <= len(events); cut++ {
		r := NewReducer()
		before, err := r.Apply(events[:cut])
		if err != nil {
			t.Fatal(err)
		}
		raw, err := r.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		restored, err := RestoreCheckpoint(raw)
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		prefix, err := restored.Apply(nil)
		if err != nil || !reflect.DeepEqual(prefix, before) {
			t.Fatalf("cut %d: prefix differs: %v", cut, err)
		}
		got, err := restored.Apply(events[cut:])
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("cut %d: restored replay differs: %v", cut, err)
		}
	}
}

func TestCheckpointRejectsCorruptionAndUnsupportedVersion(t *testing.T) {
	r := NewReducer()
	if _, err := r.Apply(base()); err != nil {
		t.Fatal(err)
	}
	raw, err := r.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range [][]byte{nil, []byte(`{}`), append(append([]byte(nil), raw...), 'x')} {
		if _, err := RestoreCheckpoint(input); !errors.Is(err, ErrInvalidCheckpoint) {
			t.Fatalf("corruption: %v", err)
		}
	}
	for _, version := range []uint32{0, CheckpointVersion + 1} {
		input := append([]byte(nil), raw...)
		binary.BigEndian.PutUint32(input[4:8], version)
		if _, err := RestoreCheckpoint(input); !errors.Is(err, ErrInvalidCheckpoint) {
			t.Fatalf("version: %v", err)
		}
	}
	input := append([]byte(nil), raw...)
	input[len(input)-1] ^= 1
	if _, err := RestoreCheckpoint(input); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("checksum: %v", err)
	}
}

func TestCheckpointRetainsDuplicateIdentityGuards(t *testing.T) {
	events := checkpointTranscript()
	// Every event which writes or consumes a unique identity remains invalid
	// when replayed again immediately after a checkpoint containing that fact.
	for _, cut := range []int{2, 3, 5, 7, 9, 10, 11, 13, 14, 24} {
		r := NewReducer()
		if _, err := r.Apply(events[:cut]); err != nil {
			t.Fatal(err)
		}
		raw, err := r.Checkpoint()
		if err != nil {
			t.Fatal(err)
		}
		r, err = RestoreCheckpoint(raw)
		if err != nil {
			t.Fatal(err)
		}
		duplicate := events[cut-1]
		duplicate.Sequence++
		if _, err = r.Apply([]Event{duplicate}); !errors.Is(err, ErrInvalidLog) {
			t.Fatalf("cut %d: duplicate accepted: %v", cut, err)
		}
		if _, err = r.Checkpoint(); !errors.Is(err, ErrInvalidCheckpoint) {
			t.Fatalf("invalid reducer checkpoint: %v", err)
		}
		if _, err = r.Apply(nil); !errors.Is(err, ErrInvalidLog) {
			t.Fatalf("invalid reducer reused: %v", err)
		}
	}
}

func TestCheckpointRejectsIdentitiesFromEarlierSteps(t *testing.T) {
	events := checkpointTranscript()
	for _, tc := range []struct {
		name    string
		cut     int
		kind    Kind
		payload any
	}{
		{"turn", 17, TurnStarted, TurnStart{ID: "t1"}},
		{"step", 19, StepStarted, StepStart{TurnID: "t2", ID: "s1", StageKey: "router", Kind: StageModel, Input: json.RawMessage(`{}`)}},
		{"attempt", 23, AttemptStarted, AttemptStart{TurnID: "t2", StepID: "s2", ID: "a1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewReducer()
			if _, err := r.Apply(events[:tc.cut]); err != nil {
				t.Fatal(err)
			}
			raw, err := r.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			r, err = RestoreCheckpoint(raw)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = r.Apply([]Event{event(uint64(tc.cut+1), tc.kind, tc.payload)}); !errors.Is(err, ErrInvalidLog) {
				t.Fatalf("old identity accepted: %v", err)
			}
		})
	}
}

func TestCheckpointCancelledUnknownEffectCanStillSettle(t *testing.T) {
	events := checkpointTranscript()[:12]
	events = append(events, event(13, SessionCancelled, struct{}{}))
	r := NewReducer()
	if _, err := r.Apply(events); err != nil {
		t.Fatal(err)
	}
	raw, err := r.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	r, err = RestoreCheckpoint(raw)
	if err != nil {
		t.Fatal(err)
	}
	settled := event(14, ToolSettled, ToolResult{TurnID: "t1", StepID: "s1", ID: "c1", Outcome: "succeeded", Result: "verified after cancel"})
	got, err := r.Apply([]Event{settled})
	if err != nil {
		t.Fatal(err)
	}
	want, err := Replay(append(events, settled))
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("cancelled reconciliation differs: %v", err)
	}
}
