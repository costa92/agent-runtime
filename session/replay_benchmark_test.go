package session

import (
	"encoding/json"
	"fmt"
	"testing"
)

// Compare full replay with restoration of a checkpoint plus one new turn.
// Both paths use exactly the same event reducer and conversation fixture.
func BenchmarkReplayLongConversation(b *testing.B) {
	for _, turns := range []int{10, 100, 500} {
		b.Run(fmt.Sprintf("turns_%d", turns), func(b *testing.B) {
			var events []Event
			var history []Message
			appendEvent := func(kind Kind, payload any) {
				raw, err := json.Marshal(payload)
				if err != nil {
					b.Fatal(err)
				}
				events = append(events, Event{Version: ProtocolVersion, SessionID: "benchmark", Sequence: uint64(len(events) + 1), Kind: kind, Payload: raw})
			}
			appendEvent(SessionStarted, SessionStart{TenantID: "tenant", PrincipalID: "principal", ConfigDigest: "pinned", PluginDigest: "pinned"})
			for i := 0; i < turns; i++ {
				turnID, stepID, attemptID := fmt.Sprintf("turn-%d", i), fmt.Sprintf("step-%d", i), fmt.Sprintf("attempt-%d", i)
				user := Message{Role: "user", Content: "A representative user request that should survive Session replay and restart."}
				answer := Message{Role: "assistant", Content: "A representative settled answer with enough content to exercise history growth."}
				appendEvent(TurnStarted, TurnStart{ID: turnID})
				appendEvent(MessageAccepted, AcceptedMessage{TurnID: turnID, Message: user})
				history = append(history, user)
				appendEvent(StepStarted, StepStart{TurnID: turnID, ID: stepID, StageKey: "conversation", Kind: StageModel, Input: json.RawMessage(`{}`)})
				appendEvent(ModelRequested, ModelRequest{TurnID: turnID, StepID: stepID, ConfigDigest: "pinned", Profile: "test/model", HistoryThrough: uint64(len(events)), History: history, NoTools: true})
				appendEvent(AttemptStarted, AttemptStart{TurnID: turnID, StepID: stepID, ID: attemptID})
				appendEvent(AttemptFinished, AttemptFinish{TurnID: turnID, StepID: stepID, ID: attemptID, Outcome: "succeeded", Message: answer, Profile: "test/model"})
				history = append(history, answer)
				appendEvent(StepEnded, StepEnd{TurnID: turnID, StepID: stepID, Status: "completed", Output: json.RawMessage(`{}`)})
				appendEvent(TurnEnded, TurnEnd{TurnID: turnID, Status: "completed"})
			}
			if _, err := Replay(events); err != nil {
				b.Fatal(err)
			}
			bytes := 0
			for _, event := range events {
				bytes += len(event.Payload)
			}
			for _, mode := range []string{"full", "checkpoint"} {
				b.Run(mode, func(b *testing.B) {
					r := NewReducer()
					if _, err := r.Apply(events[:len(events)-8]); err != nil {
						b.Fatal(err)
					}
					checkpoint, err := r.Checkpoint()
					if err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if mode == "full" {
							if _, err := Replay(events); err != nil {
								b.Fatal(err)
							}
						} else {
							restored, err := RestoreCheckpoint(checkpoint)
							if err != nil {
								b.Fatal(err)
							}
							if _, err := restored.Apply(events[len(events)-8:]); err != nil {
								b.Fatal(err)
							}
						}
					}
					b.ReportMetric(float64(bytes), "log_bytes")
					b.ReportMetric(float64(len(checkpoint)), "checkpoint_bytes")
				})
			}
		})
	}
}
