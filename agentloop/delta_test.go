package agentloop_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/costa92/agent-runtime/agentloop"
	"github.com/costa92/agent-runtime/model"
	"github.com/costa92/agent-runtime/product"
	"github.com/costa92/agent-runtime/profile"
	"github.com/costa92/agent-runtime/session"
)

type streamingService struct{ pieces []string }
type streamingCall struct{ pieces []string }

func (s streamingService) Prepare(context.Context, model.Request) (model.Call, error) {
	return streamingCall(s), nil
}
func (streamingCall) Capabilities() model.Capabilities { return model.Capabilities{Streaming: true} }
func (c streamingCall) Complete(ctx context.Context) (model.Response, error) {
	return c.Stream(ctx, nil)
}
func (c streamingCall) Stream(_ context.Context, onChunk func(model.Chunk) error) (model.Response, error) {
	for _, piece := range c.pieces {
		if onChunk != nil {
			if err := onChunk(model.Chunk{Content: piece}); err != nil {
				return model.Response{}, err
			}
		}
	}
	return model.Response{Profile: "test/model", Message: session.Message{Role: "assistant", Content: strings.Join(c.pieces, "")}}, nil
}

type answerWorkflow struct{ internal bool }

// With internal set, the turn routes first (an internal step) and then answers.
func (w answerWorkflow) Next(_ context.Context, p session.Projection) (product.Plan, error) {
	steps := len(p.Turns[len(p.Turns)-1].Steps)
	if w.internal && steps == 0 {
		return product.Plan{Key: "route", Kind: product.Model, Internal: true, Optional: true}, nil
	}
	if steps > 0 && (!w.internal || steps > 1) {
		return product.Plan{Done: true, FinalStatus: "completed"}, nil
	}
	return product.Plan{Key: "answer", Kind: product.Model}, nil
}
func (answerWorkflow) Compute(context.Context, session.Projection, product.Plan) (product.Computed, error) {
	return product.Computed{}, errors.New("unexpected compute")
}
func (answerWorkflow) Settle(context.Context, session.Projection, product.Plan) (product.Settlement, error) {
	return product.Settlement{Status: "completed", Output: json.RawMessage(`{}`)}, nil
}

func driveWithDeltas(t *testing.T, internal bool) ([]agentloop.Delta, session.Projection) {
	t.Helper()
	store := &commandStore{}
	loop, err := agentloop.New(store, streamingService{pieces: []string{"你", "好"}}, nil, answerWorkflow{internal}, agentloop.Config{Profile: "test", Digest: "pinned", Scope: profile.PrincipalScope, MaxSteps: 3, MaxAttempts: 1, MaxTokens: 64})
	if err != nil {
		t.Fatal(err)
	}
	var deltas []agentloop.Delta
	loop.OnDelta = func(d agentloop.Delta) { deltas = append(deltas, d) }
	ids := 0
	loop.NewID = func() (string, error) { ids++; return fmt.Sprintf("id-%d", ids), nil }
	if _, err = loop.CreateSession(t.Context(), "s", "tenant", "principal", 0, "pinned"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = loop.Submit(t.Context(), "s", "input", session.Message{Role: "user", Content: "hi"}); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if err = loop.Drive(t.Context(), session.Lease{SessionID: "s", WorkerID: "worker", Fence: 1}); err != nil {
			t.Fatal(err)
		}
	}
	p, err := store.Inspect(t.Context(), "s")
	if err != nil {
		t.Fatal(err)
	}
	return deltas, p
}

// Deltas are a live view of the attempt the finish event will record; they
// never become events themselves.
func TestVisibleModelStepStreamsDeltasWithoutRecordingThem(t *testing.T) {
	deltas, p := driveWithDeltas(t, false)
	if len(deltas) != 2 || deltas[0].Content != "你" || deltas[1].Content != "好" {
		t.Fatalf("deltas = %+v", deltas)
	}
	step := p.Turns[0].Steps[0]
	if deltas[0].SessionID != "s" || deltas[0].TurnID != p.Turns[0].ID || deltas[0].StepID != step.ID || deltas[0].AttemptID != step.Attempts[0].ID {
		t.Fatalf("delta identity %+v does not match step %+v", deltas[0], step)
	}
	if p.Turns[0].Status != "completed" || step.Attempts[0].Message.Content != "你好" {
		t.Fatalf("turn = %+v", p.Turns[0])
	}
}

// An internal step (a router's classification JSON) is never shown to the user.
func TestInternalModelStepDoesNotStream(t *testing.T) {
	deltas, p := driveWithDeltas(t, true)
	if p.Turns[0].Status != "completed" || len(p.Turns[0].Steps) != 2 {
		t.Fatalf("turn = %+v", p.Turns[0])
	}
	if len(deltas) != 2 {
		t.Fatalf("deltas = %+v", deltas)
	}
	for _, d := range deltas {
		if d.StepID != p.Turns[0].Steps[1].ID {
			t.Fatalf("internal step streamed %+v", d)
		}
	}
}
