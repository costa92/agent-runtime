package agentloop_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/costa92/agent-runtime/agentloop"
	"github.com/costa92/agent-runtime/capability"
	"github.com/costa92/agent-runtime/model"
	"github.com/costa92/agent-runtime/profile"
	"github.com/costa92/agent-runtime/session"
)

// blockingCall streams until its caller gives up, like a long answer the user
// stops halfway.
type blockingService struct{ started chan<- struct{} }
type blockingCall struct{ started chan<- struct{} }

func (s blockingService) Prepare(context.Context, model.Request) (model.Call, error) {
	return blockingCall(s), nil
}
func (blockingCall) Capabilities() model.Capabilities { return model.Capabilities{Streaming: true} }
func (c blockingCall) Complete(ctx context.Context) (model.Response, error) {
	return c.Stream(ctx, nil)
}
func (c blockingCall) Stream(ctx context.Context, _ func(model.Chunk) error) (model.Response, error) {
	close(c.started)
	<-ctx.Done()
	return model.Response{}, ctx.Err()
}

func TestStopTurnEndsAnInterruptedAnswerAndKeepsTheSessionOpen(t *testing.T) {
	store := &commandStore{}
	started := make(chan struct{})
	loop, err := agentloop.New(store, blockingService{started}, nil, answerWorkflow{}, agentloop.Config{Profile: "test", Digest: "pinned", Scope: profile.PrincipalScope, MaxSteps: 3, MaxAttempts: 2, MaxTokens: 64})
	if err != nil {
		t.Fatal(err)
	}
	ids := 0
	loop.NewID = func() (string, error) { ids++; return fmt.Sprintf("id-%d", ids), nil }
	if _, err = loop.CreateSession(t.Context(), "s", "tenant", "principal", 0, "pinned"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = loop.Submit(t.Context(), "s", "input", session.Message{Role: "user", Content: "写一篇长文"}); err != nil {
		t.Fatal(err)
	}
	lease := session.Lease{SessionID: "s", WorkerID: "worker", Fence: 1}
	if err = loop.Drive(t.Context(), lease); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	go func() { <-started; cancel() }()
	if err = loop.Drive(ctx, lease); !errors.Is(err, context.Canceled) {
		t.Fatalf("drive = %v", err)
	}
	before, _ := store.Inspect(t.Context(), "s")
	p, err := loop.StopTurn(t.Context(), lease, before.Turns[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	turn := p.Turns[0]
	if p.Status != "active" || turn.Status != "cancelled" || turn.Steps[0].Status != "cancelled" || turn.Steps[0].Attempts[0].Outcome != "failed" {
		t.Fatalf("stopped turn = %+v", p)
	}
	if len(p.History) != 1 || p.History[0].Role != "user" {
		t.Fatalf("history kept a partial answer: %+v", p.History)
	}
	if again, err := loop.StopTurn(t.Context(), lease, turn.ID); err != nil || again.Sequence != p.Sequence {
		t.Fatalf("stopping an ended turn = %d %v, want no-op", again.Sequence, err)
	}

	loop.Model = streamingService{pieces: []string{"好"}}
	if _, _, err = loop.Submit(t.Context(), "s", "next", session.Message{Role: "user", Content: "短一点"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = loop.Drive(t.Context(), lease); err != nil {
			t.Fatal(err)
		}
	}
	if p, _ = store.Inspect(t.Context(), "s"); len(p.Turns) != 2 || p.Turns[1].Status != "completed" {
		t.Fatalf("next turn = %+v", p.Turns)
	}
}

// A model step that declared a tool but was stopped before calling it closes
// the declaration, so the next Turn's transcript stays valid.
func TestStopTurnSkipsDeclaredToolsThatNeverRan(t *testing.T) {
	store := &commandStore{}
	tools, err := capability.New(map[string]capability.Registration{"publish": {
		Spec: capability.Spec{Name: "publish", Version: "1", Effect: capability.Write},
		Handler: func(context.Context, capability.Invocation) (capability.Result, error) {
			t.Fatal("tool ran")
			return capability.Result{}, nil
		},
	}}, capability.Hooks{
		Authorize: func(context.Context, capability.Invocation, capability.Spec) error { return nil },
		Policy: func(context.Context, capability.Invocation, capability.Spec) (capability.PolicyDecision, error) {
			return capability.PolicyDecision{Effect: capability.Allow}, nil
		},
		Budget: func(context.Context, capability.Invocation, capability.Spec) (capability.BudgetClaim, error) {
			return capability.BudgetClaim{Dimension: "tool_calls", Units: 1}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	models := modelService{complete: func(context.Context) (model.Response, error) {
		return model.Response{Profile: "test/model", Message: session.Message{Role: "assistant", ToolCalls: []session.ModelToolCall{{ID: "call", Name: "publish", Arguments: json.RawMessage(`{}`)}}}}, nil
	}}
	loop, err := agentloop.New(store, models, tools, workflow{}, agentloop.Config{Profile: "test", Digest: "pinned", Scope: profile.PrincipalScope, MaxSteps: 3, MaxAttempts: 2, MaxTokens: 64, Tools: []model.Tool{{Name: "publish"}}})
	if err != nil {
		t.Fatal(err)
	}
	ids := 0
	loop.NewID = func() (string, error) { ids++; return fmt.Sprintf("id-%d", ids), nil }
	if _, err = loop.CreateSession(t.Context(), "s", "tenant", "principal", 0, "pinned"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = loop.Submit(t.Context(), "s", "input", session.Message{Role: "user", Content: "publish"}); err != nil {
		t.Fatal(err)
	}
	stopped := errors.New("stopped before the tool")
	store.appendHook = func(_ context.Context, cmd session.AppendCommand) error {
		for _, e := range cmd.Events {
			if e.Kind == session.ToolCalled || e.Kind == session.ApprovalAsked {
				return stopped
			}
		}
		return nil
	}
	lease := session.Lease{SessionID: "s", WorkerID: "worker", Fence: 1}
	for range 6 {
		if err = loop.Drive(t.Context(), lease); err != nil {
			break
		}
	}
	if !errors.Is(err, stopped) {
		t.Fatalf("drive = %v", err)
	}
	store.appendHook = nil
	before, _ := store.Inspect(t.Context(), "s")
	p, err := loop.StopTurn(t.Context(), lease, before.Turns[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	step := p.Turns[0].Steps[0]
	if p.Turns[0].Status != "cancelled" || step.Status != "cancelled" || step.SkippedTools["call"].Reason == "" {
		t.Fatalf("stopped turn = %+v", p.Turns[0])
	}
}

// Stopping is the user's answer to an open approval: it is denied, and the
// tool it guarded never runs.
func TestStopTurnDeniesAnOpenApproval(t *testing.T) {
	loop, store, lease, calls := fixture(t, func(context.Context, capability.Invocation, capability.Spec) error { return nil }, capability.RequireApproval, false)
	for range 6 {
		if err := loop.Drive(t.Context(), lease); err != nil {
			break
		}
	}
	p, _ := store.Inspect(t.Context(), "s")
	if len(p.Turns[0].Steps[0].Approvals) != 1 || p.Turns[0].Status != "active" {
		t.Fatalf("fixture did not wait for approval: %+v", p)
	}
	p, err := loop.StopTurn(t.Context(), lease, p.Turns[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	step := p.Turns[0].Steps[0]
	for _, approval := range step.Approvals {
		if approval.Decision == nil || approval.Decision.Decision != "denied" {
			t.Fatalf("approval = %+v", approval)
		}
	}
	if p.Turns[0].Status != "cancelled" || step.SkippedTools["call"].Reason == "" || *calls != 0 {
		t.Fatalf("stopped turn = %+v, tool calls %d", p.Turns[0], *calls)
	}
}
