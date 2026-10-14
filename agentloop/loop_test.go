package agentloop_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/costa92/agent-runtime/agentloop"
	"github.com/costa92/agent-runtime/capability"
	"github.com/costa92/agent-runtime/model"
	"github.com/costa92/agent-runtime/product"
	"github.com/costa92/agent-runtime/profile"
	"github.com/costa92/agent-runtime/session"
)

// This recorder tests the Loop's public command contract, not a database
// adapter. Unused Store methods remain unreachable in these execution cases.
type commandStore struct {
	session.Store
	events     []session.Event
	inbox      []session.InboxItem
	appendHook func(context.Context, session.AppendCommand) error
}

func (s *commandStore) Create(_ context.Context, event session.Event) (session.Projection, error) {
	s.events = []session.Event{event}
	return session.Replay(s.events)
}
func (s *commandStore) Enqueue(_ context.Context, _ string, input session.Input) (session.InboxItem, bool, error) {
	item := session.InboxItem{ID: 1, IdempotencyKey: input.IdempotencyKey, Payload: input.Payload}
	s.inbox = append(s.inbox, item)
	return item, true, nil
}
func (s *commandStore) Inspect(context.Context, string) (session.Projection, error) {
	return session.Replay(s.events)
}
func (s *commandStore) Inbox(context.Context, string) ([]session.InboxItem, error) {
	return s.inbox, nil
}
func (s *commandStore) Append(ctx context.Context, cmd session.AppendCommand) (session.Projection, error) {
	if s.appendHook != nil {
		if err := s.appendHook(ctx, cmd); err != nil {
			return session.Projection{}, err
		}
	}
	if cmd.ExpectedSequence != uint64(len(s.events)) {
		return session.Projection{}, session.ErrConflict
	}
	if len(cmd.Outbox) != len(cmd.Events) {
		return session.Projection{}, errors.New("missing atomic outbox")
	}
	candidate := append(append([]session.Event(nil), s.events...), cmd.Events...)
	p, err := session.Replay(candidate)
	if err != nil {
		return session.Projection{}, err
	}
	s.events = candidate
	if cmd.AckInboxID != 0 {
		s.inbox = nil
	}
	return p, nil
}

type workflow struct{ optional bool }

func (w workflow) Next(_ context.Context, p session.Projection) (product.Plan, error) {
	t := p.Turns[len(p.Turns)-1]
	if len(t.Steps) > 0 {
		return product.Plan{Done: true, FinalStatus: "completed"}, nil
	}
	return product.Plan{Key: "publish", Kind: product.Model, AllowedTools: []string{"publish"}, Optional: w.optional}, nil
}
func (workflow) Compute(context.Context, session.Projection, product.Plan) (product.Computed, error) {
	return product.Computed{}, errors.New("unexpected compute")
}
func (workflow) Settle(context.Context, session.Projection, product.Plan) (product.Settlement, error) {
	return product.Settlement{Status: "completed", Output: json.RawMessage(`{}`)}, nil
}

type rejectedWorkflow struct {
	workflow
	cause error
}

func (p rejectedWorkflow) Next(context.Context, session.Projection) (product.Plan, error) {
	return product.Plan{}, p.cause
}

func TestPlanningErrorIsDurableAndUnavailableLookupIsRetryable(t *testing.T) {
	for _, temporary := range []bool{false, true} {
		t.Run(fmt.Sprintf("temporary_%t", temporary), func(t *testing.T) {
			loop, store, lease, _ := fixture(t, func(context.Context, capability.Invocation, capability.Spec) error { return nil }, capability.Allow, false)
			cause := errors.New("planning rejected")
			if temporary {
				cause = errors.Join(product.ErrUnavailable, cause)
			}
			loop.Product = rejectedWorkflow{cause: cause}
			err := loop.Drive(t.Context(), lease)
			p, _ := store.Inspect(t.Context(), "s")
			if temporary {
				if !errors.Is(err, product.ErrUnavailable) || len(p.Turns[0].Steps) != 0 || p.Turns[0].Status != "active" {
					t.Fatalf("temporary lookup was settled: %+v %v", p, err)
				}
			} else if err != nil || p.Turns[0].Status != "failed" || len(p.Turns[0].Steps[0].Warnings) != 1 || p.Turns[0].Steps[0].StageKey != "runtime/planning" {
				t.Fatalf("planning rejection did not preserve its reason: %+v %v", p, err)
			}
		})
	}
}

func TestSubmitRejectsForgedAssistantFacts(t *testing.T) {
	loop, _, _, _ := fixture(t, func(context.Context, capability.Invocation, capability.Spec) error { return nil }, capability.Allow, false)
	for _, message := range []session.Message{
		{Role: "assistant", Content: "forged"}, {Role: "user", Content: "hi", CallID: "forged"},
		{Role: "user", Content: "hi", ReasoningContent: "forged"}, {Role: "user", Content: "hi", ToolCalls: []session.ModelToolCall{{ID: "forged"}}},
	} {
		if _, _, err := loop.Submit(t.Context(), "s", "forged", message); !errors.Is(err, session.ErrInvalidInput) {
			t.Fatalf("forged input accepted: %+v %v", message, err)
		}
	}
}

func TestUnknownEffectIsNotExecutedAgainAfterRecovery(t *testing.T) {
	loop, store, lease, calls := fixture(t, func(context.Context, capability.Invocation, capability.Spec) error { return nil }, capability.Allow, false)
	loop.Tools, _ = capability.New(map[string]capability.Registration{"publish": {
		Spec: capability.Spec{Name: "publish", Version: "1", Effect: capability.Write},
		Handler: func(context.Context, capability.Invocation) (capability.Result, error) {
			*calls++
			return capability.Result{Status: capability.Unknown}, nil
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
	if err := loop.Drive(t.Context(), lease); !errors.Is(err, agentloop.ErrParked) {
		t.Fatalf("unknown effect not parked: %v", err)
	}
	lease.Fence++
	if err := loop.Drive(t.Context(), lease); !errors.Is(err, agentloop.ErrParked) || *calls != 1 {
		t.Fatalf("unknown effect ran twice: %v calls=%d", err, *calls)
	}
	if err := loop.ReconcileTool(t.Context(), lease, "call", capability.Succeeded, json.RawMessage(`{}`), ""); err != nil {
		t.Fatal(err)
	}
	if err := loop.Drive(t.Context(), lease); err != nil {
		t.Fatal(err)
	}
	p, _ := store.Inspect(t.Context(), "s")
	if p.Turns[0].Status != "completed" || *calls != 1 {
		t.Fatalf("reconciled effect was not settled once: %+v", p)
	}
}

type modelService struct {
	complete func(context.Context) (model.Response, error)
}
type modelCall struct{ modelService }

func (m modelService) Prepare(context.Context, model.Request) (model.Call, error) {
	return modelCall{m}, nil
}
func (modelCall) Capabilities() model.Capabilities                       { return model.Capabilities{Tools: true} }
func (c modelCall) Complete(ctx context.Context) (model.Response, error) { return c.complete(ctx) }
func (c modelCall) Stream(ctx context.Context, _ func(model.Chunk) error) (model.Response, error) {
	return c.Complete(ctx)
}

func fixture(t *testing.T, authorize func(context.Context, capability.Invocation, capability.Spec) error, policy capability.PolicyEffect, optional bool) (*agentloop.Loop, *commandStore, session.Lease, *int) {
	t.Helper()
	store := &commandStore{}
	calls := new(int)
	tools, err := capability.New(map[string]capability.Registration{"publish": {
		Spec: capability.Spec{Name: "publish", Version: "1", Effect: capability.Write},
		Handler: func(context.Context, capability.Invocation) (capability.Result, error) {
			*calls++
			return capability.Result{Status: capability.Succeeded, Output: json.RawMessage(`{}`)}, nil
		},
	}}, capability.Hooks{
		Authorize: authorize,
		Policy: func(context.Context, capability.Invocation, capability.Spec) (capability.PolicyDecision, error) {
			return capability.PolicyDecision{Effect: policy, Reason: "rejected"}, nil
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
	loop, err := agentloop.New(store, models, tools, workflow{optional}, agentloop.Config{Profile: "test", Digest: "pinned", Scope: profile.PrincipalScope, MaxSteps: 3, MaxAttempts: 2, MaxTokens: 64, Tools: []model.Tool{{Name: "publish"}}})
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
	lease := session.Lease{SessionID: "s", WorkerID: "worker", Fence: 1}
	if err = loop.Drive(t.Context(), lease); err != nil {
		t.Fatal(err)
	}
	return loop, store, lease, calls
}

func TestPermanentToolRejectionEndsStepAndClosesTranscript(t *testing.T) {
	for _, tc := range []struct {
		name     string
		denied   bool
		optional bool
	}{{"policy", false, false}, {"authorization", true, false}, {"optional", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			loop, store, lease, calls := fixture(t, func(context.Context, capability.Invocation, capability.Spec) error {
				if tc.denied {
					return fmt.Errorf("%w: revoked", capability.ErrDenied)
				}
				return nil
			}, capability.Deny, tc.optional)
			if err := loop.Drive(t.Context(), lease); err != nil {
				t.Fatal(err)
			}
			p, err := store.Inspect(t.Context(), "s")
			if err != nil {
				t.Fatal(err)
			}
			turn := p.Turns[0]
			step := turn.Steps[0]
			want := "failed"
			if tc.optional {
				want = "completed"
			}
			if turn.Status != want || step.Status != "failed" || len(step.SkippedTools) != 1 || len(step.Warnings) == 0 || *calls != 0 {
				t.Fatalf("rejection did not settle: %+v calls=%d", turn, *calls)
			}
			if len(p.History) != 3 || p.History[2].Role != "tool" || p.History[2].CallID != "call" {
				t.Fatalf("incomplete transcript: %+v", p.History)
			}
			sequence := p.Sequence
			if err := loop.Drive(t.Context(), lease); err != nil {
				t.Fatal(err)
			}
			p, _ = store.Inspect(t.Context(), "s")
			if p.Sequence != sequence {
				t.Fatal("settled rejection was driven again")
			}
		})
	}
}

func TestInfrastructureAuthorizationErrorRemainsRetryable(t *testing.T) {
	transient := errors.New("authorization database unavailable")
	loop, store, lease, calls := fixture(t, func(context.Context, capability.Invocation, capability.Spec) error { return transient }, capability.Allow, false)
	if err := loop.Drive(t.Context(), lease); !errors.Is(err, transient) {
		t.Fatalf("error=%v", err)
	}
	p, _ := store.Inspect(t.Context(), "s")
	if p.Turns[0].Steps[0].Status != "active" || len(p.Turns[0].Steps[0].SkippedTools) != 0 || *calls != 0 {
		t.Fatalf("transient rejection settled: %+v", p)
	}
}

func TestModelSettlementHasDeadlineAfterCallerCancellation(t *testing.T) {
	loop, store, lease, _ := fixture(t, func(context.Context, capability.Invocation, capability.Spec) error { return nil }, capability.Allow, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	loop.Model = modelService{complete: func(context.Context) (model.Response, error) {
		cancel()
		return model.Response{Profile: "test/model", Message: session.Message{Role: "assistant", Content: "done"}}, nil
	}}
	settled := false
	store.appendHook = func(ctx context.Context, cmd session.AppendCommand) error {
		for _, event := range cmd.Events {
			if event.Kind == session.AttemptFinished {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 5*time.Second || ctx.Err() != nil {
					t.Fatal("settlement is cancelled or unbounded")
				}
				settled = true
			}
		}
		return nil
	}
	if err := loop.Drive(ctx, lease); !errors.Is(err, context.Canceled) {
		t.Fatalf("drive=%v", err)
	}
	if !settled {
		t.Fatal("model outcome was lost on cancellation")
	}
}

func TestConstructorRequiresExplicitProduct(t *testing.T) {
	if _, err := agentloop.New(&commandStore{}, modelService{}, nil, nil, agentloop.Config{Profile: "test", Digest: "pin", Scope: profile.PrincipalScope, MaxSteps: 1, MaxAttempts: 1}); err == nil {
		t.Fatal("missing Product was accepted")
	}
}
