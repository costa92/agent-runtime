package capability

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type testRecorder struct {
	mu          sync.Mutex
	keys        map[string]bool
	beginErr    error
	settleErr   error
	receipt     BeginReceipt
	beginCount  int
	settleCount int
	settled     Result
	onBegin     func()
	mutateInput bool
}

func (r *testRecorder) Begin(_ context.Context, inv Invocation, _ Spec, claim BudgetClaim) (BeginReceipt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.beginCount++
	if r.onBegin != nil {
		r.onBegin()
	}
	if r.mutateInput && len(inv.Input) > 0 {
		inv.Input[0] = '['
	}
	if r.beginErr != nil {
		return BeginReceipt{}, r.beginErr
	}
	if claim.Units != 7 {
		return BeginReceipt{}, errors.New("budget claim was not forwarded")
	}
	if r.keys == nil {
		r.keys = make(map[string]bool)
	}
	if r.keys[inv.IdempotencyKey] {
		return BeginReceipt{}, ErrDuplicate
	}
	r.keys[inv.IdempotencyKey] = true
	return r.receipt, nil
}

func (r *testRecorder) Settle(_ context.Context, _ Invocation, _ BeginReceipt, result Result) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.settleCount++
	r.settled = result
	return r.settleErr
}

func invocation() Invocation {
	return Invocation{ID: "call-1", Name: "publish", SessionID: "s-1", TurnID: "t-1", StepID: "st-1", Principal: "user-1", Input: json.RawMessage(`{"content":"hello"}`), IdempotencyKey: "key-1", Fence: 4}
}

func service(t *testing.T, spec Spec, handler Handler, hooks Hooks) *Service {
	t.Helper()
	if hooks.Authorize == nil {
		hooks.Authorize = func(context.Context, Invocation, Spec) error { return nil }
	}
	if hooks.Policy == nil {
		hooks.Policy = func(context.Context, Invocation, Spec) (PolicyDecision, error) {
			return PolicyDecision{Effect: Allow}, nil
		}
	}
	if hooks.Budget == nil {
		hooks.Budget = func(context.Context, Invocation, Spec) (BudgetClaim, error) {
			return BudgetClaim{Dimension: "calls", Units: 7}, nil
		}
	}
	svc, err := New(map[string]Registration{spec.Name: {Spec: spec, Handler: handler}}, hooks)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func writeSpec() Spec { return Spec{Name: "publish", Version: "v1", Effect: Write} }

func TestSpecsAreSortedAndDetachedFromRegistrations(t *testing.T) {
	parameters := json.RawMessage(`{"type":"object"}`)
	hooks := Hooks{
		Authorize: func(context.Context, Invocation, Spec) error { return nil },
		Policy: func(context.Context, Invocation, Spec) (PolicyDecision, error) {
			return PolicyDecision{Effect: Allow}, nil
		},
		Budget: func(context.Context, Invocation, Spec) (BudgetClaim, error) {
			return BudgetClaim{Dimension: "tool_calls", Units: 1}, nil
		},
	}
	svc, err := New(map[string]Registration{
		"zeta":  {Spec: Spec{Name: "zeta", Version: "1", Effect: Read, Description: "Z", Parameters: parameters}, Handler: success},
		"alpha": {Spec: Spec{Name: "alpha", Version: "1", Effect: Read}, Handler: success},
	}, hooks)
	if err != nil {
		t.Fatal(err)
	}
	parameters[0] = '['
	specs := svc.Specs()
	if specs[0].Name != "alpha" || specs[1].Name != "zeta" || specs[0].Description != "alpha" || string(specs[0].Parameters) != "{}" || string(specs[1].Parameters) != `{"type":"object"}` {
		t.Fatalf("unexpected catalog: %#v", specs)
	}
	specs[1].Parameters[0] = '['
	if string(svc.Specs()[1].Parameters) != `{"type":"object"}` {
		t.Fatal("catalog mutated through returned schema")
	}
}

func success(context.Context, Invocation) (Result, error) {
	return Result{Status: Succeeded, Output: json.RawMessage(`{"id":"remote-1"}`)}, nil
}

func TestGovernedPathAndInterceptorCannotSkipGates(t *testing.T) {
	var order []string
	svc := service(t, writeSpec(), func(_ context.Context, inv Invocation) (Result, error) {
		order = append(order, "handler")
		if string(inv.Input) != `{"content":"rewritten"}` {
			t.Fatalf("handler input: %s", inv.Input)
		}
		return success(context.Background(), inv)
	}, Hooks{
		Interceptors: []InputInterceptor{func(_ context.Context, _ Spec, _ json.RawMessage) (json.RawMessage, error) {
			order = append(order, "interceptor")
			return json.RawMessage(`{"content":"rewritten"}`), nil
		}},
		Authorize: func(_ context.Context, inv Invocation, _ Spec) error {
			order = append(order, "authorize")
			if string(inv.Input) != `{"content":"rewritten"}` {
				t.Fatal("authorization saw original input")
			}
			return nil
		},
		Policy: func(context.Context, Invocation, Spec) (PolicyDecision, error) {
			order = append(order, "policy")
			return PolicyDecision{Effect: Allow}, nil
		},
		Budget: func(context.Context, Invocation, Spec) (BudgetClaim, error) {
			order = append(order, "budget")
			return BudgetClaim{Units: 7}, nil
		},
	})
	p, err := svc.Prepare(context.Background(), invocation())
	if err != nil {
		t.Fatal(err)
	}
	recorder := &testRecorder{receipt: BeginReceipt{Recorded: true, Reserved: true}}
	result, err := svc.Execute(context.Background(), p, recorder)
	if err != nil || result.Status != Succeeded {
		t.Fatalf("execute: %+v, %v", result, err)
	}
	if !reflect.DeepEqual(order, []string{"interceptor", "authorize", "policy", "budget", "handler"}) {
		t.Fatalf("gate order: %v", order)
	}
	if recorder.beginCount != 1 || recorder.settleCount != 1 {
		t.Fatalf("recorder counts: %d, %d", recorder.beginCount, recorder.settleCount)
	}
	if _, err := svc.Execute(context.Background(), p, recorder); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("prepared call reused: %v", err)
	}
}

func TestDenialAndBudgetFailureNeverBegin(t *testing.T) {
	for _, tc := range []struct {
		name  string
		hooks Hooks
	}{
		{"authorization", Hooks{Authorize: func(context.Context, Invocation, Spec) error { return errors.New("forbidden") }}},
		{"policy", Hooks{Policy: func(context.Context, Invocation, Spec) (PolicyDecision, error) {
			return PolicyDecision{Effect: Deny}, nil
		}}},
		{"budget", Hooks{Budget: func(context.Context, Invocation, Spec) (BudgetClaim, error) {
			return BudgetClaim{}, errors.New("exhausted")
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			svc := service(t, writeSpec(), func(context.Context, Invocation) (Result, error) { called = true; return Result{}, nil }, tc.hooks)
			if _, err := svc.Prepare(context.Background(), invocation()); err == nil {
				t.Fatal("denial was accepted")
			}
			if called {
				t.Fatal("handler ran during Prepare")
			}
		})
	}
}

func TestApprovalParksAndResumeChecksExactIdentity(t *testing.T) {
	approved := false
	var approvedFingerprint string
	spec := writeSpec()
	spec.RequiresApproval = true
	svc := service(t, spec, success, Hooks{Policy: func(_ context.Context, inv Invocation, _ Spec) (PolicyDecision, error) {
		if !approved {
			return PolicyDecision{Effect: RequireApproval}, nil
		}
		return PolicyDecision{Effect: RequireApproval, ApprovedFingerprint: approvedFingerprint}, nil
	}})
	inv := invocation()
	pending, err := svc.Prepare(context.Background(), inv)
	if err != nil || pending.Status() != AwaitingApproval {
		t.Fatalf("approval state: %v, %v", pending, err)
	}
	recorder := &testRecorder{receipt: BeginReceipt{Recorded: true, Reserved: true}}
	if _, err := svc.Execute(context.Background(), pending, recorder); !errors.Is(err, ErrApprovalRequired) || recorder.beginCount != 0 {
		t.Fatalf("parked call executed: %v", err)
	}
	approved = true
	approvedFingerprint = pending.ApprovalFingerprint()
	inv.ApprovalID = "approval-1"
	inv.Fence = 5 // Claiming a resumed turn changes its lease fence.
	changed := inv
	changed.Input = json.RawMessage(`{"content":"changed"}`)
	changedCall, err := svc.Prepare(context.Background(), changed)
	if err != nil || changedCall.Status() != AwaitingApproval {
		t.Fatalf("changed input reused approval: %v, %v", changedCall, err)
	}
	ready, err := svc.Prepare(context.Background(), inv)
	if err != nil || ready.Status() != Ready {
		t.Fatalf("approved retry: %v, %v", ready, err)
	}
	if _, err := svc.Execute(context.Background(), ready, recorder); err != nil {
		t.Fatal(err)
	}
}

func TestBeginFailureStaleAndDuplicatePreventExecution(t *testing.T) {
	for _, beginErr := range []error{errors.New("stale fence"), ErrDuplicate} {
		called := false
		svc := service(t, writeSpec(), func(context.Context, Invocation) (Result, error) {
			called = true
			return Result{Status: Succeeded}, nil
		}, Hooks{})
		p, err := svc.Prepare(context.Background(), invocation())
		if err != nil {
			t.Fatal(err)
		}
		recorder := &testRecorder{beginErr: beginErr}
		if _, err := svc.Execute(context.Background(), p, recorder); !errors.Is(err, beginErr) {
			t.Fatalf("begin error: %v", err)
		}
		if called || recorder.settleCount != 0 {
			t.Fatal("handler or settlement ran after failed begin")
		}
	}
	called := false
	svc := service(t, writeSpec(), func(context.Context, Invocation) (Result, error) {
		called = true
		return Result{Status: Succeeded}, nil
	}, Hooks{})
	p, err := svc.Prepare(context.Background(), invocation())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Execute(context.Background(), p, &testRecorder{receipt: BeginReceipt{Recorded: true}}); !errors.Is(err, ErrBeginUnconfirmed) || called {
		t.Fatalf("unreserved invocation executed: %v", err)
	}
}

func TestDuplicateIdempotencyKeyAcrossPreparedCalls(t *testing.T) {
	called := 0
	svc := service(t, writeSpec(), func(context.Context, Invocation) (Result, error) { called++; return Result{Status: Succeeded}, nil }, Hooks{})
	recorder := &testRecorder{receipt: BeginReceipt{Recorded: true, Reserved: true}}
	first, _ := svc.Prepare(context.Background(), invocation())
	if _, err := svc.Execute(context.Background(), first, recorder); err != nil {
		t.Fatal(err)
	}
	secondInv := invocation()
	secondInv.ID = "call-2"
	second, _ := svc.Prepare(context.Background(), secondInv)
	if _, err := svc.Execute(context.Background(), second, recorder); !errors.Is(err, ErrDuplicate) || called != 1 {
		t.Fatalf("duplicate executed: %v, called %d", err, called)
	}
}

func TestKnownFailureUnknownAndSettlementFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler Handler
		want    Outcome
		code    string
	}{
		{"known failure", func(context.Context, Invocation) (Result, error) {
			return Result{Status: Failed, ErrorCode: "remote_rejected"}, nil
		}, Failed, "remote_rejected"},
		{"uncertain error", func(context.Context, Invocation) (Result, error) { return Result{}, errors.New("timeout") }, Unknown, "execution_uncertain"},
		{"panic", func(context.Context, Invocation) (Result, error) { panic("crash") }, Unknown, "handler_panicked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := service(t, writeSpec(), tc.handler, Hooks{})
			p, err := svc.Prepare(context.Background(), invocation())
			if err != nil {
				t.Fatal(err)
			}
			recorder := &testRecorder{receipt: BeginReceipt{Recorded: true, Reserved: true}}
			result, err := svc.Execute(context.Background(), p, recorder)
			if err != nil || result.Status != tc.want || result.ErrorCode != tc.code || recorder.settled.Status != tc.want {
				t.Fatalf("result: %+v, %v, settled %+v", result, err, recorder.settled)
			}
		})
	}
	svc := service(t, writeSpec(), success, Hooks{})
	p, _ := svc.Prepare(context.Background(), invocation())
	recorder := &testRecorder{receipt: BeginReceipt{Recorded: true, Reserved: true}, settleErr: errors.New("db down")}
	result, err := svc.Execute(context.Background(), p, recorder)
	if !errors.Is(err, ErrSettlement) || result.Status != Unknown {
		t.Fatalf("settlement failure: %+v, %v", result, err)
	}
}

func TestCancellationBeforeAndAfterBegin(t *testing.T) {
	called := false
	svc := service(t, writeSpec(), func(context.Context, Invocation) (Result, error) {
		called = true
		return Result{Status: Succeeded}, nil
	}, Hooks{})
	p, _ := svc.Prepare(context.Background(), invocation())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	recorder := &testRecorder{receipt: BeginReceipt{Recorded: true, Reserved: true}}
	if _, err := svc.Execute(ctx, p, recorder); !errors.Is(err, context.Canceled) || recorder.beginCount != 0 {
		t.Fatalf("pre-begin cancellation: %v", err)
	}
	p, _ = svc.Prepare(context.Background(), invocation())
	ctx, cancel = context.WithCancel(context.Background())
	recorder.onBegin = cancel
	result, err := svc.Execute(ctx, p, recorder)
	if err != nil || result.Status != Unknown || result.ErrorCode != "canceled_after_begin" || called || recorder.settleCount != 1 {
		t.Fatalf("post-begin cancellation: %+v, %v, handler %v, settle %d", result, err, called, recorder.settleCount)
	}
}

func TestWriteRequiresIdempotencyKey(t *testing.T) {
	svc := service(t, writeSpec(), success, Hooks{})
	inv := invocation()
	inv.IdempotencyKey = ""
	if _, err := svc.Prepare(context.Background(), inv); err == nil || !strings.Contains(err.Error(), "idempotency") {
		t.Fatalf("missing key accepted: %v", err)
	}
}

func TestCallbacksCannotMutateAuthorizedInput(t *testing.T) {
	svc := service(t, writeSpec(), func(_ context.Context, inv Invocation) (Result, error) {
		if string(inv.Input) != `{"content":"hello"}` {
			t.Fatalf("handler saw mutated input: %s", inv.Input)
		}
		inv.Input[0] = '['
		return Result{Status: Succeeded}, nil
	}, Hooks{})
	inv := invocation()
	p, err := svc.Prepare(context.Background(), inv)
	if err != nil {
		t.Fatal(err)
	}
	inv.Input[0] = '['
	exposed := p.Invocation()
	exposed.Input[0] = '['
	recorder := &testRecorder{receipt: BeginReceipt{Recorded: true, Reserved: true}, mutateInput: true}
	if _, err := svc.Execute(context.Background(), p, recorder); err != nil {
		t.Fatal(err)
	}
}
