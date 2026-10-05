package agentruntime_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	agentruntime "github.com/costa92/agent-runtime"
	"github.com/costa92/agent-runtime/agent"
	"github.com/costa92/agent-runtime/authorization"
	"github.com/costa92/agent-runtime/definition"
	"github.com/costa92/agent-runtime/internal/testkit"
	"github.com/costa92/agent-runtime/policy"
	"github.com/costa92/agent-runtime/run"
	"github.com/costa92/agent-runtime/store"
	"github.com/costa92/agent-runtime/tool"
)

// --- fixtures -------------------------------------------------------------

func TestNoRequestScopedClaimReachesTheSnapshot(t *testing.T) {
	h := newHarness(t, answering("done"))
	started := start(t, h)

	encoded, err := json.Marshal(started)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(encoded) == "" {
		t.Fatal("empty snapshot")
	}
	for _, forbidden := range []string{"editor", "Claims", "claims"} {
		if contains(string(encoded), forbidden) {
			t.Fatalf("the snapshot carries request-scoped claims: %s", encoded)
		}
	}
}

func TestAnUnauthorizedDefinitionNeverCreatesARun(t *testing.T) {
	h := newHarness(t, answering("done"), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Authorization = fakeAuthorizer{
			err: run.NewError("forbidden", run.ErrorDenied, run.RetryNever),
		}
	}))

	if _, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:  principal(),
		Definition: run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
	}); run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("kind=%s want=denied", run.KindOf(err))
	}
}

// Refusing at creation is cheap; refusing later leaves a Run nobody can run.

func TestAnotherUserCannotAccessARun(t *testing.T) {
	intruder := principal()
	intruder.Ref.Subject = "other-user"

	for _, test := range []struct {
		name string
		act  func(context.Context, agentruntime.Runtime, run.ID, authorization.PrincipalContext) error
	}{
		{
			name: "inspect",
			act: func(ctx context.Context, runtime agentruntime.Runtime, id run.ID, principal authorization.PrincipalContext) error {
				_, err := runtime.Inspect(ctx, principal, id)
				return err
			},
		},
		{
			name: "events",
			act: func(ctx context.Context, runtime agentruntime.Runtime, id run.ID, principal authorization.PrincipalContext) error {
				_, err := runtime.ListEvents(ctx, principal, store.EventQuery{RunID: id, Limit: 100})
				return err
			},
		},
		{
			name: "cancel",
			act: func(ctx context.Context, runtime agentruntime.Runtime, id run.ID, principal authorization.PrincipalContext) error {
				_, err := runtime.Cancel(ctx, agentruntime.CancelRequest{RootID: id, RequestedBy: principal})
				return err
			},
		},
		{
			name: "approval",
			act: func(ctx context.Context, runtime agentruntime.Runtime, id run.ID, principal authorization.PrincipalContext) error {
				_, err := runtime.ResolveApproval(ctx, agentruntime.ApprovalDecision{
					RunID: id, ApprovalID: "approval-1", Approved: true, DecidedBy: principal,
				})
				return err
			},
		},
		{
			name: "invocation",
			act: func(ctx context.Context, runtime agentruntime.Runtime, id run.ID, principal authorization.PrincipalContext) error {
				_, err := runtime.ResolveInvocation(ctx, agentruntime.InvocationResolution{
					RunID: id, InvocationID: "invocation-1", Outcome: run.OutcomeNotApplied, ResolvedBy: principal,
				})
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t, answering("done"))
			started := start(t, h)

			err := test.act(t.Context(), h.runtime, started.ID, intruder)
			var runtimeError *run.Error
			if !errors.As(err, &runtimeError) || runtimeError.Code != run.CodeUnknownRun {
				t.Fatalf("cross-user %s error = %v, want opaque unknown Run", test.name, err)
			}
		})
	}
}

func TestRunOwnershipMatchesTheWholePrincipalReference(t *testing.T) {
	for name, mutate := range map[string]func(*authorization.PrincipalRef){
		"subject": func(ref *authorization.PrincipalRef) { ref.Subject = "other-user" },
		"tenant":  func(ref *authorization.PrincipalRef) { ref.Tenant = "other-tenant" },
		"kind":    func(ref *authorization.PrincipalRef) { ref.Kind = authorization.PrincipalService },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, answering("done"))
			started := start(t, h)
			intruder := principal()
			mutate(&intruder.Ref)

			_, err := h.runtime.Inspect(t.Context(), intruder, started.ID)
			var runtimeError *run.Error
			if !errors.As(err, &runtimeError) || runtimeError.Code != run.CodeUnknownRun {
				t.Fatalf("mismatched %s error = %v, want opaque unknown Run", name, err)
			}
		})
	}
}

// An idle queue is not a failure. A caller that could not tell them apart would
// log an error on every poll.

func TestAGrantCoversExactlyOneExecution(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"book_id":"b1","title":"兔"}`)
	if err := registry.Register(tool.Spec{
		Name: "render_picture_book", Description: "draw a book",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskHigh, SideEffect: policy.SideEffectWrite,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	var secondCallErr error
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		if request.Granted != nil {
			out, err := request.Ports.Tool(ctx, request.Granted.Name, request.Granted.Arguments)
			if err != nil {
				return agent.Response{}, err
			}
			// The approved write is done. Asking for another one is a new
			// request for a human to decide, not a continuation of the old
			// grant — the same question the resume path asks after an
			// interrupted effect is resolved.
			_, secondCallErr = request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"猫"}`))
			return agent.Response{Output: out}, nil
		}
		_, err := request.Ports.Tool(ctx, "render_picture_book", json.RawMessage(`{"theme":"兔"}`))
		return agent.Response{}, err
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "render_picture_book"}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Governance = fakeGovernance{policies: policy.Snapshot{
			Policies: []policy.Policy{{
				Name:  "writes-need-a-human",
				Scope: policy.ScopeTenant,
				Conditions: []policy.Condition{{
					Fact: policy.FactToolSideEffect, Operator: policy.OpEquals,
					Values: []string{string(policy.SideEffectWrite)},
				}},
				Decision: policy.DecisionRequireApproval,
			}},
		}}
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
	}))
	started := start(t, h)

	parked, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if parked.Run.State != run.StateWaitingApproval {
		t.Fatalf("state=%s want=waiting_approval", parked.Run.State)
	}

	if _, err := h.runtime.ResolveApproval(t.Context(), agentruntime.ApprovalDecision{
		RunID: started.ID, ApprovalID: parked.Run.PendingApprovalID,
		Approved: true, DecidedBy: principal(),
	}); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	h.clock.Advance(time.Minute)

	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance after confirm: %v", err)
	}

	if handler.Calls() != 1 {
		t.Fatalf("write calls=%d, want 1; the grant carried into a second effect", handler.Calls())
	}
	if secondCallErr == nil {
		t.Fatal("the second write was allowed to run under the first grant")
	}
	if !tool.IsApprovalRequired(secondCallErr) {
		t.Fatalf("second write refused with %v; a new write must ask a human again", secondCallErr)
	}
}

// A graph edge names its upstream by ref, and the engine follows it. What the
// downstream node receives must be the upstream's output, not the pointer to
// it: nothing on the agent side can dereference one — Ports has no read for it
// — so a node handed {"from":"output-..."} has only the pointer as its brief.
// In production the writer answered that it could not see the research and the
// assembler failed for want of a body, while every node still reported success.

func TestARunLabelRefusesATheModelInitiatedToolCallWithoutFailingTheRun(t *testing.T) {
	registry := tool.NewRegistry()
	handler := testkit.ToolSucceeding(`{"items":[{"url":"https://example.com"}]}`)
	if err := registry.Register(tool.Spec{
		Name: "search_evidence", Description: "retrieve",
		Parameters: json.RawMessage(`{"type":"object"}`),
		RiskLevel:  policy.RiskLow, SideEffect: policy.SideEffectRead,
	}, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	declared := definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: "search_evidence"}},
		RunLabels:      []string{"evidence_optional"},
	}
	noRetrieval := policy.Snapshot{
		Default: policy.DefaultRule{ReadOnly: policy.DecisionAllow, HighRisk: policy.DecisionDeny},
		Policies: []policy.Policy{{
			Name:      "no-retrieval-when-grounding-was-not-requested",
			Scope:     policy.ScopeTool,
			ScopeName: "search_evidence",
			Conditions: []policy.Condition{
				{Fact: policy.FactToolName, Operator: policy.OpEquals, Values: []string{"search_evidence"}},
				{Fact: policy.FactLabel, Operator: policy.OpIn, Values: []string{"evidence_optional"}},
			},
			Decision: policy.DecisionDeny,
		}},
	}

	// The agent asks for the tool unconditionally, the way a model that was
	// told not to search still asks for it.
	var toolErr error
	agentAsking := scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		_, toolErr = request.Ports.Tool(ctx, "search_evidence", json.RawMessage(`{"query":"k8s"}`))
		// A refusal is an answer the step can work with, which is the whole
		// reason a hard gate is usable here at all.
		return agent.Response{Output: json.RawMessage(`{"content":"written without citations"}`)}, nil
	}}

	t.Run("Labelled", func(t *testing.T) {
		toolErr = nil
		h := newHarness(t, agentAsking, withDefinition(declared), withDeps(func(deps *agentruntime.Dependencies) {
			deps.Governance = fakeGovernance{policies: noRetrieval}
			deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
		}))
		started, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
			Principal:    principal(),
			Definition:   run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Input:        json.RawMessage(`{"q":"x"}`),
			Budget:       run.Limits{LLMCalls: 10, Tokens: 10_000, ToolCalls: 10},
			Restrictions: run.Restrictions{Labels: []string{"evidence_optional"}},
		})
		if err != nil {
			t.Fatalf("start: %v", err)
		}

		result, err := h.runtime.Advance(t.Context(), started.ID)
		if err != nil {
			t.Fatalf("advance: %v", err)
		}
		if run.KindOf(toolErr) != run.ErrorDenied {
			t.Fatalf("tool error = %v; the label did not refuse the retrieval", toolErr)
		}
		if handler.Calls() != 0 {
			t.Fatalf("the refused tool ran anyway: %d calls", handler.Calls())
		}
		if result.Run.State != run.StateSucceeded {
			t.Fatalf("state=%s; the refusal failed the Run instead of letting the step finish without evidence",
				result.Run.State)
		}
	})

	t.Run("Unlabelled", func(t *testing.T) {
		toolErr = nil
		h := newHarness(t, agentAsking, withDefinition(declared), withDeps(func(deps *agentruntime.Dependencies) {
			deps.Governance = fakeGovernance{policies: noRetrieval}
			deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
		}))
		started := start(t, h)

		result, err := h.runtime.Advance(t.Context(), started.ID)
		if err != nil {
			t.Fatalf("advance: %v", err)
		}
		if toolErr != nil {
			t.Fatalf("tool error = %v; the rule refused a Run that never carried the label", toolErr)
		}
		if result.Run.State != run.StateSucceeded {
			t.Fatalf("state=%s", result.Run.State)
		}
	})
}

// A label the Definition never published is refused at Start rather than
// ignored. Ignoring it would leave the caller believing the Run is judged by a
// rule that never sees it, and the symptom is work going ahead that was
// supposed to be refused.

func TestStartRefusesALabelTheDefinitionDoesNotPublish(t *testing.T) {
	h := newHarness(t, answering("done"))

	_, err := h.runtime.Start(t.Context(), agentruntime.StartRequest{
		Principal:    principal(),
		Definition:   run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Input:        json.RawMessage(`{"q":"x"}`),
		Budget:       run.Limits{LLMCalls: 1, Tokens: 100, ToolCalls: 1},
		Restrictions: run.Restrictions{Labels: []string{"invented_by_the_caller"}},
	})
	if run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("start error = %v, want an invalid-argument refusal", err)
	}
}
