package tool_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
)

func principal() authorization.PrincipalRef {
	return authorization.PrincipalRef{Subject: "u-1", Tenant: "t-1", Kind: authorization.PrincipalUser}
}

// allowAll is a snapshot that permits even a high-risk write, so a test about
// something else does not accidentally test the default rule.
func allowAll() policy.Snapshot {
	return policy.Snapshot{
		Digest:  "snap-allow",
		Default: policy.DefaultRule{HighRisk: policy.DecisionAllow, ReadOnly: policy.DecisionAllow},
	}
}

func publishRequest() tool.InvocationRequest {
	return tool.InvocationRequest{
		InvocationID:   "inv-1",
		Tool:           "publish",
		Arguments:      json.RawMessage(`{}`),
		Principal:      principal(),
		IdempotencyKey: "k1",
		Policies:       allowAll(),
		// Declared by the fixture Definition. An empty allowlist denies
		// everything, so a request that omitted this would be refused at the
		// allowlist stage and never reach what the test is about.
		Allowlist: []string{"publish", "search"},
	}
}

func gatewayWith(t *testing.T, spec tool.Spec, handler tool.Handler, options ...tool.Option) *tool.Gateway {
	t.Helper()
	registry := tool.NewRegistry()
	if err := registry.Register(spec, handler); err != nil {
		t.Fatalf("register: %v", err)
	}
	registry.Freeze()
	return tool.NewGateway(registry, testkit.AllowAllToolAuthorizer(), options...)
}

type gatewaySchemaProcessor struct {
	normalize func(schema, value json.RawMessage) (json.RawMessage, error)
}

func (gatewaySchemaProcessor) ValidateSchema(json.RawMessage) error { return nil }

func (p gatewaySchemaProcessor) NormalizeValue(schema, value json.RawMessage) (json.RawMessage, error) {
	return p.normalize(schema, value)
}

type countingAuthorizer struct{ calls int }

func (a *countingAuthorizer) Authorize(context.Context, authorization.PrincipalRef, tool.Spec) error {
	a.calls++
	return nil
}

type decisionRecorder struct{ decisions []tool.DecisionRecord }

func (r *decisionRecorder) Decided(_ context.Context, decision tool.DecisionRecord) {
	r.decisions = append(r.decisions, decision)
}

func (*decisionRecorder) Finished(context.Context, tool.ResultRecord) {}

func TestInvalidToolArgumentsStopBeforeAuthorizationAndHandler(t *testing.T) {
	authorizer := &countingAuthorizer{}
	handler := testkit.ToolSucceeding(`{"ok":true}`)
	processor := gatewaySchemaProcessor{normalize: func(_, _ json.RawMessage) (json.RawMessage, error) {
		return nil, errors.New("count must be an integer")
	}}
	registry := tool.NewRegistry()
	spec := testkit.PublishSpec()
	spec.Parameters = json.RawMessage(`{"type":"object"}`)
	if err := registry.Register(spec, handler); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()
	gateway := tool.NewGateway(registry, authorizer, tool.WithSchema(processor))

	request := publishRequest()
	request.Arguments = json.RawMessage(`{"count":1.5}`)
	if _, err := gateway.Prepare(t.Context(), request); run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("kind = %s, want invalid: %v", run.KindOf(err), err)
	}
	if authorizer.calls != 0 {
		t.Fatalf("authorization calls = %d, want 0", authorizer.calls)
	}
	if handler.Calls() != 0 {
		t.Fatalf("handler calls = %d, want 0", handler.Calls())
	}
}

func TestNormalizedToolArgumentsReachTheInvocationRecordAndHandler(t *testing.T) {
	const normalized = `{"count":1}`
	var handled json.RawMessage
	handler := tool.HandlerFunc(func(_ context.Context, invocation tool.Invocation) (tool.Result, error) {
		handled = append(json.RawMessage(nil), invocation.Arguments...)
		return tool.Result{Output: json.RawMessage(`{"ok":true}`)}, nil
	})
	recorder := &decisionRecorder{}
	processor := gatewaySchemaProcessor{normalize: func(schema, value json.RawMessage) (json.RawMessage, error) {
		if string(value) != `{"count":1.0}` {
			return nil, fmt.Errorf("unexpected value %s", value)
		}
		return json.RawMessage(normalized), nil
	}}
	spec := testkit.PublishSpec()
	spec.Parameters = json.RawMessage(`{"type":"object"}`)
	gateway := gatewayWith(t, spec, handler,
		tool.WithSchema(processor), tool.WithRecorder(recorder))

	request := publishRequest()
	request.Arguments = json.RawMessage(`{"count":1.0}`)
	prepared, err := gateway.Prepare(t.Context(), request)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if string(prepared.Invocation.Arguments) != normalized {
		t.Fatalf("invocation arguments = %s, want %s", prepared.Invocation.Arguments, normalized)
	}
	if len(recorder.decisions) != 1 {
		t.Fatalf("decision records = %d, want 1", len(recorder.decisions))
	}
	wantDigest := fmt.Sprintf("%x", sha256.Sum256([]byte(normalized)))
	if recorder.decisions[0].ArgsDigest != wantDigest {
		t.Fatalf("argument digest = %s, want normalized digest %s", recorder.decisions[0].ArgsDigest, wantDigest)
	}
	if _, err := gateway.Execute(t.Context(), tool.CommittedInvocation{Prepared: prepared}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if string(handled) != normalized {
		t.Fatalf("handler arguments = %s, want %s", handled, normalized)
	}
}

// The whole reason unknown is a separate outcome: the effect may have happened,
// so the one thing that must not occur is a second call.
func TestGatewayNeverRetriesUnknownSideEffect(t *testing.T) {
	handler := testkit.ToolReturning(run.NewError("publish.unknown", run.ErrorUnknown, run.RetryNever))
	gateway := gatewayWith(t, testkit.PublishSpec(), handler)
	ctx := context.Background()

	prepared, err := gateway.Prepare(ctx, publishRequest())
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := gateway.Execute(ctx, tool.CommittedInvocation{Prepared: prepared})
	if run.KindOf(err) != run.ErrorUnknown {
		t.Fatalf("kind=%s error=%v", run.KindOf(err), err)
	}
	if mutation.Outcome != run.OutcomeUnknown {
		t.Fatalf("outcome=%s want=unknown", mutation.Outcome)
	}
	if handler.Calls() != 1 {
		t.Fatalf("calls=%d", handler.Calls())
	}
}

// A write tool that never declared idempotency cannot distinguish "failed" from
// "succeeded and lost the answer", so it reconciles rather than retries.
func TestUndeclaredIdempotencyFailureBecomesUnknown(t *testing.T) {
	handler := testkit.ToolReturning(run.NewError("publish.timeout", run.ErrorRetryable, run.RetryBackoff))
	gateway := gatewayWith(t, testkit.PublishSpec(), handler)
	ctx := context.Background()

	prepared, err := gateway.Prepare(ctx, publishRequest())
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := gateway.Execute(ctx, tool.CommittedInvocation{Prepared: prepared})
	if run.KindOf(err) != run.ErrorUnknown {
		t.Fatalf("kind=%s want=unknown; a non-idempotent write was treated as retryable", run.KindOf(err))
	}
	if mutation.Outcome != run.OutcomeUnknown {
		t.Fatalf("outcome=%s want=unknown", mutation.Outcome)
	}
}

// A read-only tool's failure is just a failure: nothing happened, so retrying
// is safe and calling it unknown would park a Run for no reason.
func TestReadOnlyFailureIsNotUnknown(t *testing.T) {
	handler := testkit.ToolReturning(run.NewError("search.timeout", run.ErrorRetryable, run.RetryBackoff))
	gateway := gatewayWith(t, testkit.SearchSpec(), handler)
	ctx := context.Background()

	request := publishRequest()
	request.Tool = "search"
	prepared, err := gateway.Prepare(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	mutation, _ := gateway.Execute(ctx, tool.CommittedInvocation{Prepared: prepared})
	if mutation.Outcome != run.OutcomeNotApplied {
		t.Fatalf("outcome=%s want=not_applied", mutation.Outcome)
	}
}

// The order is a property of the gateway. A chain whose order can drift is a
// chain where authorization can end up after the effect.
func TestChainRunsEveryStageInTheFixedOrder(t *testing.T) {
	recorder := testkit.NewStageRecorder()
	gateway := gatewayWith(t, testkit.PublishSpec(), testkit.ToolSucceeding(`{"ok":true}`),
		tool.WithObserver(recorder))

	if _, err := gateway.Prepare(context.Background(), publishRequest()); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	if !slices.Equal(recorder.Stages(), tool.Stages) {
		t.Fatalf("stage order:\n got %v\nwant %v", recorder.Stages(), tool.Stages)
	}
}

func TestChainStopsAtTheRefusingStage(t *testing.T) {
	recorder := testkit.NewStageRecorder()
	registry := tool.NewRegistry()
	if err := registry.Register(testkit.PublishSpec(), testkit.ToolSucceeding(`{}`)); err != nil {
		t.Fatalf("register: %v", err)
	}
	gateway := tool.NewGateway(registry, testkit.DenyToolAuthorizer("publish"), tool.WithObserver(recorder))

	if _, err := gateway.Prepare(context.Background(), publishRequest()); run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("error=%s want=denied", run.KindOf(err))
	}

	stages := recorder.Stages()
	if len(stages) == 0 || stages[len(stages)-1] != tool.StageAuthorize {
		t.Fatalf("the chain continued past the refusal: %v", stages)
	}
}

// A denied call must be non-executable: no Invocation begins and the handler is
// never reached. Anything else would mean the effect happened and governance
// merely disapproved afterwards.
func TestPolicyDenyNeverReachesTheHandler(t *testing.T) {
	handler := testkit.ToolSucceeding(`{}`)
	gateway := gatewayWith(t, testkit.PublishSpec(), handler)

	request := publishRequest()
	request.Policies = policy.Snapshot{
		Digest: "snap-deny",
		Policies: []policy.Policy{{
			Name: "no-publish", Scope: policy.ScopeTool, Decision: policy.DecisionDeny,
			Conditions: []policy.Condition{{
				Fact: policy.FactToolName, Operator: policy.OpEquals, Values: []string{"publish"},
			}},
		}},
	}

	if _, err := gateway.Prepare(context.Background(), request); run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("error=%s want=denied", run.KindOf(err))
	}
	if handler.Calls() != 0 {
		t.Fatalf("a denied call reached the handler %d times", handler.Calls())
	}
}

// A shadow policy is evaluated and reported, never enforced — which is what
// makes it safe to publish a rule and watch what it would have done.
func TestShadowPolicyReportsWithoutRefusing(t *testing.T) {
	handler := testkit.ToolSucceeding(`{"ok":true}`)
	gateway := gatewayWith(t, testkit.PublishSpec(), handler)
	ctx := context.Background()

	request := publishRequest()
	request.Policies = allowAll()
	request.Policies.Policies = []policy.Policy{{
		Name: "would-deny", Scope: policy.ScopeTool, Decision: policy.DecisionDeny, Shadow: true,
		Conditions: []policy.Condition{{
			Fact: policy.FactToolName, Operator: policy.OpEquals, Values: []string{"publish"},
		}},
	}}

	prepared, err := gateway.Prepare(ctx, request)
	if err != nil {
		t.Fatalf("a shadow policy refused the call: %v", err)
	}
	if len(prepared.Explanation.Matched) != 1 || !prepared.Explanation.Matched[0].Shadow {
		t.Fatalf("the shadow verdict was not reported: %+v", prepared.Explanation.Matched)
	}
	if prepared.Explanation.Decision != policy.DecisionAllow {
		t.Fatalf("decision=%s want=allow", prepared.Explanation.Decision)
	}
	if _, err := gateway.Execute(ctx, tool.CommittedInvocation{Prepared: prepared}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if handler.Calls() != 1 {
		t.Fatalf("calls=%d want=1", handler.Calls())
	}
}

func TestRequireApprovalParksBeforeAnyEffect(t *testing.T) {
	handler := testkit.ToolSucceeding(`{}`)
	gateway := gatewayWith(t, testkit.PublishSpec(), handler)

	request := publishRequest()
	request.Policies.Policies = []policy.Policy{{
		Name: "approve-publish", Scope: policy.ScopeTool, Decision: policy.DecisionRequireApproval,
		Conditions: []policy.Condition{{
			Fact: policy.FactToolName, Operator: policy.OpEquals, Values: []string{"publish"},
		}},
	}}

	_, err := gateway.Prepare(context.Background(), request)
	if run.KindOf(err) != run.ErrorInterrupted {
		t.Fatalf("kind=%s want=interrupted", run.KindOf(err))
	}
	if handler.Calls() != 0 {
		t.Fatal("an unapproved call reached the handler")
	}
}

// The handler is reachable only through Execute, and only for an invocation the
// Runtime committed. A caller that assembled the ticket itself skipped the
// durable record that the effect was about to happen.
func TestHandlerIsUnreachableWithoutACommittedInvocation(t *testing.T) {
	handler := testkit.ToolSucceeding(`{}`)
	gateway := gatewayWith(t, testkit.PublishSpec(), handler)

	forged := tool.CommittedInvocation{Prepared: tool.PreparedInvocation{
		Invocation: tool.Invocation{ID: "inv-1", Tool: "publish"},
		Spec:       testkit.PublishSpec(),
	}}
	if _, err := gateway.Execute(context.Background(), forged); run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("error=%s want=invalid", run.KindOf(err))
	}
	if handler.Calls() != 0 {
		t.Fatal("a forged ticket reached the handler")
	}
}

func TestToolOutsideTheDefinitionAllowlistIsDenied(t *testing.T) {
	handler := testkit.ToolSucceeding(`{}`)
	gateway := gatewayWith(t, testkit.PublishSpec(), handler)

	request := publishRequest()
	request.Allowlist = []string{"search"}

	if _, err := gateway.Prepare(context.Background(), request); run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("error=%s want=denied", run.KindOf(err))
	}
}

func TestWriteToolWithoutAnIdempotencyKeyIsRefused(t *testing.T) {
	gateway := gatewayWith(t, testkit.PublishSpec(), testkit.ToolSucceeding(`{}`))

	request := publishRequest()
	request.IdempotencyKey = ""

	if _, err := gateway.Prepare(context.Background(), request); run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("error=%s want=invalid", run.KindOf(err))
	}
}

// Truncating JSON produces something that parses as nothing, and the tool that
// overran would not be named.
func TestOversizedResultIsRefusedRatherThanTruncated(t *testing.T) {
	big := `{"data":"` + string(make([]byte, 200)) + `"}`
	spec := testkit.SearchSpec()
	spec.MaxResultBytes = 32
	gateway := gatewayWith(t, spec, testkit.ToolSucceeding(big))
	ctx := context.Background()

	request := publishRequest()
	request.Tool = "search"
	prepared, err := gateway.Prepare(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.Execute(ctx, tool.CommittedInvocation{Prepared: prepared}); run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("error=%s want=invalid", run.KindOf(err))
	}
}

func TestRegistryRefusesUndeclaredRiskOrSideEffect(t *testing.T) {
	registry := tool.NewRegistry()
	spec := testkit.SearchSpec()
	spec.RiskLevel = ""

	if err := registry.Register(spec, testkit.ToolSucceeding(`{}`)); run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("error=%s want=invalid", run.KindOf(err))
	}
}

// A Run that refuses side effects refuses a writing tool, however permissive
// its tenant's policy is.
//
// This is what an evaluation trial runs under: the same Definition as
// production, proving the agent would publish without publishing anything. A
// narrowing the caller asked for cannot be something a published rule overrides,
// or it would mean nothing on exactly the tenants whose policies are loosest.
func TestARunThatRefusesSideEffectsRefusesAWritingTool(t *testing.T) {
	handler := testkit.ToolSucceeding(`"published"`)
	gateway := gatewayWith(t, testkit.PublishSpec(), handler)

	request := publishRequest()
	request.DenySideEffects = true

	_, err := gateway.Prepare(context.Background(), request)
	if run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("err=%v; a writing tool ran under a Run that refuses side effects", err)
	}
	if handler.Calls() != 0 {
		t.Fatal("the tool was executed")
	}
}

// Refusing side effects does not refuse everything: a trial that could not read
// would prove nothing about the agent.
func TestRefusingSideEffectsStillAllowsAReadOnlyTool(t *testing.T) {
	gateway := gatewayWith(t, testkit.SearchSpec(), testkit.ToolSucceeding(`"found"`))

	request := publishRequest()
	request.Tool = "search"
	request.DenySideEffects = true

	if _, err := gateway.Prepare(context.Background(), request); err != nil {
		t.Fatalf("a read-only tool was refused: %v", err)
	}
}

// A tool the frozen registry does not carry is not callable, full stop.
//
// This replaces a test for the removed BindingLookup seam, which asserted that
// a tool published after the freeze became resolvable. Nothing wired that seam,
// so what it documented was a capability the product did not have; what the
// gateway actually guarantees is the opposite, and that is worth an assertion.
func TestGatewayRefusesAToolTheFrozenRegistryDoesNotCarry(t *testing.T) {
	registry := tool.NewRegistry()
	registry.Freeze()
	gateway := tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())

	request := publishRequest()
	request.Tool = "external.search"
	request.Allowlist = []string{"external.search"}

	if _, err := gateway.Prepare(context.Background(), request); err == nil {
		t.Fatal("a tool nothing registered was resolved")
	}
}

// A FailSafe write tool declares that a failed call left nothing behind, so
// the handler's own error Kind decides the outcome: an ErrorRetryable failure
// is not_applied and may be retried instead of parking the Run.
func TestFailSafeWriteToolFailureRespectsHandlerKind(t *testing.T) {
	spec := testkit.PublishSpec()
	spec.FailSafe = true
	handler := testkit.ToolReturning(run.NewError("publish.timeout", run.ErrorRetryable, run.RetryBackoff))
	gateway := gatewayWith(t, spec, handler)
	ctx := context.Background()

	prepared, err := gateway.Prepare(ctx, publishRequest())
	if err != nil {
		t.Fatal(err)
	}
	mutation, err := gateway.Execute(ctx, tool.CommittedInvocation{Prepared: prepared})
	if run.KindOf(err) != run.ErrorRetryable {
		t.Fatalf("kind=%s want=retryable; the handler's Kind was discarded", run.KindOf(err))
	}
	if mutation.Outcome != run.OutcomeNotApplied {
		t.Fatalf("outcome=%s want=not_applied", mutation.Outcome)
	}
}

// FailSafe does not weaken the unknown guard: an ErrorUnknown failure still
// means the effect may have happened, and that still parks for resolution.
func TestFailSafeWriteToolUnknownFailureStillUnknown(t *testing.T) {
	spec := testkit.PublishSpec()
	spec.FailSafe = true
	handler := testkit.ToolReturning(run.NewError("publish.unknown", run.ErrorUnknown, run.RetryNever))
	gateway := gatewayWith(t, spec, handler)
	ctx := context.Background()

	prepared, err := gateway.Prepare(ctx, publishRequest())
	if err != nil {
		t.Fatal(err)
	}
	mutation, _ := gateway.Execute(ctx, tool.CommittedInvocation{Prepared: prepared})
	if mutation.Outcome != run.OutcomeUnknown {
		t.Fatalf("outcome=%s want=unknown", mutation.Outcome)
	}
}

// A handler whose effect outlives the call has to be able to record what that
// effect belonged to. Withholding the Run identity did not stop a second
// writer — handlers still reach no Run state — it only left async jobs with no
// way back to the conversation that started them, which is how the assistant's
// task cards stopped appearing at all.
func TestGatewayTellsTheHandlerWhichRunTheCallBelongsTo(t *testing.T) {
	var seen run.ID
	handler := tool.HandlerFunc(func(_ context.Context, invocation tool.Invocation) (tool.Result, error) {
		seen = invocation.RunID
		return tool.Result{Output: json.RawMessage(`{}`)}, nil
	})
	gateway := gatewayWith(t, testkit.PublishSpec(), handler)

	request := publishRequest()
	request.RunID = "run-abc123"
	prepared, err := gateway.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if prepared.Invocation.RunID != "run-abc123" {
		t.Fatalf("prepared RunID = %q, want run-abc123", prepared.Invocation.RunID)
	}
	if _, err := gateway.Execute(context.Background(), tool.CommittedInvocation{Prepared: prepared}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if seen != "run-abc123" {
		t.Fatalf("handler saw RunID %q, want run-abc123", seen)
	}
}

// sleepingHandler blocks until its call context is done, the way a real
// handler that respects cancellation would, and reports the context's own
// error as its failure.
func sleepingHandler() tool.Handler {
	return tool.HandlerFunc(func(ctx context.Context, _ tool.Invocation) (tool.Result, error) {
		<-ctx.Done()
		return tool.Result{}, ctx.Err()
	})
}

// A read-only tool that outlives its call ceiling is an ordinary retryable
// failure: nothing it might have done needs reconciling.
func TestASlowReadToolTimesOutAsRetryable(t *testing.T) {
	gateway := gatewayWith(t, testkit.SearchSpec(), sleepingHandler(),
		tool.WithToolTimeout(10*time.Millisecond, 50*time.Millisecond))
	ctx := context.Background()

	request := publishRequest()
	request.Tool = "search"
	prepared, err := gateway.Prepare(ctx, request)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	mutation, err := gateway.Execute(ctx, tool.CommittedInvocation{Prepared: prepared})
	if run.KindOf(err) != run.ErrorRetryable {
		t.Fatalf("kind=%s want=retryable: %v", run.KindOf(err), err)
	}
	if run.CodeOf(err) != "tool.timeout" {
		t.Fatalf("code=%s want=tool.timeout", run.CodeOf(err))
	}
	if mutation.Outcome != run.OutcomeNotApplied {
		t.Fatalf("outcome=%s want=not_applied", mutation.Outcome)
	}
}

// The core assertion of tool-call timeouts: a non-idempotent write that times
// out must not be judged failed. The side effect may already have happened,
// and judging it failed would invite the caller to retry something that may
// have already succeeded.
func TestASlowNonIdempotentWriteTimesOutAsUnknownOutcome(t *testing.T) {
	spec := testkit.PublishSpec() // Idempotent: false, SideEffect: write, FailSafe: false
	gateway := gatewayWith(t, spec, sleepingHandler(),
		tool.WithToolTimeout(10*time.Millisecond, 50*time.Millisecond))
	ctx := context.Background()

	prepared, err := gateway.Prepare(ctx, publishRequest())
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	mutation, err := gateway.Execute(ctx, tool.CommittedInvocation{Prepared: prepared})
	if run.KindOf(err) != run.ErrorUnknown {
		t.Fatalf("kind=%s want=unknown: %v", run.KindOf(err), err)
	}
	if run.CodeOf(err) != "tool.unknown_outcome" {
		t.Fatalf("code=%s want=tool.unknown_outcome", run.CodeOf(err))
	}
	if mutation.Outcome != run.OutcomeUnknown {
		t.Fatalf("outcome=%s want=unknown", mutation.Outcome)
	}
}

// An idempotent write's timeout is safe to retry: replaying with the same
// idempotency key converges, so it stays an ordinary retryable failure rather
// than parking the Run for human resolution.
func TestASlowIdempotentWriteTimesOutAsRetryable(t *testing.T) {
	spec := testkit.PublishSpec()
	spec.Idempotent = true
	gateway := gatewayWith(t, spec, sleepingHandler(),
		tool.WithToolTimeout(10*time.Millisecond, 50*time.Millisecond))
	ctx := context.Background()

	prepared, err := gateway.Prepare(ctx, publishRequest())
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	mutation, err := gateway.Execute(ctx, tool.CommittedInvocation{Prepared: prepared})
	if run.KindOf(err) != run.ErrorRetryable {
		t.Fatalf("kind=%s want=retryable: %v", run.KindOf(err), err)
	}
	if run.CodeOf(err) != "tool.timeout" {
		t.Fatalf("code=%s want=tool.timeout", run.CodeOf(err))
	}
	if mutation.Outcome != run.OutcomeNotApplied {
		t.Fatalf("outcome=%s want=not_applied", mutation.Outcome)
	}
}

// A dead parent context means somebody upstream cancelled — a lost lease, a
// cancelled Run — and that is their decision, not our timeout expiring. It
// must not be reported as tool.timeout.
func TestAnUpstreamCancelIsNotReportedAsATimeout(t *testing.T) {
	gateway := gatewayWith(t, testkit.SearchSpec(), sleepingHandler(),
		tool.WithToolTimeout(time.Hour, time.Hour))
	parent, cancel := context.WithCancel(context.Background())

	request := publishRequest()
	request.Tool = "search"
	prepared, err := gateway.Prepare(parent, request)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	_, err = gateway.Execute(parent, tool.CommittedInvocation{Prepared: prepared})
	if run.CodeOf(err) == "tool.timeout" {
		t.Fatalf("an upstream cancel was reported as our own timeout: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want it to carry context.Canceled", err)
	}
}

// A Spec may ask for less than the ceiling, never more: the registration
// table cannot see the lease its number is about to outlive.
func TestASpecMayNotAskForLongerThanTheCeiling(t *testing.T) {
	spec := testkit.SearchSpec()
	spec.MaxDurationMS = int((10 * time.Second).Milliseconds())
	gateway := gatewayWith(t, spec, sleepingHandler(),
		tool.WithToolTimeout(10*time.Millisecond, 20*time.Millisecond))

	request := publishRequest()
	request.Tool = "search"
	prepared, err := gateway.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	started := time.Now()
	if _, err := gateway.Execute(context.Background(), tool.CommittedInvocation{Prepared: prepared}); run.CodeOf(err) != "tool.timeout" {
		t.Fatalf("code=%s want=tool.timeout", run.CodeOf(err))
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("elapsed=%s, spec's 10s request was not clamped to the 20ms ceiling", elapsed)
	}
}

// (0, 0) is the rollback path: it must restore the pre-timeout behaviour
// where nothing but lease renewal failure ever cancels a stuck handler.
// A handler that already classified its failure keeps that classification
// even when it lands past the deadline. Relabelling an approval request as a
// retryable timeout would turn "park for a human" into "retry", which is the
// failure mode the gateway's ApprovalRequired handling exists to prevent.
func TestADeadlineDoesNotRelabelAFailureTheHandlerAlreadyClassified(t *testing.T) {
	spec := testkit.SearchSpec()
	handler := tool.HandlerFunc(func(ctx context.Context, _ tool.Invocation) (tool.Result, error) {
		<-ctx.Done()
		return tool.Result{}, run.NewError("search.bad_query", run.ErrorInvalid, run.RetryNever, nil)
	})
	gateway := gatewayWith(t, spec, handler,
		tool.WithToolTimeout(10*time.Millisecond, 50*time.Millisecond))

	request := publishRequest()
	request.Tool = "search"
	prepared, err := gateway.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	_, err = gateway.Execute(context.Background(), tool.CommittedInvocation{Prepared: prepared})
	if run.CodeOf(err) != "search.bad_query" {
		t.Fatalf("code=%s want=search.bad_query: %v", run.CodeOf(err), err)
	}
	if run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("kind=%s want=invalid", run.KindOf(err))
	}
}

func TestWithToolTimeoutZeroZeroDisablesTheGatewaysOwnTimeout(t *testing.T) {
	spec := testkit.SearchSpec()
	spec.MaxDurationMS = 10 // a Spec ask, which must also be ignored once disabled
	gateway := gatewayWith(t, spec, sleepingHandler(), tool.WithToolTimeout(0, 0))

	request := publishRequest()
	request.Tool = "search"
	prepared, err := gateway.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = gateway.Execute(ctx, tool.CommittedInvocation{Prepared: prepared})
	// The caller's own context still cancels the handler (parent expiring is
	// not the gateway's timeout), so this returns — but it must not be
	// reported as our own ceiling.
	if run.CodeOf(err) == "tool.timeout" {
		t.Fatalf("gateway applied a timeout after WithToolTimeout(0, 0): %v", err)
	}
}
