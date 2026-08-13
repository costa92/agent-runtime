package testkit

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
)

// CountingHandler records how often it ran. Call counts are the evidence for
// the property that matters most about an unknown side effect: that nothing
// retried it.
type CountingHandler struct {
	mu     sync.Mutex
	calls  int
	result tool.Result
	err    error
}

func ToolReturning(err error) *CountingHandler {
	return &CountingHandler{err: err, result: tool.Result{Used: run.Limits{ToolCalls: 1}}}
}

func ToolSucceeding(output string) *CountingHandler {
	return &CountingHandler{
		result: tool.Result{Output: json.RawMessage(output), Used: run.Limits{ToolCalls: 1}},
	}
}

func (h *CountingHandler) Invoke(context.Context, tool.Invocation) (tool.Result, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	return h.result, h.err
}

func (h *CountingHandler) Calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// AllowAllToolAuthorizer permits every tool, so a test that is not about
// authorization does not have to configure it.
func AllowAllToolAuthorizer() tool.Authorizer {
	return tool.AuthorizerFunc(func(context.Context, authorization.PrincipalRef, tool.Spec) error {
		return nil
	})
}

// DenyToolAuthorizer refuses one named tool.
func DenyToolAuthorizer(name string) tool.Authorizer {
	return tool.AuthorizerFunc(func(_ context.Context, _ authorization.PrincipalRef, spec tool.Spec) error {
		if spec.Name == name {
			return run.NewError("tool.unauthorized", run.ErrorDenied, run.RetryNever)
		}
		return nil
	})
}

// PublishSpec is a high-risk write tool: the shape governance cares most about.
func PublishSpec() tool.Spec {
	return tool.Spec{
		Name:       "publish",
		RiskLevel:  policy.RiskHigh,
		SideEffect: policy.SideEffectWrite,
		Idempotent: false,
	}
}

// SearchSpec is a read-only, side-effect-free tool.
func SearchSpec() tool.Spec {
	return tool.Spec{
		Name:       "search",
		RiskLevel:  policy.RiskLow,
		SideEffect: policy.SideEffectRead,
	}
}

// StageRecorder records the gateway chain's stage order.
type StageRecorder struct {
	mu     sync.Mutex
	stages []tool.Stage
}

func NewStageRecorder() *StageRecorder { return &StageRecorder{} }

func (r *StageRecorder) Stage(stage tool.Stage, _ tool.Spec, _ error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stages = append(r.stages, stage)
}

func (r *StageRecorder) Stages() []tool.Stage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]tool.Stage(nil), r.stages...)
}
