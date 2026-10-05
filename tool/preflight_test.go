package tool_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/costa92/agent-runtime/policy"
	"github.com/costa92/agent-runtime/run"
	"github.com/costa92/agent-runtime/tool"
)

type preflightRejector struct {
	invoked   bool
	validated bool
}

func (h *preflightRejector) ValidateArguments(json.RawMessage) error {
	h.validated = true
	return run.NewError("draft.placeholder", run.ErrorInvalid, run.RetryNever)
}

func (h *preflightRejector) Invoke(context.Context, tool.Invocation) (tool.Result, error) {
	h.invoked = true
	return tool.Result{}, nil
}

func TestArgumentPreflightRejectsBeforeReservingAWrite(t *testing.T) {
	h := &preflightRejector{}
	g := gatewayWith(t, tool.Spec{Name: "publish", RiskLevel: policy.RiskHigh, SideEffect: policy.SideEffectWrite}, h)
	_, err := g.Prepare(t.Context(), publishRequest())
	if run.CodeOf(err) != "draft.placeholder" || !h.validated || h.invoked {
		t.Fatalf("err=%v validated=%v invoked=%v", err, h.validated, h.invoked)
	}
}
