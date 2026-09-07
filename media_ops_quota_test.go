package agentruntime_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/quota"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
)

// --- fixtures -------------------------------------------------------------

// mediaQuotaHarness wires one media tool, one media_ops cap and a ledger that
// already reports observed usage, then calls the tool once.
func mediaQuotaHarness(
	t *testing.T, spec tool.Spec, arguments json.RawMessage, maxOps, observed int,
) error {
	t.Helper()
	registry := tool.NewRegistry()
	if err := registry.Register(spec, testkit.ToolSucceeding(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	registry.Freeze()

	var callErr error
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		_, callErr = request.Ports.Tool(ctx, spec.Name, arguments)
		return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
	}}, withDefinition(definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "be brief",
		Model:          definition.ModelPolicy{Profile: "fast"},
		Tools:          []definition.ToolRef{{Key: spec.Name}},
	}), withDeps(func(deps *agentruntime.Dependencies) {
		deps.Tools = tool.NewGateway(registry, testkit.AllowAllToolAuthorizer())
		deps.Governance = fakeGovernance{quotas: quota.Snapshot{
			Digest: "q", Limits: []quota.Limit{{
				Name: "media-ops-per-hour", Scope: quota.Scope{Tenant: "acme"},
				Unit: quota.UnitMediaOps, Max: maxOps, Window: time.Hour,
			}},
		}}
		deps.Meter = fakeMeter{usage: map[quota.Unit]int{quota.UnitMediaOps: observed}}
	}))
	started := start(t, h)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	return callErr
}

// oneOpSpec is the generate_image / generate_video / generate_audio shape: a
// call whose output size is fixed at one. The name is the harness catalog's,
// and the risk level is low so that a refusal here is the quota's and not the
// policy's.
func oneOpSpec() tool.Spec {
	return tool.Spec{
		Name: "render_picture_book", Description: "start one image",
		Parameters:   json.RawMessage(`{"type":"object"}`),
		RiskLevel:    policy.RiskLow,
		SideEffect:   policy.SideEffectRead,
		MediaOpsBase: 1,
	}
}

func pictureBookSpec() tool.Spec {
	return tool.Spec{
		Name: "render_picture_book", Description: "render a book",
		Parameters:     json.RawMessage(`{"type":"object","properties":{"pages":{"type":"integer"}}}`),
		RiskLevel:      policy.RiskLow,
		SideEffect:     policy.SideEffectRead,
		MediaOpsBase:   1,
		MediaOpsPerArg: "pages",
	}
}

// A cap the ledger already reports as full has to refuse the next call. Before
// the gateway declared what a media tool produces, the reservation carried zero
// media_ops, a want of zero was skipped before the ledger was ever read, and
// the cap could be crossed by any margin without a single refusal.

func TestAFullMediaOpsLedgerRefusesTheNextImageGeneration(t *testing.T) {
	err := mediaQuotaHarness(t, oneOpSpec(), json.RawMessage(`{}`), 400, 400)
	if err == nil {
		t.Fatal("the call was admitted with the media_ops ledger already at its cap")
	}
	if got := run.CodeOf(err); got != "quota_exhausted" {
		t.Fatalf("code = %q, want quota_exhausted (err=%v)", got, err)
	}
	if kind := run.KindOf(err); kind != run.ErrorDenied {
		t.Fatalf("kind = %s, want denied", kind)
	}
}

// Room for one more is room for one more: the same cap must not refuse the call
// that fits, or a reservation would be a ceiling one short of the published one.

func TestAMediaOpsLedgerWithRoomLeftAdmitsTheNextImageGeneration(t *testing.T) {
	if err := mediaQuotaHarness(t, oneOpSpec(), json.RawMessage(`{}`), 400, 399); err != nil {
		t.Fatalf("the call was refused with one operation left under the cap: %v", err)
	}
}

// The page count is the request itself, so an oversized book is refused before
// a single illustration is rendered. This is what replaced the compiled-in page
// ceiling: the limit is published rather than built, and it is checked once
// rather than discovered by timing out mid-render.

func TestAnOversizedPictureBookIsRefusedBeforeItRenders(t *testing.T) {
	err := mediaQuotaHarness(t, pictureBookSpec(), json.RawMessage(`{"pages":200}`), 100, 0)
	if err == nil {
		t.Fatal("a 200-page book was admitted under a 100-operation cap")
	}
	if got := run.CodeOf(err); got != "quota_exhausted" {
		t.Fatalf("code = %q, want quota_exhausted (err=%v)", got, err)
	}
}

// A book that fits still renders: the per-argument cost must be the argument's
// value, not a flat penalty for asking for any pages at all.

func TestAPictureBookThatFitsTheCapStillRenders(t *testing.T) {
	if err := mediaQuotaHarness(t, pictureBookSpec(), json.RawMessage(`{"pages":8}`), 100, 0); err != nil {
		t.Fatalf("a 9-operation book was refused under a 100-operation cap: %v", err)
	}
}
