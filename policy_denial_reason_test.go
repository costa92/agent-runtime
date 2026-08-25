package agentruntime

import (
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// A refusal that happened before a policy Explanation existed used to record
// only its kind: "denied", and nothing else. Kind is what a caller switches on,
// so it is the right thing for control flow — but as the only thing on the
// record it makes every pre-policy refusal look identical. An allowlist miss, a
// missing permission and a wholesale side-effect ban all read as "denied", and
// telling them apart meant reading the code rather than the record.
//
// That is not a theoretical loss. A stale published graph silently emptied one
// agent's allowlist, and the six denial rows it produced named the tool and the
// word "denied" — enough to know something refused, not enough to know what.
func TestAPrePolicyRefusalRecordsWhichRefusalItWas(t *testing.T) {
	denial := run.NewError("tool.not_in_definition", run.ErrorDenied, run.RetryNever)

	decision := prePolicyDecision(run.ID("run-1"), "render_picture_book", denial)

	attrs := map[string]string{}
	for _, a := range decision.Attributes {
		attrs[a.Key] = a.Value
	}
	if got := attrs[observe.AttrDecision]; got != string(run.ErrorDenied) {
		t.Errorf("decision = %q, want %q", got, run.ErrorDenied)
	}
	if got := attrs[observe.AttrReason]; got != "tool.not_in_definition" {
		t.Errorf("reason = %q, want the error's code", got)
	}
}

// An error carrying no Runtime code must still produce a record rather than an
// attribute holding the empty string, which reads as "reason known to be blank"
// rather than "no reason available".
func TestARefusalWithoutACodeOmitsTheReason(t *testing.T) {
	decision := prePolicyDecision(run.ID("run-1"), "some_tool", errNoCode{})

	for _, a := range decision.Attributes {
		if a.Key == observe.AttrReason {
			t.Errorf("reason attribute present with value %q; expected it omitted", a.Value)
		}
	}
}

type errNoCode struct{}

func (errNoCode) Error() string { return "boom" }
