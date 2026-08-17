// Package tool is the governed extension slot for anything with an effect
// outside the Runtime.
//
// Every call goes through one fixed gateway chain. There is no fast path, no
// "trusted" tool and no off switch: a governed call and an ungoverned one would
// be two security models, and only one of them would be tested.
package tool

import (
	"context"
	"encoding/json"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Spec is what a tool declares about itself.
//
// The declaration is what governance judges, so it is made once at registration
// and never derived from the implementation. A tool that described itself
// differently from what it does is a bug the Spec makes reviewable; a tool that
// described itself by executing would be unreviewable by construction.
type Spec struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`

	RiskLevel  policy.RiskLevel  `json:"risk_level"`
	SideEffect policy.SideEffect `json:"side_effect"`
	// RequiredPermissions are checked against the principal before the call.
	RequiredPermissions []string `json:"required_permissions,omitempty"`
	// TargetHost is the outbound destination for a declarative tool, empty for
	// a Go tool that makes no request of its own.
	TargetHost string `json:"target_host,omitempty"`
	// Idempotent declares that repeating the call with the same idempotency key
	// is safe. Its absence is what forces an unknown result into
	// reconciliation rather than a retry.
	Idempotent bool `json:"idempotent,omitempty"`
	// FailSafe declares that a failed call leaves no persistent effect behind:
	// the tool only starts work and a failure means nothing was started, so the
	// handler's own error Kind decides the outcome instead of parking the Run
	// in waiting_resolution. Meant for launch-style write tools whose handler
	// already distinguishes "not applied" from "unknown".
	FailSafe bool `json:"fail_safe,omitempty"`
	// MaxResultBytes caps what the tool may return into a prompt. Zero means
	// the gateway's default; an uncapped result is an uncapped prompt.
	MaxResultBytes int `json:"max_result_bytes,omitempty"`

	Labels []string `json:"labels,omitempty"`
}

// Invocation is what a handler receives. It carries no store, no lease and no
// Run state: a handler that could reach those would be a second writer.
type Invocation struct {
	ID             run.ID
	Tool           string
	Arguments      json.RawMessage
	Principal      authorization.PrincipalRef
	IdempotencyKey string
}

// Result is what a handler returns.
type Result struct {
	Output json.RawMessage
	// Used is the spend to settle against the reservation.
	Used run.Limits
}

// Handler is a tool implementation.
type Handler interface {
	Invoke(ctx context.Context, invocation Invocation) (Result, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, invocation Invocation) (Result, error)

func (f HandlerFunc) Invoke(ctx context.Context, invocation Invocation) (Result, error) {
	return f(ctx, invocation)
}

// Authorizer is the host's answer to "may this principal use this tool".
type Authorizer interface {
	Authorize(ctx context.Context, principal authorization.PrincipalRef, spec Spec) error
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(ctx context.Context, principal authorization.PrincipalRef, spec Spec) error

func (f AuthorizerFunc) Authorize(ctx context.Context, principal authorization.PrincipalRef, spec Spec) error {
	return f(ctx, principal, spec)
}

// InvocationRequest is what the Runtime asks the gateway to prepare.
type InvocationRequest struct {
	// RunID is the Run this call belongs to. The gateway never reads it — it
	// only passes it to the Recorder, which has to file the call under
	// something, and the invocation id alone does not say which Run it served.
	RunID        run.ID
	InvocationID run.ID
	Tool         string
	Arguments    json.RawMessage
	Principal    authorization.PrincipalRef
	// IdempotencyKey is required for any tool with a write side effect: without
	// one, a replay after an unknown result has no way to be safe.
	IdempotencyKey string

	// Facts are the governance facts the caller already knows — agent name,
	// definition, budget headroom. The gateway fills in the tool's own.
	Facts policy.CallFacts
	// Policies is the Run's snapshot, taken once at the start.
	Policies policy.Snapshot
	// Allowlist is the definition's declared tool set. A tool outside it is
	// refused before policy is even consulted: the definition is what the Run
	// was published to do.
	Allowlist []string
	// Granted skips the approval stage: a human already said yes to this
	// exact effect. Schema, allowlist, authorization and quota still run.
	Granted bool
	// DenySideEffects refuses any tool that changes state the Runtime cannot
	// roll back, whatever the policy decides about it.
	//
	// Here rather than expressed as a policy, because it is the caller's own
	// narrowing of one Run and must survive whatever policy version that Run
	// pinned. An evaluation trial runs the production Definition with this set:
	// the point of a trial is to prove the agent would act, not to let it act.
	DenySideEffects bool
}

// PreparedInvocation is the exact invocation-begin fact the Runtime must commit
// before the effect may happen.
//
// It exists as a separate value, rather than the gateway simply calling the
// handler, so that "we are about to do this" is durable before "we did this"
// can be true. A crash in the window then leaves evidence rather than silence.
type PreparedInvocation struct {
	// RunID carries the Run through to Execute, which otherwise has only the
	// invocation and would leave the completion record unattributable.
	RunID      run.ID
	Invocation Invocation
	Spec       Spec
	// Explanation is the governance decision, recorded whether it allowed or
	// refused.
	Explanation policy.Explanation
	// Reserve is what the call may spend.
	Reserve run.Limits
	// ticket is unexported so a caller cannot fabricate a committed invocation
	// and skip the commit. Execute accepts only a ticket this gateway issued.
	ticket  string
	handler Handler
}

// CommittedInvocation is the Runtime's confirmation that the begin fact is
// durable. Execute takes only this.
type CommittedInvocation struct {
	Prepared PreparedInvocation
}

// ResultMutation is what the gateway returns for the Runtime to commit.
type ResultMutation struct {
	InvocationID run.ID
	Outcome      run.Outcome
	Output       json.RawMessage
	Used         run.Limits
}
