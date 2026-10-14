// Package capability provides the single governed entry point for external
// tool effects. A durable Recorder is required before any Handler executes.
package capability

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/costa92/agent-runtime/session"
)

var (
	// ErrDenied is a permanent authorization or policy rejection. Host
	// authorizers wrap it only for a definitive denial, not infrastructure errors.
	ErrDenied           = errors.New("capability: denied")
	ErrInvalid          = errors.New("capability: invalid invocation")
	ErrApprovalRequired = errors.New("capability: approval required")
	ErrDuplicate        = errors.New("capability: duplicate invocation or idempotency key")
	ErrBeginUnconfirmed = errors.New("capability: durable begin and reservation were not confirmed")
	ErrSettlement       = errors.New("capability: settlement was not confirmed")
)

type Effect string

const (
	Read  Effect = "read"
	Write Effect = "write"
)

type Spec struct {
	Name             string
	Version          string
	Description      string
	Parameters       json.RawMessage
	Effect           Effect
	RequiresApproval bool
	// MediaOps is charged atomically with tool/call before a media effect runs.
	MediaOps uint64
}

type Invocation struct {
	ID             string
	Name           string
	SessionID      string
	TurnID         string
	StepID         string
	Principal      string
	Input          json.RawMessage
	IdempotencyKey string
	ApprovalID     string
	Fence          uint64
}

// Fingerprint binds an approval to the exact invocation identity and input.
// ApprovalID is excluded because it is a reference to the approval record.
func (inv Invocation) Fingerprint() string {
	payload, _ := json.Marshal(struct {
		ID             string          `json:"id"`
		Name           string          `json:"name"`
		SessionID      string          `json:"session_id"`
		TurnID         string          `json:"turn_id"`
		StepID         string          `json:"step_id"`
		Principal      string          `json:"principal"`
		Input          json.RawMessage `json:"input"`
		IdempotencyKey string          `json:"idempotency_key"`
	}{inv.ID, inv.Name, inv.SessionID, inv.TurnID, inv.StepID, inv.Principal, inv.Input, inv.IdempotencyKey})
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

type Outcome string

const (
	Succeeded Outcome = "succeeded"
	Failed    Outcome = "failed"
	Unknown   Outcome = "unknown"
)

type Result struct {
	Status     Outcome
	Output     json.RawMessage
	ErrorCode  string
	CostUsages []session.CostUsage
}

type Handler func(context.Context, Invocation) (Result, error)

type Registration struct {
	Spec    Spec
	Handler Handler
}

type PolicyEffect string

const (
	Allow           PolicyEffect = "allow"
	Deny            PolicyEffect = "deny"
	RequireApproval PolicyEffect = "require_approval"
)

type PolicyDecision struct {
	Effect              PolicyEffect
	Reason              string
	ApprovedFingerprint string
}

// BudgetClaim is advisory until Recorder.Begin atomically reserves it with
// the invocation record. No Handler may run without a confirmed receipt.
type BudgetClaim struct {
	Dimension string
	Units     uint64
}

// InputInterceptor may only replace input. It cannot select another tool,
// change the principal, or bypass any gate in Prepare or Execute.
type InputInterceptor func(context.Context, Spec, json.RawMessage) (json.RawMessage, error)

type Hooks struct {
	Authorize    func(context.Context, Invocation, Spec) error
	Policy       func(context.Context, Invocation, Spec) (PolicyDecision, error)
	Budget       func(context.Context, Invocation, Spec) (BudgetClaim, error)
	Interceptors []InputInterceptor
}

type BeginReceipt struct {
	Recorded bool
	Reserved bool
}

// Recorder.Begin must atomically record the invocation, enforce a unique
// idempotency key, check the lease fence, and reserve the BudgetClaim. A stale
// fence or duplicate key must return an error and leave the Handler uncalled.
type Recorder interface {
	Begin(context.Context, Invocation, Spec, BudgetClaim) (BeginReceipt, error)
	Settle(context.Context, Invocation, BeginReceipt, Result) error
}

type Status string

const (
	Ready            Status = "ready"
	AwaitingApproval Status = "awaiting_approval"
)

type Prepared struct {
	service    *Service
	invocation Invocation
	spec       Spec
	handler    Handler
	claim      BudgetClaim
	status     Status
	used       atomic.Bool
}

func (p *Prepared) Status() Status              { return p.status }
func (p *Prepared) Invocation() Invocation      { return cloneInvocation(p.invocation) }
func (p *Prepared) ApprovalFingerprint() string { return p.invocation.Fingerprint() }

type Service struct {
	registrations map[string]Registration
	hooks         Hooks
}

func New(registrations map[string]Registration, hooks Hooks) (*Service, error) {
	if hooks.Authorize == nil || hooks.Policy == nil || hooks.Budget == nil {
		return nil, errors.New("capability: authorizer, policy, and budget hooks are required")
	}
	if len(registrations) == 0 {
		return nil, errors.New("capability: at least one tool is required")
	}
	copyOf := make(map[string]Registration, len(registrations))
	for name, registration := range registrations {
		if name == "" || registration.Spec.Name != name || registration.Spec.Version == "" || registration.Handler == nil {
			return nil, fmt.Errorf("capability: invalid registration %q", name)
		}
		if registration.Spec.Effect != Read && registration.Spec.Effect != Write {
			return nil, fmt.Errorf("capability: invalid effect for %q", name)
		}
		if len(registration.Spec.Parameters) > 0 && !json.Valid(registration.Spec.Parameters) {
			return nil, fmt.Errorf("capability: invalid parameters for %q", name)
		}
		if registration.Spec.Description == "" {
			registration.Spec.Description = name
		}
		if len(registration.Spec.Parameters) == 0 {
			registration.Spec.Parameters = json.RawMessage(`{}`)
		}
		registration.Spec.Parameters = append(json.RawMessage(nil), registration.Spec.Parameters...)
		copyOf[name] = registration
	}
	for i, interceptor := range hooks.Interceptors {
		if interceptor == nil {
			return nil, fmt.Errorf("capability: nil interceptor %d", i)
		}
	}
	hooks.Interceptors = append([]InputInterceptor(nil), hooks.Interceptors...)
	return &Service{registrations: copyOf, hooks: hooks}, nil
}

// Specs is the authoritative, deterministic tool catalog for model requests.
// The returned JSON schemas are copies and cannot alter registered handlers.
func (s *Service) Specs() []Spec {
	names := make([]string, 0, len(s.registrations))
	for name := range s.registrations {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]Spec, 0, len(names))
	for _, name := range names {
		spec := s.registrations[name].Spec
		spec.Parameters = append(json.RawMessage(nil), spec.Parameters...)
		result = append(result, spec)
	}
	return result
}

// Prepare runs every input interceptor before the authorization, policy, and
// budget gates. A parked approval must be prepared again after durable approval.
func (s *Service) Prepare(ctx context.Context, inv Invocation) (*Prepared, error) {
	registration, ok := s.registrations[inv.Name]
	if !ok {
		return nil, fmt.Errorf("%w: unknown tool %q", ErrInvalid, inv.Name)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if inv.ID == "" || inv.SessionID == "" || inv.TurnID == "" || inv.StepID == "" || inv.Principal == "" || inv.Fence == 0 {
		return nil, fmt.Errorf("%w: invocation identity and fence are required", ErrInvalid)
	}
	if registration.Spec.Effect == Write && inv.IdempotencyKey == "" {
		return nil, fmt.Errorf("%w: write requires an idempotency key", ErrInvalid)
	}
	if !json.Valid(inv.Input) {
		return nil, fmt.Errorf("%w: input must be valid JSON", ErrInvalid)
	}
	inv.Input = append(json.RawMessage(nil), inv.Input...)
	for _, interceptor := range s.hooks.Interceptors {
		input, err := interceptor(ctx, registration.Spec, inv.Input)
		if err != nil {
			return nil, err
		}
		if !json.Valid(input) {
			return nil, fmt.Errorf("%w: interceptor returned invalid JSON", ErrInvalid)
		}
		inv.Input = append(json.RawMessage(nil), input...)
	}
	if err := s.hooks.Authorize(ctx, inv, registration.Spec); err != nil {
		return nil, err
	}
	decision, err := s.hooks.Policy(ctx, inv, registration.Spec)
	if err != nil {
		return nil, err
	}
	if decision.Effect == Deny {
		return nil, fmt.Errorf("%w: policy denied %q: %s", ErrDenied, inv.Name, decision.Reason)
	}
	if decision.Effect != Allow && decision.Effect != RequireApproval {
		return nil, fmt.Errorf("%w: policy returned invalid effect", ErrInvalid)
	}
	claim, err := s.hooks.Budget(ctx, inv, registration.Spec)
	if err != nil {
		return nil, err
	}
	prepared := &Prepared{service: s, invocation: inv, spec: registration.Spec, handler: registration.Handler, claim: claim, status: Ready}
	approved := inv.ApprovalID != "" && decision.ApprovedFingerprint == inv.Fingerprint()
	if decision.Effect == RequireApproval && !approved {
		prepared.status = AwaitingApproval
		return prepared, nil
	}
	if registration.Spec.RequiresApproval || inv.ApprovalID != "" {
		if !approved {
			return nil, fmt.Errorf("%w: approval does not match invocation", ErrDenied)
		}
	}
	return prepared, nil
}

// Execute is single-use. Begin is the only path to Handler, and Settle is
// attempted for every execution outcome including failure and uncertainty.
func (s *Service) Execute(ctx context.Context, prepared *Prepared, recorder Recorder) (Result, error) {
	if prepared == nil || prepared.service != s || recorder == nil {
		return Result{}, errors.New("capability: prepared call and recorder are required")
	}
	if prepared.status == AwaitingApproval {
		return Result{}, ErrApprovalRequired
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if !prepared.used.CompareAndSwap(false, true) {
		return Result{}, ErrDuplicate
	}
	receipt, err := recorder.Begin(ctx, cloneInvocation(prepared.invocation), prepared.spec, prepared.claim)
	if err != nil {
		return Result{}, err
	}
	if !receipt.Recorded || !receipt.Reserved {
		return Result{}, ErrBeginUnconfirmed
	}
	result := Result{Status: Unknown, ErrorCode: "canceled_after_begin"}
	if ctx.Err() == nil {
		result = callHandler(ctx, prepared.handler, cloneInvocation(prepared.invocation))
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := recorder.Settle(settleCtx, cloneInvocation(prepared.invocation), receipt, result); err != nil {
		return Result{Status: Unknown, ErrorCode: "settlement_unconfirmed"}, errors.Join(ErrSettlement, err)
	}
	return result, nil
}

func cloneInvocation(inv Invocation) Invocation {
	inv.Input = append(json.RawMessage(nil), inv.Input...)
	return inv
}

func callHandler(ctx context.Context, handler Handler, inv Invocation) (result Result) {
	defer func() {
		if recover() != nil {
			result = Result{Status: Unknown, ErrorCode: "handler_panicked"}
		}
	}()
	var err error
	result, err = handler(ctx, inv)
	if err != nil {
		return Result{Status: Unknown, ErrorCode: "execution_uncertain"}
	}
	if result.Status != Succeeded && result.Status != Failed && result.Status != Unknown {
		return Result{Status: Unknown, ErrorCode: "invalid_handler_outcome"}
	}
	if len(result.Output) > 0 && !json.Valid(result.Output) {
		return Result{Status: Unknown, ErrorCode: "invalid_handler_output"}
	}
	return result
}
