package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Stage names one step of the fixed chain. The order is a property of the
// gateway, asserted by a test, because a chain whose order can drift is a chain
// where authorization can end up after the effect.
type Stage string

const (
	StageResolve     Stage = "resolve"
	StageSchema      Stage = "schema"
	StageAllowlist   Stage = "allowlist"
	StageAuthorize   Stage = "authorize"
	StagePolicy      Stage = "policy"
	StageQuota       Stage = "quota"
	StageBudget      Stage = "budget"
	StageApproval    Stage = "approval"
	StageIdempotency Stage = "idempotency"
)

// Stages is the fixed order.
var Stages = []Stage{
	StageResolve, StageSchema, StageAllowlist, StageAuthorize,
	StagePolicy, StageQuota, StageBudget, StageApproval, StageIdempotency,
}

// Observer watches the chain. It observes and never decides: a hook that could
// decide would be a governance path outside the published policy set.
type Observer interface {
	Stage(stage Stage, spec Spec, err error)
}

// ObserverFunc adapts a function to Observer.
type ObserverFunc func(stage Stage, spec Spec, err error)

func (f ObserverFunc) Stage(stage Stage, spec Spec, err error) { f(stage, spec, err) }

// QuotaChecker is the host's tenant-limit check, consulted inside the chain so
// that a long Run's slow overspend is caught at the call rather than only at
// creation.
type QuotaChecker interface {
	CheckTenant(ctx context.Context, tenant string, spec Spec) error
}

// ApprovalRequired is returned when governance parks the call. It is a distinct
// error rather than a flag because every caller has to handle it, and a flag
// gets forgotten.
var ApprovalRequired = run.NewError("tool.approval_required", run.ErrorInterrupted, run.RetryAfterInput)

// IsApprovalRequired reports whether err is the park-for-a-human signal.
//
// The assistant loop must return this unwrapped so runNode can park. Treating
// it as a refused tool lets the model take a second turn, and that second
// turn is what failed the picture-book session with no reply.
func IsApprovalRequired(err error) bool {
	var runtimeErr *run.Error
	return errors.As(err, &runtimeErr) && runtimeErr.Code == ApprovalRequired.Code
}

// DefaultToolCallTimeout is the default ceiling on one tool call's wall clock,
// used when a Spec declares no MaxDurationMS of its own.
const DefaultToolCallTimeout = 60 * time.Second

// DefaultToolCallTimeoutCeiling is the hard cap no Spec's MaxDurationMS may
// exceed, whatever the registration table asks for.
const DefaultToolCallTimeoutCeiling = 180 * time.Second

// Gateway is the one path to a tool handler.
type Gateway struct {
	registry   *Registry
	authorizer Authorizer
	quota      QuotaChecker
	schema     definition.SchemaProcessor
	strategies []policy.Strategy
	observers  []Observer
	recorder   Recorder

	// defaultMaxResultBytes caps a result that declares no cap of its own.
	defaultMaxResultBytes int

	// defaultToolTimeout and toolTimeoutCeiling bound one call's wall clock.
	// See WithToolTimeout for how (0, 0) turns this off entirely.
	defaultToolTimeout time.Duration
	toolTimeoutCeiling time.Duration
	// toolTimeoutDisabled is the rollback path: WithToolTimeout(0, 0) sets it,
	// and callTimeout then ignores both the gateway's defaults and the Spec's
	// own MaxDurationMS, restoring the pre-timeout behaviour where only lease
	// renewal failure could ever cancel a stuck handler.
	toolTimeoutDisabled bool
}

// Option configures a Gateway.
type Option func(*Gateway)

func WithQuota(checker QuotaChecker) Option { return func(g *Gateway) { g.quota = checker } }
func WithSchema(processor definition.SchemaProcessor) Option {
	return func(g *Gateway) { g.schema = processor }
}

func WithObserver(observer Observer) Option {
	return func(g *Gateway) { g.observers = append(g.observers, observer) }
}

func WithStrategies(strategies ...policy.Strategy) Option {
	return func(g *Gateway) { g.strategies = append(g.strategies, strategies...) }
}

func WithMaxResultBytes(limit int) Option {
	return func(g *Gateway) { g.defaultMaxResultBytes = limit }
}

// WithToolTimeout sets the default and ceiling a call may run under. A Spec
// may ask for less than the default (via MaxDurationMS) but never more than
// the ceiling.
//
// Non-positive in both is the rollback path: it disables the gateway's own timeout
// entirely, regardless of what any Spec declares, restoring the behaviour
// before this option existed — a call's context is cancelled only when lease
// renewal fails.
func WithToolTimeout(def, ceiling time.Duration) Option {
	return func(g *Gateway) {
		g.defaultToolTimeout = def
		g.toolTimeoutCeiling = ceiling
		g.toolTimeoutDisabled = def <= 0 && ceiling <= 0
	}
}

// LookupSpec returns a registered declaration. Used to show the model the
// tools this Run was published with.
func (g *Gateway) LookupSpec(name string) (Spec, bool) {
	if g == nil || g.registry == nil {
		return Spec{}, false
	}
	spec, _, err := g.resolve(context.Background(), name)
	if err != nil {
		return Spec{}, false
	}
	return spec, true
}

func NewGateway(registry *Registry, authorizer Authorizer, options ...Option) *Gateway {
	gateway := &Gateway{
		registry:              registry,
		authorizer:            authorizer,
		defaultMaxResultBytes: 64 * 1024,
		defaultToolTimeout:    DefaultToolCallTimeout,
		toolTimeoutCeiling:    DefaultToolCallTimeoutCeiling,
	}
	for _, option := range options {
		option(gateway)
	}
	return gateway
}

// Prepare runs the whole chain and returns the invocation-begin fact.
//
// It performs no effect. The Runtime commits what this returns, and only then
// may Execute run — which is what makes "we are about to do this" durable
// before "we did this" can be true.
func (g *Gateway) Prepare(ctx context.Context, request InvocationRequest) (PreparedInvocation, error) {
	// One record per call, written on the way out whichever stage stopped it.
	// The stage closure below is what makes that true without a return-by-return
	// audit: a stage added later records itself, and a refusal cannot leave the
	// audit believing the call never happened.
	decision := DecisionRecord{
		RunID:        request.RunID,
		InvocationID: request.InvocationID,
		Tool:         request.Tool,
		ArgsDigest:   digest(request.Arguments),
		Granted:      request.Granted,
	}
	defer func() { g.recordDecision(ctx, decision) }()
	stage := func(at Stage, spec Spec, err error) {
		g.observe(at, spec, err)
		decision.Stage, decision.Spec, decision.Err = at, spec, err
	}

	spec, handler, err := g.resolve(ctx, request.Tool)
	stage(StageResolve, spec, err)
	if err != nil {
		return PreparedInvocation{}, err
	}

	if request.InvocationID == "" {
		return PreparedInvocation{}, run.NewError("missing_invocation_id", run.ErrorInvalid, run.RetryNever)
	}

	arguments, err := g.normalizeArguments(spec, request.Arguments)
	stage(StageSchema, spec, err)
	if err != nil {
		return PreparedInvocation{}, err
	}
	decision.ArgsDigest = digest(arguments)

	err = allowlisted(spec, request.Allowlist)
	stage(StageAllowlist, spec, err)
	if err != nil {
		return PreparedInvocation{}, err
	}

	err = g.authorizer.Authorize(ctx, request.Principal, spec)
	stage(StageAuthorize, spec, err)
	if err != nil {
		return PreparedInvocation{}, err
	}

	facts := request.Facts
	facts.ToolName = spec.Name
	facts.ToolRiskLevel = spec.RiskLevel
	facts.ToolSideEffect = spec.SideEffect
	facts.ToolTargetHost = spec.TargetHost
	facts.ToolPermissions = spec.RequiredPermissions
	facts.Labels = append(slices.Clone(facts.Labels), spec.Labels...)
	facts.PrincipalKind = string(request.Principal.Kind)
	facts.PrincipalTenant = request.Principal.Tenant

	if request.DenySideEffects && spec.SideEffect == policy.SideEffectWrite {
		// Checked before policy, and refused whatever policy would have said: a
		// narrowing the caller asked for is not something a published rule may
		// override, or the narrowing would mean nothing on exactly the tenants
		// whose policies are most permissive.
		err = run.NewError("tool.side_effects_denied", run.ErrorDenied, run.RetryNever,
			fmt.Errorf("tool %q writes and this Run refuses side effects", spec.Name))
		stage(StagePolicy, spec, err)
		return PreparedInvocation{}, err
	}

	explanation, err := policy.Evaluate(request.Policies, facts, g.strategies...)
	decision.Explanation = explanation
	if err == nil {
		err = enforce(explanation.Decision, spec)
	}
	stage(StagePolicy, spec, err)
	if err != nil {
		// A refusal returns before any Invocation exists, so nothing begins and
		// no handler is reachable. That is what "non-executable" means here.
		return PreparedInvocation{}, err
	}

	err = g.checkQuota(ctx, request.Principal.Tenant, spec)
	stage(StageQuota, spec, err)
	if err != nil {
		return PreparedInvocation{}, err
	}

	reserve := run.Limits{ToolCalls: 1}
	stage(StageBudget, spec, nil)

	if explanation.Decision == policy.DecisionRequireApproval && !request.Granted {
		err = ApprovalRequired
	}
	stage(StageApproval, spec, err)
	if err != nil {
		return PreparedInvocation{}, err
	}

	err = requiresIdempotencyKey(spec, request.IdempotencyKey)
	stage(StageIdempotency, spec, err)
	if err != nil {
		return PreparedInvocation{}, err
	}

	return PreparedInvocation{
		RunID: request.RunID,
		Invocation: Invocation{
			ID:             request.InvocationID,
			Tool:           spec.Name,
			RunID:          request.RunID,
			Arguments:      arguments,
			Principal:      request.Principal,
			IdempotencyKey: request.IdempotencyKey,
		},
		Spec:        spec,
		Explanation: explanation,
		Reserve:     reserve,
		ticket:      g.ticketFor(request.InvocationID, spec.Name),
		handler:     handler,
	}, nil
}

// resolve answers from the frozen registry, and only from it.
//
// There used to be a second source here: a BindingLookup that could produce a
// tool published after the registry froze, so an operator could add one without
// a release. Nothing ever wired it — no host passed WithBindings, no resource
// seeding published a ToolBinding — so every Run resolved through the registry
// alone while the code read as though two tiers existed. It was removed rather
// than connected, because connecting it is a real design decision (a handler
// arriving at runtime is outside every capability guarantee the frozen registry
// makes) and an unwired branch is not a decision, it is the appearance of one.
func (g *Gateway) resolve(_ context.Context, name string) (Spec, Handler, error) {
	spec, err := g.registry.Lookup(name)
	if err != nil {
		return Spec{}, nil, err
	}
	found, lookupErr := g.registry.handlerFor(name)
	if lookupErr != nil {
		return Spec{}, nil, lookupErr
	}
	return spec, found.handler, nil
}

// Execute calls the handler, exactly once, for a committed invocation.
func (g *Gateway) Execute(ctx context.Context, committed CommittedInvocation) (ResultMutation, error) {
	prepared := committed.Prepared
	if prepared.ticket == "" || prepared.ticket != g.ticketFor(prepared.Invocation.ID, prepared.Spec.Name) {
		// A caller that built a CommittedInvocation by hand skipped the commit,
		// and with it the durable record that the effect was about to happen.
		return ResultMutation{}, run.NewError("uncommitted_invocation", run.ErrorInvalid, run.RetryNever)
	}

	handler := prepared.handler
	if handler == nil {
		found, err := g.registry.handlerFor(prepared.Spec.Name)
		if err != nil {
			return ResultMutation{}, err
		}
		handler = found.handler
	}

	startedAt := time.Now()
	callCtx := ctx
	cancel := func() {}
	if d := g.callTimeout(prepared.Spec); d > 0 {
		callCtx, cancel = context.WithTimeout(ctx, d)
	}
	defer cancel()
	result, err := handler.Invoke(callCtx, prepared.Invocation)
	if err != nil {
		err = classifyDeadline(ctx, callCtx, err)
	}
	// Recorded on every path out of here, including the ones that classify a
	// failure as unknown: "we do not know whether this happened" is the single
	// most important thing an audit can carry, and it is exactly the outcome a
	// caller cannot reconstruct afterwards.
	finish := func(outcome run.Outcome, output []byte, failure error) {
		g.recordResult(ctx, ResultRecord{
			RunID: prepared.RunID, InvocationID: prepared.Invocation.ID,
			Tool: prepared.Spec.Name, Outcome: outcome,
			ResultDigest: digest(output), ResultBytes: len(output),
			Duration: time.Since(startedAt), Err: failure,
		})
	}
	if err != nil {
		mutation := ResultMutation{InvocationID: prepared.Invocation.ID, Used: result.Used}
		switch {
		case run.KindOf(err) == run.ErrorUnknown:
			// Never retried, whatever the handler suggests: the effect may have
			// happened and nothing here can tell.
			mutation.Outcome = run.OutcomeUnknown
			slog.Error("gateway: handler failed with unknown outcome",
				"tool", prepared.Spec.Name, "kind", string(run.KindOf(err)), "err", err)
		case !prepared.Spec.Idempotent && prepared.Spec.SideEffect == policy.SideEffectWrite && !prepared.Spec.FailSafe:
			// An undeclared-idempotency write that failed is indistinguishable
			// from one that succeeded and lost its answer, so it reconciles
			// rather than retries. FailSafe tools opt out: a failed call left
			// nothing behind, so the handler's own Kind decides the outcome.
			mutation.Outcome = run.OutcomeUnknown
			err = run.NewError("tool.unknown_outcome", run.ErrorUnknown, run.RetryReconcile, err)
			slog.Error("gateway: non-idempotent write failed, parking for resolution",
				"tool", prepared.Spec.Name, "err", err)
		default:
			mutation.Outcome = run.OutcomeNotApplied
			slog.Warn("gateway: tool call failed (not applied)",
				"tool", prepared.Spec.Name, "kind", string(run.KindOf(err)), "err", err)
		}
		finish(mutation.Outcome, nil, err)
		return mutation, err
	}

	capped, err := g.capResult(prepared.Spec, result.Output)
	if err != nil {
		finish(run.OutcomeApplied, result.Output, err)
		return ResultMutation{InvocationID: prepared.Invocation.ID, Outcome: run.OutcomeApplied, Used: result.Used}, err
	}

	finish(run.OutcomeApplied, capped, nil)
	return ResultMutation{
		InvocationID: prepared.Invocation.ID,
		Outcome:      run.OutcomeApplied,
		Output:       capped,
		Used:         result.Used,
	}, nil
}

func (g *Gateway) observe(stage Stage, spec Spec, err error) {
	for _, observer := range g.observers {
		observer.Stage(stage, spec, err)
	}
}

func (g *Gateway) normalizeArguments(spec Spec, arguments json.RawMessage) (json.RawMessage, error) {
	if g.schema == nil || len(spec.Parameters) == 0 {
		return arguments, nil
	}
	normalized, err := g.schema.NormalizeValue(spec.Parameters, arguments)
	if err != nil {
		return nil, run.NewError("tool.invalid_arguments", run.ErrorInvalid, run.RetryNever, err)
	}
	return normalized, nil
}

func (g *Gateway) checkQuota(ctx context.Context, tenant string, spec Spec) error {
	if g.quota == nil {
		return nil
	}
	return g.quota.CheckTenant(ctx, tenant, spec)
}

func (g *Gateway) capResult(spec Spec, output json.RawMessage) (json.RawMessage, error) {
	limit := spec.MaxResultBytes
	if limit == 0 {
		limit = g.defaultMaxResultBytes
	}
	if limit > 0 && len(output) > limit {
		// Truncating JSON would produce something that parses as nothing.
		// Refusing names the tool that overran, which truncation would not.
		return nil, run.NewError(run.CodeToolResultTooLarge, run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("tool %q returned %d bytes, limit %d", spec.Name, len(output), limit))
	}
	return output, nil
}

// callTimeout answers how long one call to spec may run. Zero means no
// timeout is applied at all — the toolTimeoutDisabled rollback path.
func (g *Gateway) callTimeout(spec Spec) time.Duration {
	if g.toolTimeoutDisabled {
		return 0
	}
	d := g.defaultToolTimeout
	if spec.MaxDurationMS > 0 {
		d = time.Duration(spec.MaxDurationMS) * time.Millisecond
	}
	if d <= 0 {
		return 0
	}
	if g.toolTimeoutCeiling > 0 && d > g.toolTimeoutCeiling {
		return g.toolTimeoutCeiling
	}
	return d
}

// classifyDeadline follows modelprovider.callError's order: look at the parent
// first. A dead parent means somebody upstream cancelled — a lost lease, a
// cancelled Run — and that is their decision, not our timeout. Only a live
// parent with a dead call context is our own ceiling expiring, and that is
// translated into an ordinary retryable error so Execute's existing outcome
// switch can decide whether it retries clean or must park as unknown: a
// timeout on a non-idempotent write is exactly the "the side effect may
// already have happened" case that switch already knows how to handle.
//
// A handler that already said what its failure was keeps it. The deadline
// tells us the clock ran out, not that the clock is why the handler failed:
// a handler racing the deadline can return an approval request or an invalid
// argument microseconds late, and relabelling either would turn "park for a
// human" into "retry" — the failure the ApprovalRequired comment above records.
func classifyDeadline(parent, call context.Context, err error) error {
	if parent.Err() != nil {
		return err
	}
	var classified *run.Error
	if errors.As(err, &classified) {
		return err
	}
	if errors.Is(call.Err(), context.DeadlineExceeded) {
		return run.NewError("tool.timeout", run.ErrorRetryable, run.RetryBackoff, err)
	}
	return err
}

// ticketFor derives the ticket deterministically from the invocation, so a
// gateway can validate one without holding state that a restart would lose.
func (g *Gateway) ticketFor(id run.ID, tool string) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s", id, tool))
	return hex.EncodeToString(sum[:8])
}

// allowlisted refuses anything the Definition did not declare.
//
// An empty allowlist denies everything, the same rule the egress host list
// follows and for the same reason: a Definition that declares no tools is one
// that may call none, and reading the empty case as "unrestricted" makes the
// least-configured Definition the most powerful one.
func allowlisted(spec Spec, allowlist []string) error {
	if slices.Contains(allowlist, spec.Name) {
		return nil
	}
	return run.NewError("tool.not_in_definition", run.ErrorDenied, run.RetryNever,
		fmt.Errorf("tool %q is not declared by this definition", spec.Name))
}

func requiresIdempotencyKey(spec Spec, key string) error {
	if spec.SideEffect != policy.SideEffectWrite || key != "" {
		return nil
	}
	return run.NewError("tool.missing_idempotency_key", run.ErrorInvalid, run.RetryNever,
		fmt.Errorf("write tool %q needs an idempotency key", spec.Name))
}

// enforce turns a policy decision into the gateway's action at the policy
// stage, exhaustively.
//
// A switch rather than the two ifs that used to be here, because the two ifs
// had a fall-through and the fall-through was silent consent. cap_budget and
// require_reconciler both reach this point, match, get written into the audit
// explanation — and then proceed, because nothing named them. A rule that
// publishes, matches and is recorded reads as enforced to everyone except the
// person who greps the gateway.
//
// So the two unenforced decisions are named here, saying what they actually do
// rather than being absent. They are not converted to denials: cap_budget is
// live in the published Policy document, and changing what a published rule
// does is a publish, not a deploy. See TD-058 for the ordering that removes
// them for good.
//
// The default branch fails closed, and TestEveryDecisionIsNamedInTheGateway
// keeps it unreachable: a fourth decision added to policy.Decisions() without a
// case here fails the test rather than quietly joining the allow side.
func enforce(decision policy.Decision, spec Spec) error {
	switch decision {
	case policy.DecisionDeny:
		return run.NewError("tool.denied_by_policy", run.ErrorDenied, run.RetryNever,
			fmt.Errorf("tool %q denied", spec.Name))
	case policy.DecisionAllow:
		return nil
	case policy.DecisionRequireApproval:
		// Handled further down, after the quota and budget stages: parking a
		// Run that would have been refused anyway would put a human in front of
		// a question that has already been answered.
		return nil
	case policy.DecisionCapBudget, policy.DecisionRequireReconciler:
		// No enforcement point. cap_budget would need a ceiling on Explanation
		// to cap anything, and require_reconciler has no mechanism at all.
		return nil
	default:
		return run.NewError("tool.unknown_policy_decision", run.ErrorDenied, run.RetryNever,
			fmt.Errorf("decision %q has no enforcement point", decision))
	}
}
