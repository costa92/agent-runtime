package agentruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/memory"
	"github.com/kart-io/wechat-account/agent-runtime/observe"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/quota"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

// DefinitionSource loads a published Definition by pinned ref.
type DefinitionSource interface {
	// Load returns the Definition and the graph ref that was compiled when it
	// was published. The graph ref comes from here rather than being recomputed,
	// because recomputing it is compiling, and the Runtime does not compile.
	Load(ctx context.Context, ref run.DefinitionRef) (definition.Definition, run.ExecutionGraphRef, error)
}

// GovernanceSource supplies the rule sets a Run is pinned to.
//
// Both are fetched by digest on resume and by "current" only at Start. That
// asymmetry is the point: a Run decides once which rules judge it, and every
// later worker that picks it up asks for that same version rather than for
// whatever is current when it happens to resume.
type GovernanceSource interface {
	PolicySnapshot(ctx context.Context, tenant, digest string) (policy.Snapshot, error)
	QuotaSnapshot(ctx context.Context, tenant, digest string) (quota.Snapshot, error)
}

// Dependencies is everything the Runtime needs from its host.
type Dependencies struct {
	Store         store.Execution
	Definitions   DefinitionSource
	Graphs        GraphSource
	Governance    GovernanceSource
	Authorization authorization.Authorizer
	Meter         quota.Meter

	Agents *agent.Registry
	// Router, Candidates and Synthesis are the delegation ports. All three are
	// optional: a deployment with no orchestrators needs none of them, and a
	// Runtime that demanded them would make single-agent embedding harder than
	// it already is.
	Router     workflow.Router
	Candidates CandidateSource
	Synthesis  workflow.SynthesisStrategy
	// Schemas validates a node's output before anything downstream binds it.
	// Optional: a host that ships no validator still gets every other check.
	Schemas  definition.SchemaValidator
	Models   llm.Registry
	Tools    *tool.Gateway
	Memories *memory.Gateway

	Clock    observe.Clock
	IDs      observe.IDGenerator
	Events   *observe.EventSpecRegistry
	Tracer   observe.Tracer
	Observer observe.Observer

	// LeaseFor is how long a claim holds. The renew loop refreshes well inside
	// it; the value only has to outlast a single effect's start.
	LeaseFor time.Duration
	// GraphCacheSize bounds the digest-keyed graph cache.
	GraphCacheSize int
	// Owner identifies this worker in a lease.
	Owner string
}

// StartRequest creates one Run.
type StartRequest struct {
	// Principal is the request-scoped identity. Only its durable half is ever
	// persisted; the claims are used for the authorization decision made here
	// and then dropped.
	Principal  authorization.PrincipalContext
	Definition run.DefinitionRef
	Input      json.RawMessage
	Budget     run.Limits

	// Projections are enqueued in the transaction that creates the Run.
	//
	// The host's only way to record the turn that caused a Run atomically with
	// it. The Runtime does not inspect them beyond the Store's own validation:
	// a session id and a user's message are host vocabulary, and a Runtime that
	// understood them would be a Runtime with a session concept.
	Projections []store.ProjectionFact

	// Restrictions narrow this Run below what its Definition allows. Applied by
	// intersection, so nothing here can grant anything.
	Restrictions run.Restrictions
}

// AdvanceResult is what one Advance accomplished.
type AdvanceResult struct {
	Run run.Snapshot
	// Waiting marks a Run that stopped on something outside itself rather than
	// finishing. A caller cannot tell that from the state alone without knowing
	// which states are waiting states, and getting that wrong means either
	// spinning on a parked Run or dropping a runnable one.
	Waiting bool
}

// AdvanceNextRequest is the Worker entry point.
type AdvanceNextRequest struct {
	Limit int
	// RootQuantum caps how much of one batch a single root tree may take, so a
	// fanned-out root cannot starve every other Run in the deployment.
	RootQuantum int
}

// ApprovalDecision is a human's answer to a parked Run.
type ApprovalDecision struct {
	RunID      run.ID
	ApprovalID run.ID
	Approved   bool
	Reason     string
	// DecidedBy is re-authorized at resolution time. The approval may have been
	// requested hours ago, and whoever answers now must still be allowed to.
	DecidedBy authorization.PrincipalContext
}

// InvocationResolution settles an Invocation whose outcome was never
// established.
type InvocationResolution struct {
	RunID        run.ID
	InvocationID run.ID
	Outcome      run.Outcome
	Reason       string
	ResolvedBy   authorization.PrincipalContext
}

// CancelRequest stops a whole tree.
type CancelRequest struct {
	RootID      run.ID
	RequestedBy authorization.PrincipalContext
	Reason      string
}

// Runtime is the only way to execute anything.
//
// Model, Tool and Memory effects are unreachable except through it: a host that
// could call a gateway directly would be performing an effect outside the
// fence, the budget and the invocation record, and nothing downstream could
// tell that call from one that never happened.
type Runtime interface {
	Start(ctx context.Context, request StartRequest) (run.Snapshot, error)
	Advance(ctx context.Context, id run.ID) (AdvanceResult, error)
	AdvanceNext(ctx context.Context, request AdvanceNextRequest) (AdvanceResult, bool, error)
	ResolveApproval(ctx context.Context, decision ApprovalDecision) (run.Snapshot, error)
	ResolveInvocation(ctx context.Context, resolution InvocationResolution) (run.Snapshot, error)
	Cancel(ctx context.Context, request CancelRequest) (run.Snapshot, error)
	Inspect(ctx context.Context, id run.ID) (run.Snapshot, error)
	ListEvents(ctx context.Context, query store.EventQuery) (store.EventPage, error)
}

type runtime struct {
	deps     Dependencies
	graphs   *GraphCache
	recorder *observe.Recorder
}

// New validates the dependency graph and freezes it.
//
// It fails closed on a missing dependency rather than nil-checking at each use.
// A Runtime assembled without a Store or an Authorizer would run until the
// first Run needed one, which is both later and further from the mistake.
func New(deps Dependencies) (Runtime, error) {
	required := []struct {
		name    string
		present bool
	}{
		{"store", deps.Store != nil},
		{"definitions", deps.Definitions != nil},
		{"graphs", deps.Graphs != nil},
		{"governance", deps.Governance != nil},
		{"authorization", deps.Authorization != nil},
		{"meter", deps.Meter != nil},
		{"agents", deps.Agents != nil},
		{"clock", deps.Clock != nil},
		{"ids", deps.IDs != nil},
		{"events", deps.Events != nil},
	}
	for _, dependency := range required {
		if !dependency.present {
			return nil, run.NewError("missing_dependency", run.ErrorInternal, run.RetryNever,
				fmt.Errorf("no %s", dependency.name))
		}
	}
	if !deps.Agents.Frozen() {
		// An open registry would make "this agent exists" answerable
		// differently at publish and at execution.
		return nil, run.NewError("registry_not_frozen", run.ErrorInternal, run.RetryNever)
	}

	// Every event the Runtime can emit must be declared, checked once at
	// assembly rather than discovered when an emission is dropped in
	// production.
	for _, spec := range observe.BuiltinEventSpecs() {
		if _, err := deps.Events.Lookup(spec.Name); err != nil {
			return nil, err
		}
	}

	if deps.LeaseFor <= 0 {
		deps.LeaseFor = 30 * time.Second
	}
	if deps.Owner == "" {
		deps.Owner = "agent-runtime"
	}
	if deps.Tracer == nil {
		deps.Tracer = observe.NopTracer{}
	}

	return &runtime{
		deps:     deps,
		graphs:   NewGraphCache(deps.Graphs, deps.GraphCacheSize),
		recorder: observe.NewRecorder(deps.Events, deps.Observer),
	}, nil
}

// Start creates a Run with everything it will be judged by already pinned.
//
// Pinning happens here and only here. The Definition, the graph digest, the
// policy set and the quota set are all captured at creation, so a publish that
// lands while the Run is in flight applies to the next Run — never to one
// already executing under the previous answer.
func (r *runtime) Start(ctx context.Context, request StartRequest) (run.Snapshot, error) {
	principal := request.Principal.Ref
	if principal.Zero() {
		return run.Snapshot{}, run.NewError("missing_principal", run.ErrorInvalid, run.RetryNever)
	}

	// Authorization uses the request-scoped claims and records only the durable
	// reference. This is the one moment the claims are legitimately in hand.
	if err := r.deps.Authorization.AuthorizeUse(ctx, request.Principal, authorization.ResourceRef{
		Kind: "definition", ID: request.Definition.ID,
	}); err != nil {
		return run.Snapshot{}, err
	}

	quotas, err := r.deps.Governance.QuotaSnapshot(ctx, principal.Tenant, "")
	if err != nil {
		return run.Snapshot{}, err
	}
	enforcer, err := quota.NewEnforcer(quotas, r.deps.Meter)
	if err != nil {
		return run.Snapshot{}, err
	}
	scope := quota.Scope{Tenant: principal.Tenant, Principal: principal.Subject}
	decision, err := enforcer.AdmitRun(ctx, scope)
	if err != nil {
		return run.Snapshot{}, err
	}
	if !decision.Allowed {
		r.record(observe.Decision{
			Name: observe.EventQuotaRejected, RunID: "pending",
			Attributes: quotaAttributes(decision, scope),
		})
		return run.Snapshot{}, run.NewError("quota_exhausted", run.ErrorDenied, run.RetryBackoff,
			fmt.Errorf("quota %q: %d used of %d", decision.Limit.Name, decision.Observed, decision.Limit.Max))
	}

	declared, graphRef, err := r.deps.Definitions.Load(ctx, request.Definition)
	if err != nil {
		return run.Snapshot{}, err
	}
	// Loaded and verified, never compiled. A Runtime that compiled here would
	// let a deployment whose compiler changed resume a Run as a different shape.
	if _, err := r.graphs.Get(ctx, graphRef); err != nil {
		return run.Snapshot{}, err
	}
	if err := declaredLabels(declared, request.Restrictions.Labels); err != nil {
		return run.Snapshot{}, err
	}

	policies, err := r.deps.Governance.PolicySnapshot(ctx, principal.Tenant, "")
	if err != nil {
		return run.Snapshot{}, err
	}

	id := r.deps.IDs.NewID("run")
	traceID := r.deps.IDs.NewID("trace")
	return r.deps.Store.Create(ctx, store.CreateCommand{
		ID:           id,
		Definition:   request.Definition,
		Graph:        graphRef,
		Principal:    principal,
		Budget:       run.Budget{Envelope: envelopeFor(request.Budget, declared.Budget)},
		Input:        request.Input,
		Restrictions: request.Restrictions,
		Projections:  request.Projections,
		Pins: run.Pins{
			PolicyDigest: policies.Digest,
			QuotaDigest:  quotas.Digest,
			Trace:        run.TraceContext{TraceID: string(traceID), SpanID: string(id), Sampled: true},
		},
	})
}

// declaredLabels refuses a Run label the Definition did not publish.
//
// Refused at Start rather than ignored at evaluation: a label that silently
// did nothing would leave the caller believing the Run is governed by a rule
// that never sees it, and the failure would show up as work that was supposed
// to be refused going ahead.
func declaredLabels(declared definition.Definition, requested []string) error {
	for _, label := range requested {
		if !slices.Contains(declared.RunLabels, label) {
			return run.NewError("undeclared_run_label", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("definition %q publishes no run label %q", declared.Ref.ID, label))
		}
	}
	return nil
}

// envelopeFor prefers the caller's budget and falls back to the Definition's.
//
// The caller wins because it is the one paying: a Definition's budget is a
// default its author chose, and a host that computed a per-request envelope
// from a customer's plan must not have it silently overridden.
func envelopeFor(requested, declared run.Limits) run.Limits {
	if !requested.Zero() {
		return requested
	}
	return declared
}

func (r *runtime) Inspect(ctx context.Context, id run.ID) (run.Snapshot, error) {
	return r.deps.Store.Get(ctx, id)
}

func (r *runtime) ListEvents(ctx context.Context, query store.EventQuery) (store.EventPage, error) {
	return r.deps.Store.Events(ctx, query)
}

// Cancel stops a whole tree. It takes no lease: cancellation has to work while
// a worker holds the Run, which is the case where it matters most.
func (r *runtime) Cancel(ctx context.Context, request CancelRequest) (run.Snapshot, error) {
	if err := r.deps.Authorization.AuthorizeUse(ctx, request.RequestedBy, authorization.ResourceRef{
		Kind: "run", ID: string(request.RootID),
	}); err != nil {
		return run.Snapshot{}, err
	}
	return r.deps.Store.CancelTree(ctx, store.CancelTreeCommand{
		RootID:      request.RootID,
		RequestedBy: request.RequestedBy.Ref,
		Reason:      request.Reason,
	})
}

// ResolveApproval records a human decision through the control-plane fence.
//
// No lease is involved. The worker that parked the Run may have exited hours
// before anyone answered, and requiring its lease would make approvals
// undeliverable. What is required instead is a live authorization for whoever
// is answering now.
func (r *runtime) ResolveApproval(ctx context.Context, decision ApprovalDecision) (run.Snapshot, error) {
	snapshot, err := r.deps.Store.Get(ctx, decision.RunID)
	if err != nil {
		return run.Snapshot{}, err
	}
	if err := r.deps.Authorization.AuthorizeUse(ctx, decision.DecidedBy, authorization.ResourceRef{
		Kind: "run", ID: string(decision.RunID),
	}); err != nil {
		return run.Snapshot{}, err
	}

	command := run.Command{Kind: run.CommandResume}
	if !decision.Approved {
		// A refusal is a refusal of one effect, not of the Run. Resuming with
		// the hold marked denied keeps the refused write from ever being
		// granted, while letting the agent answer without it — an assistant
		// whose picture-book request was refused still owes the user a reply.
		if hold := loadApprovalHold(snapshot.Checkpoint); hold != nil {
			hold.Denied = true
			command = run.Command{Kind: run.CommandResume, Checkpoint: encodeApprovalHold(*hold)}
		}
	}
	transition, err := run.Reduce(snapshot, command)
	if err != nil {
		return run.Snapshot{}, err
	}

	r.record(observe.Decision{
		Name: observe.EventApprovalDecided, RunID: decision.RunID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrApprovalID, string(decision.ApprovalID)),
			observe.Attr(observe.AttrApproved, boolText(decision.Approved)),
		},
	})

	return r.deps.Store.ResolveApproval(ctx, store.ResolveApprovalCommand{
		Fence: store.ResolutionFence{
			RunID:                         decision.RunID,
			ExpectedRevision:              snapshot.Revision,
			ExpectedRootCancellationEpoch: snapshot.RootCancellationEpoch,
			TargetID:                      string(decision.ApprovalID),
			RequestedBy:                   decision.DecidedBy.Ref,
		},
		Decision: store.ApprovalDecision{
			ID: decision.ApprovalID, Approved: decision.Approved, Reason: decision.Reason,
		},
		Commit: store.CommitContext{Transition: transition, Events: transition.Events},
	})
}

// ResolveInvocation settles an unknown Invocation.
//
// still_unknown keeps the Run parked and its reservation unavailable. Releasing
// it would let the budget be spent twice if the effect turns out to have
// happened, and "we looked and still cannot tell" is not the same answer as
// "it did not happen".
func (r *runtime) ResolveInvocation(ctx context.Context, resolution InvocationResolution) (run.Snapshot, error) {
	snapshot, err := r.deps.Store.Get(ctx, resolution.RunID)
	if err != nil {
		return run.Snapshot{}, err
	}
	if err := r.deps.Authorization.AuthorizeUse(ctx, resolution.ResolvedBy, authorization.ResourceRef{
		Kind: "run", ID: string(resolution.RunID),
	}); err != nil {
		return run.Snapshot{}, err
	}

	transition, err := run.Reduce(snapshot, run.Command{
		Kind:         run.CommandResolveInvocation,
		InvocationID: resolution.InvocationID,
		Outcome:      resolution.Outcome,
	})
	if err != nil {
		return run.Snapshot{}, err
	}

	invocation := snapshot.Invocations[resolution.InvocationID]
	settlement := store.BudgetSettlement{ReservationID: resolution.InvocationID}
	switch resolution.Outcome {
	case run.OutcomeApplied:
		settlement.Charged = invocation.Reserved
		settlement.Release = true
	case run.OutcomeNotApplied:
		settlement.Release = true
	}

	r.record(observe.Decision{
		Name: observe.EventInvocationReconciled, RunID: resolution.RunID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrInvocationID, string(resolution.InvocationID)),
			observe.Attr(observe.AttrIdempotencyKey, invocation.IdempotencyKey),
			observe.Attr(observe.AttrOutcome, string(resolution.Outcome)),
		},
	})

	return r.deps.Store.ResolveInvocation(ctx, store.ResolveInvocationCommand{
		Fence: store.ResolutionFence{
			RunID:                         resolution.RunID,
			ExpectedRevision:              snapshot.Revision,
			ExpectedRootCancellationEpoch: snapshot.RootCancellationEpoch,
			TargetID:                      string(resolution.InvocationID),
			RequestedBy:                   resolution.ResolvedBy.Ref,
		},
		Decision: store.InvocationResolution{
			ID: resolution.InvocationID, Outcome: resolution.Outcome, Reason: resolution.Reason,
		},
		Budget: settlement,
		Commit: store.CommitContext{Transition: transition, Events: transition.Events},
	})
}

// record forwards a decision, ignoring the validation error.
//
// New has already proven every event the Runtime emits is declared, so a
// failure here is impossible rather than merely unlikely — and failing a Run
// because its telemetry did not validate would be worse than the missing event.
func (r *runtime) record(decision observe.Decision) {
	_ = r.recorder.Record(decision)
}

func quotaAttributes(decision quota.Decision, scope quota.Scope) []observe.Attribute {
	return []observe.Attribute{
		observe.Attr(observe.AttrQuotaName, decision.Limit.Name),
		observe.Attr(observe.AttrQuotaScope, scope.String()),
		observe.Attr(observe.AttrUnit, string(decision.Limit.Unit)),
		observe.Attr(observe.AttrLimit, strconv.Itoa(decision.Limit.Max)),
		observe.Attr(observe.AttrObserved, strconv.Itoa(decision.Observed)),
	}
}

func boolText(value bool) string {
	if value {
		return "true"
	}
	return "false"
}
