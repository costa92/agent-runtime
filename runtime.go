package agentruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/costa92/agent-runtime/agent"
	"github.com/costa92/agent-runtime/authorization"
	"github.com/costa92/agent-runtime/definition"
	"github.com/costa92/agent-runtime/llm"
	"github.com/costa92/agent-runtime/memory"
	"github.com/costa92/agent-runtime/observe"
	"github.com/costa92/agent-runtime/policy"
	"github.com/costa92/agent-runtime/quota"
	"github.com/costa92/agent-runtime/run"
	"github.com/costa92/agent-runtime/store"
	"github.com/costa92/agent-runtime/tool"
)

const (
	defaultLeaseDuration   = 30 * time.Second
	defaultMaxNodeDuration = 10 * time.Minute
	defaultWorkerOwner     = "agent-runtime"
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
	// Schemas validates and normalizes values at every declared schema boundary.
	// Optional: a host that ships no processor still gets every other check.
	Schemas  definition.SchemaProcessor
	Models   llm.Registry
	Tools    *tool.Gateway
	Memories *memory.Gateway

	Clock    observe.Clock
	IDs      observe.IDGenerator
	Events   *observe.EventSpecRegistry
	Tracer   observe.Tracer
	Observer observe.Observer
	// Logger is where the Runtime writes diagnostics. Optional: the default
	// writes through log/slog. A host supplies its own so a Runtime line
	// carries the same trace id as the host's lines around it.
	Logger observe.Logger

	// LeaseFor is how long a claim holds. The renew loop refreshes well inside
	// it; the value only has to outlast a single effect's start.
	LeaseFor time.Duration
	// MaxNodeDuration bounds one node's execution in wall-clock time. The renew
	// loop stops here whatever the Store says: a lease that renews forever means
	// a wedged handler holds a worker slot forever, and the loop has no other way
	// to find out. It must exceed the tool timeout ceiling, or a node would be
	// abandoned before the tool call inside it was allowed to give up.
	//
	// Zero takes the 10m default; a negative value restores unbounded renewal —
	// the rollback, at config level, and deliberate rather than a forgotten field.
	MaxNodeDuration time.Duration
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
	// ExpectedRevision is the caller's observed Run version. Internal
	// reconcilers may leave it zero and use the version read by Runtime.
	ExpectedRevision uint64
	Outcome          run.Outcome
	Reason           string
	ResolvedBy       authorization.PrincipalContext
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
	Inspect(ctx context.Context, principal authorization.PrincipalContext, id run.ID) (run.Snapshot, error)
	ListEvents(ctx context.Context, principal authorization.PrincipalContext, query store.EventQuery) (store.EventPage, error)
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
		deps.LeaseFor = defaultLeaseDuration
	}
	if deps.MaxNodeDuration == 0 {
		// 10m: comfortably above the 180s tool ceiling a node may spend inside a
		// single call, and low enough that a wedged worker slot comes back the
		// same hour. Only an explicitly negative value disables it, so that
		// unbounded renewal is something a host asks for rather than forgets.
		deps.MaxNodeDuration = defaultMaxNodeDuration
	}
	if deps.Owner == "" {
		deps.Owner = defaultWorkerOwner
	}
	if deps.Tracer == nil {
		deps.Tracer = observe.NopTracer{}
	}
	if deps.Logger == nil {
		deps.Logger = observe.StdLogger{}
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
		if err := r.record(ctx, observe.Decision{
			Name: observe.EventQuotaRejected, RunID: "pending",
			Attributes: quotaAttributes(decision, scope),
		}); err != nil {
			return run.Snapshot{}, err
		}
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
	if request.Restrictions.ModelProfile != "" && declared.Model.Profile != "" {
		return run.Snapshot{}, run.NewError("model_profile_pinned", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("definition %q pins model profile %q", declared.Ref.ID, declared.Model.Profile))
	}
	input := request.Input
	if r.deps.Schemas != nil && len(declared.InputSchema) > 0 {
		input, err = r.deps.Schemas.NormalizeValue(declared.InputSchema, request.Input)
		if err != nil {
			return run.Snapshot{}, run.NewError("invalid_definition_input", run.ErrorInvalid, run.RetryNever, err)
		}
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
		Input:        input,
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

func (r *runtime) Inspect(
	ctx context.Context, principal authorization.PrincipalContext, id run.ID,
) (run.Snapshot, error) {
	return r.authorizeRunAccess(ctx, principal, id)
}

func (r *runtime) ListEvents(
	ctx context.Context, principal authorization.PrincipalContext, query store.EventQuery,
) (store.EventPage, error) {
	if _, err := r.authorizeRunAccess(ctx, principal, query.RunID); err != nil {
		return store.EventPage{}, err
	}
	return r.deps.Store.Events(ctx, query)
}

// authorizeRunAccess applies both halves of Run access: the caller must still
// hold the host permission and must be the exact durable principal recorded on
// the Run. A broad RBAC grant is deliberately insufficient; Run snapshots and
// events contain user content, and control-plane decisions can release effects.
func (r *runtime) authorizeRunAccess(
	ctx context.Context, principal authorization.PrincipalContext, id run.ID,
) (run.Snapshot, error) {
	snapshot, err := r.deps.Store.Get(ctx, id)
	if err != nil {
		return run.Snapshot{}, err
	}
	if principal.Ref.Zero() || principal.Ref != snapshot.Principal {
		return run.Snapshot{}, run.NewError(
			run.CodeUnknownRun, run.ErrorInvalid, run.RetryNever,
		)
	}
	if err := r.deps.Authorization.AuthorizeUse(ctx, principal, authorization.ResourceRef{
		Kind: "run", ID: string(id),
	}); err != nil {
		return run.Snapshot{}, err
	}
	return snapshot, nil
}

// Cancel stops a whole tree. It takes no lease: cancellation has to work while
// a worker holds the Run, which is the case where it matters most.
func (r *runtime) Cancel(ctx context.Context, request CancelRequest) (run.Snapshot, error) {
	if _, err := r.authorizeRunAccess(ctx, request.RequestedBy, request.RootID); err != nil {
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
	snapshot, err := r.authorizeRunAccess(ctx, decision.DecidedBy, decision.RunID)
	if err != nil {
		return run.Snapshot{}, err
	}

	command := run.Command{Kind: run.CommandResume, ApprovalID: decision.ApprovalID, Approved: decision.Approved}
	if !decision.Approved {
		// A refusal is a refusal of one effect, not of the Run. Resuming with
		// the hold marked denied keeps the refused write from ever being
		// granted, while letting the agent answer without it — an assistant
		// whose picture-book request was refused still owes the user a reply.
		if hold := loadApprovalHold(snapshot.Checkpoint); hold != nil {
			hold.Denied = true
			encoded, err := encodeApprovalHold(snapshot.Checkpoint, *hold)
			if err != nil {
				return run.Snapshot{}, err
			}
			command.Checkpoint = encoded
		}
	}
	transition, err := run.Reduce(snapshot, command)
	if err != nil {
		return run.Snapshot{}, err
	}

	audit := observe.Decision{
		Name: observe.EventApprovalDecided, RunID: decision.RunID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrApprovalID, string(decision.ApprovalID)),
			observe.Attr(observe.AttrApproved, boolText(decision.Approved)),
		},
	}
	if err := r.deps.Events.Validate(audit); err != nil {
		return run.Snapshot{}, err
	}
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
		Commit: store.CommitContext{Transition: transition, Events: transition.Events, Decisions: []observe.Decision{audit}},
	})
}

func terminalResultFact(snapshot run.Snapshot) (store.ProjectionFact, error) {
	userID, _ := strconv.ParseInt(snapshot.Principal.Subject, 10, 64)
	return store.NewProjectionFact(store.TerminalResultPayload{
		State:        string(snapshot.State),
		UserID:       userID,
		UsedLLMCalls: int64(snapshot.Budget.Used.LLMCalls),
		UsedTokens:   int64(snapshot.Budget.Used.Tokens),
	})
}

// ResolveInvocation settles an unknown Invocation.
//
// still_unknown keeps the Run parked and its reservation unavailable. Releasing
// it would let the budget be spent twice if the effect turns out to have
// happened, and "we looked and still cannot tell" is not the same answer as
// "it did not happen".
func (r *runtime) ResolveInvocation(ctx context.Context, resolution InvocationResolution) (run.Snapshot, error) {
	snapshot, err := r.authorizeRunAccess(ctx, resolution.ResolvedBy, resolution.RunID)
	if err != nil {
		return run.Snapshot{}, err
	}
	if resolution.ExpectedRevision != 0 && snapshot.Revision != resolution.ExpectedRevision {
		return run.Snapshot{}, run.NewError("stale_revision", run.ErrorConflict, run.RetryNever)
	}

	command := run.Command{
		Kind:         run.CommandResolveInvocation,
		InvocationID: resolution.InvocationID,
		Outcome:      resolution.Outcome,
	}

	invocation := snapshot.Invocations[resolution.InvocationID]
	if resolution.Outcome == run.OutcomeApplied {
		command.Usage = invocation.RemainingCharge()
	}
	if snapshot.State == run.StateWaitingResolution && (invocation.Outcome == run.OutcomeUnknown || invocation.Outcome == run.OutcomeStillUnknown) &&
		resolution.Outcome == run.OutcomeApplied && invocation.Write {
		if invocation.NodeID != "" {
			command.Checkpoint, err = checkpointWithAppliedWrite(snapshot.Checkpoint, invocation.NodeID, resolution.InvocationID)
			if err != nil {
				return run.Snapshot{}, err
			}
			command.ReplaceCheckpoint = true
		}
	}
	transition, err := run.Reduce(snapshot, command)
	if err != nil {
		return run.Snapshot{}, err
	}
	if transition.Next.Revision == snapshot.Revision {
		return snapshot, nil
	}
	settlement := store.BudgetSettlement{ReservationID: resolution.InvocationID}
	switch resolution.Outcome {
	case run.OutcomeApplied:
		settlement.Charged = command.Usage
		settlement.Release = true
	case run.OutcomeNotApplied:
		settlement.Release = true
	}

	audit := observe.Decision{
		Name: observe.EventInvocationReconciled, RunID: resolution.RunID,
		Attributes: []observe.Attribute{
			observe.Attr(observe.AttrInvocationID, string(resolution.InvocationID)),
			observe.Attr(observe.AttrIdempotencyKey, invocation.IdempotencyKey),
			observe.Attr(observe.AttrOutcome, string(resolution.Outcome)),
		},
	}
	if err := r.deps.Events.Validate(audit); err != nil {
		return run.Snapshot{}, err
	}
	var projections []store.ProjectionFact
	if transition.Next.State.Terminal() {
		terminal, projectionErr := terminalResultFact(transition.Next)
		if projectionErr != nil {
			return run.Snapshot{}, projectionErr
		}
		projections = append(projections, terminal)
	}

	expectedRevision := snapshot.Revision
	if resolution.ExpectedRevision != 0 {
		expectedRevision = resolution.ExpectedRevision
	}
	return r.deps.Store.ResolveInvocation(ctx, store.ResolveInvocationCommand{
		Fence: store.ResolutionFence{
			RunID:                         resolution.RunID,
			ExpectedRevision:              expectedRevision,
			ExpectedRootCancellationEpoch: snapshot.RootCancellationEpoch,
			TargetID:                      string(resolution.InvocationID),
			RequestedBy:                   resolution.ResolvedBy.Ref,
		},
		Decision: store.InvocationResolution{
			ID: resolution.InvocationID, Outcome: resolution.Outcome, Reason: resolution.Reason,
		},
		Usage:  command.Usage,
		Budget: settlement,
		Commit: store.CommitContext{Transition: transition, Events: transition.Events, Decisions: []observe.Decision{audit}, Projections: projections},
	})
}

// record persists a governance decision before the action it explains.
// A failed audit write stops the action; otherwise the decision ledger could
// silently omit the very refusal or grant an operator needs to investigate.
func (r *runtime) record(ctx context.Context, decision observe.Decision) error {
	return r.recorder.Record(ctx, decision)
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
