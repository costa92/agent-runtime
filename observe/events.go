package observe

import (
	"fmt"
	"sort"

	"github.com/costa92/agent-runtime/run"
)

// The declared decision events. Every governance decision the Runtime makes is
// one of these, because a decision that was only logged cannot be queried, and
// "why did this Run refuse" is asked long after the log line has rotated away.
const (
	// EventRouterPlanSelected is emitted once per node the scheduler picks, with
	// the graph digest as its plan. The "router" in the name is historical: it
	// was shared with a delegation router that has since been deleted, and the
	// name is kept because it is a StableEvent whose value is already persisted
	// in agent_run_decisions rows. Renaming would split one fact across two
	// names for every query that reads history.
	EventRouterPlanSelected = "runtime.router.plan_selected"
	EventModelSelected      = "runtime.model.selected"
	EventPolicyEvaluated    = "runtime.policy.evaluated"
	EventQuotaRejected      = "runtime.quota.rejected"
	EventQuotaDegraded      = "runtime.quota.degraded"
	EventBudgetRefused      = "runtime.budget.refused"
	// EventQuotaUnreadable is a refusal caused by a meter that could not
	// answer, not by a limit that was reached. The two must not share a name:
	// one says the deployment is at its ceiling, the other says the ceiling
	// cannot be read, and the second is an availability fault that happens to
	// look like enforcement working.
	EventQuotaUnreadable      = "runtime.quota.unreadable"
	EventApprovalRequested    = "runtime.approval.requested"
	EventApprovalDecided      = "runtime.approval.decided"
	EventEgressHostResolved   = "runtime.egress.host_resolved"
	EventInvocationReconciled = "runtime.invocation.reconciled"
	// EventNodeAbandoned is emitted when a node hit the wall-clock cap and did
	// not return within the grace period after being cancelled. The Run is parked
	// with an unknown outcome and the handler's goroutine is left running: a
	// handler that reaches this ignored its context, and this event is the only
	// thing that says so.
	EventNodeAbandoned = "runtime.node.abandoned"
	// EventRunUnrunnable is emitted when a Run is ended because its session
	// could not be assembled and never will be — a pin resolving to nothing, a
	// graph that moved, a stored payload disagreeing with its digest.
	//
	// It exists because the state change alone does not say why. A Run that
	// ends this way never reached a node, so there is no node failure to read
	// and no tool to blame; without this event an operator sees a Run that
	// failed having done nothing, which is indistinguishable from a bug in the
	// Runtime. AttrReason carries the assembly error's code.
	EventRunUnrunnable = "runtime.run.unrunnable"
	// EventNodeLoopRetry is emitted when a node triggers a backward self-correction loop.
	EventNodeLoopRetry = "runtime.node.loop_retry"
)

// The declared attribute keys. Named constants rather than string literals at
// the call site so that a typo is a compile error instead of an attribute the
// registry silently rejects at run time.
const (
	AttrAgent        = "agent"
	AttrNode         = "node"
	AttrTargetNode   = "target_node"
	AttrAttempt      = "attempt"
	AttrPlan         = "plan"
	AttrProfile      = "profile"
	AttrCapability   = "capability"
	AttrPolicyName   = "policy_name"
	AttrPolicyDigest = "policy_digest"
	AttrDecision     = "decision"
	// AttrReason names which refusal it was, for the refusals that happen
	// before a policy Explanation exists. AttrDecision carries the kind, which
	// is what a caller switches on; several distinct refusals share one kind,
	// so the kind alone cannot say what to fix.
	AttrReason         = "reason"
	AttrShadow         = "shadow"
	AttrQuotaName      = "quota_name"
	AttrQuotaScope     = "quota_scope"
	AttrLimit          = "limit"
	AttrObserved       = "observed"
	AttrUnit           = "unit"
	AttrApprovalID     = "approval_id"
	AttrApproved       = "approved"
	AttrInvocationID   = "invocation_id"
	AttrIdempotencyKey = "idempotency_key"
	AttrOutcome        = "outcome"
	AttrHost           = "host"
	AttrTool           = "tool"
)

// BuiltinEventSpecs is the frozen set the Runtime itself emits.
//
// Note what no spec declares: no credential, no claim, no prompt, no tool
// argument, no model output. The closed field set is the enforcement — an
// attribute nobody declared cannot be recorded, so putting a secret in an event
// requires declaring a field for it, in review, on purpose.
func BuiltinEventSpecs() []EventSpec {
	return []EventSpec{
		{
			Name: EventRouterPlanSelected, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrPlan, AttrAgent, AttrNode},
		},
		{
			Name: EventModelSelected, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrProfile, AttrAgent, AttrNode, AttrCapability},
		},
		{
			Name: EventPolicyEvaluated, APIVersion: "v1", Stability: StableEvent,
			// The rule set is identified by its snapshot digest, not by a
			// per-policy version: a Policy has no version of its own, and a
			// field the emitter can never fill is a promise to consumers that
			// nothing keeps.
			Fields: []string{
				AttrTool, AttrDecision, AttrPolicyName, AttrPolicyDigest, AttrShadow,
				AttrReason,
			},
		},
		{
			Name: EventQuotaRejected, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrQuotaName, AttrQuotaScope, AttrLimit, AttrObserved, AttrUnit},
		},
		{
			Name: EventQuotaDegraded, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrQuotaName, AttrQuotaScope, AttrLimit, AttrObserved, AttrUnit},
		},
		{
			// No quota name: the failure is the meter's, and which limit was
			// being read when it failed is a detail of iteration order rather
			// than a property of the fault.
			Name: EventQuotaUnreadable, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrQuotaScope},
		},
		{
			Name: EventBudgetRefused, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrUnit, AttrLimit, AttrObserved, AttrTool},
		},
		{
			Name: EventApprovalRequested, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrApprovalID, AttrTool, AttrPolicyName},
		},
		{
			Name: EventApprovalDecided, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrApprovalID, AttrApproved},
		},
		{
			// The resolved host, not the URL: the path and query are where
			// identifiers and tokens end up, and the governance question is
			// only ever which host was reached.
			Name: EventEgressHostResolved, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrHost, AttrTool},
		},
		{
			Name: EventInvocationReconciled, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrInvocationID, AttrIdempotencyKey, AttrOutcome},
		},
		{
			Name: EventRunUnrunnable, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrReason},
		},
		{
			Name: EventNodeAbandoned, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrNode, AttrAgent, AttrInvocationID},
		},
		{
			Name: EventNodeLoopRetry, APIVersion: "v1", Stability: StableEvent,
			Fields: []string{AttrNode, AttrTargetNode, AttrAttempt, AttrReason},
		},
	}
}

// Attribute is one declared key and its rendered value. Values are strings
// because an event is read by operators and by consumers in other languages;
// a typed payload would push the Runtime into shipping a schema for each event.
type Attribute struct {
	Key   string
	Value string
}

// Decision is one governance decision, ready to commit alongside the Run
// transition that produced it.
type Decision struct {
	Name       string
	RunID      run.ID
	Attributes []Attribute
}

// Attr builds an attribute.
func Attr(key, value string) Attribute { return Attribute{Key: key, Value: value} }

// Validate checks a decision against the frozen registry.
//
// Both directions matter. An undeclared event has no consumer contract, and an
// undeclared attribute is the path by which something that was never reviewed —
// a credential, a claim, a prompt — reaches durable storage.
func (r *EventSpecRegistry) Validate(decision Decision) error {
	spec, err := r.Lookup(decision.Name)
	if err != nil {
		return err
	}
	if decision.RunID == "" {
		return run.NewError("unattributed_event", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("event %q names no Run", decision.Name))
	}

	declared := make(map[string]bool, len(spec.Fields))
	for _, field := range spec.Fields {
		declared[field] = true
	}
	var undeclared []string
	seen := map[string]bool{}
	for _, attribute := range decision.Attributes {
		if !declared[attribute.Key] {
			undeclared = append(undeclared, attribute.Key)
		}
		if seen[attribute.Key] {
			return run.NewError("duplicate_event_attribute", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("event %q carries %q twice", decision.Name, attribute.Key))
		}
		seen[attribute.Key] = true
	}
	if len(undeclared) > 0 {
		sort.Strings(undeclared)
		return run.NewError("undeclared_event_attribute", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("event %q carries undeclared %v", decision.Name, undeclared))
	}
	return nil
}
