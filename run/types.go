package run

import (
	"encoding/json"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
)

// ID identifies a Run, a node, an invocation or an approval. One type rather
// than five, because they are all opaque host-chosen strings and five identical
// named types would only invite conversions.
type ID string

// DefinitionRef pins the immutable definition a Run executes. A Run captures it
// at creation and never re-resolves it: a definition published mid-flight
// applies to the next Run, not to this one.
type DefinitionRef struct {
	ID       string `json:"id"`
	Version  uint64 `json:"version"`
	Protocol uint32 `json:"protocol"`
}

// ExecutionGraphRef pins the graph that was compiled at publish time.
//
// Digest is what makes it safe to cache and to reload after a crash: recovery
// loads by digest rather than recompiling the definition, so a Run cannot come
// back as a different shape because the deployment's compiler changed under it.
type ExecutionGraphRef struct {
	ID       string `json:"id"`
	Version  uint64 `json:"version"`
	Protocol uint32 `json:"protocol"`
	Digest   string `json:"digest"`
}

// TraceContext is the durable half of a trace: identifiers only.
//
// It lives in the Snapshot rather than in a call stack because a Run can be
// parked overnight, resumed in another process, and delegated to children that
// outlive their parent's goroutine. In every one of those cases an in-memory
// parent span is gone, and the continuation would open a new trace nothing
// links back to the original.
type TraceContext struct {
	TraceID string `json:"trace_id,omitempty"`
	SpanID  string `json:"span_id,omitempty"`
	// Sampled travels with the context so a resumed Run makes the same sampling
	// decision the Run that started it made. Re-deciding on resume produces
	// traces with holes exactly where a Run waited.
	Sampled bool `json:"sampled,omitempty"`
}

// Zero reports whether no trace was started.
func (c TraceContext) Zero() bool { return c.TraceID == "" }

// Pins are the versioned decisions a Run captured at creation and holds for its
// whole life.
//
// Digests rather than the rule sets themselves: the sets are large, they are
// already stored as published resources, and what a Run needs to be reproducible
// is which version it was judged against. A publish that lands mid-flight cannot
// reach a Run that pinned the version before it.
type Pins struct {
	PolicyDigest string       `json:"policy_digest,omitempty"`
	QuotaDigest  string       `json:"quota_digest,omitempty"`
	Trace        TraceContext `json:"trace,omitzero"`
}

// NodeState is one node's progress within the graph.
type NodeState struct {
	Status State `json:"status"`
	// Attempts counts starts, not failures, so a retry loop is visible even
	// when every attempt ended the same way.
	Attempts int `json:"attempts"`
	// OutputRef points at where the node's output was stored. The Runtime does
	// not hold outputs: they are unbounded, and a Snapshot is written on every
	// transition.
	OutputRef string `json:"output_ref,omitempty"`
}

// Snapshot is the whole durable state of one Run.
//
// It holds a PrincipalRef and never a PrincipalContext: the Snapshot is
// persisted and handed to hosts, transports and storage, so a request-scoped
// claim that reached it would be written to disk and replayed into every later
// reader. A test walks this type and fails if PrincipalContext is reachable.
type Snapshot struct {
	ID         ID                `json:"id"`
	Revision   uint64            `json:"revision"`
	Definition DefinitionRef     `json:"definition"`
	Graph      ExecutionGraphRef `json:"graph"`
	State      State             `json:"state"`

	// RootID and ParentID are empty on a root Run. A delegated child shares
	// the root's budget envelope and cancellation epoch.
	RootID   ID `json:"root_id,omitempty"`
	ParentID ID `json:"parent_id,omitempty"`

	Principal authorization.PrincipalRef `json:"principal"`

	Pins Pins `json:"pins,omitzero"`

	// RootCancellationEpoch fences work issued before a cancellation against
	// work issued after it. A child that was already in flight when the root
	// was cancelled carries the old epoch and is refused at commit.
	RootCancellationEpoch uint64 `json:"root_cancellation_epoch"`

	Budget Budget `json:"budget"`

	Nodes       map[string]NodeState `json:"nodes,omitempty"`
	Invocations map[ID]Invocation    `json:"invocations,omitempty"`

	// LastEventSequence is the last sequence number emitted for this Run.
	// Sequences are contiguous within a Run, which is what lets a consumer
	// detect a gap rather than silently missing an event.
	LastEventSequence uint64 `json:"last_event_sequence"`

	// Checkpoint is opaque to the Runtime: it is the executing graph's own
	// resume state, and the Runtime only carries it.
	Checkpoint json.RawMessage `json:"checkpoint,omitempty"`

	// Input is what the Run was started on, opaque to the Runtime and set once
	// at creation.
	//
	// It is held rather than passed through because the Run outlives the
	// request that started it: a worker picking the Run up minutes later — or a
	// takeover after a crash — has nothing but the Snapshot, and an input that
	// lived only in the Start call would leave that worker executing an agent
	// with no idea what it was asked. It is never rewritten: an input that
	// changed after the Run began would make the Run's own pins describe an
	// execution nobody can reproduce.
	Input json.RawMessage `json:"input,omitempty"`

	// Upstreams is where each dependency's output was stored, by plan key.
	//
	// Set once at creation for a delegated child, empty for every other Run. A
	// child that depends on another and cannot see what it produced executes on
	// a task that says "use the result above" with no result attached — and it
	// answers anyway, which is the failure mode this exists to prevent.
	Upstreams map[string]string `json:"upstreams,omitempty"`

	// Restrictions narrow what this one Run may do, below whatever its
	// Definition and the tenant's policies already allow.
	//
	// Set once at Start and never widened: they are applied by intersection, so
	// a caller cannot grant itself anything by asking. That direction is the
	// whole point — a Run-scoped grant that could widen would be a privilege
	// escalation performed by whoever happened to start the Run, and the
	// published policy that was supposed to decide would never see it.
	//
	// This is what an evaluation trial runs under: the same Definition as
	// production, with its side effects refused, so a trial cannot publish
	// something for real while proving that it can.
	Restrictions Restrictions `json:"restrictions,omitzero"`
}

// Restrictions are the per-Run narrowing.
type Restrictions struct {
	// Tools, when non-empty, is intersected with the Definition's declared
	// tools. Empty means "no narrowing", not "no tools": an empty allowlist
	// meaning nothing-allowed would silently disarm every Run that did not ask
	// for a restriction.
	Tools []string `json:"tools,omitempty"`
	// DenySideEffects refuses any call that changes state the Runtime cannot
	// roll back, whatever the policy says about it.
	DenySideEffects bool `json:"deny_side_effects,omitempty"`
}

// Narrow returns the allowlist a Run may actually use.
func (r Restrictions) Narrow(declared []string) []string {
	if len(r.Tools) == 0 {
		return declared
	}
	allowed := make(map[string]bool, len(r.Tools))
	for _, key := range r.Tools {
		allowed[key] = true
	}
	kept := make([]string, 0, len(declared))
	for _, key := range declared {
		if allowed[key] {
			kept = append(kept, key)
		}
	}
	// Intersection, so a key the Definition never declared cannot be added by
	// listing it here.
	return kept
}

// CommandKind is the closed set of things that can happen to a Run. Nothing
// else changes Run state — there is no SetStatus and no generic mutation, so a
// host cannot move a Run into a state the machine would have refused.
type CommandKind string

const (
	CommandStart CommandKind = "start"
	// CommandResume returns a parked Run to running. It does not apply to
	// waiting_resolution, which only a resolution may leave.
	CommandResume CommandKind = "resume"
	CommandRetry  CommandKind = "retry"

	CommandWaitApproval CommandKind = "wait_approval"
	CommandWaitChildren CommandKind = "wait_children"
	CommandWaitRetry    CommandKind = "wait_retry"

	// The three governed effects. Each reserves budget before the effect is
	// issued, which is what makes the envelope a limit rather than a report.
	CommandInvokeModel CommandKind = "invoke_model"
	CommandInvokeTool  CommandKind = "invoke_tool"
	CommandWriteMemory CommandKind = "write_memory"

	CommandCreateChildren CommandKind = "create_children"

	// CommandRecordUnknown parks the Run because an Invocation's outcome could
	// not be established.
	CommandRecordUnknown CommandKind = "record_unknown"
	// CommandResolveInvocation is the only way out of waiting_resolution.
	CommandResolveInvocation CommandKind = "resolve_invocation"

	CommandSucceed CommandKind = "succeed"
	CommandPartial CommandKind = "partial"
	CommandFail    CommandKind = "fail"
	CommandCancel  CommandKind = "cancel"
)

// Command is one typed instruction. Fields are populated per kind rather than
// there being one command type per kind, because Reduce dispatches on Kind
// anyway and a dozen near-identical structs would add no checking the reducer
// does not already do.
type Command struct {
	Kind CommandKind

	// Reserve is the spend an effect command charges before issuing.
	Reserve Limits
	// Slices are the child envelopes CommandCreateChildren grants.
	Slices map[ID]Limits

	// InvocationID names the Invocation for record_unknown and
	// resolve_invocation.
	InvocationID ID
	// Outcome is the resolver's answer for resolve_invocation.
	Outcome Outcome
	// IdempotencyKey is recorded with a new Invocation.
	IdempotencyKey string
}

// EffectKind is what the Runtime asks the caller to actually do. Reduce is
// pure: it decides, records the decision, and returns the effect for the
// Advance loop to execute inside the fixed governance path.
type EffectKind string

const (
	EffectModelCall   EffectKind = "model_call"
	EffectToolCall    EffectKind = "tool_call"
	EffectMemoryWrite EffectKind = "memory_write"
	EffectChildCreate EffectKind = "child_create"
	// EffectUnknown records an unresolved Invocation for reconciliation. It
	// never makes the Run itself unknown.
	EffectUnknown EffectKind = "unknown"
)

// Effect is a request for the caller to perform something with a side effect.
type Effect struct {
	Kind         EffectKind
	InvocationID ID
	Reserve      Limits
	Slices       map[ID]Limits
}

// EventKind is the observable record of a decision. Router, model selection,
// policy, quota, budget and approval decisions are events rather than log
// lines, so that a consumer can reconstruct why a Run did what it did.
type EventKind string

const (
	EventStateChanged       EventKind = "state_changed"
	EventBudgetReserved     EventKind = "budget_reserved"
	EventBudgetDenied       EventKind = "budget_denied"
	EventChildrenGranted    EventKind = "children_granted"
	EventInvocationParked   EventKind = "invocation_parked"
	EventInvocationResolved EventKind = "invocation_resolved"
)

// Event is one entry in a Run's contiguous, monotonic sequence.
type Event struct {
	Sequence uint64
	Kind     EventKind
	RunID    ID
	// From and To are populated for state changes; both empty means the event
	// records something other than a transition.
	From State
	To   State
	// InvocationID is populated for invocation events.
	InvocationID ID
	Reserve      Limits
}

// Transition is what Reduce returns: the next snapshot, the events to append
// with it, and the effects to perform after it commits. All three commit
// together — an event that outlives its transition describes something that did
// not happen.
type Transition struct {
	Next    Snapshot
	Events  []Event
	Effects []Effect
}
