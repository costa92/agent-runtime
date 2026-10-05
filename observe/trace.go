package observe

import (
	"context"

	"github.com/costa92/agent-runtime/run"
)

// TraceContext is the durable half of a trace, defined with the Run state it is
// persisted in.
//
// It is an alias rather than a parallel type: two structs with the same fields
// would need a conversion at the Snapshot boundary, and a conversion is a place
// a field can be forgotten. It carries identifiers only — no claims, no
// baggage, nothing a later reader could mistake for a live authorization.
type TraceContext = run.TraceContext

// SpanKind is what a span measures.
type SpanKind string

const (
	SpanRun SpanKind = "run"
	// SpanNode is one node's execution inside an advance. Effect spans hang
	// under it, so a multi-node graph reads as nodes rather than as a flat
	// list of calls.
	SpanNode        SpanKind = "node"
	SpanModelCall   SpanKind = "model_call"
	SpanToolCall    SpanKind = "tool_call"
	SpanMemoryRead  SpanKind = "memory_read"
	SpanMemoryWrite SpanKind = "memory_write"
	// SpanResumed is the segment after a wait. A Run parked for approval gets
	// a new linked span rather than one span covering the wait: a span whose
	// duration is mostly a human thinking makes every latency percentile
	// meaningless.
	SpanResumed SpanKind = "resumed"
)

// SpanRequest is what the Runtime asks the host's tracer to open.
type SpanRequest struct {
	Kind   SpanKind
	Name   string
	RunID  run.ID
	Parent TraceContext
	// InvocationID and IdempotencyKey are attached to effect spans, so that a
	// trace can be joined to the Invocation record that reconciliation works
	// from.
	InvocationID   run.ID
	IdempotencyKey string
	// NodeID and AgentKey identify a node span. They are attributes, never part
	// of the name: node ids are authored per Definition and would make the
	// span name unbounded across tenants.
	NodeID   string
	AgentKey string
}

// Span is an open measurement. End is idempotent.
type Span interface {
	Context() TraceContext
	// RecordPayloadSizes records byte lengths only. The host decides whether
	// its configured capture level permits exporting them.
	RecordPayloadSizes(inputBytes, outputBytes int)
	End(err error)
}

// Tracer is the host's tracing implementation.
//
// The Runtime does not ship one: tracing means a vendor SDK, and the Runtime's
// production graph is standard-library only. A host that already traces adapts
// what it has.
type Tracer interface {
	Start(ctx context.Context, request SpanRequest) (context.Context, Span)
}

// NopTracer produces spans that measure nothing but still carry the parent
// context forward, so that a host without tracing does not break the linkage a
// host with tracing depends on.
type NopTracer struct{}

func (NopTracer) Start(ctx context.Context, request SpanRequest) (context.Context, Span) {
	return ctx, nopSpan{parent: request.Parent}
}

type nopSpan struct{ parent TraceContext }

func (s nopSpan) Context() TraceContext     { return s.parent }
func (nopSpan) RecordPayloadSizes(int, int) {}
func (nopSpan) End(error)                   {}
