package observe

import (
	"context"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// TraceContext is the durable half of a trace.
//
// It is persisted with the Run and never held only in a call stack. A Run can
// be parked for a day waiting on a human, resumed by a different worker in a
// different process, and delegated to children that outlive their parent's
// goroutine — an in-memory parent span would be gone in every one of those
// cases, and the child would open a new trace that nothing links back.
//
// It carries identifiers only: no claims, no baggage, nothing a later reader
// could mistake for a live authorization.
type TraceContext struct {
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
	// Sampled travels with the context so that a resumed Run makes the same
	// sampling decision the Run that started it made. Re-deciding on resume
	// would produce traces with holes exactly where a Run waited.
	Sampled bool `json:"sampled,omitempty"`
}

// Zero reports whether no trace was started.
func (c TraceContext) Zero() bool { return c.TraceID == "" }

// SpanKind is what a span measures.
type SpanKind string

const (
	SpanRun         SpanKind = "run"
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
}

// Span is an open measurement. End is idempotent.
type Span interface {
	Context() TraceContext
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

func (s nopSpan) Context() TraceContext { return s.parent }
func (nopSpan) End(error)               {}
