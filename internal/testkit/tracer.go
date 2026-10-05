package testkit

import (
	"context"
	"fmt"
	"sync"

	"github.com/costa92/agent-runtime/observe"
)

// RecordedSpan is one span a RecordingTracer opened, and how it ended.
type RecordedSpan struct {
	Request     observe.SpanRequest
	Context     observe.TraceContext
	Ended       bool
	Err         error
	InputBytes  int
	OutputBytes int
}

// RecordingTracer keeps every span the Runtime opens. It is the evidence that
// an instrumentation point exists: a NopTracer cannot tell a call from no call.
type RecordingTracer struct {
	mu    sync.Mutex
	spans []*RecordedSpan
}

func NewRecordingTracer() *RecordingTracer { return &RecordingTracer{} }

func (t *RecordingTracer) Start(ctx context.Context, request observe.SpanRequest) (context.Context, observe.Span) {
	t.mu.Lock()
	defer t.mu.Unlock()
	traceID := request.Parent.TraceID
	if traceID == "" {
		traceID = "trace-test"
	}
	recorded := &RecordedSpan{
		Request: request,
		Context: observe.TraceContext{
			TraceID: traceID,
			SpanID:  fmt.Sprintf("span-%d", len(t.spans)+1),
			Sampled: request.Parent.Sampled,
		},
	}
	t.spans = append(t.spans, recorded)
	return ctx, &recordedSpan{tracer: t, span: recorded}
}

// Spans returns copies of every span opened so far, in start order.
func (t *RecordingTracer) Spans() []RecordedSpan {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]RecordedSpan, len(t.spans))
	for i, span := range t.spans {
		out[i] = *span
	}
	return out
}

// OfKind returns the spans of one kind.
func (t *RecordingTracer) OfKind(kind observe.SpanKind) []RecordedSpan {
	var out []RecordedSpan
	for _, span := range t.Spans() {
		if span.Request.Kind == kind {
			out = append(out, span)
		}
	}
	return out
}

type recordedSpan struct {
	tracer *RecordingTracer
	span   *RecordedSpan
}

func (s *recordedSpan) Context() observe.TraceContext { return s.span.Context }

func (s *recordedSpan) RecordPayloadSizes(inputBytes, outputBytes int) {
	s.tracer.mu.Lock()
	defer s.tracer.mu.Unlock()
	s.span.InputBytes = inputBytes
	s.span.OutputBytes = outputBytes
}

func (s *recordedSpan) End(err error) {
	s.tracer.mu.Lock()
	defer s.tracer.mu.Unlock()
	if s.span.Ended {
		return
	}
	s.span.Ended = true
	s.span.Err = err
}
