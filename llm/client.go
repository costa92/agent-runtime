package llm

import (
	"context"

	"github.com/costa92/agent-runtime/run"
)

// CodeCapabilityUnsupported is returned when a request asks for something the
// engine cannot do. It is a stable code rather than a message because the
// Runtime's response is to degrade — drop the tool loop and ask again — and a
// decision that important cannot be made by matching an error string.
const CodeCapabilityUnsupported = "model.capability_unsupported"

// CodeCancelled is returned when the caller's context ended.
const CodeCancelled = "model.cancelled"

// ErrCapabilityUnsupported is the canonical unsupported-capability error.
var ErrCapabilityUnsupported = run.NewError(CodeCapabilityUnsupported, run.ErrorInvalid, run.RetryNever)

// Cancelled builds the canonical cancellation error.
//
// The retry hint is never. Cancellation is a decision somebody made, and a
// worker that retried it would be overriding that decision — the one case where
// "the call did not complete" must not mean "try again".
func Cancelled(cause error) *run.Error {
	return run.NewError(CodeCancelled, run.ErrorInterrupted, run.RetryNever, cause)
}

// Client is one engine, seen through capabilities rather than through a
// provider's API shape.
//
// Implementations execute exactly the attempt they are given. They do not
// retry, do not fan out and do not fall back to another model: attempts are
// budget, and a client spending against an envelope it cannot see is how a
// budgeted Run overruns without anything appearing to be wrong.
type Client interface {
	Capabilities(ctx context.Context, model ModelRef) (Capabilities, error)
	Complete(ctx context.Context, request Request) (Response, error)
	// Stream delivers chunks in order and returns the same Response Complete
	// would have. onChunk's error aborts the stream and is returned to the
	// caller, so a consumer can stop reading without a side channel.
	Stream(ctx context.Context, request Request, onChunk func(Chunk) error) (Response, error)
}

// Registry resolves a ModelRef to a client. It is the seam where a published
// ModelProfile becomes a concrete engine, and the only place the Runtime learns
// that engines differ.
type Registry interface {
	Resolve(ctx context.Context, model ModelRef) (Client, error)
}
