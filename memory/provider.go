package memory

import (
	"context"

	"github.com/costa92/agent-runtime/run"
)

// ErrReadOnly is what a retrieval-only provider returns from Write. A sentinel
// rather than a second interface: the Definition's `writable` flag is checked
// at publish time, and two ways to express "cannot write" would mean two places
// that check could disagree.
var ErrReadOnly = run.NewError("memory_read_only", run.ErrorDenied, run.RetryNever)

// Provider is a memory implementation: retrieval and, optionally, writing.
//
// Write returns a Mutation rather than committing, because the commit has to be
// atomic with the Run's usage, events and budget settlement — and only the
// Runtime is inside that transaction. A provider that persisted on its own
// would be a second writer outside the fence, and a crash between its write and
// the Runtime's commit would leave the two disagreeing.
type Provider interface {
	Retrieve(ctx context.Context, query Query) ([]Record, error)
	Write(ctx context.Context, scope Scope, ref, text string) (Mutation, error)
}

// RecallObserver is optional bookkeeping after the Gateway has applied both
// ceilings. A failure here must not change the retrieval result.
type RecallObserver interface {
	Recalled(ctx context.Context, refs []string) error
}
