package observe

import (
	"context"
	"time"

	"github.com/costa92/agent-runtime/run"
)

const auditWriteTimeout = 5 * time.Second

// AuditWriteError marks a failed durable decision write. The executor must
// retry without committing a failed node when no governed action followed it.
type AuditWriteError struct{ Cause error }

func (e *AuditWriteError) Error() string { return "audit write: " + e.Cause.Error() }
func (e *AuditWriteError) Unwrap() error { return e.Cause }

// Clock is the Runtime's time source. A port rather than time.Now so that a
// test can make a lease expire without sleeping, and so nothing in the Runtime
// reaches for wall time directly.
type Clock interface {
	Now() time.Time
}

// IDGenerator issues Run, Invocation and approval identities.
//
// It is a port because these identities are the idempotency keys the whole
// reconciliation story rests on: a host that needs them to be ULIDs, or to
// carry a shard prefix, must be able to say so, and a test needs them to be
// predictable.
type IDGenerator interface {
	NewID(prefix string) run.ID
}

// Observer receives decisions and streamed model output.
//
// Decisions are audit facts. A recorder failure must stop the Run before it
// takes the governed action; streamed output remains observational.
//
// Implementations must be safe for concurrent use. Both methods are called from
// whichever goroutine reached them: an agent may call the model from several at
// once, and a node abandoned at its wall-clock cap is reported while the handler
// it gave up on is still running and still emitting. The obvious implementation
// — append to a slice — is the one that breaks, and it breaks as a corrupted
// buffer or a runtime fatal rather than as a missing event, so nothing points
// back here. NopObserver is safe because it holds nothing.
type Observer interface {
	// Decision reports a governance decision that has been committed.
	Decision(ctx context.Context, decision Decision) error
	// Chunk reports streamed model output. It carries no Run payload beyond
	// the text the caller is already receiving.
	Chunk(runID run.ID, text string)
}

// NopObserver discards everything. It is the default so that a host which does
// not care about observability does not have to implement an interface to say
// so.
type NopObserver struct{}

func (NopObserver) Decision(context.Context, Decision) error { return nil }
func (NopObserver) Chunk(run.ID, string)                     {}

// Recorder is the one path from a decision to an Observer.
//
// Everything goes through the frozen registry first. A decision that does not
// validate is dropped rather than emitted: the alternative is a consumer
// receiving a payload no contract describes, which is exactly what declaring
// the events was meant to prevent. The drop is returned so the caller can fail
// closed at assembly time — the Runtime refuses to start if any event it can
// emit is undeclared, so this cannot fire in production.
type Recorder struct {
	registry *EventSpecRegistry
	observer Observer
}

func NewRecorder(registry *EventSpecRegistry, observer Observer) *Recorder {
	if observer == nil {
		observer = NopObserver{}
	}
	return &Recorder{registry: registry, observer: observer}
}

// Record validates and forwards a decision.
func (r *Recorder) Record(ctx context.Context, decision Decision) error {
	if r == nil || r.registry == nil {
		return run.NewError("missing_event_registry", run.ErrorInternal, run.RetryNever)
	}
	if err := r.registry.Validate(decision); err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, auditWriteTimeout)
	defer cancel()
	if err := r.observer.Decision(writeCtx, decision); err != nil {
		return &AuditWriteError{Cause: err}
	}
	return nil
}

// Chunk forwards streamed output.
func (r *Recorder) Chunk(runID run.ID, text string) {
	if r == nil || r.observer == nil {
		return
	}
	r.observer.Chunk(runID, text)
}
