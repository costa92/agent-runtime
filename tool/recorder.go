package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/costa92/agent-runtime/policy"
	"github.com/costa92/agent-runtime/run"
)

// Recorder is the host's audit sink for governed calls, consulted at the two
// moments a call becomes a fact: when the chain has decided about it, and when
// its effect is over.
//
// It is a host port for the same reason Authorizer and QuotaChecker are. What
// an audit must keep, how long, and where, are deployment questions; the
// Runtime only knows when there is something to say. A host that supplies no
// recorder is a supported deployment and loses only the audit.
//
// Recording at Decide rather than at Execute is the point. A call refused by
// policy never reaches a handler, so anything watching handlers can only ever
// see what was allowed — and "which calls were refused, and why" is most of
// what an audit is consulted for.
//
// Like Observer, it observes and never decides: it returns nothing, and a
// recorder that failed must not turn an allowed call into a refused one.
// Implementations must not block; the Runtime calls them on the execution path.
type Recorder interface {
	Decided(ctx context.Context, decision DecisionRecord)
	Finished(ctx context.Context, result ResultRecord)
}

// DecisionRecord is what the gateway chain concluded about one call, before any
// effect. Emitted for refusals too — that is the case it exists for.
type DecisionRecord struct {
	// RunID is the Run the call belongs to. Empty when the caller did not
	// supply one, in which case the host has an invocation with no run to file
	// it under and should drop it rather than invent one.
	RunID        run.ID
	InvocationID run.ID
	Tool         string
	// Spec is empty when the tool could not be resolved at all: a call for a
	// tool that does not exist is still a call that was attempted.
	Spec        Spec
	Explanation policy.Explanation
	// ArgsDigest is a hash of the arguments, never the arguments themselves.
	// Digesting here rather than handing the payload over is what makes "the
	// audit stores no tool payloads" a property of the port instead of a rule
	// every recorder has to remember.
	ArgsDigest string
	// Stage names where the chain stopped, empty when it ran to the end.
	Stage Stage
	// Err is why the call was refused, nil when it was allowed.
	Err error
	// Granted records that a human had already approved this exact effect, so
	// an audit can tell an approved write from one policy never questioned.
	Granted bool
	At      time.Time
}

// Allowed reports whether the chain let the call through.
func (d DecisionRecord) Allowed() bool { return d.Err == nil }

// ResultRecord is what happened once the effect was performed. It arrives only
// for calls that were allowed and committed.
type ResultRecord struct {
	RunID        run.ID
	InvocationID run.ID
	Tool         string
	Outcome      run.Outcome
	// ResultDigest hashes what the tool returned; see DecisionRecord.ArgsDigest.
	// Two invocations can only be compared through it, because the results
	// themselves are not kept.
	ResultDigest string
	// ResultBytes is the size of what the tool returned after capping, which is
	// what actually reached the prompt.
	ResultBytes int
	Duration    time.Duration
	// Err is the handler's failure, nil on success. The host derives an error
	// code from it; the Runtime does not classify on the host's behalf.
	Err error
	At  time.Time
}

// WithRecorder installs the audit sink.
func WithRecorder(recorder Recorder) Option {
	return func(g *Gateway) { g.recorder = recorder }
}

func (g *Gateway) recordDecision(ctx context.Context, record DecisionRecord) {
	if g.recorder == nil {
		return
	}
	if record.At.IsZero() {
		record.At = time.Now().UTC()
	}
	g.recorder.Decided(ctx, record)
}

func (g *Gateway) recordResult(ctx context.Context, record ResultRecord) {
	if g.recorder == nil {
		return
	}
	if record.At.IsZero() {
		record.At = time.Now().UTC()
	}
	g.recorder.Finished(ctx, record)
}

// digest hashes a payload for the audit. Empty in, empty out: a digest of
// nothing would read as a real value that simply never matches anything.
func digest(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
