// Package store is the persistence contract, expressed as typed commands.
//
// There is no generic Commit, Update, SetStatus or AppendEvent here, and no
// optional mutation bag. Each command carries exactly the facts its business
// action can legally change, so a host adapter cannot be asked to interpret Run
// state — and cannot move a Run somewhere the reducer would have refused.
//
// Every command commits its state, checkpoint, usage, events and projections in
// one transaction. An event that outlives its transition describes something
// that did not happen, and a projection written beside a Run rather than with it
// is the "commit then write transcript" sequence this contract exists to make
// impossible.
package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/resource"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Lease is exclusive, time-bounded ownership of a Run by one worker.
//
// Deadline is Store-authoritative. A worker that judged expiry by its own clock
// would hand ownership to a second worker whenever the two disagreed, and the
// resulting double-write is precisely what the lease prevents.
type Lease struct {
	RunID    run.ID
	Token    string
	Owner    string
	Deadline time.Time
}

// Expired reports whether the lease has lapsed at the Store's now.
func (l Lease) Expired(now time.Time) bool { return !now.Before(l.Deadline) }

// ClaimedRun pairs a lease with the snapshot it was taken against.
type ClaimedRun struct {
	Lease    Lease
	Snapshot run.Snapshot
}

// EventQuery pages a Run's event sequence. After is exclusive, so a consumer
// resumes with the last sequence it saw rather than having to remember whether
// the bound was inclusive.
type EventQuery struct {
	RunID run.ID
	After uint64
	Limit int
}

// EventPage is one page of events plus the cursor to continue from.
type EventPage struct {
	Events []run.Event
	Next   uint64
}

// Execution is the Run aggregate's store. Every method is a typed command or a
// read; there is no escape hatch.
type Execution interface {
	Create(ctx context.Context, command CreateCommand) (run.Snapshot, error)

	Claim(ctx context.Context, command ClaimCommand) (Lease, run.Snapshot, error)
	ClaimBatch(ctx context.Context, command ClaimBatchCommand) ([]ClaimedRun, error)
	Renew(ctx context.Context, command RenewCommand) (Lease, error)
	// SealForCommit re-checks ownership against the Store's own clock,
	// immediately before a commit, so that a commit and a takeover cannot both
	// believe they own the Run.
	//
	// The check is the whole mechanism. It does NOT close the lease to further
	// renewal, and it must not: the engine seals once per node commit while one
	// Advance spans several nodes, so a seal that stopped renewal would leave a
	// multi-node graph unable to hold its lease after the first commit. The
	// implementations do record a lease_sealed flag, but nothing reads it —
	// see TD-073 — and this comment previously said the opposite, which is how
	// an analysis of this path reached a wrong conclusion about token lifetime
	// before the code was measured.
	SealForCommit(ctx context.Context, command SealCommand) (Lease, error)

	BeginInvocation(ctx context.Context, command BeginInvocationCommand) (run.Snapshot, error)
	CompleteInvocation(ctx context.Context, command CompleteInvocationCommand) (run.Snapshot, error)

	EnterApproval(ctx context.Context, command EnterApprovalCommand) (run.Snapshot, error)
	// ResolveApproval and ResolveInvocation are control-plane commands: they
	// require no worker lease, because the worker that parked the Run may be
	// long gone by the time a human answers.
	ResolveApproval(ctx context.Context, command ResolveApprovalCommand) (run.Snapshot, error)
	ResolveInvocation(ctx context.Context, command ResolveInvocationCommand) (run.Snapshot, error)

	CommitNodeResult(ctx context.Context, command CommitNodeResultCommand) (run.Snapshot, error)
	CommitMemoryMutation(ctx context.Context, command CommitMemoryMutationCommand) (run.Snapshot, error)

	CancelTree(ctx context.Context, command CancelTreeCommand) (run.Snapshot, error)

	Get(ctx context.Context, id run.ID) (run.Snapshot, error)
	Events(ctx context.Context, query EventQuery) (EventPage, error)
	// NodeOutput returns what the node behind an OutputRef produced.
	//
	// A graph edge names its upstream by ref rather than by value because the
	// Runtime does not carry outputs in the Snapshot — they are unbounded, and
	// the Snapshot is written on every transition. Following the ref is
	// therefore a Store read, and it has to exist: a downstream node handed a
	// bare pointer has nothing to work from, which is how a writer received
	// {"from":"output-..."} and answered that it could not see the research.
	// An unknown ref is an error, never an empty output.
	NodeOutput(ctx context.Context, id run.ID, outputRef string) (json.RawMessage, error)
}

// PublishedResource is one immutable published version.
type PublishedResource struct {
	Ref resource.Ref
	// Payload is the converted, storage-version bytes. What was published is
	// what is stored; nothing reinterprets it on read.
	Payload []byte
	// Pending versions are approval-gated: out of the head and invisible to
	// every reader until approved.
	Pending     bool
	PublishedBy authorization.PrincipalRef
	PublishedAt time.Time
}

// ResourceQuery selects the active set of one Kind.
type ResourceQuery struct {
	Kind   resource.Kind
	Tenant string
}

// ResourceReader is the read path. It has no conversion and no publish entry
// point; see resource.Reader for why.
type ResourceReader interface {
	GetPublished(ctx context.Context, ref resource.Ref) (PublishedResource, error)
	ListActive(ctx context.Context, query ResourceQuery) ([]PublishedResource, error)
}

// ResourceDigestReader resolves a pinned digest back to the version that
// carries it.
//
// Deliberately beside ResourceReader rather than inside it. A pin records the
// bytes a Run was admitted under, not the number the version happened to be
// filed as, so a Run that outlives the process which started it holds a digest
// and nothing else — and looking that up by (name, version) would need the one
// thing a pin has never carried.
// Absence must be reported as run.CodeResourceNotFound. Callers resolving a
// pin have to tell "this digest was never published" from "the store did not
// answer", and the kind cannot carry that: an unclassified transport failure
// is wrapped as ErrorInternal/RetryNever, which is indistinguishable from a
// permanent absence. Getting it wrong in one direction strands a Run forever;
// in the other, a dropped connection condemns every Run in flight.
type ResourceDigestReader interface {
	GetByDigest(ctx context.Context, kind resource.Kind, name, digest string) (PublishedResource, error)
}

// PublishIntent is what a publish authorization decision is made about. It
// carries the Kind because publish rights are granted per Kind: Policy and
// Quota must be grantable independently of Definition, since publishing either
// can relax what Definition publishing is constrained by.
type PublishIntent struct {
	Kind resource.Kind
	Name string
}

// ResourceAuthorizer answers both authorization questions, kept apart on
// purpose — the right to run a definition must never imply the right to
// rewrite it.
type ResourceAuthorizer interface {
	AuthorizeUse(ctx context.Context, principal authorization.PrincipalRef, ref resource.Ref) error
	AuthorizePublish(ctx context.Context, principal authorization.PrincipalRef, intent PublishIntent) error
}

// ResourcePublisher is the single write path for every Kind. A test asserts no
// Kind has a second one.
type ResourcePublisher interface {
	Publish(ctx context.Context, command PublishResourceCommand) (PublishedResource, error)
}

// Clock is the Store's authoritative time. It is a port so that conformance
// tests can advance time deterministically instead of sleeping.
type Clock interface {
	Now() time.Time
}
