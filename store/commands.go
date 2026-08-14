package store

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// ExecutionFence is what a worker-driven command proves before it may write:
// that it is looking at the revision it thinks it is, that it still owns the
// lease, and that the root has not been cancelled since it started.
//
// All three are checked together because each alone is insufficient. The
// revision catches a stale read, the token catches a takeover, and the epoch
// catches work that was already in flight when the tree was cancelled.
type ExecutionFence struct {
	RunID                         run.ID
	ExpectedRevision              uint64
	LeaseToken                    string
	ExpectedRootCancellationEpoch uint64
}

func (f ExecutionFence) validate() error {
	if f.RunID == "" {
		return run.NewError("missing_run_id", run.ErrorInvalid, run.RetryNever)
	}
	if f.LeaseToken == "" {
		return run.NewError("missing_lease_token", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// ResolutionFence is the control-plane fence. It deliberately requires no lease
// token: the worker that parked the Run may have exited hours before a human
// answered, and requiring its lease would make approvals undeliverable. What it
// requires instead is the target's identity and a live authorization for the
// operator, checked at resolution time rather than at park time.
type ResolutionFence struct {
	RunID                         run.ID
	ExpectedRevision              uint64
	ExpectedRootCancellationEpoch uint64
	TargetID                      string
	RequestedBy                   authorization.PrincipalRef
}

func (f ResolutionFence) validate() error {
	if f.RunID == "" {
		return run.NewError("missing_run_id", run.ErrorInvalid, run.RetryNever)
	}
	if f.TargetID == "" {
		return run.NewError("missing_target", run.ErrorInvalid, run.RetryNever)
	}
	if f.RequestedBy.Zero() {
		return run.NewError("missing_principal", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// CreateCommand creates one Run with its refs pinned.
type CreateCommand struct {
	ID         run.ID
	Definition run.DefinitionRef
	Graph      run.ExecutionGraphRef
	Principal  authorization.PrincipalRef
	Budget     run.Budget
	// Pins are the rule-set versions and trace context the Run is created
	// with. They are set once and never updated: that is what makes a Run
	// reproducible against the rules that actually judged it.
	Pins run.Pins

	// RootID and ParentID are set for a delegated child.
	RootID   run.ID
	ParentID run.ID

	// NextRunnableAt orders claiming. Zero means immediately.
	NextRunnableAt time.Time

	// Input is what the Run executes on. Written once here and never updated:
	// the Store is the only thing that can make it durable, and every later
	// reader — a worker that claimed the Run, a takeover after a crash — has
	// nothing else to learn it from.
	Input json.RawMessage

	// Projections are enqueued in the same transaction that creates the Run.
	//
	// This is how a host records the turn that caused the Run — the one Kind
	// the engine cannot produce, because a Run carries no session and no user
	// text. Committing it here rather than afterwards is not a convenience: a
	// host that created the Run and then wrote the transcript loses the message
	// on any crash in between, and the user is left looking at an answer to a
	// question that is not there.
	Projections []ProjectionFact
}

func (c CreateCommand) Validate() error {
	if c.ID == "" {
		return run.NewError("missing_run_id", run.ErrorInvalid, run.RetryNever)
	}
	if c.Definition.ID == "" || c.Definition.Version == 0 {
		return run.NewError("mutable_definition_ref", run.ErrorInvalid, run.RetryNever)
	}
	if c.Graph.Digest == "" {
		// Without a digest, recovery would have to recompile the definition
		// against deployment-local state, and a Run could come back a
		// different shape than it started.
		return run.NewError("missing_graph_digest", run.ErrorInvalid, run.RetryNever)
	}
	if c.Principal.Zero() {
		return run.NewError("missing_principal", run.ErrorInvalid, run.RetryNever)
	}
	if (c.RootID == "") != (c.ParentID == "") {
		return run.NewError("half_linked_child", run.ErrorInvalid, run.RetryNever)
	}
	for _, projection := range c.Projections {
		if err := projection.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// ClaimCommand takes a lease on one specific Run.
type ClaimCommand struct {
	RunID run.ID
	Owner string
	// LeaseFor is how long the lease should hold. The Store adds it to its own
	// clock; the caller's clock never enters the decision.
	LeaseFor time.Duration
}

func (c ClaimCommand) Validate() error {
	if c.RunID == "" {
		return run.NewError("missing_run_id", run.ErrorInvalid, run.RetryNever)
	}
	if c.Owner == "" {
		return run.NewError("missing_owner", run.ErrorInvalid, run.RetryNever)
	}
	if c.LeaseFor <= 0 {
		return run.NewError("non_positive_lease", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// ClaimBatchCommand atomically leases up to Limit runnable Runs.
//
// RootQuantum caps how much of one batch a single root tree may take. Without
// it, one fanned-out root fills every batch and starves every other Run in the
// deployment — which looks like a hang, not like a busy queue.
type ClaimBatchCommand struct {
	Owner       string
	Limit       int
	LeaseFor    time.Duration
	RootQuantum int
}

func (c ClaimBatchCommand) Validate() error {
	if c.Owner == "" {
		return run.NewError("missing_owner", run.ErrorInvalid, run.RetryNever)
	}
	if c.Limit <= 0 {
		return run.NewError("non_positive_limit", run.ErrorInvalid, run.RetryNever)
	}
	if c.LeaseFor <= 0 {
		return run.NewError("non_positive_lease", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// RenewCommand extends a lease the caller still owns.
type RenewCommand struct {
	RunID      run.ID
	LeaseToken string
	LeaseFor   time.Duration
}

func (c RenewCommand) Validate() error {
	if c.RunID == "" || c.LeaseToken == "" {
		return run.NewError("missing_lease", run.ErrorInvalid, run.RetryNever)
	}
	if c.LeaseFor <= 0 {
		return run.NewError("non_positive_lease", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// SealCommand closes a lease to further renewal ahead of a commit.
type SealCommand struct {
	RunID      run.ID
	LeaseToken string
}

func (c SealCommand) Validate() error {
	if c.RunID == "" || c.LeaseToken == "" {
		return run.NewError("missing_lease", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// InvocationBegin is the record written before an external call leaves the
// Store, so that a crash in the window that follows leaves evidence rather than
// silence.
type InvocationBegin struct {
	ID             run.ID
	IdempotencyKey string
	Reservation    BudgetReservation
}

// InvocationResult is how one external call ended.
type InvocationResult struct {
	ID      run.ID
	Outcome run.Outcome
}

// InvocationResolution is a resolver's answer for an unknown Invocation.
type InvocationResolution struct {
	ID      run.ID
	Outcome run.Outcome
	// Reason is recorded for audit. Nothing branches on it.
	Reason string
}

// ApprovalDecision is a human's answer.
type ApprovalDecision struct {
	ID       run.ID
	Approved bool
	Reason   string
}

// BudgetReservation is capacity committed before a paid effect starts. The ID
// is stable so that settling charges exactly what was reserved, even when the
// settle arrives after a crash and a reload.
type BudgetReservation struct {
	ID     run.ID
	Amount run.Limits
}

// BudgetSettlement closes a reservation.
//
// Charged records what was actually consumed, including on failure — a failed
// call still cost tokens, and a ledger that only counts successes understates
// the spend it exists to bound. Release is the unused remainder, and it is only
// returned when the outcome is known: an unknown reservation stays unavailable
// until it is reconciled, because releasing it would let the budget be spent
// twice if the effect turns out to have happened.
type BudgetSettlement struct {
	ReservationID run.ID
	Charged       run.Limits
	Release       bool
}

// CommitContext is the shared tail of every state-changing command: the
// reducer's transition, the events to append with it, and the durable
// projections to enqueue. One struct rather than four repeated fields, so a new
// command cannot accidentally omit the projections.
type CommitContext struct {
	Transition  run.Transition
	Events      []run.Event
	Projections []ProjectionFact
}

type BeginInvocationCommand struct {
	Fence      ExecutionFence
	Invocation InvocationBegin
	Commit     CommitContext
}

func (c BeginInvocationCommand) Validate() error {
	if err := c.Fence.validate(); err != nil {
		return err
	}
	if c.Invocation.ID == "" {
		return run.NewError("missing_invocation_id", run.ErrorInvalid, run.RetryNever)
	}
	if c.Invocation.Reservation.ID == "" {
		// Beginning a paid effect with no reservation is the double-spend the
		// reserve-before-effect order exists to prevent.
		return run.NewError("missing_reservation", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

type CompleteInvocationCommand struct {
	Fence  ExecutionFence
	Result InvocationResult
	Usage  run.Limits
	Budget BudgetSettlement
	Commit CommitContext
}

func (c CompleteInvocationCommand) Validate() error {
	if err := c.Fence.validate(); err != nil {
		return err
	}
	if c.Result.ID == "" {
		return run.NewError("missing_invocation_id", run.ErrorInvalid, run.RetryNever)
	}
	if c.Budget.ReservationID == "" {
		return run.NewError("missing_reservation", run.ErrorInvalid, run.RetryNever)
	}
	if c.Result.Outcome == run.OutcomeUnknown && c.Budget.Release {
		return run.NewError("released_unknown_reservation", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("invocation %s", c.Result.ID))
	}
	return nil
}

type EnterApprovalCommand struct {
	Fence      ExecutionFence
	ApprovalID run.ID
	Commit     CommitContext
}

func (c EnterApprovalCommand) Validate() error {
	if err := c.Fence.validate(); err != nil {
		return err
	}
	if c.ApprovalID == "" {
		return run.NewError("missing_approval_id", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

type ResolveApprovalCommand struct {
	Fence    ResolutionFence
	Decision ApprovalDecision
	Commit   CommitContext
}

func (c ResolveApprovalCommand) Validate() error {
	if err := c.Fence.validate(); err != nil {
		return err
	}
	if c.Decision.ID == "" || string(c.Decision.ID) != c.Fence.TargetID {
		return run.NewError("target_mismatch", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

type ResolveInvocationCommand struct {
	Fence    ResolutionFence
	Decision InvocationResolution
	Usage    run.Limits
	Budget   BudgetSettlement
	Commit   CommitContext
}

func (c ResolveInvocationCommand) Validate() error {
	if err := c.Fence.validate(); err != nil {
		return err
	}
	if c.Decision.ID == "" || string(c.Decision.ID) != c.Fence.TargetID {
		return run.NewError("target_mismatch", run.ErrorInvalid, run.RetryNever)
	}
	if !c.Decision.Outcome.Resolvable() {
		return run.NewError("invalid_resolution", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("outcome %q", c.Decision.Outcome))
	}
	if c.Decision.Outcome == run.OutcomeStillUnknown && c.Budget.Release {
		return run.NewError("released_unknown_reservation", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// LinkFact records a parent/child edge durably, so that a tree can be walked
// after a crash without inferring structure from timing.
type LinkFact struct {
	ParentID run.ID
	ChildID  run.ID
	NodeName string
}

// CreateChildrenCommand creates a whole child generation or none of it.
//
// All-or-nothing because a half-created generation leaves the parent waiting on
// children that do not exist, and nothing in the tree can tell that apart from
// children that have not started.
type CreateChildrenCommand struct {
	Fence        ExecutionFence
	Children     []CreateCommand
	Links        []LinkFact
	Reservations []BudgetReservation
	Commit       CommitContext
}

func (c CreateChildrenCommand) Validate() error {
	if err := c.Fence.validate(); err != nil {
		return err
	}
	if len(c.Children) == 0 {
		return run.NewError("no_children", run.ErrorInvalid, run.RetryNever)
	}
	for _, child := range c.Children {
		if err := child.Validate(); err != nil {
			return err
		}
		if child.RootID == "" {
			return run.NewError("unrooted_child", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("child %s", child.ID))
		}
	}
	if len(c.Links) != len(c.Children) {
		return run.NewError("missing_links", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

type CommitNodeResultCommand struct {
	Fence     ExecutionFence
	NodeName  string
	OutputRef string
	Usage     run.Limits
	Budget    BudgetSettlement
	Commit    CommitContext
}

func (c CommitNodeResultCommand) Validate() error {
	if err := c.Fence.validate(); err != nil {
		return err
	}
	if c.NodeName == "" {
		return run.NewError("missing_node_name", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// CommitSynthesisCommand records the root's combined answer.
//
// It is at-most-once: a second synthesis for the same root is a conflict rather
// than an overwrite, because the first is what every downstream consumer has
// already been told.
type CommitSynthesisCommand struct {
	Fence     ExecutionFence
	OutputRef string
	Usage     run.Limits
	Budget    BudgetSettlement
	Commit    CommitContext
}

func (c CommitSynthesisCommand) Validate() error {
	if err := c.Fence.validate(); err != nil {
		return err
	}
	if c.OutputRef == "" {
		return run.NewError("missing_output_ref", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// MemoryMutationFact is a committed memory write.
type MemoryMutationFact struct {
	Key       string
	Namespace string
	Ref       string
}

type CommitMemoryMutationCommand struct {
	Fence      ExecutionFence
	Invocation InvocationResult
	Mutation   MemoryMutationFact
	Usage      run.Limits
	Budget     BudgetSettlement
	Commit     CommitContext
}

func (c CommitMemoryMutationCommand) Validate() error {
	if err := c.Fence.validate(); err != nil {
		return err
	}
	if c.Invocation.ID == "" {
		return run.NewError("missing_invocation_id", run.ErrorInvalid, run.RetryNever)
	}
	if c.Mutation.Key == "" || c.Mutation.Namespace == "" {
		return run.NewError("unscoped_mutation", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// CancelTreeCommand cancels a root and everything beneath it.
//
// It takes no lease: cancellation must work while a worker holds the Run, which
// is the case where it matters most.
type CancelTreeCommand struct {
	RootID      run.ID
	RequestedBy authorization.PrincipalRef
	Reason      string
}

func (c CancelTreeCommand) Validate() error {
	if c.RootID == "" {
		return run.NewError("missing_run_id", run.ErrorInvalid, run.RetryNever)
	}
	if c.RequestedBy.Zero() {
		return run.NewError("missing_principal", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}
