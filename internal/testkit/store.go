// Package testkit holds deterministic fixtures for the Runtime's own tests and
// for the reusable conformance suites.
//
// It is internal: it exposes fault injection and clock control that would be
// wrong in production code, and an adapter author should implement the Store
// ports rather than embedding this.
package testkit

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
)

// Clock is a manually advanced clock. Lease expiry is Store-authoritative, so
// the conformance suite has to be able to move the Store's time without
// sleeping — a suite that slept would be slow and would still be racy.
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

func NewClock() *Clock {
	return &Clock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type runRecord struct {
	snapshot       run.Snapshot
	lease          *store.Lease
	sealed         bool
	createdAt      time.Time
	nextRunnableAt time.Time
	approvals      map[run.ID]bool
}

// MemoryStore is an in-process Execution store with the same atomicity and
// fencing semantics a real adapter must have.
//
// It exists so the conformance suite can be developed and run against something
// before a database adapter is written, and so the suite itself is proven to
// detect the failures it claims to.
type MemoryStore struct {
	mu    sync.Mutex
	clock *Clock
	// Renews counts keep-alive extensions so a long Advance can be shown to
	// hold the lease rather than losing it to expiry.
	Renews atomic.Int64

	runs        map[run.ID]*runRecord
	events      map[run.ID][]run.Event
	projections []store.ProjectionFact
	// projectionSequence is the per-Run outbox counter. Per Run rather than
	// global, so a fact's sequence means the same thing to a projector reading
	// one Run as it does to the Store writing it.
	projectionSequence map[run.ID]uint64
	links              []store.LinkFact
	// reservations are held per root, because a child's spend is charged
	// against the root envelope rather than its own.
	reservations map[run.ID]store.BudgetReservation
	// syntheses enforces at-most-once: a second synthesis for one root is a
	// conflict, not an overwrite.
	syntheses map[run.ID]bool
	// nodeModelUsage records every CommitNodeResult's per-profile detail so
	// tests can assert the engine passed what the agent reported.
	nodeModelUsage map[run.ID][]run.ModelUsage
	// epochs is the root cancellation epoch, held on the root so a child does
	// not have to be re-read to learn the tree was cancelled.
	epochs map[run.ID]uint64

	tokenSeq int
	// failChildCreateAt makes CreateChildren fail partway, to prove the
	// generation is created all-or-nothing.
	failChildCreateAt int
}

// MemoryStoreOption configures fault injection.
type MemoryStoreOption func(*MemoryStore)

// FailChildCreateAt makes the index-th child creation fail.
func FailChildCreateAt(index int) MemoryStoreOption {
	return func(s *MemoryStore) { s.failChildCreateAt = index }
}

func NewMemoryStore(clock *Clock, options ...MemoryStoreOption) *MemoryStore {
	s := &MemoryStore{
		clock:              clock,
		runs:               make(map[run.ID]*runRecord),
		events:             make(map[run.ID][]run.Event),
		reservations:       make(map[run.ID]store.BudgetReservation),
		syntheses:          make(map[run.ID]bool),
		epochs:             make(map[run.ID]uint64),
		projectionSequence: make(map[run.ID]uint64),
		failChildCreateAt:  -1,
	}
	for _, option := range options {
		option(s)
	}
	return s
}

var _ store.Execution = (*MemoryStore)(nil)

// Children returns a root's children, for tests that assert all-or-nothing
// creation.
func (s *MemoryStore) Children(root run.ID) []run.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	var children []run.Snapshot
	for _, record := range s.runs {
		if record.snapshot.RootID == root && record.snapshot.ID != root {
			children = append(children, record.snapshot)
		}
	}
	sort.Slice(children, func(i, j int) bool { return children[i].ID < children[j].ID })
	return children
}

// Projections returns the durable outbox, for tests that assert a fact was
// committed with the state that produced it.
func (s *MemoryStore) Projections() []store.ProjectionFact {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.ProjectionFact(nil), s.projections...)
}

// Links returns the recorded parent/child edges.
func (s *MemoryStore) Links() []store.LinkFact {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]store.LinkFact(nil), s.links...)
}

// Reservation returns an outstanding budget reservation.
func (s *MemoryStore) Reservation(id run.ID) (store.BudgetReservation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reservation, ok := s.reservations[id]
	return reservation, ok
}

func (s *MemoryStore) Create(_ context.Context, command store.CreateCommand) (run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createLocked(command)
}

func (s *MemoryStore) createLocked(command store.CreateCommand) (run.Snapshot, error) {
	if _, exists := s.runs[command.ID]; exists {
		return run.Snapshot{}, run.NewError("duplicate_run", run.ErrorConflict, run.RetryNever,
			fmt.Errorf("run %s already exists", command.ID))
	}

	root := command.RootID
	if root == "" {
		root = command.ID
	}
	snapshot := run.Snapshot{
		ID:                    command.ID,
		Revision:              1,
		Definition:            command.Definition,
		Graph:                 command.Graph,
		State:                 run.StateQueued,
		RootID:                command.RootID,
		ParentID:              command.ParentID,
		Principal:             command.Principal,
		Pins:                  command.Pins,
		Budget:                command.Budget,
		Input:                 command.Input,
		Upstreams:             command.Upstreams,
		Restrictions:          command.Restrictions,
		RootCancellationEpoch: s.epochs[root],
	}
	// Enqueued with the Run, in the same critical section that creates it.
	for _, projection := range command.Projections {
		projection.RunID = command.ID
		projection.Sequence = s.projectionSequence[command.ID] + 1
		s.projectionSequence[command.ID] = projection.Sequence
		s.projections = append(s.projections, projection)
	}
	s.runs[command.ID] = &runRecord{
		snapshot:       snapshot,
		createdAt:      s.clock.Now(),
		nextRunnableAt: command.NextRunnableAt,
		approvals:      make(map[run.ID]bool),
	}
	return snapshot, nil
}

func (s *MemoryStore) Get(_ context.Context, id run.ID) (run.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.runs[id]
	if !ok {
		return run.Snapshot{}, run.NewError("unknown_run", run.ErrorInvalid, run.RetryNever)
	}
	return withPendingApproval(record.snapshot, record.approvals), nil
}

func (s *MemoryStore) Events(_ context.Context, query store.EventQuery) (store.EventPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var page store.EventPage
	for _, event := range s.events[query.RunID] {
		if event.Sequence <= query.After {
			continue
		}
		page.Events = append(page.Events, event)
		page.Next = event.Sequence
		if query.Limit > 0 && len(page.Events) >= query.Limit {
			break
		}
	}
	return page, nil
}

func (s *MemoryStore) Claim(_ context.Context, command store.ClaimCommand) (store.Lease, run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return store.Lease{}, run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.runs[command.RunID]
	if !ok {
		return store.Lease{}, run.Snapshot{}, run.NewError("unknown_run", run.ErrorInvalid, run.RetryNever)
	}
	if !s.claimableLocked(record) {
		return store.Lease{}, run.Snapshot{}, run.NewError("not_claimable", run.ErrorConflict, run.RetryBackoff)
	}
	lease := s.grantLeaseLocked(record, command.Owner, command.LeaseFor)
	return lease, record.snapshot, nil
}

func (s *MemoryStore) ClaimBatch(_ context.Context, command store.ClaimBatchCommand) ([]store.ClaimedRun, error) {
	if err := command.Validate(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	eligible := make([]*runRecord, 0, len(s.runs))
	for _, record := range s.runs {
		if s.claimableLocked(record) {
			eligible = append(eligible, record)
		}
	}
	// Deterministic eligibility: next_runnable_at, then created_at, then id.
	// Anything less makes "which Run ran first" depend on map iteration, and a
	// starved Run indistinguishable from an unlucky one.
	sort.Slice(eligible, func(i, j int) bool {
		if !eligible[i].nextRunnableAt.Equal(eligible[j].nextRunnableAt) {
			return eligible[i].nextRunnableAt.Before(eligible[j].nextRunnableAt)
		}
		if !eligible[i].createdAt.Equal(eligible[j].createdAt) {
			return eligible[i].createdAt.Before(eligible[j].createdAt)
		}
		return eligible[i].snapshot.ID < eligible[j].snapshot.ID
	})

	claimed := make([]store.ClaimedRun, 0, command.Limit)
	perRoot := map[run.ID]int{}
	for _, record := range eligible {
		if len(claimed) >= command.Limit {
			break
		}
		root := rootOf(record.snapshot)
		if command.RootQuantum > 0 && perRoot[root] >= command.RootQuantum {
			// One fanned-out root would otherwise fill every batch and starve
			// the rest of the deployment, which looks like a hang.
			continue
		}
		perRoot[root]++
		lease := s.grantLeaseLocked(record, command.Owner, command.LeaseFor)
		claimed = append(claimed, store.ClaimedRun{Lease: lease, Snapshot: record.snapshot})
	}
	return claimed, nil
}

func (s *MemoryStore) Renew(_ context.Context, command store.RenewCommand) (store.Lease, error) {
	if err := command.Validate(); err != nil {
		return store.Lease{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.runs[command.RunID]
	if !ok {
		return store.Lease{}, run.NewError("unknown_run", run.ErrorInvalid, run.RetryNever)
	}
	if err := s.ownsLocked(record, command.LeaseToken); err != nil {
		return store.Lease{}, err
	}
	// Sealed only blocks takeover (Claim). The owner must still be able to
	// extend the deadline: start() seals before the node runs, and a
	// picture-book render outlives the original 30s lease.
	if s.cancelledLocked(record) {
		return store.Lease{}, run.NewError("root_cancelled", run.ErrorInterrupted, run.RetryNever)
	}
	record.lease.Deadline = s.clock.Now().Add(command.LeaseFor)
	s.Renews.Add(1)
	return *record.lease, nil
}

func (s *MemoryStore) SealForCommit(_ context.Context, command store.SealCommand) (store.Lease, error) {
	if err := command.Validate(); err != nil {
		return store.Lease{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.runs[command.RunID]
	if !ok {
		return store.Lease{}, run.NewError("unknown_run", run.ErrorInvalid, run.RetryNever)
	}
	if err := s.ownsLocked(record, command.LeaseToken); err != nil {
		return store.Lease{}, err
	}
	if s.cancelledLocked(record) {
		return store.Lease{}, run.NewError("root_cancelled", run.ErrorInterrupted, run.RetryNever)
	}
	record.sealed = true
	return *record.lease, nil
}

func (s *MemoryStore) BeginInvocation(_ context.Context, command store.BeginInvocationCommand) (run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, err := s.checkExecutionFenceLocked(command.Fence)
	if err != nil {
		return run.Snapshot{}, err
	}
	if _, exists := s.reservations[command.Invocation.Reservation.ID]; exists {
		return run.Snapshot{}, run.NewError("duplicate_reservation", run.ErrorConflict, run.RetryNever)
	}
	s.reservations[command.Invocation.Reservation.ID] = command.Invocation.Reservation
	return s.commitLocked(record, command.Commit)
}

func (s *MemoryStore) CompleteInvocation(_ context.Context, command store.CompleteInvocationCommand) (run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, err := s.checkExecutionFenceLocked(command.Fence)
	if err != nil {
		return run.Snapshot{}, err
	}
	if err := s.settleLocked(command.Budget, command.Result.Outcome); err != nil {
		return run.Snapshot{}, err
	}
	commit := command.Commit
	// The same contract as the postgres implementation: a completed invocation
	// reads back as its result outcome, whatever the transition carried. The
	// engine now carries it too, but a transition that forgets must not make a
	// completed call read as in_flight — that is exactly what an approval-resumed
	// Run mistakes for a crashed effect and parks itself over.
	if existing, ok := commit.Transition.Next.Invocations[command.Result.ID]; ok {
		existing.Outcome = command.Result.Outcome
		commit.Transition.Next.Invocations[command.Result.ID] = existing
	}
	return s.commitLocked(record, commit)
}

func (s *MemoryStore) EnterApproval(_ context.Context, command store.EnterApprovalCommand) (run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, err := s.checkExecutionFenceLocked(command.Fence)
	if err != nil {
		return run.Snapshot{}, err
	}
	record.approvals[command.ApprovalID] = true
	return s.commitLocked(record, command.Commit)
}

func (s *MemoryStore) ResolveApproval(_ context.Context, command store.ResolveApprovalCommand) (run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, err := s.checkResolutionFenceLocked(command.Fence, run.StateWaitingApproval)
	if err != nil {
		return run.Snapshot{}, err
	}
	if !record.approvals[command.Decision.ID] {
		return run.Snapshot{}, run.NewError("unknown_approval", run.ErrorInvalid, run.RetryNever)
	}
	delete(record.approvals, command.Decision.ID)
	return s.commitLocked(record, command.Commit)
}

func (s *MemoryStore) ResolveInvocation(_ context.Context, command store.ResolveInvocationCommand) (run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, err := s.checkResolutionFenceLocked(command.Fence, run.StateWaitingResolution)
	if err != nil {
		return run.Snapshot{}, err
	}
	invocation, ok := record.snapshot.Invocations[command.Decision.ID]
	if !ok {
		return run.Snapshot{}, run.NewError("unknown_invocation", run.ErrorInvalid, run.RetryNever)
	}
	if invocation.Outcome != run.OutcomeUnknown && invocation.Outcome != command.Decision.Outcome {
		return run.Snapshot{}, run.NewError("resolution_conflict", run.ErrorConflict, run.RetryNever)
	}
	if err := s.settleLocked(command.Budget, command.Decision.Outcome); err != nil {
		return run.Snapshot{}, err
	}
	return s.commitLocked(record, command.Commit)
}

func (s *MemoryStore) CreateChildren(_ context.Context, command store.CreateChildrenCommand) (run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, err := s.checkExecutionFenceLocked(command.Fence)
	if err != nil {
		return run.Snapshot{}, err
	}

	// Staged, then applied. A partially created generation would leave the
	// parent waiting on children that do not exist, and nothing in the tree
	// could tell that apart from children that have not started.
	created := make([]run.ID, 0, len(command.Children))
	rollback := func() {
		for _, id := range created {
			delete(s.runs, id)
		}
	}
	for index, child := range command.Children {
		if index == s.failChildCreateAt {
			rollback()
			return run.Snapshot{}, run.NewError("injected_child_failure", run.ErrorInternal, run.RetryBackoff)
		}
		if _, createErr := s.createLocked(child); createErr != nil {
			rollback()
			return run.Snapshot{}, createErr
		}
		created = append(created, child.ID)
	}
	for _, reservation := range command.Reservations {
		if _, exists := s.reservations[reservation.ID]; exists {
			rollback()
			return run.Snapshot{}, run.NewError("duplicate_reservation", run.ErrorConflict, run.RetryNever)
		}
		s.reservations[reservation.ID] = reservation
	}
	s.links = append(s.links, command.Links...)
	return s.commitLocked(record, command.Commit)
}

func (s *MemoryStore) CommitNodeResult(_ context.Context, command store.CommitNodeResultCommand) (run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, err := s.checkExecutionFenceLocked(command.Fence)
	if err != nil {
		return run.Snapshot{}, err
	}
	if err := s.settleLocked(command.Budget, run.OutcomeApplied); err != nil {
		return run.Snapshot{}, err
	}
	if len(command.ModelUsage) > 0 {
		if s.nodeModelUsage == nil {
			s.nodeModelUsage = make(map[run.ID][]run.ModelUsage)
		}
		for _, usage := range command.ModelUsage {
			s.nodeModelUsage[record.snapshot.ID] = run.MergeModelUsage(
				s.nodeModelUsage[record.snapshot.ID], usage.Profile, usage.InputTokens, usage.OutputTokens)
		}
	}
	return s.commitLocked(record, command.Commit)
}

// NodeModelUsage returns the per-profile detail committed for a Run, so tests
// can assert the engine forwarded what the agent reported.
func (s *MemoryStore) NodeModelUsage(id run.ID) []run.ModelUsage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nodeModelUsage[id]
}

func (s *MemoryStore) CommitSynthesis(_ context.Context, command store.CommitSynthesisCommand) (run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, err := s.checkExecutionFenceLocked(command.Fence)
	if err != nil {
		return run.Snapshot{}, err
	}
	root := rootOf(record.snapshot)
	if s.syntheses[root] {
		// At-most-once. The first synthesis is what every downstream consumer
		// has already been told; a second would rewrite an answer that has
		// left the building.
		return run.Snapshot{}, run.NewError("synthesis_exists", run.ErrorConflict, run.RetryNever)
	}
	if err := s.settleLocked(command.Budget, run.OutcomeApplied); err != nil {
		return run.Snapshot{}, err
	}
	s.syntheses[root] = true
	return s.commitLocked(record, command.Commit)
}

func (s *MemoryStore) CommitMemoryMutation(_ context.Context, command store.CommitMemoryMutationCommand) (run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, err := s.checkExecutionFenceLocked(command.Fence)
	if err != nil {
		return run.Snapshot{}, err
	}
	if err := s.settleLocked(command.Budget, command.Invocation.Outcome); err != nil {
		return run.Snapshot{}, err
	}
	commit := command.Commit
	// Same contract as CompleteInvocation: the memory write's outcome is what
	// reads back, not whatever the transition carried.
	if existing, ok := commit.Transition.Next.Invocations[command.Invocation.ID]; ok {
		existing.Outcome = command.Invocation.Outcome
		commit.Transition.Next.Invocations[command.Invocation.ID] = existing
	}
	return s.commitLocked(record, commit)
}

func (s *MemoryStore) CancelTree(_ context.Context, command store.CancelTreeCommand) (run.Snapshot, error) {
	if err := command.Validate(); err != nil {
		return run.Snapshot{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.runs[command.RootID]
	if !ok {
		return run.Snapshot{}, run.NewError("unknown_run", run.ErrorInvalid, run.RetryNever)
	}
	if record.snapshot.RootID != "" && record.snapshot.RootID != command.RootID {
		return run.Snapshot{}, run.NewError("not_a_root", run.ErrorInvalid, run.RetryNever)
	}

	// The epoch is bumped first and it is what fences everything else. Every
	// claim, renew, seal and command re-reads it, so a child already in flight
	// cannot commit afterwards however large the tree is.
	s.epochs[command.RootID]++
	epoch := s.epochs[command.RootID]

	for _, candidate := range s.runs {
		if rootOf(candidate.snapshot) != command.RootID {
			continue
		}
		candidate.snapshot.RootCancellationEpoch = epoch
		candidate.lease = nil
		candidate.sealed = false
		if !candidate.snapshot.State.Terminal() {
			candidate.snapshot.Revision++
			candidate.snapshot.State = run.StateCancelled
		}
	}
	return s.runs[command.RootID].snapshot, nil
}

// --- internals ---

func rootOf(snapshot run.Snapshot) run.ID {
	if snapshot.RootID != "" {
		return snapshot.RootID
	}
	return snapshot.ID
}

func (s *MemoryStore) claimableLocked(record *runRecord) bool {
	if record.snapshot.State.Terminal() || record.snapshot.State == run.StateWaitingApproval ||
		record.snapshot.State == run.StateWaitingResolution {
		return false
	}
	if record.nextRunnableAt.After(s.clock.Now()) {
		return false
	}
	// An expired lease is available again: that is takeover, and it is the
	// only way a Run whose worker died ever runs again.
	return record.lease == nil || record.lease.Expired(s.clock.Now())
}

func (s *MemoryStore) grantLeaseLocked(record *runRecord, owner string, leaseFor time.Duration) store.Lease {
	s.tokenSeq++
	lease := store.Lease{
		RunID:    record.snapshot.ID,
		Token:    fmt.Sprintf("lease-%d", s.tokenSeq),
		Owner:    owner,
		Deadline: s.clock.Now().Add(leaseFor),
	}
	record.lease = &lease
	record.sealed = false
	return lease
}

func (s *MemoryStore) ownsLocked(record *runRecord, token string) error {
	if record.lease == nil || record.lease.Token != token {
		return run.NewError("lease_lost", run.ErrorConflict, run.RetryNever)
	}
	if record.lease.Expired(s.clock.Now()) {
		return run.NewError("lease_expired", run.ErrorConflict, run.RetryNever)
	}
	return nil
}

func (s *MemoryStore) cancelledLocked(record *runRecord) bool {
	return s.epochs[rootOf(record.snapshot)] != record.snapshot.RootCancellationEpoch
}

func (s *MemoryStore) checkExecutionFenceLocked(fence store.ExecutionFence) (*runRecord, error) {
	record, ok := s.runs[fence.RunID]
	if !ok {
		return nil, run.NewError("unknown_run", run.ErrorInvalid, run.RetryNever)
	}
	if err := s.ownsLocked(record, fence.LeaseToken); err != nil {
		return nil, err
	}
	if record.snapshot.Revision != fence.ExpectedRevision {
		return nil, run.NewError("revision_conflict", run.ErrorConflict, run.RetryNever,
			fmt.Errorf("expected %d, have %d", fence.ExpectedRevision, record.snapshot.Revision))
	}
	if s.epochs[rootOf(record.snapshot)] != fence.ExpectedRootCancellationEpoch {
		return nil, run.NewError("root_cancelled", run.ErrorInterrupted, run.RetryNever)
	}
	return record, nil
}

func (s *MemoryStore) checkResolutionFenceLocked(fence store.ResolutionFence, want run.State) (*runRecord, error) {
	record, ok := s.runs[fence.RunID]
	if !ok {
		return nil, run.NewError("unknown_run", run.ErrorInvalid, run.RetryNever)
	}
	if record.snapshot.State != want {
		// Resolution is only meaningful in the state that is waiting for it.
		// Accepting it elsewhere would let a control-plane call move a running
		// Run, which is the bypass the fence exists to prevent.
		return nil, run.NewError("not_waiting", run.ErrorInvalid, run.RetryNever)
	}
	if record.snapshot.Revision != fence.ExpectedRevision {
		return nil, run.NewError("revision_conflict", run.ErrorConflict, run.RetryNever)
	}
	if s.epochs[rootOf(record.snapshot)] != fence.ExpectedRootCancellationEpoch {
		return nil, run.NewError("root_cancelled", run.ErrorInterrupted, run.RetryNever)
	}
	return record, nil
}

// settleLocked closes a reservation. An unknown outcome keeps it outstanding:
// releasing it would let the same capacity be spent twice if the effect turns
// out to have happened.
func (s *MemoryStore) settleLocked(settlement store.BudgetSettlement, outcome run.Outcome) error {
	if settlement.ReservationID == "" {
		return nil
	}
	reservation, ok := s.reservations[settlement.ReservationID]
	if !ok {
		return run.NewError("unknown_reservation", run.ErrorInvalid, run.RetryNever)
	}
	if outcome == run.OutcomeUnknown || outcome == run.OutcomeStillUnknown {
		return nil
	}
	if settlement.Release {
		delete(s.reservations, settlement.ReservationID)
		return nil
	}
	reservation.Amount = settlement.Charged
	s.reservations[settlement.ReservationID] = reservation
	return nil
}

// commitLocked applies the transition, its events and its projections in one
// step. Nothing here interprets state: the reducer already decided, and a Store
// that re-derived the next state would be a second implementation of the state
// machine.
func (s *MemoryStore) commitLocked(record *runRecord, commit store.CommitContext) (run.Snapshot, error) {
	next := commit.Transition.Next
	if next.ID == "" {
		return run.Snapshot{}, run.NewError("empty_transition", run.ErrorInvalid, run.RetryNever)
	}

	last := lastSequence(s.events[record.snapshot.ID])
	for _, event := range commit.Events {
		if event.Sequence != last+1 {
			// A gap or a repeat would make a consumer unable to tell a missed
			// event from a delivered one.
			return run.Snapshot{}, run.NewError("non_contiguous_events", run.ErrorInternal, run.RetryNever,
				fmt.Errorf("expected %d, got %d", last+1, event.Sequence))
		}
		last = event.Sequence
	}
	for _, projection := range commit.Projections {
		if err := projection.Validate(); err != nil {
			return run.Snapshot{}, err
		}
	}
	// Sequences are assigned here, under the same lock as the commit, because
	// the producer cannot know the next one without reading the outbox.
	assigned := make([]store.ProjectionFact, 0, len(commit.Projections))
	for _, projection := range commit.Projections {
		projection.RunID = record.snapshot.ID
		projection.Sequence = s.projectionSequence[projection.RunID] + 1
		s.projectionSequence[projection.RunID] = projection.Sequence
		assigned = append(assigned, projection)
	}

	next.RootCancellationEpoch = record.snapshot.RootCancellationEpoch
	next.Invocations = maps.Clone(next.Invocations)
	record.snapshot = next
	s.events[record.snapshot.ID] = append(s.events[record.snapshot.ID], commit.Events...)
	s.projections = append(s.projections, assigned...)
	return record.snapshot, nil
}

func withPendingApproval(snapshot run.Snapshot, approvals map[run.ID]bool) run.Snapshot {
	snapshot.PendingApprovalID = ""
	if snapshot.State != run.StateWaitingApproval {
		return snapshot
	}
	for id := range approvals {
		snapshot.PendingApprovalID = id
		break
	}
	return snapshot
}

func lastSequence(events []run.Event) uint64 {
	if len(events) == 0 {
		return 0
	}
	return events[len(events)-1].Sequence
}

// Principal is a convenience for fixtures.
func Principal(subject string) authorization.PrincipalRef {
	return authorization.PrincipalRef{Subject: subject, Tenant: "t", Kind: authorization.PrincipalUser}
}

// OverwriteNodes replaces a Run's node map.
//
// Fault injection for the conformance and delegation tests: it produces a shape
// no commit path can currently build — a child with outputs on several nodes —
// so the guard against it can be exercised at all.
func (s *MemoryStore) OverwriteNodes(id run.ID, nodes map[string]run.NodeState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record, ok := s.runs[id]; ok {
		record.snapshot.Nodes = nodes
	}
}
