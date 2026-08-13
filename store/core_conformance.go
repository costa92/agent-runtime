package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// CoreHarness is what an adapter supplies to be checked.
//
// The suite needs more than the Execution port: lease expiry is
// Store-authoritative, so it must be able to move the Store's clock rather than
// sleep, and all-or-nothing child creation can only be observed by making one
// child fail. Both are properties of the adapter, so the adapter provides them.
type CoreHarness struct {
	Store Execution
	// Advance moves the Store's authoritative clock forward.
	Advance func(time.Duration)
	// NewWithChildFailure returns an equivalent Store that fails the index-th
	// child creation, plus a reader for the children it did create.
	NewWithChildFailure func(index int) (Execution, func(root run.ID) []run.Snapshot)
	// Projections returns the durable outbox.
	Projections func() []ProjectionFact
	// Reservation reports whether a budget reservation is still outstanding.
	Reservation func(id run.ID) (BudgetReservation, bool)
}

// CoreStoreConformance is the reusable suite every Execution adapter must pass.
//
// It lives in the Runtime rather than in each adapter's tests so that "what the
// Store must guarantee" has one definition. An adapter that passes a suite it
// wrote itself has only proved it agrees with itself.
func CoreStoreConformance(t *testing.T, newHarness func(t *testing.T) CoreHarness) {
	t.Helper()

	t.Run("CreateIsUniqueAndPinsImmutableRefs", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		snapshot, err := harness.Store.Create(ctx, sampleCreate("run-1"))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if snapshot.State != run.StateQueued {
			t.Fatalf("state=%s want=queued", snapshot.State)
		}
		if snapshot.Graph.Digest == "" || snapshot.Definition.Version == 0 {
			t.Fatal("a Run was created without a pinned definition and graph")
		}

		if _, err := harness.Store.Create(ctx, sampleCreate("run-1")); run.KindOf(err) != run.ErrorConflict {
			t.Fatalf("duplicate create error=%s want=conflict", run.KindOf(err))
		}
	})

	t.Run("CreateRejectsMutableRefsAndHalfLinkedChildren", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		mutable := sampleCreate("run-mutable")
		mutable.Definition.Version = 0
		if _, err := harness.Store.Create(ctx, mutable); run.KindOf(err) != run.ErrorInvalid {
			t.Errorf("a version-zero definition ref was accepted: %v", err)
		}

		noDigest := sampleCreate("run-nodigest")
		noDigest.Graph.Digest = ""
		if _, err := harness.Store.Create(ctx, noDigest); run.KindOf(err) != run.ErrorInvalid {
			t.Errorf("a graph ref with no digest was accepted: %v", err)
		}

		halfLinked := sampleCreate("run-half")
		halfLinked.RootID = "root"
		if _, err := harness.Store.Create(ctx, halfLinked); run.KindOf(err) != run.ErrorInvalid {
			t.Errorf("a child with a root but no parent was accepted: %v", err)
		}
	})

	t.Run("EventSequenceIsContiguousAndMonotonic", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		snapshot := claimed.Snapshot
		for range 3 {
			transition, err := run.Reduce(snapshot, run.Command{
				Kind: run.CommandInvokeModel, Reserve: run.Limits{Tokens: 10},
			})
			if err != nil {
				t.Fatalf("reduce: %v", err)
			}
			snapshot, err = harness.Store.BeginInvocation(ctx, BeginInvocationCommand{
				Fence:      fenceFor(claimed, snapshot.Revision),
				Invocation: InvocationBegin{ID: run.ID("inv-" + snapshot.State), Reservation: BudgetReservation{ID: run.ID("res-" + itoa(int(snapshot.Revision)))}},
				Commit:     CommitContext{Transition: transition, Events: transition.Events},
			})
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
		}

		page, err := harness.Store.Events(ctx, EventQuery{RunID: "run-1"})
		if err != nil {
			t.Fatalf("events: %v", err)
		}
		if len(page.Events) == 0 {
			t.Fatal("no events were recorded")
		}
		for i, event := range page.Events {
			if event.Sequence != uint64(i+1) {
				t.Fatalf("event %d has sequence %d; the sequence is not contiguous", i, event.Sequence)
			}
		}
	})

	t.Run("StaleRevisionIsRefused", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		transition, err := run.Reduce(claimed.Snapshot, run.Command{Kind: run.CommandWaitApproval})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		stale := fenceFor(claimed, claimed.Snapshot.Revision-1)
		_, err = harness.Store.EnterApproval(ctx, EnterApprovalCommand{
			Fence: stale, ApprovalID: "ap-1",
			Commit: CommitContext{Transition: transition, Events: transition.Events},
		})
		if run.KindOf(err) != run.ErrorConflict {
			t.Fatalf("stale revision error=%s want=conflict", run.KindOf(err))
		}
	})

	t.Run("WrongAndExpiredLeasesAreRefusedOnStoreTime", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		transition, err := run.Reduce(claimed.Snapshot, run.Command{Kind: run.CommandWaitApproval})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}

		wrong := fenceFor(claimed, claimed.Snapshot.Revision)
		wrong.LeaseToken = "not-my-token"
		if _, err := harness.Store.EnterApproval(ctx, EnterApprovalCommand{
			Fence: wrong, ApprovalID: "ap-1",
			Commit: CommitContext{Transition: transition, Events: transition.Events},
		}); run.KindOf(err) != run.ErrorConflict {
			t.Errorf("a foreign lease token was accepted: %v", err)
		}

		// The Store's clock, not the caller's: a worker that judged expiry
		// itself would hand ownership over whenever the two disagreed.
		harness.Advance(time.Hour)
		if _, err := harness.Store.EnterApproval(ctx, EnterApprovalCommand{
			Fence: fenceFor(claimed, claimed.Snapshot.Revision), ApprovalID: "ap-1",
			Commit: CommitContext{Transition: transition, Events: transition.Events},
		}); run.KindOf(err) != run.ErrorConflict {
			t.Errorf("an expired lease was accepted: %v", err)
		}
	})

	t.Run("TakeoverLeavesOneWriter", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		first := mustStart(t, harness, "run-1")

		harness.Advance(time.Hour)
		second, _, err := harness.Store.Claim(ctx, ClaimCommand{RunID: "run-1", Owner: "worker-2", LeaseFor: time.Minute})
		if err != nil {
			t.Fatalf("takeover: %v", err)
		}
		if second.Token == first.Lease.Token {
			t.Fatal("takeover reissued the same token")
		}

		if _, err := harness.Store.Renew(ctx, RenewCommand{
			RunID: "run-1", LeaseToken: first.Lease.Token, LeaseFor: time.Minute,
		}); run.KindOf(err) != run.ErrorConflict {
			t.Errorf("the superseded worker could still renew: %v", err)
		}
	})

	t.Run("SealClosesTheRenewWindow", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		if _, err := harness.Store.SealForCommit(ctx, SealCommand{
			RunID: "run-1", LeaseToken: claimed.Lease.Token,
		}); err != nil {
			t.Fatalf("seal: %v", err)
		}
		if _, err := harness.Store.Renew(ctx, RenewCommand{
			RunID: "run-1", LeaseToken: claimed.Lease.Token, LeaseFor: time.Minute,
		}); run.KindOf(err) != run.ErrorConflict {
			t.Errorf("a sealed lease could still be renewed: %v", err)
		}
	})

	t.Run("ClaimBatchIsDeterministicAndCapsOneRootsShare", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		// One fanned-out root plus one lonely Run. Without the quantum the root
		// fills the batch and the other Run never runs, which looks like a hang.
		root := sampleCreate("root-1")
		if _, err := harness.Store.Create(ctx, root); err != nil {
			t.Fatalf("create root: %v", err)
		}
		for _, id := range []run.ID{"child-1", "child-2", "child-3"} {
			child := sampleCreate(id)
			child.RootID = "root-1"
			child.ParentID = "root-1"
			if _, err := harness.Store.Create(ctx, child); err != nil {
				t.Fatalf("create %s: %v", id, err)
			}
		}
		if _, err := harness.Store.Create(ctx, sampleCreate("other-1")); err != nil {
			t.Fatalf("create other: %v", err)
		}

		claimed, err := harness.Store.ClaimBatch(ctx, ClaimBatchCommand{
			Owner: "worker-1", Limit: 3, LeaseFor: time.Minute, RootQuantum: 2,
		})
		if err != nil {
			t.Fatalf("claim batch: %v", err)
		}
		if len(claimed) == 0 {
			t.Fatal("claim batch returned nothing")
		}

		perRoot := map[run.ID]int{}
		var sawOther bool
		for _, item := range claimed {
			if item.Lease.Token == "" {
				t.Fatal("claim batch returned an unleased Run")
			}
			root := item.Snapshot.RootID
			if root == "" {
				root = item.Snapshot.ID
			}
			perRoot[root]++
			if item.Snapshot.ID == "other-1" {
				sawOther = true
			}
		}
		if perRoot["root-1"] > 2 {
			t.Fatalf("one root took %d of the batch, over its quantum of 2", perRoot["root-1"])
		}
		if !sawOther {
			t.Fatal("the lonely Run was starved by the fanned-out root")
		}
	})

	t.Run("ChildGenerationIsAllOrNothing", func(t *testing.T) {
		newHarness(t) // establish the harness contract even if unused below
		failing, children := mustChildFailureHarness(t, newHarness)
		ctx := context.Background()

		claimed := mustStartOn(t, failing, "root-1")
		transition, err := run.Reduce(claimed.Snapshot, run.Command{
			Kind:   run.CommandCreateChildren,
			Slices: map[run.ID]run.Limits{"child-1": {Tokens: 10}, "child-2": {Tokens: 10}},
		})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}

		childCommands := []CreateCommand{childCreate("child-1", "root-1"), childCreate("child-2", "root-1")}
		_, err = failing.CreateChildren(ctx, CreateChildrenCommand{
			Fence:    fenceFor(claimed, claimed.Snapshot.Revision),
			Children: childCommands,
			Links: []LinkFact{
				{ParentID: "root-1", ChildID: "child-1"},
				{ParentID: "root-1", ChildID: "child-2"},
			},
			Commit: CommitContext{Transition: transition, Events: transition.Events},
		})
		if err == nil {
			t.Fatal("the injected failure did not surface")
		}
		if created := children("root-1"); len(created) != 0 {
			t.Fatalf("a partial generation survived: %d children", len(created))
		}
	})

	t.Run("CancelTreeFencesInFlightWork", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		if _, err := harness.Store.CancelTree(ctx, CancelTreeCommand{
			RootID: "run-1", RequestedBy: samplePrincipal(),
		}); err != nil {
			t.Fatalf("cancel: %v", err)
		}

		// The worker was mid-step and knows nothing about the cancellation. Its
		// commit must fail on the epoch, not succeed and resurrect the Run.
		transition, err := run.Reduce(claimed.Snapshot, run.Command{Kind: run.CommandWaitApproval})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		if _, err := harness.Store.EnterApproval(ctx, EnterApprovalCommand{
			Fence: fenceFor(claimed, claimed.Snapshot.Revision), ApprovalID: "ap-1",
			Commit: CommitContext{Transition: transition, Events: transition.Events},
		}); run.KindOf(err) == "" {
			t.Fatal("a commit from before the cancellation was accepted")
		}

		if _, err := harness.Store.Renew(ctx, RenewCommand{
			RunID: "run-1", LeaseToken: claimed.Lease.Token, LeaseFor: time.Minute,
		}); err == nil {
			t.Error("a lease survived the cancellation")
		}

		snapshot, err := harness.Store.Get(ctx, "run-1")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if snapshot.State != run.StateCancelled {
			t.Fatalf("state=%s want=cancelled", snapshot.State)
		}
	})

	t.Run("ControlPlaneResolutionNeedsNoLease", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		parked, err := run.Reduce(claimed.Snapshot, run.Command{Kind: run.CommandWaitApproval})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		snapshot, err := harness.Store.EnterApproval(ctx, EnterApprovalCommand{
			Fence: fenceFor(claimed, claimed.Snapshot.Revision), ApprovalID: "ap-1",
			Commit: CommitContext{Transition: parked, Events: parked.Events},
		})
		if err != nil {
			t.Fatalf("enter approval: %v", err)
		}

		// The worker is gone: its lease has expired and nobody re-claimed. A
		// human answering now must still be able to.
		harness.Advance(time.Hour)

		resumed, err := run.Reduce(snapshot, run.Command{Kind: run.CommandResume})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		if _, err := harness.Store.ResolveApproval(ctx, ResolveApprovalCommand{
			Fence: ResolutionFence{
				RunID: "run-1", ExpectedRevision: snapshot.Revision,
				TargetID: "ap-1", RequestedBy: samplePrincipal(),
			},
			Decision: ApprovalDecision{ID: "ap-1", Approved: true},
			Commit:   CommitContext{Transition: resumed, Events: resumed.Events},
		}); err != nil {
			t.Fatalf("resolve without a lease: %v", err)
		}
	})

	t.Run("ControlPlaneResolutionChecksStateTargetAndRevision", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		parked, err := run.Reduce(claimed.Snapshot, run.Command{Kind: run.CommandWaitApproval})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		snapshot, err := harness.Store.EnterApproval(ctx, EnterApprovalCommand{
			Fence: fenceFor(claimed, claimed.Snapshot.Revision), ApprovalID: "ap-1",
			Commit: CommitContext{Transition: parked, Events: parked.Events},
		})
		if err != nil {
			t.Fatalf("enter approval: %v", err)
		}
		resumed, err := run.Reduce(snapshot, run.Command{Kind: run.CommandResume})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}

		cases := map[string]ResolutionFence{
			"stale revision": {
				RunID: "run-1", ExpectedRevision: snapshot.Revision - 1,
				TargetID: "ap-1", RequestedBy: samplePrincipal(),
			},
			"cancelled root": {
				RunID: "run-1", ExpectedRevision: snapshot.Revision,
				ExpectedRootCancellationEpoch: 99,
				TargetID:                      "ap-1", RequestedBy: samplePrincipal(),
			},
		}
		for name, fence := range cases {
			if _, err := harness.Store.ResolveApproval(ctx, ResolveApprovalCommand{
				Fence: fence, Decision: ApprovalDecision{ID: "ap-1"},
				Commit: CommitContext{Transition: resumed, Events: resumed.Events},
			}); err == nil {
				t.Errorf("%s: accepted", name)
			}
		}

		// A decision naming a different target than the fence is a caller bug
		// that would otherwise resolve the wrong approval.
		if _, err := harness.Store.ResolveApproval(ctx, ResolveApprovalCommand{
			Fence: ResolutionFence{
				RunID: "run-1", ExpectedRevision: snapshot.Revision,
				TargetID: "ap-1", RequestedBy: samplePrincipal(),
			},
			Decision: ApprovalDecision{ID: "ap-2"},
			Commit:   CommitContext{Transition: resumed, Events: resumed.Events},
		}); run.KindOf(err) != run.ErrorInvalid {
			t.Errorf("a mismatched target was accepted: %v", err)
		}
	})

	t.Run("UnknownReservationsStayOutstandingUntilReconciled", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		began, err := run.Reduce(claimed.Snapshot, run.Command{
			Kind: run.CommandInvokeTool, Reserve: run.Limits{ToolCalls: 1},
			InvocationID: "inv-1",
		})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		snapshot, err := harness.Store.BeginInvocation(ctx, BeginInvocationCommand{
			Fence:      fenceFor(claimed, claimed.Snapshot.Revision),
			Invocation: InvocationBegin{ID: "inv-1", Reservation: BudgetReservation{ID: "res-1", Amount: run.Limits{ToolCalls: 1}}},
			Commit:     CommitContext{Transition: began, Events: began.Events},
		})
		if err != nil {
			t.Fatalf("begin: %v", err)
		}

		parked, err := run.Reduce(snapshot, run.Command{Kind: run.CommandRecordUnknown, InvocationID: "inv-1"})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		if _, err := harness.Store.CompleteInvocation(ctx, CompleteInvocationCommand{
			Fence:  fenceFor(claimed, snapshot.Revision),
			Result: InvocationResult{ID: "inv-1", Outcome: run.OutcomeUnknown},
			Budget: BudgetSettlement{ReservationID: "res-1"},
			Commit: CommitContext{Transition: parked, Events: parked.Events},
		}); err != nil {
			t.Fatalf("complete: %v", err)
		}

		if _, outstanding := harness.Reservation("res-1"); !outstanding {
			t.Fatal("an unknown reservation was released; the capacity could now be spent twice")
		}
	})

	t.Run("ReleasingAnUnknownReservationIsRefused", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		if _, err := harness.Store.CompleteInvocation(ctx, CompleteInvocationCommand{
			Fence:  fenceFor(claimed, claimed.Snapshot.Revision),
			Result: InvocationResult{ID: "inv-1", Outcome: run.OutcomeUnknown},
			Budget: BudgetSettlement{ReservationID: "res-1", Release: true},
		}); run.KindOf(err) != run.ErrorInvalid {
			t.Fatalf("error=%s want=invalid", run.KindOf(err))
		}
	})

	t.Run("ProjectionsCommitWithTheStateThatProducedThem", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		transition, err := run.Reduce(claimed.Snapshot, run.Command{Kind: run.CommandWaitApproval})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		fact := ProjectionFact{
			Kind: ProjectionPendingApproval, RunID: "run-1",
			Sequence: transition.Events[0].Sequence,
		}
		if _, err := harness.Store.EnterApproval(ctx, EnterApprovalCommand{
			Fence: fenceFor(claimed, claimed.Snapshot.Revision), ApprovalID: "ap-1",
			Commit: CommitContext{Transition: transition, Events: transition.Events, Projections: []ProjectionFact{fact}},
		}); err != nil {
			t.Fatalf("enter approval: %v", err)
		}

		projections := harness.Projections()
		if len(projections) != 1 || projections[0].Kind != ProjectionPendingApproval {
			t.Fatalf("projection not committed with the state: %+v", projections)
		}

		// A rejected command must leave no projection behind, or the host's
		// tables would describe a Run that never advanced.
		bad := ProjectionFact{Kind: "invented", RunID: "run-1", Sequence: 9}
		before := len(harness.Projections())
		if _, err := harness.Store.EnterApproval(ctx, EnterApprovalCommand{
			Fence: fenceFor(claimed, 999), ApprovalID: "ap-2",
			Commit: CommitContext{Transition: transition, Projections: []ProjectionFact{bad}},
		}); err == nil {
			t.Fatal("a stale command was accepted")
		}
		if len(harness.Projections()) != before {
			t.Fatal("a rejected command still wrote a projection")
		}
	})

	t.Run("ConcurrentClaimsYieldOneOwner", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		if _, err := harness.Store.Create(ctx, sampleCreate("run-1")); err != nil {
			t.Fatalf("create: %v", err)
		}

		// The whole point of a lease is that this race has exactly one winner.
		const racers = 16
		var wg sync.WaitGroup
		tokens := make(chan string, racers)
		for i := range racers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lease, _, err := harness.Store.Claim(ctx, ClaimCommand{
					RunID: "run-1", Owner: "worker-" + itoa(i), LeaseFor: time.Minute,
				})
				if err == nil {
					tokens <- lease.Token
				}
			}()
		}
		wg.Wait()
		close(tokens)

		var winners int
		for range tokens {
			winners++
		}
		if winners != 1 {
			t.Fatalf("%d workers claimed the same Run", winners)
		}
	})

	t.Run("ConcurrentChildReservationsRespectTheRootEnvelope", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "root-1")

		// Fifty children, each affordable alone. Only the root sees the sum,
		// which is why the check is there and not on the parent's remainder.
		const children = 50
		var wg sync.WaitGroup
		accepted := make(chan run.ID, children)
		for i := range children {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id := run.ID("child-" + itoa(i))
				snapshot, err := harness.Store.Get(ctx, "root-1")
				if err != nil {
					return
				}
				transition, err := run.Reduce(snapshot, run.Command{
					Kind:   run.CommandCreateChildren,
					Slices: map[run.ID]run.Limits{id: {Tokens: 5000}},
				})
				if err != nil {
					return
				}
				if _, err := harness.Store.CreateChildren(ctx, CreateChildrenCommand{
					Fence:    fenceFor(claimed, snapshot.Revision),
					Children: []CreateCommand{childCreate(id, "root-1")},
					Links:    []LinkFact{{ParentID: "root-1", ChildID: id}},
					Reservations: []BudgetReservation{
						{ID: run.ID("res-" + itoa(i)), Amount: run.Limits{Tokens: 5000}},
					},
					Commit: CommitContext{Transition: transition, Events: transition.Events},
				}); err == nil {
					accepted <- id
				}
			}()
		}
		wg.Wait()
		close(accepted)

		final, err := harness.Store.Get(ctx, "root-1")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		var granted run.Limits
		for _, slice := range final.Budget.Slices {
			granted = granted.Add(slice)
		}
		if granted.Tokens > final.Budget.Envelope.Tokens {
			t.Fatalf("granted %d tokens against an envelope of %d; concurrent children overran the root",
				granted.Tokens, final.Budget.Envelope.Tokens)
		}
	})

	t.Run("SynthesisIsAtMostOnce", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		transition, err := run.Reduce(claimed.Snapshot, run.Command{Kind: run.CommandInvokeModel})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		snapshot, err := harness.Store.CommitSynthesis(ctx, CommitSynthesisCommand{
			Fence: fenceFor(claimed, claimed.Snapshot.Revision), OutputRef: "out-1",
			Commit: CommitContext{Transition: transition, Events: transition.Events},
		})
		if err != nil {
			t.Fatalf("first synthesis: %v", err)
		}

		second, err := run.Reduce(snapshot, run.Command{Kind: run.CommandInvokeModel})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		if _, err := harness.Store.CommitSynthesis(ctx, CommitSynthesisCommand{
			Fence: fenceFor(claimed, snapshot.Revision), OutputRef: "out-2",
			Commit: CommitContext{Transition: second, Events: second.Events},
		}); run.KindOf(err) != run.ErrorConflict {
			t.Fatalf("a second synthesis error=%s want=conflict", run.KindOf(err))
		}
	})
}

// --- fixtures ---

func sampleCreate(id run.ID) CreateCommand {
	return CreateCommand{
		ID:         id,
		Definition: run.DefinitionRef{ID: "writer", Version: 1, Protocol: 1},
		Graph:      run.ExecutionGraphRef{ID: "writer", Version: 1, Protocol: 1, Digest: "sha-1"},
		Principal:  samplePrincipal(),
		Budget:     run.Budget{Envelope: run.Limits{LLMCalls: 100, Tokens: 100000, ToolCalls: 100}},
	}
}

func childCreate(id, root run.ID) CreateCommand {
	command := sampleCreate(id)
	command.RootID = root
	command.ParentID = root
	return command
}

func samplePrincipal() authorization.PrincipalRef {
	return authorization.PrincipalRef{Subject: "u-1", Tenant: "t-1", Kind: authorization.PrincipalUser}
}

func fenceFor(claimed ClaimedRun, revision uint64) ExecutionFence {
	return ExecutionFence{
		RunID:            claimed.Snapshot.ID,
		ExpectedRevision: revision,
		LeaseToken:       claimed.Lease.Token,
	}
}

// mustStart creates, claims and starts a Run, returning it running.
func mustStart(t *testing.T, harness CoreHarness, id run.ID) ClaimedRun {
	t.Helper()
	return mustStartOn(t, harness.Store, id)
}

func mustStartOn(t *testing.T, execution Execution, id run.ID) ClaimedRun {
	t.Helper()
	ctx := context.Background()

	if _, err := execution.Create(ctx, sampleCreate(id)); err != nil {
		t.Fatalf("create: %v", err)
	}
	lease, snapshot, err := execution.Claim(ctx, ClaimCommand{RunID: id, Owner: "worker-1", LeaseFor: time.Minute})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	transition, err := run.Reduce(snapshot, run.Command{Kind: run.CommandStart})
	if err != nil {
		t.Fatalf("reduce start: %v", err)
	}
	started, err := execution.CommitNodeResult(ctx, CommitNodeResultCommand{
		Fence:    ExecutionFence{RunID: id, ExpectedRevision: snapshot.Revision, LeaseToken: lease.Token},
		NodeName: "start",
		Commit:   CommitContext{Transition: transition, Events: transition.Events},
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	return ClaimedRun{Lease: lease, Snapshot: started}
}

func mustChildFailureHarness(t *testing.T, newHarness func(t *testing.T) CoreHarness) (Execution, func(run.ID) []run.Snapshot) {
	t.Helper()
	harness := newHarness(t)
	if harness.NewWithChildFailure == nil {
		t.Skip("adapter cannot inject a child-creation failure")
	}
	return harness.NewWithChildFailure(1)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
