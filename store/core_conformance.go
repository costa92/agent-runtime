package store

import (
	"context"
	"encoding/json"
	"slices"
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
		// The rule set the Run agreed to, which is not the same thing as the
		// definition and graph above. A Store that dropped these would leave
		// the Run executing under whatever policy and quota happen to be
		// current when it next advances — silently, because an absent digest
		// reads as "give me the current set" rather than as an error.
		if snapshot.Pins != sampleCreate("run-1").Pins {
			t.Fatalf("pins = %+v, want the ones the Run was created with", snapshot.Pins)
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

		// An absent rule-set digest is not "no rules" — the governance lookup
		// reads it as "the current set", so accepting one here creates a Run
		// that will execute under rules nobody pinned, and report nothing.
		unpinnedPolicy := sampleCreate("run-unpinned-policy")
		unpinnedPolicy.Pins.PolicyDigest = ""
		if _, err := harness.Store.Create(ctx, unpinnedPolicy); run.KindOf(err) != run.ErrorInvalid {
			t.Errorf("a Run with no pinned policy set was accepted: %v", err)
		}

		unpinnedQuota := sampleCreate("run-unpinned-quota")
		unpinnedQuota.Pins.QuotaDigest = ""
		if _, err := harness.Store.Create(ctx, unpinnedQuota); run.KindOf(err) != run.ErrorInvalid {
			t.Errorf("a Run with no pinned quota set was accepted: %v", err)
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

	t.Run("SealedLeaseRenewsForItsOwner", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		if _, err := harness.Store.SealForCommit(ctx, SealCommand{
			RunID: "run-1", LeaseToken: claimed.Lease.Token,
		}); err != nil {
			t.Fatalf("seal: %v", err)
		}
		// Sealing marks a commit window; it does not stop the owner holding
		// the Run from extending the deadline. The engine seals on every node
		// commit and re-claims before the next node, and that re-claim is not
		// always granted, so a long-running node renews a lease that is
		// already sealed. Refusing it would cancel a render that outlives the
		// original lease.
		harness.Advance(30 * time.Second)
		renewed, err := harness.Store.Renew(ctx, RenewCommand{
			RunID: "run-1", LeaseToken: claimed.Lease.Token, LeaseFor: time.Minute,
		})
		if err != nil {
			t.Fatalf("the sealed owner could not renew: %v", err)
		}
		if !renewed.Deadline.After(claimed.Lease.Deadline) {
			t.Fatalf("renewed deadline %v did not extend %v", renewed.Deadline, claimed.Lease.Deadline)
		}

		// The seal still lets the lease die on the Store's clock. An expired
		// sealed lease is claimable exactly like an expired unsealed one: a
		// worker that stops renewing gives up ownership, sealed or not.
		harness.Advance(time.Hour)
		if _, _, err := harness.Store.Claim(ctx, ClaimCommand{RunID: "run-1", Owner: "worker-2", LeaseFor: time.Minute}); err != nil {
			t.Fatalf("takeover of an expired sealed lease: %v", err)
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

	t.Run("ParkingAnInvocationThroughTheCommitIsDurable", func(t *testing.T) {
		// The engine parks via CommitNodeResult, not CompleteInvocation: a
		// process that died mid-effect has no result to complete, only a
		// decision to record. That decision must be as durable as the event
		// that announces it — if the row still reads in_flight after the
		// commit, every later resolution is refused as a conflict against the
		// resurrected in-flight invocation, and waiting_resolution has no exit.
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		began, err := run.Reduce(claimed.Snapshot, run.Command{
			Kind: run.CommandInvokeTool, Reserve: run.Limits{ToolCalls: 1},
			InvocationID: "inv-1", Tool: "search_evidence",
		})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		snapshot, err := harness.Store.BeginInvocation(ctx, BeginInvocationCommand{
			Fence: fenceFor(claimed, claimed.Snapshot.Revision),
			Invocation: InvocationBegin{
				ID: "inv-1", Tool: "search_evidence",
				Reservation: BudgetReservation{ID: "res-1", Amount: run.Limits{ToolCalls: 1}},
			},
			Commit: CommitContext{Transition: began, Events: began.Events},
		})
		if err != nil {
			t.Fatalf("begin: %v", err)
		}

		parked, err := run.Reduce(snapshot, run.Command{Kind: run.CommandRecordUnknown, InvocationID: "inv-1"})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		if _, err := harness.Store.CommitNodeResult(ctx, CommitNodeResultCommand{
			Fence: fenceFor(claimed, snapshot.Revision), NodeName: "node-1",
			Commit: CommitContext{Transition: parked, Events: parked.Events},
		}); err != nil {
			t.Fatalf("commit park: %v", err)
		}

		readBack, err := harness.Store.Get(ctx, "run-1")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got := readBack.Invocations["inv-1"].Outcome; got != run.OutcomeUnknown {
			t.Fatalf("outcome after the commit = %s, want unknown; the park was not durable", got)
		}
		// Read back from a Run that has since parked, which is the case an
		// in-memory counter cannot serve: the session that observed this call is
		// gone. A per-Run tool ceiling counts from here, so a Store that drops
		// the tool key turns the ceiling back into a per-claim one without
		// anything failing.
		if got := readBack.Invocations["inv-1"].Tool; got != "search_evidence" {
			t.Fatalf("tool after the commit = %q, want search_evidence; "+
				"a per-Run call ceiling cannot count what the ledger forgot", got)
		}

		// The parked invocation must now be resolvable, which is the whole
		// point of parking. The first decision is what the ledger acts on, and
		// a store that still shows in_flight refuses it as a conflict.
		resolved, err := run.Reduce(readBack, run.Command{
			Kind: run.CommandResolveInvocation, InvocationID: "inv-1", Outcome: run.OutcomeApplied,
		})
		if err != nil {
			t.Fatalf("reduce resolve: %v", err)
		}
		if _, err := harness.Store.ResolveInvocation(ctx, ResolveInvocationCommand{
			Fence:    ResolutionFence{RunID: "run-1", ExpectedRevision: readBack.Revision, TargetID: "inv-1", RequestedBy: samplePrincipal()},
			Decision: InvocationResolution{ID: "inv-1", Outcome: run.OutcomeApplied, Reason: "verified"},
			Budget:   BudgetSettlement{ReservationID: "res-1", Release: true},
			Commit:   CommitContext{Transition: resolved, Events: resolved.Events},
		}); err != nil {
			t.Fatalf("resolve: %v", err)
		}
	})

	t.Run("NodeProgressSurvivesAReload", func(t *testing.T) {
		// Node progress is the graph's only memory of what already ran: the
		// scheduler skips a node that appears in Snapshot.Nodes and picks
		// everything else. A Store that drops the map hands the next Advance a
		// Run that looks untouched, so the first node is selected again, and
		// again — a multi-node Definition never reaches its second node and
		// burns its whole budget on its first. That is not a degraded result,
		// it is an infinite loop, and it is invisible to any suite that keeps
		// the Snapshot in memory.
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		// What the engine commits when the graph advanced but the Run's own
		// state did not (session.transitionFor's no-command branch).
		next := claimed.Snapshot
		next.Revision = claimed.Snapshot.Revision + 1
		next.Nodes = map[string]run.NodeState{
			"planner": {Status: run.StateSucceeded, Attempts: 1, OutputRef: "output-1"},
		}
		if _, err := harness.Store.CommitNodeResult(ctx, CommitNodeResultCommand{
			Fence: fenceFor(claimed, claimed.Snapshot.Revision), NodeName: "planner",
			OutputRef: "output-1",
			Commit:    CommitContext{Transition: run.Transition{Next: next}},
		}); err != nil {
			t.Fatalf("commit node: %v", err)
		}

		readBack, err := harness.Store.Get(ctx, "run-1")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		state, ok := readBack.Nodes["planner"]
		if !ok {
			t.Fatal("the committed node is absent after a reload; the scheduler will run it again")
		}
		if state.Status != run.StateSucceeded {
			t.Fatalf("node status = %s, want succeeded", state.Status)
		}
		if state.OutputRef != "output-1" {
			t.Fatalf("node output ref = %q, want output-1; a downstream node reads its input from this", state.OutputRef)
		}
		if state.Attempts != 1 {
			t.Fatalf("node attempts = %d, want 1", state.Attempts)
		}
	})

	t.Run("ANodesOutputIsReadableByItsRef", func(t *testing.T) {
		// A graph edge hands the downstream node its upstream's ref, not the
		// value: outputs are unbounded and the Snapshot is rewritten on every
		// transition, so the Snapshot cannot carry them. Following the ref is
		// this read. Without it the engine passes a bare pointer along, and the
		// node on the other end has nothing to work from — a writer handed
		// {"from":"output-..."} answers that it cannot see the research, and
		// the assembler at the end of the graph fails for want of a body.
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		produced := json.RawMessage(`{"content":"## 大纲\n1. 起因"}`)
		fact, err := NewProjectionFact(AssistantMessagePayload{
			Output: produced, OutputRef: "output-1", AgentKey: "planner", NodeID: "planner",
		})
		if err != nil {
			t.Fatalf("fact: %v", err)
		}
		next := claimed.Snapshot
		next.Revision = claimed.Snapshot.Revision + 1
		next.Nodes = map[string]run.NodeState{
			"planner": {Status: run.StateSucceeded, Attempts: 1, OutputRef: "output-1"},
		}
		if _, err := harness.Store.CommitNodeResult(ctx, CommitNodeResultCommand{
			Fence: fenceFor(claimed, claimed.Snapshot.Revision), NodeName: "planner",
			OutputRef: "output-1",
			Commit:    CommitContext{Transition: run.Transition{Next: next}, Projections: []ProjectionFact{fact}},
		}); err != nil {
			t.Fatalf("commit node: %v", err)
		}

		output, err := harness.Store.NodeOutput(ctx, "run-1", "output-1")
		if err != nil {
			t.Fatalf("node output: %v; the downstream node has nothing to read", err)
		}
		// Compared by value, not bytes: a JSON column round-trips whitespace.
		var got, want map[string]any
		if err := json.Unmarshal(output, &got); err != nil {
			t.Fatalf("decode output: %v", err)
		}
		if err := json.Unmarshal(produced, &want); err != nil {
			t.Fatalf("decode produced: %v", err)
		}
		if got["content"] != want["content"] {
			t.Fatalf("output content = %v, want %v", got["content"], want["content"])
		}

		// An unknown ref is an error. Returning nothing would look to the
		// downstream node exactly like an upstream that produced nothing, and
		// it would answer from an empty brief rather than fail.
		if _, err := harness.Store.NodeOutput(ctx, "run-1", "output-missing"); err == nil {
			t.Fatal("an unknown output ref read back as success")
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

	t.Run("CompletingAnInvocationPersistsItsOutcome", func(t *testing.T) {
		// A completed invocation must read back as its result, never as the
		// begin-time in_flight. The engine's complete() now carries the outcome
		// in its transition; this test exercises the store's half of the
		// contract by committing a transition that does not (a future engine
		// regression) and demanding the store still not resurrect in_flight
		// over applied. An in_flight read-back is exactly what the recovery
		// path mistakes for a crashed effect: an approval-resumed Run parks
		// itself in waiting_resolution over calls that already finished.
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

		// The transition as the engine used to produce it: budget settled,
		// invocation outcome untouched.
		stale := snapshot
		stale.Revision = snapshot.Revision + 1
		stale.Budget = snapshot.Budget.Settle(run.Limits{ToolCalls: 1}, run.Limits{ToolCalls: 1}, true)
		if _, err := harness.Store.CompleteInvocation(ctx, CompleteInvocationCommand{
			Fence:  fenceFor(claimed, snapshot.Revision),
			Result: InvocationResult{ID: "inv-1", Outcome: run.OutcomeApplied},
			Budget: BudgetSettlement{ReservationID: "res-1"},
			Commit: CommitContext{Transition: run.Transition{Next: stale}},
		}); err != nil {
			t.Fatalf("complete: %v", err)
		}

		readBack, err := harness.Store.Get(ctx, "run-1")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got := readBack.Invocations["inv-1"].Outcome; got != run.OutcomeApplied {
			t.Fatalf("outcome after complete = %s, want applied", got)
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
		fact, err := NewProjectionFact(PendingApprovalPayload{
			ApprovalID: "ap-1", Action: "publish",
		})
		if err != nil {
			t.Fatalf("fact: %v", err)
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
		bad := ProjectionFact{Kind: "invented", RunID: "run-1"}
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

	t.Run("ARunKeepsTheInputItWasCreatedOn", func(t *testing.T) {
		// A Run outlives the request that started it. A worker claiming it
		// later, or taking it over after a crash, has nothing but the stored
		// row — an input the Store dropped leaves that worker executing an
		// agent with no idea what it was asked.
		harness := newHarness(t)
		ctx := context.Background()

		create := sampleCreate("run-1")
		create.Input = json.RawMessage(`{"q":"why is the sky blue"}`)
		created, err := harness.Store.Create(ctx, create)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if string(created.Input) != string(create.Input) {
			t.Fatalf("input = %s, want %s", created.Input, create.Input)
		}

		// And it survives the commits that follow, because it is what every
		// later attempt re-executes against.
		claimed := mustStart(t, harness, "run-2")
		reloaded, err := harness.Store.Get(ctx, claimed.Snapshot.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if string(reloaded.Input) != string(sampleCreate("run-2").Input) {
			t.Fatalf("input after commits = %s", reloaded.Input)
		}
	})

	t.Run("ARunKeepsTheRestrictionsItWasStartedUnder", func(t *testing.T) {
		// Enforced on every later attempt, by workers that never saw the
		// request that asked for them. A restriction lost at the first takeover
		// means the retry does for real what the first attempt was only allowed
		// to prove it would do.
		harness := newHarness(t)
		ctx := context.Background()

		create := sampleCreate("run-1")
		create.Restrictions = run.Restrictions{
			ToolNarrowing: []string{"search"}, DenySideEffects: true,
			Labels: []string{"evidence_optional"},
		}
		created, err := harness.Store.Create(ctx, create)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if !created.Restrictions.DenySideEffects ||
			len(created.Restrictions.ToolNarrowing) != 1 {
			t.Fatalf("restrictions = %+v", created.Restrictions)
		}

		reloaded, err := harness.Store.Get(ctx, "run-1")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !reloaded.Restrictions.DenySideEffects {
			t.Fatal("a Run came back able to perform the side effects it gave up")
		}
		// The labels are what a policy condition on "label" reads, and the
		// worker that evaluates it is not the process that started the Run. A
		// store that dropped them would let a call the label was meant to
		// refuse go through, and the audit would record it as allowed.
		if !slices.Equal(reloaded.Restrictions.Labels, []string{"evidence_optional"}) {
			t.Fatalf("a Run came back without the labels it is judged by: %v",
				reloaded.Restrictions.Labels)
		}

		unrestricted, err := harness.Store.Create(ctx, sampleCreate("run-2"))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if unrestricted.Restrictions.DenySideEffects ||
			len(unrestricted.Restrictions.ToolNarrowing) != 0 ||
			len(unrestricted.Restrictions.Labels) != 0 {
			t.Fatalf("an unrestricted Run came back restricted: %+v",
				unrestricted.Restrictions)
		}
	})

	t.Run("AChildKeepsTheUpstreamRefsItWasCreatedWith", func(t *testing.T) {
		// A child that cannot see what its dependency produced executes on a
		// task saying "use the result above" with no result attached, and
		// answers anyway. The Store is the only place that fact can survive the
		// parent's own commit.
		harness := newHarness(t)
		ctx := context.Background()

		create := sampleCreate("run-1")
		create.Upstreams = map[string]string{"research": "out-research"}
		created, err := harness.Store.Create(ctx, create)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if created.Upstreams["research"] != "out-research" {
			t.Fatalf("upstreams = %v", created.Upstreams)
		}

		reloaded, err := harness.Store.Get(ctx, "run-1")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if reloaded.Upstreams["research"] != "out-research" {
			t.Fatalf("upstreams after reload = %v", reloaded.Upstreams)
		}

		// A Run with no dependencies is distinguishable from one whose
		// dependencies were lost on the way in.
		plain, err := harness.Store.Create(ctx, sampleCreate("run-2"))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		if len(plain.Upstreams) != 0 {
			t.Fatalf("a Run with no dependencies came back with %v", plain.Upstreams)
		}
	})

	t.Run("TheTurnThatCausedARunIsCommittedWithIt", func(t *testing.T) {
		// The host enqueues the user turn with Create because it cannot enqueue
		// it afterwards: a Run created and then a transcript written loses the
		// message on any crash in between, and the user is left looking at an
		// answer to a question that is not there.
		harness := newHarness(t)
		ctx := context.Background()

		turn, err := NewProjectionFact(UserTurnPayload{SessionID: 7, Text: "why"})
		if err != nil {
			t.Fatalf("fact: %v", err)
		}
		create := sampleCreate("run-1")
		create.Projections = []ProjectionFact{turn}
		if _, err := harness.Store.Create(ctx, create); err != nil {
			t.Fatalf("create: %v", err)
		}

		projections := harness.Projections()
		if len(projections) != 1 || projections[0].Kind != ProjectionUserTurn {
			t.Fatalf("projections = %+v; the turn was not committed with the Run", projections)
		}
		if projections[0].Sequence == 0 {
			t.Fatal("the turn was stored with no sequence")
		}

		// A refused Create must leave nothing behind, or the host's tables
		// would carry a question for a Run that does not exist.
		if _, err := harness.Store.Create(ctx, create); err == nil {
			t.Fatal("a duplicate Run was created")
		}
		if len(harness.Projections()) != 1 {
			t.Fatal("a refused Create still enqueued its turn")
		}
	})

	t.Run("TheStoreAssignsADistinctSequenceToEveryFact", func(t *testing.T) {
		// The producer leaves Sequence zero: it cannot know the next value
		// without reading the outbox, and the Store is already holding the
		// Run's row lock. If the Store did not assign, two facts in one commit
		// would share a key and the outbox's conflict clause would drop the
		// second one — silently, which is the whole problem.
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		transition, err := run.Reduce(claimed.Snapshot, run.Command{Kind: run.CommandWaitApproval})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		first, err := NewProjectionFact(ProgressPayload{NodeID: "a", AgentKey: "writer"})
		if err != nil {
			t.Fatalf("fact: %v", err)
		}
		second, err := NewProjectionFact(ProgressPayload{NodeID: "b", AgentKey: "writer"})
		if err != nil {
			t.Fatalf("fact: %v", err)
		}

		if _, err := harness.Store.EnterApproval(ctx, EnterApprovalCommand{
			Fence: fenceFor(claimed, claimed.Snapshot.Revision), ApprovalID: "ap-1",
			Commit: CommitContext{
				Transition: transition, Events: transition.Events,
				Projections: []ProjectionFact{first, second},
			},
		}); err != nil {
			t.Fatalf("enter approval: %v", err)
		}

		projections := harness.Projections()
		if len(projections) != 2 {
			t.Fatalf("%d facts survived the commit; two were enqueued", len(projections))
		}
		seen := map[uint64]bool{}
		for _, fact := range projections {
			if fact.Sequence == 0 {
				t.Fatalf("%s was stored with no sequence", fact.Kind)
			}
			if seen[fact.Sequence] {
				t.Fatalf("sequence %d was assigned twice", fact.Sequence)
			}
			seen[fact.Sequence] = true
		}
	})

	t.Run("EventNodeIDRoundTrips", func(t *testing.T) {
		// The engine stamps NodeID on events at commitNode (engine.go's
		// stampNode), after Reduce returns and before the Store persists them —
		// Reduce is pure over the Snapshot and knows no current node. This test
		// reproduces that stamping and checks the Store carries the field back,
		// which is what lets an operator find "which node" without replaying
		// the whole run. A run-level event (no node executing) must round-trip
		// as empty, not as some sentinel.
		harness := newHarness(t)
		ctx := context.Background()
		claimed := mustStart(t, harness, "run-1")

		transition, err := run.Reduce(claimed.Snapshot, run.Command{
			Kind: run.CommandInvokeModel, Reserve: run.Limits{Tokens: 10},
		})
		if err != nil {
			t.Fatalf("reduce: %v", err)
		}
		for i := range transition.Events {
			transition.Events[i].NodeID = "planner"
		}
		if _, err := harness.Store.BeginInvocation(ctx, BeginInvocationCommand{
			Fence:      fenceFor(claimed, claimed.Snapshot.Revision),
			Invocation: InvocationBegin{ID: "inv-1", Reservation: BudgetReservation{ID: "res-1"}},
			Commit:     CommitContext{Transition: transition, Events: transition.Events},
		}); err != nil {
			t.Fatalf("begin: %v", err)
		}

		page, err := harness.Store.Events(ctx, EventQuery{RunID: "run-1"})
		if err != nil {
			t.Fatalf("events: %v", err)
		}
		var sawNode, sawEmpty bool
		for _, event := range page.Events {
			switch event.NodeID {
			case "planner":
				sawNode = true
			case "":
				sawEmpty = true
			default:
				t.Fatalf("unexpected node id %q", event.NodeID)
			}
		}
		if !sawNode {
			t.Fatal("the stamped node id did not survive the round trip")
		}
		if !sawEmpty {
			t.Fatal("the run's own start event should carry no node, but every event came back with one")
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
}

func sampleCreate(id run.ID) CreateCommand {
	return CreateCommand{
		ID:         id,
		Definition: run.DefinitionRef{ID: "writer", Version: 1, Protocol: 1},
		Graph:      run.ExecutionGraphRef{ID: "writer", Version: 1, Protocol: 1, Digest: "sha-1"},
		Principal:  samplePrincipal(),
		Budget:     run.Budget{Envelope: run.Limits{LLMCalls: 100, Tokens: 100000, ToolCalls: 100}},
		// Every sample Run carries Pins, for the same reason it carries an
		// input: a Store that drops them should fail the whole suite, not one
		// case. Until this was here the suite was blind to the field — four
		// adapters carried it through correctly and nothing said they had to.
		Pins: run.Pins{
			PolicyDigest: "policy-sha-1",
			QuotaDigest:  "quota-sha-1",
			Trace:        run.TraceContext{TraceID: "trace-1", SpanID: "span-1", Sampled: true},
		},
		// Every sample Run carries an input, so a Store that drops it fails
		// the whole suite rather than only the one case that looks for it.
		Input: json.RawMessage(`{"task":"sample"}`),
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
