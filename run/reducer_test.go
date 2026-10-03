package run

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
)

func runningSnapshot() Snapshot {
	return Snapshot{
		ID:     "run-1",
		State:  StateRunning,
		Budget: Budget{Envelope: Limits{LLMCalls: 10, Tokens: 10000, ToolCalls: 10}},
	}
}

func TestReduceStateTransitions(t *testing.T) {
	cases := []struct {
		from    State
		command CommandKind
		want    State
		wantErr ErrorKind
	}{
		{StateQueued, CommandStart, StateRunning, ""},
		{StateRunning, CommandWaitApproval, StateWaitingApproval, ""},
		{StateWaitingApproval, CommandResume, StateRunning, ""},
		{StateRunning, CommandSucceed, StateSucceeded, ""},
		{StateRunning, CommandFail, StateFailed, ""},
		{StateRunning, CommandCancel, StateCancelled, ""},
		{StateQueued, CommandCancel, StateCancelled, ""},
		{StateWaitingApproval, CommandCancel, StateCancelled, ""},

		// Illegal: the state is preserved and the error names why.
		{StateSucceeded, CommandResume, StateSucceeded, ErrorInvalid},
		{StateQueued, CommandResume, StateQueued, ErrorInvalid},
		{StateRunning, CommandStart, StateRunning, ErrorInvalid},
		{StateWaitingApproval, CommandStart, StateWaitingApproval, ErrorInvalid},
	}

	for _, tc := range cases {
		snapshot := Snapshot{ID: "run-1", State: tc.from}
		got, err := Reduce(snapshot, Command{Kind: tc.command})
		if got.Next.State != tc.want {
			t.Errorf("%s/%s state=%s want=%s", tc.from, tc.command, got.Next.State, tc.want)
		}
		if KindOf(err) != tc.wantErr {
			t.Errorf("%s/%s error=%s want=%s", tc.from, tc.command, KindOf(err), tc.wantErr)
		}
	}
}

// Resuming clears the pending approval. The Run was parked on one decision and
// resumed because that decision was made; the ID must not linger into the
// resumed Run, where the granted-write path keys on it being empty.
func TestResumeClearsPendingApproval(t *testing.T) {
	snapshot := Snapshot{ID: "run-1", State: StateWaitingApproval, PendingApprovalID: "ap-1"}
	got, err := Reduce(snapshot, Command{Kind: CommandResume})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got.Next.State != StateRunning {
		t.Fatalf("state=%s want=running", got.Next.State)
	}
	if got.Next.PendingApprovalID != "" {
		t.Fatalf("pending_approval_id=%q; an approval the human already decided is still marked pending", got.Next.PendingApprovalID)
	}
}

func TestSettleInvocationOwnsBudgetOutcomeAndUnknownParking(t *testing.T) {
	snapshot := runningSnapshot()
	snapshot.Invocations = map[ID]Invocation{"call-1": {
		ID: "call-1", Outcome: OutcomeInFlight, Reserved: Limits{LLMCalls: 1, Tokens: 100},
	}}
	snapshot.Budget.Reserved = Limits{LLMCalls: 1, Tokens: 100}
	checkpoint := json.RawMessage(`{"protocol":2,"applied_write":{"node":"writer","invocation":"call-1"}}`)
	applied, err := Reduce(snapshot, Command{
		Kind: CommandSettleInvocation, InvocationID: "call-1", Outcome: OutcomeApplied,
		Usage: Limits{LLMCalls: 1, Tokens: 9}, ReplaceCheckpoint: true, Checkpoint: checkpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	if applied.Next.Invocations["call-1"].Outcome != OutcomeApplied || applied.Next.Budget.Used.Tokens != 9 || applied.Next.Budget.Reserved.Tokens != 0 {
		t.Fatalf("applied settlement = %+v", applied.Next)
	}
	if string(applied.Next.Checkpoint) != string(checkpoint) || applied.Next.Revision != snapshot.Revision+1 {
		t.Fatalf("checkpoint/revision = %s/%d", applied.Next.Checkpoint, applied.Next.Revision)
	}
	if snapshot.Invocations["call-1"].Outcome != OutcomeInFlight {
		t.Fatal("reducer mutated its input invocation map")
	}

	unknown, err := Reduce(snapshot, Command{
		Kind: CommandSettleInvocation, InvocationID: "call-1", Outcome: OutcomeUnknown,
		Usage: Limits{LLMCalls: 1, Tokens: 9},
	})
	if err != nil {
		t.Fatal(err)
	}
	if unknown.Next.State != StateWaitingResolution || unknown.Next.Budget.Reserved.Tokens != 100 ||
		unknown.Next.Budget.Used.Tokens != 9 || unknown.Next.Invocations["call-1"].Outcome != OutcomeUnknown {
		t.Fatalf("unknown settlement = %+v", unknown.Next)
	}
	if len(unknown.Events) != 2 || unknown.Events[1].Kind != EventInvocationParked {
		t.Fatalf("unknown events = %+v", unknown.Events)
	}
}

func TestUnknownInvocationResolutionChargesOnlyUnbilledUsage(t *testing.T) {
	for _, tc := range []struct {
		name        string
		outcome     Outcome
		wantTokens  int
		wantCharged int
	}{
		{name: "applied", outcome: OutcomeApplied, wantTokens: 100, wantCharged: 100},
		{name: "not_applied", outcome: OutcomeNotApplied, wantTokens: 9, wantCharged: 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := runningSnapshot()
			snapshot.Invocations = map[ID]Invocation{"call-1": {
				ID: "call-1", Outcome: OutcomeInFlight, Reserved: Limits{LLMCalls: 1, Tokens: 100},
			}}
			snapshot.Budget.Reserved = Limits{LLMCalls: 1, Tokens: 100}
			unknown, err := Reduce(snapshot, Command{
				Kind: CommandSettleInvocation, InvocationID: "call-1", Outcome: OutcomeUnknown,
				Usage: Limits{LLMCalls: 1, Tokens: 9},
			})
			if err != nil {
				t.Fatal(err)
			}
			invocation := unknown.Next.Invocations["call-1"]
			if invocation.Charged.Tokens != 9 || invocation.Charged.LLMCalls != 1 {
				t.Fatalf("unknown charged = %+v", invocation.Charged)
			}
			charge := Limits{}
			if tc.outcome == OutcomeApplied {
				charge = invocation.RemainingCharge()
			}
			resolved, err := Reduce(unknown.Next, Command{
				Kind: CommandResolveInvocation, InvocationID: "call-1", Outcome: tc.outcome, Usage: charge,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := resolved.Next.Budget.Used.Tokens; got != tc.wantTokens {
				t.Errorf("used tokens = %d, want %d", got, tc.wantTokens)
			}
			if got := resolved.Next.Budget.Used.LLMCalls; got != 1 {
				t.Errorf("used model calls = %d, want 1", got)
			}
			if got := resolved.Next.Budget.Reserved.Tokens; got != 0 {
				t.Errorf("reserved tokens = %d, want 0", got)
			}
			if got := resolved.Next.Invocations["call-1"].Charged.Tokens; got != tc.wantCharged {
				t.Errorf("invocation charged tokens = %d, want %d", got, tc.wantCharged)
			}
		})
	}
}

func TestNodeAndApprovalProgressAreReducerCommands(t *testing.T) {
	snapshot := runningSnapshot()
	nodes := map[string]NodeState{"writer": {Status: StateSucceeded, Attempts: 1}}
	advanced, err := Reduce(snapshot, Command{Kind: CommandAdvanceNodes, Nodes: nodes})
	if err != nil {
		t.Fatal(err)
	}
	if advanced.Next.State != StateRunning || advanced.Next.Nodes["writer"].Status != StateSucceeded || advanced.Next.Revision != 1 {
		t.Fatalf("node advance = %+v", advanced.Next)
	}
	nodes["writer"] = NodeState{Status: StateFailed}
	if advanced.Next.Nodes["writer"].Status != StateSucceeded {
		t.Fatal("reducer retained caller's mutable node map")
	}
	checkpoint := json.RawMessage(`{"protocol":2,"approval":{"tool":"publish_article"}}`)
	parked, err := Reduce(snapshot, Command{
		Kind: CommandWaitApproval, ApprovalID: "approval-1", ReplaceCheckpoint: true, Checkpoint: checkpoint,
	})
	if err != nil {
		t.Fatal(err)
	}
	if parked.Next.PendingApprovalID != "approval-1" || string(parked.Next.Checkpoint) != string(checkpoint) {
		t.Fatalf("parked approval = %+v", parked.Next)
	}
}

func TestAcceptedTransitionCarriesItsReplayCommand(t *testing.T) {
	snapshot := runningSnapshot()
	command := Command{Kind: CommandAdvanceNodes, Nodes: map[string]NodeState{"writer": {Status: StateSucceeded}}}
	transition, err := Reduce(snapshot, command)
	if err != nil {
		t.Fatal(err)
	}
	if transition.Command == nil || transition.Command.Kind != command.Kind ||
		transition.Command.Nodes["writer"].Status != StateSucceeded {
		t.Fatalf("missing command on transition: %+v", transition.Command)
	}
	command.Nodes["writer"] = NodeState{Status: StateFailed}
	if transition.Command.Nodes["writer"].Status != StateSucceeded {
		t.Fatal("replay command retained caller's mutable node map")
	}
}

// A terminal Run is a fact, not a state that happens to have no outgoing edges.
// Nothing reopens it — not a resume, not a retry, not another terminal command,
// and not a cancellation that arrived after the Run had already finished.
func TestTerminalStatesAreIrreversible(t *testing.T) {
	terminal := []State{StateSucceeded, StatePartial, StateFailed, StateCancelled}
	commands := []CommandKind{
		CommandStart, CommandResume, CommandCancel,
		CommandSucceed, CommandFail, CommandWaitApproval,
		CommandInvokeModel, CommandInvokeTool, CommandWriteMemory,
		CommandRecordUnknown, CommandResolveInvocation,
	}
	for _, state := range terminal {
		for _, kind := range commands {
			snapshot := Snapshot{ID: "run-1", State: state, Revision: 7}
			got, err := Reduce(snapshot, Command{Kind: kind})
			if KindOf(err) != ErrorInvalid {
				t.Errorf("%s/%s error=%s want=invalid", state, kind, KindOf(err))
			}
			if got.Next.State != state || got.Next.Revision != 7 {
				t.Errorf("%s/%s mutated the snapshot: %s rev=%d", state, kind, got.Next.State, got.Next.Revision)
			}
		}
	}
}

// Reserving before the effect is what makes the envelope a limit rather than a
// report. A command that reserves nothing has already spent the budget by the
// time anyone can refuse it.
func TestEffectCommandsReserveBudgetBeforeTheEffect(t *testing.T) {
	for _, kind := range []CommandKind{CommandInvokeModel, CommandInvokeTool, CommandWriteMemory} {
		snapshot := runningSnapshot()
		got, err := Reduce(snapshot, Command{Kind: kind, Reserve: Limits{LLMCalls: 1, Tokens: 100, ToolCalls: 1}})
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if got.Next.Budget.Reserved.Tokens != 100 {
			t.Errorf("%s reserved %d tokens, want 100", kind, got.Next.Budget.Reserved.Tokens)
		}
		if len(got.Effects) != 1 {
			t.Fatalf("%s produced %d effects, want 1", kind, len(got.Effects))
		}
	}
}

func TestEffectCommandOverTheEnvelopeIsDenied(t *testing.T) {
	snapshot := runningSnapshot()
	snapshot.Budget.Used = Limits{Tokens: 9950}

	got, err := Reduce(snapshot, Command{Kind: CommandInvokeModel, Reserve: Limits{Tokens: 100}})

	if KindOf(err) != ErrorDenied {
		t.Fatalf("error=%s want=denied", KindOf(err))
	}
	if got.Next.Budget.Reserved.Tokens != 0 {
		t.Errorf("a denied reservation still consumed the envelope: %+v", got.Next.Budget.Reserved)
	}
	if len(got.Effects) != 0 {
		t.Errorf("a denied command produced %d effects", len(got.Effects))
	}
}

// A child cannot be given what the root does not have. Checked against the root
// envelope rather than against the parent's remaining slice, because slices are
// handed out in parallel and each one looks affordable on its own.
func TestRecordUnknownParksTheRunInWaitingResolution(t *testing.T) {
	snapshot := runningSnapshot()
	snapshot.Invocations = map[ID]Invocation{"inv-1": {ID: "inv-1", Outcome: OutcomeInFlight}}

	got, err := Reduce(snapshot, Command{Kind: CommandRecordUnknown, InvocationID: "inv-1"})
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}

	if got.Next.State != StateWaitingResolution {
		t.Fatalf("state=%s want=waiting_resolution", got.Next.State)
	}
	if got.Next.Invocations["inv-1"].Outcome != OutcomeUnknown {
		t.Fatalf("invocation outcome=%s want=unknown", got.Next.Invocations["inv-1"].Outcome)
	}
	if len(got.Effects) != 1 || got.Effects[0].Kind != EffectUnknown {
		t.Fatalf("effects=%+v want one EffectUnknown", got.Effects)
	}
}

func TestRecordUnknownRejectsInvocationWithoutDurableBegin(t *testing.T) {
	snapshot := runningSnapshot()
	got, err := Reduce(snapshot, Command{Kind: CommandRecordUnknown, InvocationID: "phantom"})
	if CodeOf(err) != "invocation_not_in_flight" {
		t.Fatalf("error=%v, want invocation_not_in_flight", err)
	}
	if got.Next.State != StateRunning || len(got.Next.Invocations) != 0 {
		t.Fatalf("refused command changed Run: %+v", got.Next)
	}
}

func TestHistoricalWriteResolutionRecordsAppliedAndStopsRun(t *testing.T) {
	snapshot := runningSnapshot()
	snapshot.State = StateWaitingResolution
	snapshot.Invocations = map[ID]Invocation{
		"old-write": {ID: "old-write", Write: true, Outcome: OutcomeUnknown},
	}
	got, err := Reduce(snapshot, Command{Kind: CommandResolveInvocation, InvocationID: "old-write", Outcome: OutcomeApplied})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.Next.State != StateFailed || got.Next.Invocations["old-write"].Outcome != OutcomeApplied {
		t.Fatalf("historical write was not safely resolved: %+v", got.Next)
	}
}

// Blind retry is exactly the mistake this state exists to prevent: the side
// effect may already have happened, and nothing in the Run knows whether it did.
func TestWaitingResolutionRefusesEverythingButResolution(t *testing.T) {
	snapshot := runningSnapshot()
	snapshot.State = StateWaitingResolution
	snapshot.Invocations = map[ID]Invocation{"inv-1": {ID: "inv-1", Outcome: OutcomeUnknown}}

	for _, kind := range []CommandKind{CommandResume, CommandStart, CommandInvokeTool} {
		got, err := Reduce(snapshot, Command{Kind: kind})
		if KindOf(err) != ErrorInvalid {
			t.Errorf("%s error=%s want=invalid", kind, KindOf(err))
		}
		if got.Next.State != StateWaitingResolution {
			t.Errorf("%s left waiting_resolution as %s", kind, got.Next.State)
		}
	}

	got, err := Reduce(snapshot, Command{
		Kind: CommandResolveInvocation, InvocationID: "inv-1", Outcome: OutcomeApplied,
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got.Next.State != StateRunning {
		t.Fatalf("state=%s want=running", got.Next.State)
	}
}

// The resolver is a human or a host reconciler, and both retry. Replaying the
// same decision has to be free; replaying a different one has to be refused,
// because the second decision would silently overwrite the first.
func TestResolutionIsIdempotentButConflictsAreRefused(t *testing.T) {
	snapshot := runningSnapshot()
	snapshot.State = StateWaitingResolution
	snapshot.Invocations = map[ID]Invocation{"inv-1": {ID: "inv-1", Outcome: OutcomeUnknown}}

	resolve := Command{Kind: CommandResolveInvocation, InvocationID: "inv-1", Outcome: OutcomeApplied}
	first, err := Reduce(snapshot, resolve)
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}

	repeat, err := Reduce(first.Next, resolve)
	if err != nil {
		t.Fatalf("repeated identical resolution must be idempotent, got %v", err)
	}
	if repeat.Next.Revision != first.Next.Revision {
		t.Errorf("idempotent replay advanced the revision %d → %d", first.Next.Revision, repeat.Next.Revision)
	}
	if len(repeat.Events) != 0 {
		t.Errorf("idempotent replay emitted %d events", len(repeat.Events))
	}

	conflicting := Command{Kind: CommandResolveInvocation, InvocationID: "inv-1", Outcome: OutcomeNotApplied}
	if _, err := Reduce(first.Next, conflicting); KindOf(err) != ErrorConflict {
		t.Fatalf("conflicting resolution error=%s want=conflict", KindOf(err))
	}
}

func TestResolutionStillUnknownStaysParked(t *testing.T) {
	snapshot := runningSnapshot()
	snapshot.State = StateWaitingResolution
	snapshot.Invocations = map[ID]Invocation{"inv-1": {ID: "inv-1", Outcome: OutcomeUnknown}}

	got, err := Reduce(snapshot, Command{
		Kind: CommandResolveInvocation, InvocationID: "inv-1", Outcome: OutcomeStillUnknown,
	})
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}
	if got.Next.State != StateWaitingResolution {
		t.Fatalf("state=%s want=waiting_resolution", got.Next.State)
	}
	repeated, err := Reduce(got.Next, Command{Kind: CommandResolveInvocation, InvocationID: "inv-1", Outcome: OutcomeStillUnknown})
	if err != nil || repeated.Next.Revision != got.Next.Revision {
		t.Fatalf("repeat still_unknown: transition=%+v err=%v", repeated, err)
	}
	final, err := Reduce(got.Next, Command{Kind: CommandResolveInvocation, InvocationID: "inv-1", Outcome: OutcomeApplied})
	if err != nil || final.Next.State != StateRunning || final.Next.Invocations["inv-1"].Outcome != OutcomeApplied {
		t.Fatalf("final resolution: transition=%+v err=%v", final, err)
	}
}

func TestResolvingAnUnknownInvocationIDIsInvalid(t *testing.T) {
	snapshot := runningSnapshot()
	snapshot.State = StateWaitingResolution
	snapshot.Invocations = map[ID]Invocation{"inv-1": {ID: "inv-1", Outcome: OutcomeUnknown}}

	if _, err := Reduce(snapshot, Command{
		Kind: CommandResolveInvocation, InvocationID: "inv-absent", Outcome: OutcomeApplied,
	}); KindOf(err) != ErrorInvalid {
		t.Fatalf("error=%s want=invalid", KindOf(err))
	}
}

// Reduce is the only writer of Run state, so it must not also be a writer of
// its argument: the caller holds the pre-image for the CAS commit, and an
// in-place mutation would make the expected revision describe the new value.
func TestReduceDoesNotMutateItsInput(t *testing.T) {
	snapshot := runningSnapshot()
	snapshot.Invocations = map[ID]Invocation{"inv-1": {ID: "inv-1", Outcome: OutcomeUnknown}}
	before, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// A command that writes into the one map the Snapshot still carries. If
	// Reduce wrote through instead of copying, inv-2 would appear below.
	if _, err := Reduce(snapshot, Command{
		Kind:         CommandInvokeModel,
		Reserve:      Limits{LLMCalls: 1},
		InvocationID: "inv-2",
	}); err != nil {
		t.Fatalf("reduce: %v", err)
	}

	after, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(before) != string(after) {
		t.Fatalf("Reduce mutated its input:\nbefore %s\nafter  %s", before, after)
	}
}

func TestEveryTransitionAdvancesRevisionAndEventSequence(t *testing.T) {
	snapshot := Snapshot{ID: "run-1", State: StateQueued, Revision: 3, LastEventSequence: 9}

	got, err := Reduce(snapshot, Command{Kind: CommandStart})
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}

	if got.Next.Revision != 4 {
		t.Errorf("revision=%d want=4", got.Next.Revision)
	}
	if len(got.Events) != 1 || got.Events[0].Sequence != 10 {
		t.Fatalf("events=%+v want one at sequence 10", got.Events)
	}
	if got.Next.LastEventSequence != 10 {
		t.Errorf("last event sequence=%d want=10", got.Next.LastEventSequence)
	}
}

// The Snapshot is durable and is handed to hosts, transports and storage. A
// request-scoped credential that reaches it is written to disk and replayed
// into every later reader — so the type itself must be unable to carry one.
func TestSnapshotCarriesPrincipalRefButNoRequestScopedIdentity(t *testing.T) {
	forbidden := map[reflect.Type]string{
		reflect.TypeFor[authorization.PrincipalContext](): "request-scoped principal context",
	}
	forbiddenFieldNames := []string{"claims", "credential", "credentials", "secret", "token", "password", "apikey"}

	var sawPrincipalRef bool
	seen := map[reflect.Type]bool{}

	var walk func(t reflect.Type, path string)
	walk = func(typ reflect.Type, path string) {
		if reason, bad := forbidden[typ]; bad {
			t.Errorf("Snapshot reaches %s at %s: %s", typ, path, reason)
			return
		}
		if typ == reflect.TypeFor[authorization.PrincipalRef]() {
			sawPrincipalRef = true
		}
		if seen[typ] {
			return
		}
		seen[typ] = true

		switch typ.Kind() {
		case reflect.Struct:
			for i := range typ.NumField() {
				field := typ.Field(i)
				for _, banned := range forbiddenFieldNames {
					if strings.EqualFold(field.Name, banned) {
						t.Errorf("Snapshot carries a request-scoped secret at %s.%s", path, field.Name)
					}
				}
				walk(field.Type, path+"."+field.Name)
			}
		case reflect.Slice, reflect.Array, reflect.Ptr, reflect.Map:
			if typ.Kind() == reflect.Map {
				walk(typ.Key(), path+"[key]")
			}
			walk(typ.Elem(), path+"[]")
		}
	}
	walk(reflect.TypeFor[Snapshot](), "Snapshot")

	if !sawPrincipalRef {
		t.Error("Snapshot does not carry a PrincipalRef; the durable identity is missing")
	}
}

// A transition that emits two events advances the sequence twice. Sharing one
// number would make the second event invisible to a consumer keyed by sequence,
// and would break the contiguity that lets a consumer detect a gap at all.
func TestMultiEventTransitionsNumberEveryEvent(t *testing.T) {
	snapshot := runningSnapshot()
	snapshot.LastEventSequence = 4
	snapshot.Invocations = map[ID]Invocation{"inv-1": {ID: "inv-1", Outcome: OutcomeInFlight}}

	got, err := Reduce(snapshot, Command{Kind: CommandRecordUnknown, InvocationID: "inv-1"})
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}

	if len(got.Events) != 2 {
		t.Fatalf("events=%d want=2", len(got.Events))
	}
	if got.Events[0].Sequence != 5 || got.Events[1].Sequence != 6 {
		t.Fatalf("sequences=%d,%d want=5,6", got.Events[0].Sequence, got.Events[1].Sequence)
	}
	if got.Next.LastEventSequence != 6 {
		t.Fatalf("last sequence=%d want=6", got.Next.LastEventSequence)
	}
}

// Store adapters may apply their authoritative outcome to a reduced transition
// after the effect result is committed. That copy must not mutate the prior
// snapshot if a fence rejects the commit.
func TestWithInvocationOutcomeLeavesTheReceiverUntouched(t *testing.T) {
	snapshot := runningSnapshot()
	begun, err := Reduce(snapshot, Command{
		Kind: CommandInvokeTool, InvocationID: "inv-1", Tool: "publish",
		Reserve: Limits{ToolCalls: 1},
	})
	if err != nil {
		t.Fatalf("reduce: %v", err)
	}
	durable := begun.Next

	settled := durable.WithInvocationOutcome("inv-1", OutcomeApplied)

	if got := durable.Invocations["inv-1"].Outcome; got != OutcomeInFlight {
		t.Fatalf("receiver outcome=%s want=in_flight: the settlement wrote through the shared map", got)
	}
	if got := settled.Invocations["inv-1"].Outcome; got != OutcomeApplied {
		t.Fatalf("copy outcome=%s want=applied", got)
	}
}

func TestWithInvocationOutcomeIgnoresAnUnknownInvocation(t *testing.T) {
	snapshot := runningSnapshot()

	if got := snapshot.WithInvocationOutcome("absent", OutcomeApplied); len(got.Invocations) != 0 {
		t.Fatalf("invocations=%+v want none", got.Invocations)
	}
}

func TestCancelRejectsAnOlderRootEpoch(t *testing.T) {
	snapshot := runningSnapshot()
	snapshot.RootCancellationEpoch = 2
	if _, err := Reduce(snapshot, Command{Kind: CommandCancel, CancelEpoch: 1}); err == nil {
		t.Fatal("cancel accepted an older root epoch")
	}
	transition, err := Reduce(snapshot, Command{Kind: CommandCancel, CancelEpoch: 3})
	if err != nil {
		t.Fatal(err)
	}
	if transition.Next.RootCancellationEpoch != 3 || transition.Next.State != StateCancelled {
		t.Fatalf("cancel transition=%+v", transition.Next)
	}
}
