package run

import "maps"

// Reduce applies one Command to one Snapshot and returns what should happen.
//
// It is the sole writer of Run state and it is pure: it reads no clock, touches
// no store, performs no effect, and does not mutate its argument. The caller
// still holds the pre-image, which is what it commits the CAS against — an
// in-place mutation would make the expected revision describe the new value and
// every optimistic commit would succeed against itself.
//
// On refusal it returns the snapshot unchanged alongside the error, so that a
// caller which ignores the error still cannot advance the Run by accident.
func Reduce(snapshot Snapshot, command Command) (Transition, error) {
	refuse := func(err error) (Transition, error) {
		return Transition{Next: snapshot}, err
	}

	// Terminal first, and before anything else. A terminal Run is a fact; a
	// command arriving late must not be able to reopen it, whatever it is.
	if snapshot.State.Terminal() {
		return refuse(NewError("terminal_state", ErrorInvalid, RetryNever))
	}

	// waiting_resolution is the one waiting state that does not accept a
	// generic resume or retry: the side effect may already have happened, and
	// repeating it is the specific mistake this state exists to prevent.
	if snapshot.State == StateWaitingResolution && command.Kind != CommandResolveInvocation && command.Kind != CommandCancel {
		return refuse(NewError("resolution_required", ErrorInvalid, RetryReconcile))
	}

	switch command.Kind {
	case CommandStart:
		if snapshot.State != StateQueued {
			return refuse(NewError("not_queued", ErrorInvalid, RetryNever))
		}
		return transition(snapshot, StateRunning), nil

	case CommandResume:
		// Not from queued: a Run that never started has nothing to resume, and
		// accepting it here would make start optional.
		if !snapshot.State.Waiting() {
			return refuse(NewError("not_waiting", ErrorInvalid, RetryNever))
		}
		next := snapshot
		next.State = StateRunning
		// The Run was parked on one approval and resumed because that approval
		// was decided. The ID must not linger into the resumed Run: a consumer
		// keying on "no pending approval" (the granted-write path) would never
		// see the decision it just made.
		next.PendingApprovalID = ""
		// A resume may carry a replacement checkpoint — the approval resolver
		// uses it to mark a refused hold as denied. It is the only command that
		// rewrites the checkpoint, because every other resume is exactly the
		// state it parked in.
		if command.Checkpoint != nil {
			next.Checkpoint = command.Checkpoint
		}
		return emit(advance(snapshot, next), snapshot, stateEvent(snapshot, StateRunning)), nil

	case CommandRetry:
		if snapshot.State != StateWaitingRetry {
			return refuse(NewError("not_waiting_retry", ErrorInvalid, RetryNever))
		}
		return transition(snapshot, StateRunning), nil

	case CommandWaitApproval:
		return parkFromRunning(snapshot, StateWaitingApproval)
	case CommandWaitRetry:
		return parkFromRunning(snapshot, StateWaitingRetry)

	case CommandInvokeModel:
		return reserveEffect(snapshot, command, EffectModelCall)
	case CommandInvokeTool:
		return reserveEffect(snapshot, command, EffectToolCall)
	case CommandWriteMemory:
		return reserveEffect(snapshot, command, EffectMemoryWrite)

	case CommandRecordUnknown:
		return recordUnknown(snapshot, command)

	case CommandResolveInvocation:
		return resolveInvocation(snapshot, command)

	case CommandSucceed:
		return terminate(snapshot, StateSucceeded)
	case CommandPartial:
		return terminate(snapshot, StatePartial)
	case CommandFail:
		return terminate(snapshot, StateFailed)
	case CommandCancel:
		// Cancellation is accepted from every non-terminal state, including
		// the waiting ones: a Run parked on a human who never answers must
		// still be stoppable.
		return transition(snapshot, StateCancelled), nil

	default:
		return refuse(NewError("unknown_command", ErrorInvalid, RetryNever))
	}
}

func parkFromRunning(snapshot Snapshot, to State) (Transition, error) {
	if snapshot.State != StateRunning {
		return Transition{Next: snapshot}, NewError("not_running", ErrorInvalid, RetryNever)
	}
	return transition(snapshot, to), nil
}

func terminate(snapshot Snapshot, to State) (Transition, error) {
	if snapshot.State == StateQueued {
		// A queued Run has produced nothing, so it cannot have succeeded,
		// partially succeeded, or failed at anything. Cancel is the only
		// terminal it can reach, and it is handled above.
		return Transition{Next: snapshot}, NewError("not_started", ErrorInvalid, RetryNever)
	}
	return transition(snapshot, to), nil
}

// reserveEffect charges the budget before the effect is issued. The order is
// the point: a command that reserves after the call has already spent the
// budget by the time anything could refuse it.
func reserveEffect(snapshot Snapshot, command Command, kind EffectKind) (Transition, error) {
	if snapshot.State != StateRunning {
		return Transition{Next: snapshot}, NewError("not_running", ErrorInvalid, RetryNever)
	}
	if !snapshot.Budget.Affords(command.Reserve) {
		return Transition{Next: snapshot}, NewError(CodeBudgetExhausted, ErrorDenied, RetryNever)
	}

	next := snapshot
	next.Budget = snapshot.Budget.reserve(command.Reserve)
	if command.InvocationID != "" {
		next.Invocations = putInvocation(snapshot.Invocations, Invocation{
			ID:             command.InvocationID,
			Tool:           command.Tool,
			IdempotencyKey: command.IdempotencyKey,
			Outcome:        OutcomeInFlight,
			Reserved:       command.Reserve,
		})
	}

	result := emit(advance(snapshot, next), snapshot, Event{
		Kind:    EventBudgetReserved,
		RunID:   snapshot.ID,
		Reserve: command.Reserve,
	})
	result.Effects = []Effect{{Kind: kind, InvocationID: command.InvocationID, Reserve: command.Reserve}}
	return result, nil
}

func recordUnknown(snapshot Snapshot, command Command) (Transition, error) {
	if snapshot.State != StateRunning {
		return Transition{Next: snapshot}, NewError("not_running", ErrorInvalid, RetryNever)
	}
	if command.InvocationID == "" {
		return Transition{Next: snapshot}, NewError("invocation_required", ErrorInvalid, RetryNever)
	}

	existing := snapshot.Invocations[command.InvocationID]
	existing.ID = command.InvocationID
	existing.Outcome = OutcomeUnknown

	next := snapshot
	next.State = StateWaitingResolution
	next.Invocations = putInvocation(snapshot.Invocations, existing)

	result := emit(advance(snapshot, next), snapshot,
		stateEvent(snapshot, StateWaitingResolution),
		Event{
			Kind:         EventInvocationParked,
			RunID:        snapshot.ID,
			InvocationID: command.InvocationID,
		})
	result.Effects = []Effect{{Kind: EffectUnknown, InvocationID: command.InvocationID}}
	return result, nil
}

// resolveInvocation is the only exit from waiting_resolution.
//
// A resolver is a human or a host reconciler and both retry, so replaying the
// same decision has to be free. Replaying a different one has to be refused:
// the second decision would silently overwrite the first, and the first is what
// the ledger and every downstream consumer already acted on.
func resolveInvocation(snapshot Snapshot, command Command) (Transition, error) {
	if command.InvocationID == "" || !command.Outcome.Resolvable() {
		return Transition{Next: snapshot}, NewError("invalid_resolution", ErrorInvalid, RetryNever)
	}
	existing, ok := snapshot.Invocations[command.InvocationID]
	if !ok {
		return Transition{Next: snapshot}, NewError("unknown_invocation", ErrorInvalid, RetryNever)
	}

	switch existing.Outcome {
	case OutcomeUnknown:
		// Awaiting a decision — this is the case that does work.
	case command.Outcome:
		// Already resolved the same way. Idempotent: no revision bump, no
		// event, no error.
		return Transition{Next: snapshot}, nil
	default:
		return Transition{Next: snapshot}, NewError("resolution_conflict", ErrorConflict, RetryNever)
	}

	resolved := existing
	resolved.Outcome = command.Outcome

	next := snapshot
	next.Invocations = putInvocation(snapshot.Invocations, resolved)
	// still_unknown is an answer, but not one that unblocks anything: the Run
	// stays parked, now with the fact that somebody looked.
	if command.Outcome != OutcomeStillUnknown {
		next.State = StateRunning
	}

	events := []Event{{
		Kind:         EventInvocationResolved,
		RunID:        snapshot.ID,
		InvocationID: command.InvocationID,
	}}
	if next.State != snapshot.State {
		events = append(events, stateEvent(snapshot, next.State))
	}
	return emit(advance(snapshot, next), snapshot, events...), nil
}

// transition moves the Run to a new state and emits the state event.
func transition(snapshot Snapshot, to State) Transition {
	next := snapshot
	next.State = to
	return emit(advance(snapshot, next), snapshot, stateEvent(snapshot, to))
}

// advance bumps the revision exactly once per accepted command and detaches the
// maps so the returned snapshot shares no mutable state with the caller's.
func advance(previous, next Snapshot) Transition {
	next.Revision = previous.Revision + 1
	if next.Invocations == nil {
		next.Invocations = copyInvocations(previous.Invocations)
	}
	return Transition{Next: next}
}

// emit numbers a transition's events.
//
// Sequences are contiguous within a Run: that is what lets a consumer detect a
// gap rather than silently miss an event. A transition that emits two events
// therefore advances the sequence twice — they were two things that happened,
// and sharing one number would make the second invisible to a consumer keyed by
// sequence.
func emit(result Transition, previous Snapshot, events ...Event) Transition {
	sequence := previous.LastEventSequence
	for i := range events {
		sequence++
		events[i].Sequence = sequence
	}
	result.Events = append(result.Events, events...)
	result.Next.LastEventSequence = sequence
	return result
}

func stateEvent(previous Snapshot, to State) Event {
	return Event{
		Kind:  EventStateChanged,
		RunID: previous.ID,
		From:  previous.State,
		To:    to,
	}
}

func putInvocation(existing map[ID]Invocation, invocation Invocation) map[ID]Invocation {
	next := copyInvocations(existing)
	if next == nil {
		next = make(map[ID]Invocation, 1)
	}
	next[invocation.ID] = invocation
	return next
}

func copyInvocations(invocations map[ID]Invocation) map[ID]Invocation {
	if invocations == nil {
		return nil
	}
	out := make(map[ID]Invocation, len(invocations))
	maps.Copy(out, invocations)
	return out
}
