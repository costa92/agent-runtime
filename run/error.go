package run

import (
	"errors"
	"fmt"
	"strings"
)

// ErrorKind is what a caller switches on. The set is closed and stable: a
// transport maps it to HTTP or gRPC, a worker maps it to retry or park, and
// neither is allowed to read the message to decide. Kinds are about what the
// caller must do; Code is about which specific thing happened.
type ErrorKind string

const (
	// CodeUnknownRun is returned for both a missing Run and a Run hidden by
	// ownership. Keeping one code lets a host expose an opaque not-found
	// contract without inspecting error text or learning why the lookup failed.
	CodeUnknownRun = "unknown_run"

	// CodeUndeclaredMemoryKey is returned when a Run reads a memory its
	// Definition never declared.
	//
	// Promoted for the same reason as CodeUnknownRun: something outside this
	// module branches on it. An Agent asking for notes it was not published
	// with means "this Run has no notes", not "this Run failed", so the Agent
	// has to tell that refusal apart from every other denial and carry on —
	// see article_review. That was a string literal on the far side of a module
	// boundary, so renaming the code here would have compiled cleanly and
	// silently turned a recoverable case into a failed Run.
	//
	// The other ~250 codes in this module stay literals on purpose. They are
	// read by operators, not matched by callers; a caller that needs to act
	// switches on ErrorKind, which is the closed set built for exactly that.
	// A code becomes a constant when something starts branching on it, and the
	// constant is then the evidence that it did.
	CodeUndeclaredMemoryKey = "undeclared_memory_key"

	// CodeBudgetExhausted is returned when a Run's envelope can no longer
	// afford the effect being admitted.
	//
	// Promoted for the same reason as the two above: a tool loop has to tell it
	// apart from every other ErrorDenied, because the right response differs.
	// A policy denying one tool means ask for a different one; the Run's budget
	// being gone means no tool will ever be affordable again, so leaving the
	// definitions in front of the model buys nothing but rounds. The kind alone
	// cannot separate them — that is the coarseness CodeOf's doc describes.
	//
	// Safe to act on permanently because the envelope is written once, at Run
	// creation, and Used only grows: nothing makes an exhausted budget
	// affordable again within the Run, not approval resume and not cap_budget,
	// which evaluate.go treats as a pass-through.
	CodeBudgetExhausted = "budget_exhausted"

	// CodeToolResultTooLarge is returned when a tool ran, its effect is
	// applied, and only its result was too large to carry back.
	//
	// Promoted because it is the one ErrorInvalid a caller must not read as "it
	// did not happen". The kind is right — the identical call will overflow
	// again, so it must not be retried — but the invocation is settled as
	// OutcomeApplied, and a loop that renders every ErrorInvalid as a plain
	// failure tells the model a write it performed did not occur. The model then
	// varies its arguments and writes a second time, which is the one outcome the
	// size limit exists to be cheaper than.
	//
	// Anything acting on this must say "done, result undeliverable" and must not
	// offer a retry.
	CodeToolResultTooLarge = "tool.result_too_large"

	// CodeResourceNotFound is what a resource read returns when the version it
	// was asked for is genuinely absent — as opposed to unreadable, which is a
	// different answer with a different remedy.
	//
	// Promoted because governance branches on it to decide whether a Run's pin
	// is unresolvable or the store merely did not answer. That distinction
	// cannot be made from the kind: a store wraps an unclassified transport
	// failure as ErrorInternal/RetryNever, so "the database is down" and "this
	// digest never existed" arrive with identical kinds and identical retry
	// hints. The code is the only thing that separates them, and a Run must not
	// be judged unresolvable because a connection dropped.
	//
	// It is part of the ResourceDigestReader contract rather than one store's
	// private vocabulary: an implementation that reports absence some other way
	// is not interchangeable with this one.
	CodeResourceNotFound = "resource.not_found"

	// ErrorInvalid: the definition, schema or command is not legal here. The
	// same call will never succeed; something has to change first.
	ErrorInvalid ErrorKind = "invalid"
	// ErrorConflict: a revision CAS, lease, or concurrent ownership conflict.
	// Reload and decide again — the caller's view was stale, not wrong.
	ErrorConflict ErrorKind = "conflict"
	// ErrorDenied: authorization, budget or quota refused it.
	ErrorDenied ErrorKind = "denied"
	// ErrorInterrupted: waiting on approval, children, or external input. Not
	// a failure; the Run is parked and something else must move first.
	ErrorInterrupted ErrorKind = "interrupted"
	// ErrorRetryable: a transient failure the Runtime has established is safe
	// to repeat. Never inferred from a message — only set where the effect is
	// known to be idempotent or known not to have happened.
	ErrorRetryable ErrorKind = "retryable"
	// ErrorUnknown: the side effect may or may not have happened. It must be
	// reconciled, never blindly retried; that distinction is the whole reason
	// this kind exists separately from retryable.
	ErrorUnknown ErrorKind = "unknown"
	// ErrorInternal: a Runtime invariant or adapter fault.
	ErrorInternal ErrorKind = "internal"
)

// RetryHint tells a worker what to do next without it having to re-derive the
// decision from the kind. Kinds are stable and coarse; a hint can differ
// between two errors of the same kind.
type RetryHint string

const (
	RetryNever      RetryHint = "never"
	RetryImmediate  RetryHint = "immediate"
	RetryBackoff    RetryHint = "backoff"
	RetryReconcile  RetryHint = "reconcile"
	RetryAfterInput RetryHint = "after_input"
)

// Error is the Runtime's only error type.
//
// It carries no HTTP status and no user-facing message. Both belong to the
// host's transport adapter, which knows the protocol and the locale; putting
// either here would make every embedder inherit one product's choices.
type Error struct {
	// Code is the stable, specific identifier — "budget_exhausted",
	// "terminal_state". Callers may match on it; it never changes wording.
	Code string
	Kind ErrorKind
	// Retry is advice, not permission. The Runtime still enforces the state
	// machine when the retry arrives.
	Retry RetryHint
	// Causes are the underlying errors, kept for operators. Nothing branches
	// on them.
	Causes []error
}

func (e *Error) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s", e.Kind, e.Code)
	for _, cause := range e.Causes {
		if cause != nil {
			fmt.Fprintf(&b, ": %v", cause)
		}
	}
	return b.String()
}

// Unwrap exposes the causes to errors.Is/As. A single cause unwraps the usual
// way; several unwrap as a group, which is what the multi-error form of
// errors.Is expects.
func (e *Error) Unwrap() []error { return e.Causes }

// NewError builds a Runtime error. Every field is required at the call site so
// that a new error has to answer "what must the caller do" before it exists —
// which is the question a kind-less error leaves the caller to guess at.
func NewError(code string, kind ErrorKind, retry RetryHint, causes ...error) *Error {
	return &Error{Code: code, Kind: kind, Retry: retry, Causes: causes}
}

// KindOf reports the kind of an error, or "" when it is not a Runtime error.
// The empty kind is deliberate: a caller comparing against a specific kind
// then falls through to its default branch rather than matching by accident.
func KindOf(err error) ErrorKind {
	var target *Error
	if errors.As(err, &target) {
		return target.Kind
	}
	return ""
}

// RunIsUnrunnable reports whether an error means this Run can never advance,
// as opposed to this attempt not having worked.
//
// It is deliberately not RetryOf(err) == RetryNever, and the difference is the
// whole point. RetryNever is what an unclassified failure gets — RetryOf
// returns it for anything that is not a *Error, and a store wraps an
// unrecognised transport fault as ErrorInternal/RetryNever. That default is
// right for a caller asking "may I repeat this?", because an unknown error is
// no evidence that repeating is safe. It is catastrophic for a caller asking
// "may I destroy this?", because there the same default means "kill anything I
// do not recognise": one database outage would end every Run in flight.
//
// So the test is a declared ErrorInvalid, which is the kind reserved for "the
// definition, schema or command is not legal here" — a fact about the Run
// itself. ErrorInternal is excluded even when permanent, because internal means
// the deployment is at fault, and a deployment can be fixed; ending the Runs
// would throw away work that a redeploy would have let finish. ErrorConflict is
// excluded because it promises reloading resolves it. Anything that is not a
// *Error at all is excluded, which is exactly the raw driver error a blip
// produces.
//
// The cost of each direction is asymmetric and that is why the line sits here:
// judging a doomed Run runnable leaves it stuck, which is visible and fixable;
// judging a runnable Run doomed destroys it.
func RunIsUnrunnable(err error) bool {
	var target *Error
	if !errors.As(err, &target) {
		return false
	}
	return target.Kind == ErrorInvalid && target.Retry == RetryNever
}

// CodeOf reports the code of an error, or "" when it is not a Runtime error.
//
// Mostly for observability: codes are read by operators and the closed set
// built for branching is ErrorKind. It exists because a kind is deliberately
// coarse — several unrelated refusals share one — so a record carrying only
// the kind cannot say which thing to go and fix.
//
// That same coarseness is why a few callers do branch on a code, and the rule
// for when that is legitimate is the one stated on CodeUndeclaredMemoryKey: the
// code must first be promoted to a constant here. A caller matching a string
// literal across a module boundary would survive a rename in this file and
// silently stop matching; a caller referencing the constant cannot.
func CodeOf(err error) string {
	var target *Error
	if errors.As(err, &target) {
		return target.Code
	}
	return ""
}

// RetryOf reports the retry hint of an error, or RetryNever for anything that
// is not a Runtime error. An unrecognized error is not evidence that repeating
// the call is safe.
func RetryOf(err error) RetryHint {
	var target *Error
	if errors.As(err, &target) {
		return target.Retry
	}
	return RetryNever
}
