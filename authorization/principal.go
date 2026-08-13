// Package authorization defines who a Run acts as and who may act on it.
//
// It draws one line and draws it in the type system: the durable half of an
// identity and the request-scoped half are different types. Everything a Run
// persists can hold only the durable half.
package authorization

import "time"

// PrincipalKind distinguishes the sorts of actor a Runtime serves. Hosts map
// their own account model onto these; the Runtime only needs to tell an
// end user from a machine from an operator acting on someone's behalf.
type PrincipalKind string

const (
	PrincipalUser    PrincipalKind = "user"
	PrincipalService PrincipalKind = "service"
	PrincipalSystem  PrincipalKind = "system"
)

// PrincipalRef is the durable, serializable identity of an actor. It is the
// only identity a Snapshot, Event or Checkpoint may hold.
//
// Subject and Tenant are opaque to the Runtime: it compares and records them,
// never parses them. That is what lets a host use whatever identifier it
// already has without teaching the Runtime its account model.
type PrincipalRef struct {
	Subject string        `json:"subject"`
	Tenant  string        `json:"tenant"`
	Kind    PrincipalKind `json:"kind"`
}

// Zero reports whether the reference names nobody.
func (p PrincipalRef) Zero() bool {
	return p.Subject == "" && p.Tenant == "" && p.Kind == ""
}

// PrincipalContext is the request-scoped identity: the reference plus the
// claims that were true at authentication time.
//
// It is deliberately not serializable into Run state and must never be
// embedded in a Snapshot, Event or Checkpoint. Claims expire, get revoked, and
// are frequently privileged; persisting them turns a momentary authorization
// into a durable one and replays it to every later reader of the Run. A
// reducer test walks the Snapshot type and fails if this type is reachable
// from it.
//
// Authorization decisions are therefore made from this at the moment of the
// decision, and only the reference is recorded afterwards.
type PrincipalContext struct {
	Ref             PrincipalRef
	Claims          map[string]string
	AuthenticatedAt time.Time
}
