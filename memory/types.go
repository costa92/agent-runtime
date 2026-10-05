// Package memory is the first-class extension slot for agent context.
//
// Memory is deliberately not modelled as an ordinary tool. Retrieval and
// writing have their own scope, budget and isolation rules, and hiding them
// behind a generic tool call would mean those rules are enforced by whichever
// tool implementation remembered to.
package memory

import (
	"github.com/costa92/agent-runtime/authorization"
	"github.com/costa92/agent-runtime/run"
)

// Scope is the isolation boundary of one retrieval or write.
//
// Tenant and Namespace together are the boundary; neither is optional and
// neither is defaulted, because a default here means one tenant's retrieval
// reaching another's records.
type Scope struct {
	Tenant    string
	Namespace string
	Principal authorization.PrincipalRef
	AgentKey  string
	RunID     run.ID
}

// Validate rejects a scope that does not isolate.
func (s Scope) Validate() error {
	if s.Tenant == "" {
		return run.NewError("missing_tenant", run.ErrorInvalid, run.RetryNever)
	}
	if s.Namespace == "" {
		return run.NewError("missing_namespace", run.ErrorInvalid, run.RetryNever)
	}
	if s.Principal.Zero() {
		return run.NewError("missing_principal", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// RetrievalContext is governance the Runtime already owns and forwards.
//
// Providers must not invent it. Gateway does not filter on it: Lesson tool
// intersection belongs in the host Search, and Knowledge ignores it.
type RetrievalContext struct {
	Tools []string
}

// Query is a bounded retrieval. Both ceilings are required: an unbounded
// retrieval is an unbounded prompt, and it fails as a model error far from the
// read that caused it.
type Query struct {
	Scope      Scope
	Text       string
	MaxRecords int
	MaxTokens  int
	Context    RetrievalContext
}

// Validate rejects a query that is not bounded or not scoped.
func (q Query) Validate() error {
	if err := q.Scope.Validate(); err != nil {
		return err
	}
	if q.MaxRecords <= 0 || q.MaxTokens <= 0 {
		return run.NewError("unbounded_query", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// Record is one retrieved item.
//
// Ref, Source and Score are all required by consumers rather than decorative:
// the reference lets a citation be checked, the source lets a reader tell a
// stored fact from a retrieved one, and the score lets the ordering be
// explained. Providers return this, never a host ORM type.
type Record struct {
	Ref    string
	Source string
	Score  float64
	Text   string
}

// Mutation is the result of a governed write, returned by the provider for the
// Runtime to commit atomically with usage, events and budget settlement. The
// provider does not commit it: a provider that wrote directly would be a second
// writer of Run-adjacent state, outside the fence.
type Mutation struct {
	Ref  string
	Used run.Limits
}
