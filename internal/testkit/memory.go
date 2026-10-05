package testkit

import (
	"context"
	"strings"
	"sync"

	"github.com/costa92/agent-runtime/authorization"
	"github.com/costa92/agent-runtime/memory"
	"github.com/costa92/agent-runtime/run"
)

type storedRecord struct {
	scope  memory.Scope
	record memory.Record
}

// MemoryProvider is an in-process memory provider that isolates on the full
// scope, so the conformance suite's isolation cases have something real to
// check rather than a stub that returns whatever it was given.
type MemoryProvider struct {
	mu      sync.Mutex
	records []storedRecord
	writes  map[string]memory.Mutation
	// failWrite makes the next write fail with the given error, for the
	// unknown-outcome cases.
	failWrite error
}

func NewMemoryProvider() *MemoryProvider {
	return &MemoryProvider{writes: make(map[string]memory.Mutation)}
}

// FailWritesWith makes every write return err.
func (p *MemoryProvider) FailWritesWith(err error) { p.failWrite = err }

// Seed adds a record visible only within the given scope.
func (p *MemoryProvider) Seed(scope memory.Scope, ref, text string, score float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.records = append(p.records, storedRecord{
		scope:  scope,
		record: memory.Record{Ref: ref, Source: "seed", Score: score, Text: text},
	})
}

// Writes returns how many distinct writes landed, which is how the suite tells
// an idempotent replay from a duplicate.
func (p *MemoryProvider) Writes() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.writes)
}

var _ memory.Provider = (*MemoryProvider)(nil)

func (p *MemoryProvider) Retrieve(ctx context.Context, query memory.Query) ([]memory.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, run.NewError("memory.cancelled", run.ErrorInterrupted, run.RetryNever, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var matched []memory.Record
	for _, stored := range p.records {
		// Every component of the scope isolates. Dropping any one of them is
		// the bug the suite is looking for.
		if stored.scope.Tenant != query.Scope.Tenant ||
			stored.scope.Namespace != query.Scope.Namespace ||
			stored.scope.Principal != query.Scope.Principal {
			continue
		}
		if query.Text != "" && !strings.Contains(stored.record.Text, query.Text) {
			continue
		}
		matched = append(matched, stored.record)
	}
	return matched, nil
}

func (p *MemoryProvider) Write(ctx context.Context, scope memory.Scope, ref, text string) (memory.Mutation, error) {
	if err := ctx.Err(); err != nil {
		return memory.Mutation{}, run.NewError("memory.cancelled", run.ErrorInterrupted, run.RetryNever, err)
	}
	if p.failWrite != nil {
		return memory.Mutation{}, p.failWrite
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	key := scope.Tenant + "\x00" + scope.Namespace + "\x00" + ref
	if existing, ok := p.writes[key]; ok {
		// Idempotent by ref within a scope: a replay returns the first result
		// rather than storing a second copy.
		return existing, nil
	}
	mutation := memory.Mutation{Ref: ref, Used: run.Limits{ToolCalls: 1}}
	p.writes[key] = mutation
	p.records = append(p.records, storedRecord{
		scope:  scope,
		record: memory.Record{Ref: ref, Source: "write", Score: 1, Text: text},
	})
	return mutation, nil
}

// AllowAllMemoryAuthorizer permits every scope.
func AllowAllMemoryAuthorizer() memory.Authorizer { return staticMemoryAuthorizer{} }

type staticMemoryAuthorizer struct{}

func (staticMemoryAuthorizer) AuthorizeRead(context.Context, authorization.PrincipalRef, memory.Scope) error {
	return nil
}

func (staticMemoryAuthorizer) AuthorizeWrite(context.Context, authorization.PrincipalRef, memory.Scope) error {
	return nil
}

// RevocableMemoryAuthorizer starts permissive and can be revoked, so the suite
// can check that authorization is re-asked at the call rather than remembered
// from when the Run started.
type RevocableMemoryAuthorizer struct {
	mu      sync.Mutex
	revoked bool
}

func NewRevocableMemoryAuthorizer() *RevocableMemoryAuthorizer { return &RevocableMemoryAuthorizer{} }

func (a *RevocableMemoryAuthorizer) Revoke() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.revoked = true
}

func (a *RevocableMemoryAuthorizer) check() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.revoked {
		return run.NewError("memory.denied", run.ErrorDenied, run.RetryNever)
	}
	return nil
}

func (a *RevocableMemoryAuthorizer) AuthorizeRead(context.Context, authorization.PrincipalRef, memory.Scope) error {
	return a.check()
}

func (a *RevocableMemoryAuthorizer) AuthorizeWrite(context.Context, authorization.PrincipalRef, memory.Scope) error {
	return a.check()
}
