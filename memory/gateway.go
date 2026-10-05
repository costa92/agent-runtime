package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/costa92/agent-runtime/authorization"
	"github.com/costa92/agent-runtime/run"
)

// Authorizer is the host's answer to "may this principal touch this namespace".
//
// It is consulted at the moment of the call, not at the moment the Run started.
// A Run parked for approval overnight must be re-checked: the permission that
// was true when it began may have been revoked since.
type Authorizer interface {
	AuthorizeRead(ctx context.Context, principal authorization.PrincipalRef, scope Scope) error
	AuthorizeWrite(ctx context.Context, principal authorization.PrincipalRef, scope Scope) error
}

// RetrieveRequest is a bounded, scoped read.
type RetrieveRequest struct {
	Key   string
	Query Query
}

// Result is what a retrieval returned, after the gateway applied the limits.
type Result struct {
	Records []Record
	// Truncated marks a result cut to the declared ceiling, so a caller can
	// tell "there was no more" from "we stopped looking".
	Truncated bool
}

// WriteRequest is a governed memory write.
type WriteRequest struct {
	InvocationID run.ID
	Key          string
	Scope        Scope
	Ref          string
	Text         string
	// IdempotencyKey makes a replay after an unknown result safe. It is
	// required, because a memory write is a side effect the Runtime cannot
	// roll back.
	IdempotencyKey string
}

// PreparedWrite is the begin-before-effect fact, mirroring the tool gateway:
// the Runtime commits it, and only then may the write happen.
type PreparedWrite struct {
	Request WriteRequest
	Reserve run.Limits
	ticket  string
}

// CommittedWrite is the Runtime's confirmation that the begin fact is durable.
type CommittedWrite struct {
	Prepared PreparedWrite
}

// ResultFact is what the gateway returns for the Runtime to commit.
type ResultFact struct {
	InvocationID run.ID
	Outcome      run.Outcome
	Mutation     Mutation
}

// Gateway is the one path to a memory provider.
//
// Agents never receive a Provider or a repository. If they did, the scope
// check, the budget and the invocation record would all be optional — enforced
// by whichever agent remembered them.
type Gateway struct {
	registry   *Registry
	authorizer Authorizer
}

func NewGateway(registry *Registry, authorizer Authorizer) *Gateway {
	return &Gateway{registry: registry, authorizer: authorizer}
}

// Retrieve reads, enforcing scope, live authorization, ordering and the
// declared ceilings.
func (g *Gateway) Retrieve(ctx context.Context, principal authorization.PrincipalRef, request RetrieveRequest) (Result, error) {
	if err := request.Query.Validate(); err != nil {
		return Result{}, err
	}
	if request.Query.Scope.Principal != principal {
		// The scope is what the provider isolates on. A caller that could pass
		// someone else's scope with its own principal would read across the
		// boundary while looking authorized.
		return Result{}, run.NewError("scope_principal_mismatch", run.ErrorDenied, run.RetryNever)
	}
	if err := g.authorizer.AuthorizeRead(ctx, principal, request.Query.Scope); err != nil {
		return Result{}, err
	}

	provider, err := g.registry.Lookup(request.Key)
	if err != nil {
		return Result{}, err
	}

	records, err := provider.Retrieve(ctx, request.Query)
	if err != nil {
		return Result{}, err
	}

	// Score descending, stable so equal scores keep the provider/SQL order
	// (created_at, id) instead of comparing "lesson:11" against "lesson:2".
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].Score > records[j].Score
	})

	result := Result{Records: records}
	if len(result.Records) > request.Query.MaxRecords {
		result.Records = result.Records[:request.Query.MaxRecords]
		result.Truncated = true
	}

	var tokens int
	for i, record := range result.Records {
		tokens += estimateTokens(record.Text)
		if tokens > request.Query.MaxTokens {
			result.Records = result.Records[:i]
			result.Truncated = true
			break
		}
	}
	if observer, ok := provider.(RecallObserver); ok {
		refs := make([]string, len(result.Records))
		for i, record := range result.Records {
			refs[i] = record.Ref
		}
		_ = observer.Recalled(ctx, refs)
	}
	return result, nil
}

// PrepareWrite runs the checks and returns the begin fact.
func (g *Gateway) PrepareWrite(ctx context.Context, principal authorization.PrincipalRef, request WriteRequest) (PreparedWrite, error) {
	if request.InvocationID == "" {
		return PreparedWrite{}, run.NewError("missing_invocation_id", run.ErrorInvalid, run.RetryNever)
	}
	if request.IdempotencyKey == "" {
		return PreparedWrite{}, run.NewError("missing_idempotency_key", run.ErrorInvalid, run.RetryNever)
	}
	if err := request.Scope.Validate(); err != nil {
		return PreparedWrite{}, err
	}
	if request.Scope.Principal != principal {
		return PreparedWrite{}, run.NewError("scope_principal_mismatch", run.ErrorDenied, run.RetryNever)
	}
	if err := g.authorizer.AuthorizeWrite(ctx, principal, request.Scope); err != nil {
		return PreparedWrite{}, err
	}
	if _, err := g.registry.Lookup(request.Key); err != nil {
		return PreparedWrite{}, err
	}

	return PreparedWrite{
		Request: request,
		Reserve: run.Limits{ToolCalls: 1},
		ticket:  writeTicket(request.InvocationID, request.Key),
	}, nil
}

// ExecuteWrite performs the write for a committed begin fact.
func (g *Gateway) ExecuteWrite(ctx context.Context, committed CommittedWrite) (ResultFact, error) {
	prepared := committed.Prepared
	if prepared.ticket == "" || prepared.ticket != writeTicket(prepared.Request.InvocationID, prepared.Request.Key) {
		return ResultFact{}, run.NewError("uncommitted_write", run.ErrorInvalid, run.RetryNever)
	}

	provider, err := g.registry.Lookup(prepared.Request.Key)
	if err != nil {
		return ResultFact{}, err
	}

	mutation, err := provider.Write(ctx, prepared.Request.Scope, prepared.Request.Ref, prepared.Request.Text)
	if err != nil {
		fact := ResultFact{InvocationID: prepared.Request.InvocationID, Outcome: run.OutcomeNotApplied}
		if run.KindOf(err) == run.ErrorUnknown {
			// The same rule as tools: the write may have landed, so it
			// reconciles rather than retries.
			fact.Outcome = run.OutcomeUnknown
		}
		return fact, err
	}

	return ResultFact{
		InvocationID: prepared.Request.InvocationID,
		Outcome:      run.OutcomeApplied,
		Mutation:     mutation,
	}, nil
}

func writeTicket(id run.ID, key string) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s", id, key))
	return hex.EncodeToString(sum[:8])
}

// estimateTokens is a whitespace count. It is deliberately crude and
// deliberately the gateway's: an exact count needs the model's tokenizer, which
// would make the Runtime depend on a provider, and the ceiling exists to bound
// a prompt rather than to bill it.
func estimateTokens(text string) int {
	var count, inWord int
	for _, r := range text {
		if r == ' ' || r == '\n' || r == '\t' || r == '\r' {
			inWord = 0
			continue
		}
		if inWord == 0 {
			count++
			inWord = 1
		}
	}
	return count
}
