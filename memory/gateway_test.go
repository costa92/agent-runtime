package memory_test

import (
	"context"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/memory"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

func principal() authorization.PrincipalRef {
	return authorization.PrincipalRef{Subject: "u-1", Tenant: "t-1", Kind: authorization.PrincipalUser}
}

func scope() memory.Scope {
	return memory.Scope{Tenant: "t-1", Namespace: "notes", Principal: principal(), RunID: "run-1"}
}

func gatewayWith(t *testing.T, provider memory.Provider, authorizer memory.Authorizer) *memory.Gateway {
	t.Helper()
	registry := memory.NewRegistry()
	if err := registry.Register("notes", provider); err != nil {
		t.Fatalf("register: %v", err)
	}
	registry.Freeze()
	return memory.NewGateway(registry, authorizer)
}

// The ceilings exist to bound a prompt. Truncated is reported so a caller can
// tell "there was no more" from "we stopped looking".
func TestRetrieveAppliesTheDeclaredCeilings(t *testing.T) {
	provider := testkit.NewMemoryProvider()
	for _, ref := range []string{"a", "b", "c", "d"} {
		provider.Seed(scope(), ref, "one two three four five", 1)
	}
	gateway := gatewayWith(t, provider, testkit.AllowAllMemoryAuthorizer())

	result, err := gateway.Retrieve(context.Background(), principal(), memory.RetrieveRequest{
		Key:   "notes",
		Query: memory.Query{Scope: scope(), MaxRecords: 2, MaxTokens: 1000},
	})
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(result.Records) != 2 || !result.Truncated {
		t.Fatalf("record ceiling not applied: %d records, truncated=%v", len(result.Records), result.Truncated)
	}

	tokenCapped, err := gateway.Retrieve(context.Background(), principal(), memory.RetrieveRequest{
		Key:   "notes",
		Query: memory.Query{Scope: scope(), MaxRecords: 10, MaxTokens: 6},
	})
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if !tokenCapped.Truncated || len(tokenCapped.Records) >= 4 {
		t.Fatalf("token ceiling not applied: %d records, truncated=%v",
			len(tokenCapped.Records), tokenCapped.Truncated)
	}
}

// Ordering is the gateway's. Two providers ordering ties differently would make
// one Definition produce different prompts on different deployments.
func TestRetrieveOrdersDeterministically(t *testing.T) {
	provider := testkit.NewMemoryProvider()
	provider.Seed(scope(), "z", "text", 0.5)
	provider.Seed(scope(), "a", "text", 0.5)
	provider.Seed(scope(), "m", "text", 0.9)
	gateway := gatewayWith(t, provider, testkit.AllowAllMemoryAuthorizer())

	result, err := gateway.Retrieve(context.Background(), principal(), memory.RetrieveRequest{
		Key:   "notes",
		Query: memory.Query{Scope: scope(), MaxRecords: 10, MaxTokens: 1000},
	})
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(result.Records) != 3 {
		t.Fatalf("records=%d want=3", len(result.Records))
	}
	if result.Records[0].Ref != "m" || result.Records[1].Ref != "a" || result.Records[2].Ref != "z" {
		t.Fatalf("order is not score-then-ref: %s %s %s",
			result.Records[0].Ref, result.Records[1].Ref, result.Records[2].Ref)
	}
}

// A caller passing someone else's scope with its own principal would read
// across the boundary while looking authorized.
func TestScopeMustBelongToTheCallingPrincipal(t *testing.T) {
	gateway := gatewayWith(t, testkit.NewMemoryProvider(), testkit.AllowAllMemoryAuthorizer())

	foreign := scope()
	foreign.Principal = authorization.PrincipalRef{Subject: "someone-else", Tenant: "t-1", Kind: authorization.PrincipalUser}

	if _, err := gateway.Retrieve(context.Background(), principal(), memory.RetrieveRequest{
		Key:   "notes",
		Query: memory.Query{Scope: foreign, MaxRecords: 1, MaxTokens: 1},
	}); run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("error=%s want=denied", run.KindOf(err))
	}
}

// Authorization is asked at the call, not remembered from when the Run started.
// A Run parked overnight must be re-checked against permissions that may since
// have been revoked.
func TestAuthorizationIsRecheckedAtEveryCall(t *testing.T) {
	authorizer := testkit.NewRevocableMemoryAuthorizer()
	provider := testkit.NewMemoryProvider()
	provider.Seed(scope(), "a", "text", 1)
	gateway := gatewayWith(t, provider, authorizer)
	ctx := context.Background()

	request := memory.RetrieveRequest{
		Key:   "notes",
		Query: memory.Query{Scope: scope(), MaxRecords: 10, MaxTokens: 1000},
	}
	if _, err := gateway.Retrieve(ctx, principal(), request); err != nil {
		t.Fatalf("retrieve: %v", err)
	}

	authorizer.Revoke()
	if _, err := gateway.Retrieve(ctx, principal(), request); run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("a revoked permission still read: %v", err)
	}
}

// Same begin-before-effect rule as tools: the write is durable as an intention
// before it can be true as a fact.
func TestWriteRequiresACommittedBeginFact(t *testing.T) {
	provider := testkit.NewMemoryProvider()
	gateway := gatewayWith(t, provider, testkit.AllowAllMemoryAuthorizer())
	ctx := context.Background()

	request := memory.WriteRequest{
		InvocationID: "inv-1", Key: "notes", Scope: scope(),
		Ref: "note-1", Text: "alpha", IdempotencyKey: "k1",
	}

	forged := memory.CommittedWrite{Prepared: memory.PreparedWrite{Request: request}}
	if _, err := gateway.ExecuteWrite(ctx, forged); run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("a forged write ticket was accepted: %v", err)
	}
	if provider.Writes() != 0 {
		t.Fatal("a forged ticket reached the provider")
	}

	prepared, err := gateway.PrepareWrite(ctx, principal(), request)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	fact, err := gateway.ExecuteWrite(ctx, memory.CommittedWrite{Prepared: prepared})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if fact.Outcome != run.OutcomeApplied {
		t.Fatalf("outcome=%s want=applied", fact.Outcome)
	}
}

func TestWriteWithoutAnIdempotencyKeyIsRefused(t *testing.T) {
	gateway := gatewayWith(t, testkit.NewMemoryProvider(), testkit.AllowAllMemoryAuthorizer())

	if _, err := gateway.PrepareWrite(context.Background(), principal(), memory.WriteRequest{
		InvocationID: "inv-1", Key: "notes", Scope: scope(), Ref: "note-1",
	}); run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("error=%s want=invalid", run.KindOf(err))
	}
}

// The same reconciliation rule as tools: the write may have landed.
func TestUnknownWriteReconcilesRatherThanRetries(t *testing.T) {
	provider := testkit.NewMemoryProvider()
	provider.FailWritesWith(run.NewError("memory.unknown", run.ErrorUnknown, run.RetryReconcile))
	gateway := gatewayWith(t, provider, testkit.AllowAllMemoryAuthorizer())
	ctx := context.Background()

	prepared, err := gateway.PrepareWrite(ctx, principal(), memory.WriteRequest{
		InvocationID: "inv-1", Key: "notes", Scope: scope(),
		Ref: "note-1", Text: "alpha", IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	fact, err := gateway.ExecuteWrite(ctx, memory.CommittedWrite{Prepared: prepared})
	if run.KindOf(err) != run.ErrorUnknown {
		t.Fatalf("kind=%s want=unknown", run.KindOf(err))
	}
	if fact.Outcome != run.OutcomeUnknown {
		t.Fatalf("outcome=%s want=unknown", fact.Outcome)
	}
}

// A takeover replays the write. Two copies is the failure this prevents.
func TestTakeoverReplayDoesNotDuplicateTheWrite(t *testing.T) {
	provider := testkit.NewMemoryProvider()
	gateway := gatewayWith(t, provider, testkit.AllowAllMemoryAuthorizer())
	ctx := context.Background()

	request := memory.WriteRequest{
		InvocationID: "inv-1", Key: "notes", Scope: scope(),
		Ref: "note-1", Text: "alpha", IdempotencyKey: "k1",
	}
	prepared, err := gateway.PrepareWrite(ctx, principal(), request)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := gateway.ExecuteWrite(ctx, memory.CommittedWrite{Prepared: prepared}); err != nil {
		t.Fatalf("first write: %v", err)
	}

	// The worker lost its lease; a second one recovers the same committed
	// invocation and replays it.
	replayed, err := gateway.PrepareWrite(ctx, principal(), request)
	if err != nil {
		t.Fatalf("prepare after takeover: %v", err)
	}
	if _, err := gateway.ExecuteWrite(ctx, memory.CommittedWrite{Prepared: replayed}); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if provider.Writes() != 1 {
		t.Fatalf("writes=%d want=1; takeover duplicated the write", provider.Writes())
	}
}
