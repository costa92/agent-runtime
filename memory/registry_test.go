package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/authorization"
	"github.com/kart-io/wechat-account/agent-runtime/memory"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

type stubProvider struct{}

func (stubProvider) Retrieve(context.Context, memory.Query) ([]memory.Record, error) {
	return nil, nil
}

func (stubProvider) Write(context.Context, memory.Scope, string, string) (memory.Mutation, error) {
	return memory.Mutation{}, memory.ErrReadOnly
}

func TestRegistryRefusesEmptyDuplicateAndPostFreezeKeys(t *testing.T) {
	registry := memory.NewRegistry()

	if err := registry.Register("lessons", stubProvider{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if run.KindOf(registry.Register("lessons", stubProvider{})) != run.ErrorInvalid {
		t.Error("a duplicate key was accepted")
	}
	if run.KindOf(registry.Register(" ", stubProvider{})) != run.ErrorInvalid {
		t.Error("an empty key was accepted")
	}
	if run.KindOf(registry.Register("other", nil)) != run.ErrorInvalid {
		t.Error("a nil provider was accepted")
	}

	registry.Freeze()
	if run.KindOf(registry.Register("late", stubProvider{})) != run.ErrorInvalid {
		t.Error("registration after freeze was accepted")
	}
	if _, err := registry.Lookup("absent"); run.KindOf(err) != run.ErrorInvalid {
		t.Errorf("lookup of an unknown key error=%s want=invalid", run.KindOf(err))
	}
}

// Tenant and namespace are the isolation boundary. A default for either means
// one tenant's retrieval reaching another's records, which is a failure nobody
// sees until it has already happened.
func TestScopeValidationRequiresFullIsolation(t *testing.T) {
	complete := memory.Scope{
		Tenant:    "t",
		Namespace: "n",
		Principal: authorization.PrincipalRef{Subject: "u", Tenant: "t", Kind: authorization.PrincipalUser},
	}
	if err := complete.Validate(); err != nil {
		t.Fatalf("complete scope rejected: %v", err)
	}

	for name, mutate := range map[string]func(*memory.Scope){
		"no tenant":    func(s *memory.Scope) { s.Tenant = "" },
		"no namespace": func(s *memory.Scope) { s.Namespace = "" },
		"no principal": func(s *memory.Scope) { s.Principal = authorization.PrincipalRef{} },
	} {
		scope := complete
		mutate(&scope)
		if run.KindOf(scope.Validate()) != run.ErrorInvalid {
			t.Errorf("%s: accepted", name)
		}
	}
}

// An unbounded retrieval is an unbounded prompt, and it fails as a model error
// far away from the read that caused it.
func TestQueryMustBeBounded(t *testing.T) {
	scope := memory.Scope{
		Tenant:    "t",
		Namespace: "n",
		Principal: authorization.PrincipalRef{Subject: "u", Tenant: "t", Kind: authorization.PrincipalUser},
	}

	bounded := memory.Query{Scope: scope, MaxRecords: 5, MaxTokens: 500}
	if err := bounded.Validate(); err != nil {
		t.Fatalf("bounded query rejected: %v", err)
	}

	for name, query := range map[string]memory.Query{
		"no record ceiling": {Scope: scope, MaxTokens: 500},
		"no token ceiling":  {Scope: scope, MaxRecords: 5},
		"negative ceiling":  {Scope: scope, MaxRecords: -1, MaxTokens: 500},
		"unscoped":          {MaxRecords: 5, MaxTokens: 500},
	} {
		if run.KindOf(query.Validate()) != run.ErrorInvalid {
			t.Errorf("%s: accepted", name)
		}
	}
}

// One way to say "cannot write", so the publish-time check and the run-time
// behaviour cannot disagree.
func TestReadOnlyProviderRefusesWritesWithTheSentinel(t *testing.T) {
	_, err := stubProvider{}.Write(context.Background(), memory.Scope{}, "ref", "text")
	if !errors.Is(err, memory.ErrReadOnly) {
		t.Fatalf("error=%v want=ErrReadOnly", err)
	}
	if run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("kind=%s want=denied", run.KindOf(err))
	}
}
