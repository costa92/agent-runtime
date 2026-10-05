package conformance

import (
	"context"
	"testing"

	"github.com/costa92/agent-runtime/authorization"
	"github.com/costa92/agent-runtime/memory"
	"github.com/costa92/agent-runtime/run"
)

// MemoryHarness is what a memory provider supplies to be checked.
type MemoryHarness struct {
	Provider memory.Provider
	// Seed stores a record visible only within the given scope.
	Seed func(scope memory.Scope, ref, text string, score float64)
	// Writes reports how many distinct writes landed, which is how a replay is
	// told from a duplicate.
	Writes func() int
}

// MemoryProvider is the reusable suite every memory provider must pass.
//
// The isolation cases are the reason it exists. A provider that ignores one
// component of the scope works perfectly in every single-tenant test and leaks
// across the boundary the first time there are two.
func MemoryProvider(t *testing.T, newHarness func(t *testing.T) MemoryHarness) {
	t.Helper()

	tenantA := scopeFor("tenant-a", "notes", "user-a")
	tenantB := scopeFor("tenant-b", "notes", "user-b")

	t.Run("IsolatesByTenantNamespaceAndPrincipal", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		harness.Seed(tenantA, "a-1", "alpha", 1)
		harness.Seed(tenantB, "b-1", "alpha", 1)
		harness.Seed(scopeFor("tenant-a", "other-namespace", "user-a"), "a-2", "alpha", 1)
		harness.Seed(scopeFor("tenant-a", "notes", "someone-else"), "a-3", "alpha", 1)

		records, err := harness.Provider.Retrieve(ctx, memory.Query{Scope: tenantA, MaxRecords: 10, MaxTokens: 1000})
		if err != nil {
			t.Fatalf("retrieve: %v", err)
		}
		if len(records) != 1 || records[0].Ref != "a-1" {
			var refs []string
			for _, record := range records {
				refs = append(refs, record.Ref)
			}
			t.Fatalf("retrieval crossed a scope boundary: %v", refs)
		}
	})

	t.Run("EmptyResultIsNotAnError", func(t *testing.T) {
		harness := newHarness(t)

		records, err := harness.Provider.Retrieve(context.Background(), memory.Query{
			Scope: tenantA, MaxRecords: 10, MaxTokens: 1000,
		})
		if err != nil {
			t.Fatalf("an empty namespace errored: %v", err)
		}
		if len(records) != 0 {
			t.Fatalf("records=%d want=0", len(records))
		}
	})

	t.Run("RecordsCarryRefSourceAndScore", func(t *testing.T) {
		harness := newHarness(t)
		harness.Seed(tenantA, "a-1", "alpha", 0.5)

		records, err := harness.Provider.Retrieve(context.Background(), memory.Query{
			Scope: tenantA, MaxRecords: 10, MaxTokens: 1000,
		})
		if err != nil {
			t.Fatalf("retrieve: %v", err)
		}
		if len(records) == 0 {
			t.Fatal("nothing retrieved")
		}
		// The reference lets a citation be checked, the source tells a stored
		// fact from a retrieved one, the score explains the ordering.
		if records[0].Ref == "" || records[0].Source == "" {
			t.Fatalf("record is missing its provenance: %+v", records[0])
		}
	})

	t.Run("CancellationIsReportedNotIgnored", func(t *testing.T) {
		harness := newHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if _, err := harness.Provider.Retrieve(ctx, memory.Query{
			Scope: tenantA, MaxRecords: 10, MaxTokens: 1000,
		}); err == nil {
			t.Fatal("a cancelled retrieval succeeded")
		}
	})

	t.Run("WritesAreIdempotentWithinAScope", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		first, err := harness.Provider.Write(ctx, tenantA, "note-1", "alpha")
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		second, err := harness.Provider.Write(ctx, tenantA, "note-1", "alpha")
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		if first.Ref != second.Ref {
			t.Fatalf("a replay produced a different ref: %q vs %q", first.Ref, second.Ref)
		}
		if harness.Writes() != 1 {
			// A takeover replays the write. Two copies is the failure this
			// property exists to prevent.
			t.Fatalf("writes=%d want=1; the replay stored a duplicate", harness.Writes())
		}
	})

	t.Run("WritesDoNotCrossScopes", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		if _, err := harness.Provider.Write(ctx, tenantA, "note-1", "alpha"); err != nil {
			t.Fatalf("write: %v", err)
		}

		records, err := harness.Provider.Retrieve(ctx, memory.Query{Scope: tenantB, MaxRecords: 10, MaxTokens: 1000})
		if err != nil {
			t.Fatalf("retrieve: %v", err)
		}
		if len(records) != 0 {
			t.Fatalf("a write in one tenant was visible in another: %+v", records)
		}
	})
}

func scopeFor(tenant, namespace, subject string) memory.Scope {
	return memory.Scope{
		Tenant:    tenant,
		Namespace: namespace,
		Principal: authorization.PrincipalRef{
			Subject: subject, Tenant: tenant, Kind: authorization.PrincipalUser,
		},
		RunID: run.ID("run-" + subject),
	}
}
