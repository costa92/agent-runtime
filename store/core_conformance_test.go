package store_test

import (
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
)

// The suite runs against the in-memory Store so that it is itself exercised. A
// conformance suite nothing runs is a document, and it drifts like one.
//
// That it actually bites was checked by hand rather than by a meta-test:
// wrapping the Store so that it overwrites the caller's expected revision — the
// most common way an adapter is subtly wrong, since everything works until two
// workers overlap — fails StaleRevisionIsRefused and
// ProjectionsCommitWithTheStateThatProducedThem. A meta-test would have needed
// a testing.TB shim through the whole suite, which is a lot of abstraction to
// carry for one assertion.
func TestMemoryStoreConformance(t *testing.T) {
	store.CoreStoreConformance(t, func(t *testing.T) store.CoreHarness {
		clock := testkit.NewClock()
		memory := testkit.NewMemoryStore(clock)
		return store.CoreHarness{
			Store:       memory,
			Advance:     clock.Advance,
			Projections: memory.Projections,
			Reservation: memory.Reservation,
			NewWithChildFailure: func(index int) (store.Execution, func(run.ID) []run.Snapshot) {
				failing := testkit.NewMemoryStore(testkit.NewClock(), testkit.FailChildCreateAt(index))
				return failing, failing.Children
			},
		}
	})
}

func TestMemoryResourceConformance(t *testing.T) {
	store.ResourceConformance(t, func(t *testing.T) store.ResourceHarness {
		resources := testkit.NewMemoryResources(testkit.NewClock(), "publisher")
		return store.ResourceHarness{
			Reader:     resources,
			Publisher:  resources,
			Authorizer: resources,
			Head:       resources.Head,
			Audit:      resources.Audit,
		}
	})
}

func TestMemoryQuotaConformance(t *testing.T) {
	store.QuotaConformance(t, func(t *testing.T) store.QuotaHarness {
		quotas := testkit.NewMemoryQuota()
		return store.QuotaHarness{
			Enforcer:     quotas,
			Publish:      quotas.Publish,
			Metered:      quotas.Metered,
			LedgerCharge: quotas.LedgerCharge,
		}
	})
}
