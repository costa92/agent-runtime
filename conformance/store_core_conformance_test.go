package conformance_test

import (
	"testing"

	"github.com/costa92/agent-runtime/conformance"
	"github.com/costa92/agent-runtime/internal/testkit"
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
	conformance.CoreStore(t, func(t *testing.T) conformance.CoreHarness {
		clock := testkit.NewClock()
		memory := testkit.NewMemoryStore(clock)
		return conformance.CoreHarness{
			Store:       memory,
			Advance:     clock.Advance,
			Projections: memory.Projections,
			Reservation: memory.Reservation,
		}
	})
}

func TestMemoryResourceConformance(t *testing.T) {
	conformance.Resource(t, func(t *testing.T) conformance.ResourceHarness {
		resources := testkit.NewMemoryResources(testkit.NewClock(), "publisher")
		return conformance.ResourceHarness{
			Reader:     resources,
			Publisher:  resources,
			Authorizer: resources,
			Head:       resources.Head,
			Audit:      resources.Audit,
		}
	})
}

func TestMemoryQuotaConformance(t *testing.T) {
	conformance.Quota(t, func(t *testing.T) conformance.QuotaHarness {
		quotas := testkit.NewMemoryQuota()
		return conformance.QuotaHarness{
			Enforcer:     quotas,
			Publish:      quotas.Publish,
			Metered:      quotas.Metered,
			LedgerCharge: quotas.LedgerCharge,
		}
	})
}
