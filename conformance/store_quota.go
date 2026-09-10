package conformance

import (
	"context"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/quota"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
)

// QuotaHarness is what a quota adapter supplies.
type QuotaHarness struct {
	Enforcer store.QuotaEnforcer
	// Publish installs a quota for the tenant, as an operator would.
	Publish func(q quota.Quota) error
	// Metered returns the usage the enforcer has counted for a tenant, so the
	// suite can prove it is the ledger's number and not a second one.
	Metered func(tenant string) run.Limits
	// LedgerCharge records a charge through the Root Budget Ledger, bypassing
	// the quota enforcer entirely. If the two share metering, the enforcer sees
	// it; if the enforcer keeps its own count, it does not.
	LedgerCharge func(tenant string, amount run.Limits)
}

// Quota checks tenant limits.
func Quota(t *testing.T, newHarness func(t *testing.T) QuotaHarness) {
	t.Helper()

	t.Run("ConcurrencyAndQueueCapsApplyAtRunCreate", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		if err := harness.Publish(quota.Quota{
			Name: "q", Tenant: "t-1", MaxConcurrentRuns: 1, OnExceeded: quota.OutcomeReject,
		}); err != nil {
			t.Fatalf("publish quota: %v", err)
		}

		first, err := harness.Enforcer.Check(ctx, store.CheckpointRunCreate, "t-1", run.Limits{})
		if err != nil || !first.Allowed {
			t.Fatalf("the first Run was refused: %+v %v", first, err)
		}
		if err := harness.Enforcer.Reserve(ctx, "t-1", store.BudgetReservation{ID: "run-1"}); err != nil {
			t.Fatalf("reserve: %v", err)
		}

		second, err := harness.Enforcer.Check(ctx, store.CheckpointRunCreate, "t-1", run.Limits{})
		if err != nil {
			t.Fatalf("check: %v", err)
		}
		if second.Allowed {
			t.Fatal("the concurrency cap did not apply at Run creation")
		}
		if second.Limit == "" {
			t.Error("an over-quota decision did not name the limit it hit")
		}
		if second.Outcome != quota.OutcomeReject {
			t.Errorf("outcome=%s want=reject (as declared)", second.Outcome)
		}
	})

	t.Run("WindowedUsageCapsApplyAtTheGateway", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		if err := harness.Publish(quota.Quota{
			Name: "q", Tenant: "t-1", OnExceeded: quota.OutcomeDegrade,
			Usage: []quota.UsageLimit{{Window: quota.WindowDay, Tokens: 1000}},
		}); err != nil {
			t.Fatalf("publish quota: %v", err)
		}

		// A long Run overspends slowly; the creation check is long past and
		// has no further opportunity to see it.
		if err := harness.Enforcer.Reserve(ctx, "t-1", store.BudgetReservation{
			ID: "res-1", Amount: run.Limits{Tokens: 900},
		}); err != nil {
			t.Fatalf("reserve: %v", err)
		}

		decision, err := harness.Enforcer.Check(ctx, store.CheckpointGateway, "t-1", run.Limits{Tokens: 200})
		if err != nil {
			t.Fatalf("check: %v", err)
		}
		if decision.Allowed {
			t.Fatal("the windowed cap did not apply at the gateway")
		}
		if decision.Outcome != quota.OutcomeDegrade {
			t.Errorf("outcome=%s want=degrade (as declared)", decision.Outcome)
		}
	})

	t.Run("MeteringIsSharedWithTheRootBudgetLedger", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		if err := harness.Publish(quota.Quota{
			Name: "q", Tenant: "t-1", OnExceeded: quota.OutcomeReject,
			Usage: []quota.UsageLimit{{Window: quota.WindowDay, Tokens: 1000}},
		}); err != nil {
			t.Fatalf("publish quota: %v", err)
		}

		// Charged through the ledger, never through the enforcer. A second,
		// enforcer-private counter would not see this — and the tenant would
		// be allowed to spend its ceiling twice.
		harness.LedgerCharge("t-1", run.Limits{Tokens: 950})

		if metered := harness.Metered("t-1"); metered.Tokens != 950 {
			t.Fatalf("the enforcer counted %d tokens, the ledger charged 950; it keeps a second count",
				metered.Tokens)
		}
		decision, err := harness.Enforcer.Check(ctx, store.CheckpointGateway, "t-1", run.Limits{Tokens: 100})
		if err != nil {
			t.Fatalf("check: %v", err)
		}
		if decision.Allowed {
			t.Fatal("ledger-charged usage did not count against the quota")
		}
	})

	t.Run("FailedAndUnknownSpendStillCounts", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		if err := harness.Publish(quota.Quota{
			Name: "q", Tenant: "t-1", OnExceeded: quota.OutcomeReject,
			Usage: []quota.UsageLimit{{Window: quota.WindowDay, Tokens: 1000}},
		}); err != nil {
			t.Fatalf("publish quota: %v", err)
		}

		// A failed call still cost tokens. A ledger that only counted successes
		// would understate exactly the spend it exists to bound.
		harness.LedgerCharge("t-1", run.Limits{Tokens: 1200})

		decision, err := harness.Enforcer.Check(ctx, store.CheckpointGateway, "t-1", run.Limits{Tokens: 1})
		if err != nil {
			t.Fatalf("check: %v", err)
		}
		if decision.Allowed {
			t.Fatal("spend from a failed call was not counted")
		}
	})

	t.Run("QuotaChangesDoNotRevokeInFlightReservations", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		if err := harness.Publish(quota.Quota{
			Name: "q", Tenant: "t-1", OnExceeded: quota.OutcomeReject,
			Usage: []quota.UsageLimit{{Window: quota.WindowDay, Tokens: 10000}},
		}); err != nil {
			t.Fatalf("publish quota: %v", err)
		}
		if err := harness.Enforcer.Reserve(ctx, "t-1", store.BudgetReservation{
			ID: "res-1", Amount: run.Limits{Tokens: 5000},
		}); err != nil {
			t.Fatalf("reserve: %v", err)
		}

		// Tightening the quota applies to the next checkpoint. Retroactively
		// revoking a granted reservation would kill Runs that were within their
		// limit when they started, for a change they could not have seen.
		if err := harness.Publish(quota.Quota{
			Name: "q", Tenant: "t-1", OnExceeded: quota.OutcomeReject,
			Usage: []quota.UsageLimit{{Window: quota.WindowDay, Tokens: 1000}},
		}); err != nil {
			t.Fatalf("republish quota: %v", err)
		}

		if metered := harness.Metered("t-1"); metered.Tokens != 5000 {
			t.Fatalf("the in-flight reservation was revoked: metered %d want 5000", metered.Tokens)
		}
		decision, err := harness.Enforcer.Check(ctx, store.CheckpointGateway, "t-1", run.Limits{Tokens: 1})
		if err != nil {
			t.Fatalf("check: %v", err)
		}
		if decision.Allowed {
			t.Error("the tightened quota did not apply to the next checkpoint")
		}
	})
}
