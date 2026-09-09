package store

import (
	"context"

	"github.com/kart-io/wechat-account/agent-runtime/quota"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// QuotaDecision is the outcome of one quota check, with the limit that produced
// it. The limit is part of the decision rather than a log line: a tenant that is
// being throttled has to be able to see which ceiling it hit, and an over-quota
// Run that produced no decision is indistinguishable from a lost one.
type QuotaDecision struct {
	Allowed bool
	Outcome quota.Outcome
	// Limit names the ceiling — "max_concurrent_runs", "tokens/day".
	Limit string
}

// QuotaCheckpoint distinguishes the two places a tenant limit is enforced.
//
// Both are needed and neither substitutes for the other: the creation check
// stops a queue flood before it starts, and the gateway check stops a long-
// running Run from overspending slowly, which the creation check has no further
// opportunity to see.
type QuotaCheckpoint string

const (
	CheckpointRunCreate QuotaCheckpoint = "run_create"
	CheckpointGateway   QuotaCheckpoint = "gateway"
)

// QuotaEnforcer is the port a host implements to enforce tenant limits.
type QuotaEnforcer interface {
	// Check judges one attempt. Spend is what the attempt would consume; at
	// run-create time it is the Run's requested envelope.
	Check(ctx context.Context, checkpoint QuotaCheckpoint, tenant string, spend run.Limits) (QuotaDecision, error)
	// Reserve records in-flight usage against the tenant. It shares the Root
	// Budget Ledger's metering rather than keeping a second count.
	Reserve(ctx context.Context, tenant string, reservation BudgetReservation) error
}
