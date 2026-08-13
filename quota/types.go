// Package quota is the per-tenant limit, which is a different question from
// the per-Run budget envelope.
//
// A budget bounds one Run. A quota bounds a tenant across all of them. Neither
// substitutes for the other: a tenant can exhaust a cluster with a thousand
// perfectly well-behaved Runs, and a single Run can run away inside a tenant
// that is nowhere near its ceiling.
package quota

import (
	"fmt"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Window is the period a usage limit is measured over.
type Window string

const (
	WindowDay   Window = "day"
	WindowMonth Window = "month"
)

func (w Window) Valid() bool { return w == WindowDay || w == WindowMonth }

// Outcome is what happens when a check fails. Silently dropping is not among
// them: an over-quota Run must produce a decision event naming the limit, or
// the tenant cannot tell "rejected" from "lost".
type Outcome string

const (
	OutcomeReject Outcome = "reject"
	OutcomeQueue  Outcome = "queue"
	// OutcomeDegrade proceeds with a reduced capability — a cheaper model, no
	// delegation — rather than refusing outright.
	OutcomeDegrade Outcome = "degrade"
)

func (o Outcome) Valid() bool {
	switch o {
	case OutcomeReject, OutcomeQueue, OutcomeDegrade:
		return true
	default:
		return false
	}
}

// UsageLimit caps metered usage over a window.
//
// The units are usage units, not money. Cost limits are decided by pricing the
// ledger's usage read-only at the gateway; the ledger itself never holds
// amounts, so that there is exactly one place a currency figure is produced.
type UsageLimit struct {
	Window    Window `json:"window"`
	Tokens    int    `json:"tokens,omitempty"`
	LLMCalls  int    `json:"llm_calls,omitempty"`
	ToolCalls int    `json:"tool_calls,omitempty"`
}

// Quota is one tenant's ceiling.
type Quota struct {
	Name   string `json:"name"`
	Tenant string `json:"tenant"`

	MaxConcurrentRuns int `json:"max_concurrent_runs,omitempty"`
	MaxQueuedRuns     int `json:"max_queued_runs,omitempty"`

	Usage []UsageLimit `json:"usage,omitempty"`

	// MaxRunBudget caps what any single Definition may ask for, which is what
	// stops one Run from being written to consume the whole tenant.
	MaxRunBudget run.Limits `json:"max_run_budget,omitzero"`

	// Allowed scopes. An empty list means unrestricted rather than empty: a
	// quota that has to enumerate every tool would be unmaintainable, and the
	// tenant-wide switch is the concurrency and usage limits above.
	AllowedDefinitions []string `json:"allowed_definitions,omitempty"`
	AllowedTools       []string `json:"allowed_tools,omitempty"`
	AllowedMemories    []string `json:"allowed_memories,omitempty"`
	AllowedModels      []string `json:"allowed_models,omitempty"`
	AllowedTargetHosts []string `json:"allowed_target_hosts,omitempty"`

	// OnExceeded is required: leaving it unset would make the behaviour of a
	// breach implicit, and that is the moment behaviour most needs to be
	// declared.
	OnExceeded Outcome `json:"on_exceeded"`
}

// Validate rejects a quota that could not be enforced as written.
func (q Quota) Validate() error {
	if q.Name == "" {
		return run.NewError("missing_name", run.ErrorInvalid, run.RetryNever)
	}
	if q.Tenant == "" {
		return run.NewError("missing_tenant", run.ErrorInvalid, run.RetryNever)
	}
	if !q.OnExceeded.Valid() {
		return run.NewError("unknown_outcome", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("on_exceeded %q", q.OnExceeded))
	}
	if q.MaxConcurrentRuns < 0 || q.MaxQueuedRuns < 0 {
		return run.NewError("negative_limit", run.ErrorInvalid, run.RetryNever)
	}
	seen := map[Window]bool{}
	for _, limit := range q.Usage {
		if !limit.Window.Valid() {
			return run.NewError("unknown_window", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("window %q", limit.Window))
		}
		if seen[limit.Window] {
			// Two limits for one window would need a precedence rule, and the
			// stricter-wins rule people assume is not what a reader can see.
			return run.NewError("duplicate_window", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("window %q declared twice", limit.Window))
		}
		seen[limit.Window] = true
		if limit.Tokens < 0 || limit.LLMCalls < 0 || limit.ToolCalls < 0 {
			return run.NewError("negative_limit", run.ErrorInvalid, run.RetryNever)
		}
	}
	return nil
}
