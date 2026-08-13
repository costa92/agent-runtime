// Package quota bounds what a tenant may consume, at the two points where the
// answer can still change something.
//
// Quota is not the Run budget. A budget bounds one Run against an envelope its
// creator agreed to; a quota bounds a tenant against capacity the deployment
// has. A Run can be perfectly within its budget and still be the run that
// exhausts the tenant's tokens for the hour, and only one of those two checks
// would catch it.
package quota

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Unit is what a limit counts.
type Unit string

const (
	// UnitConcurrentRuns and UnitQueuedRuns are admission units: they are
	// checked when a Run is created, because that is the last moment refusing
	// is free.
	UnitConcurrentRuns Unit = "concurrent_runs"
	UnitQueuedRuns     Unit = "queued_runs"
	// UnitTokens and UnitToolCalls are windowed consumption units, checked at
	// the gateway. A long-running Run that was admitted an hour ago has to be
	// re-checked, or a single Run outlives every cap in the system.
	UnitTokens    Unit = "tokens"
	UnitToolCalls Unit = "tool_calls"
)

// Admission reports whether a unit is checked at Run creation.
func (u Unit) Admission() bool {
	return u == UnitConcurrentRuns || u == UnitQueuedRuns
}

// Scope is who a limit applies to.
type Scope struct {
	Tenant string
	// Principal narrows a limit to one actor. Empty means the whole tenant.
	Principal string
}

func (s Scope) matches(other Scope) bool {
	if s.Tenant != "" && s.Tenant != other.Tenant {
		return false
	}
	if s.Principal != "" && s.Principal != other.Principal {
		return false
	}
	return true
}

func (s Scope) String() string {
	if s.Principal == "" {
		return "tenant:" + s.Tenant
	}
	return "tenant:" + s.Tenant + "/principal:" + s.Principal
}

// Limit is one published cap.
type Limit struct {
	Name  string `json:"name"`
	Scope Scope  `json:"scope"`
	Unit  Unit   `json:"unit"`
	Max   int    `json:"max"`
	// Window is the period a consumption unit is measured over. Ignored for
	// admission units, which are instantaneous counts.
	Window time.Duration `json:"window,omitempty"`
	// Degrade asks the Runtime to do less rather than refuse: drop the tool
	// loop, take the cheaper model. It is declared per limit because the right
	// answer differs — running without tools is useful, running without tokens
	// is not.
	Degrade bool `json:"degrade,omitempty"`
}

func (l Limit) validate() error {
	if l.Name == "" {
		return run.NewError("unnamed_quota", run.ErrorInvalid, run.RetryNever)
	}
	if l.Max < 0 {
		return run.NewError("negative_quota", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("quota %q", l.Name))
	}
	switch l.Unit {
	case UnitConcurrentRuns, UnitQueuedRuns, UnitTokens, UnitToolCalls:
	default:
		return run.NewError("unknown_quota_unit", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("quota %q counts %q", l.Name, l.Unit))
	}
	if !l.Unit.Admission() && l.Window <= 0 {
		// A consumption cap with no window is a lifetime cap that nothing ever
		// resets, which is never what anybody meant by "1M tokens".
		return run.NewError("unwindowed_quota", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("quota %q needs a window", l.Name))
	}
	return nil
}

// Snapshot is the published quota set a Run is judged against.
//
// It is a value and it is pinned for the Run's life, exactly like the Policy
// snapshot. A quota published mid-flight applies to the next Run: retroactively
// revoking a reservation an effect is already spending against would leave the
// ledger describing a charge the enforcer says never should have happened.
type Snapshot struct {
	Limits []Limit
	Digest string
}

// Validate checks the whole published set.
func (s Snapshot) Validate() error {
	seen := map[string]bool{}
	for _, limit := range s.Limits {
		if err := limit.validate(); err != nil {
			return err
		}
		if seen[limit.Name] {
			return run.NewError("duplicate_quota", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("quota %q declared twice", limit.Name))
		}
		seen[limit.Name] = true
	}
	return nil
}

// Meter is the host's count of what a scope has already used.
//
// The Runtime does not keep these counts: they are per-tenant, cross-process,
// and must survive a restart, which makes them the host's storage problem. What
// the Runtime owns is when to ask and what to do with the answer.
type Meter interface {
	// Observe reports current usage of a unit in a scope. For a windowed unit,
	// window is the period to measure; for an admission unit it is zero.
	Observe(ctx context.Context, scope Scope, unit Unit, window time.Duration) (int, error)
}

// Decision is one enforcement answer.
type Decision struct {
	// Allowed is false only for a refusal. A degraded decision is allowed:
	// something still runs, just less of it.
	Allowed  bool
	Degraded bool
	// Limit is the cap that decided, empty when nothing was near the line.
	Limit    Limit
	Observed int
	Want     int
}

// Enforcer applies a pinned Snapshot at both enforcement points.
type Enforcer struct {
	snapshot Snapshot
	meter    Meter
}

// NewEnforcer pins a snapshot. It validates it here rather than trusting the
// publisher, because an invalid limit would otherwise be discovered as a
// refusal nobody can explain.
func NewEnforcer(snapshot Snapshot, meter Meter) (*Enforcer, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	if meter == nil {
		return nil, run.NewError("missing_quota_meter", run.ErrorInternal, run.RetryNever)
	}
	return &Enforcer{snapshot: snapshot, meter: meter}, nil
}

// Digest identifies the pinned set, for the decision event.
func (e *Enforcer) Digest() string { return e.snapshot.Digest }

// AdmitRun is the creation-time check: concurrency and queue depth.
//
// Refusing here is cheap. Refusing after the Run exists means a Run in the
// store that never ran, which every operator then has to triage.
func (e *Enforcer) AdmitRun(ctx context.Context, scope Scope) (Decision, error) {
	return e.check(ctx, scope, func(unit Unit) int {
		if unit.Admission() {
			return 1
		}
		return 0
	})
}

// AdmitEffect is the gateway check, made against what the effect is about to
// spend.
//
// It happens per effect rather than once per Run because a Run can outlive any
// window: admitted at noon, still calling models at one, and the one-o'clock
// cap has to apply to it too.
func (e *Enforcer) AdmitEffect(ctx context.Context, scope Scope, want run.Limits) (Decision, error) {
	return e.check(ctx, scope, func(unit Unit) int {
		switch unit {
		case UnitTokens:
			return want.Tokens
		case UnitToolCalls:
			return want.ToolCalls
		default:
			return 0
		}
	})
}

// check walks the applicable limits in a fixed order and returns the first
// refusal, or the strictest degradation.
func (e *Enforcer) check(ctx context.Context, scope Scope, wantFor func(Unit) int) (Decision, error) {
	limits := e.applicable(scope)
	degraded := Decision{Allowed: true}

	for _, limit := range limits {
		want := wantFor(limit.Unit)
		if want == 0 {
			continue
		}
		observed, err := e.meter.Observe(ctx, scope, limit.Unit, limit.Window)
		if err != nil {
			// A meter that cannot answer fails closed. Treating an unreachable
			// meter as "no usage" removes every cap at exactly the moment the
			// system is already unhealthy.
			return Decision{}, run.NewError("quota_unreadable", run.ErrorDenied, run.RetryBackoff,
				fmt.Errorf("quota %q: %w", limit.Name, err))
		}
		if observed+want <= limit.Max {
			continue
		}

		decision := Decision{Limit: limit, Observed: observed, Want: want}
		if !limit.Degrade {
			return decision, nil
		}
		decision.Allowed = true
		decision.Degraded = true
		degraded = decision
	}
	return degraded, nil
}

// applicable returns the limits for a scope in a stable order, so that two
// limits that would both refuse always report the same one.
func (e *Enforcer) applicable(scope Scope) []Limit {
	var limits []Limit
	for _, limit := range e.snapshot.Limits {
		if limit.Scope.matches(scope) {
			limits = append(limits, limit)
		}
	}
	sort.Slice(limits, func(i, j int) bool { return limits[i].Name < limits[j].Name })
	return limits
}
