package quota_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kart-io/wechat-account/agent-runtime/quota"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

type fakeMeter struct {
	usage map[quota.Unit]int
	err   error
	// asked records what was measured, to prove a check actually happened
	// rather than being skipped.
	asked []quota.Unit
}

func (m *fakeMeter) Observe(_ context.Context, _ quota.Scope, unit quota.Unit, _ time.Duration) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	m.asked = append(m.asked, unit)
	return m.usage[unit], nil
}

func tenant() quota.Scope { return quota.Scope{Tenant: "acme"} }

func enforcer(t *testing.T, meter quota.Meter, limits ...quota.Limit) *quota.Enforcer {
	t.Helper()
	enforcer, err := quota.NewEnforcer(quota.Snapshot{Limits: limits, Digest: "quota-1"}, meter)
	if err != nil {
		t.Fatalf("new enforcer: %v", err)
	}
	return enforcer
}

// Refusing at creation is cheap. Refusing later leaves a Run in the store that
// never ran, which somebody then has to triage.
func TestConcurrencyIsRefusedAtRunCreation(t *testing.T) {
	meter := &fakeMeter{usage: map[quota.Unit]int{quota.UnitConcurrentRuns: 10}}
	subject := enforcer(t, meter, quota.Limit{
		Name: "concurrency", Scope: tenant(), Unit: quota.UnitConcurrentRuns, Max: 10,
	})

	decision, err := subject.AdmitRun(t.Context(), tenant())
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if decision.Allowed {
		t.Fatal("an eleventh concurrent Run was admitted against a cap of ten")
	}
	if decision.Limit.Name != "concurrency" {
		t.Errorf("the decision does not name the cap that refused: %+v", decision)
	}
}

func TestRoomUnderTheCapIsAdmitted(t *testing.T) {
	meter := &fakeMeter{usage: map[quota.Unit]int{quota.UnitConcurrentRuns: 9}}
	subject := enforcer(t, meter, quota.Limit{
		Name: "concurrency", Scope: tenant(), Unit: quota.UnitConcurrentRuns, Max: 10,
	})

	decision, err := subject.AdmitRun(t.Context(), tenant())
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if !decision.Allowed || decision.Degraded {
		t.Fatalf("decision=%+v", decision)
	}
}

// A Run admitted an hour ago is still spending now. Checking only at creation
// lets one long Run outlive every cap in the system.
func TestWindowedConsumptionIsCheckedAtTheGateway(t *testing.T) {
	meter := &fakeMeter{usage: map[quota.Unit]int{quota.UnitTokens: 900_000}}
	subject := enforcer(t, meter, quota.Limit{
		Name: "hourly-tokens", Scope: tenant(), Unit: quota.UnitTokens,
		Max: 1_000_000, Window: time.Hour,
	})

	// The Run was admitted: admission units are not what this cap counts.
	admitted, err := subject.AdmitRun(t.Context(), tenant())
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if !admitted.Allowed {
		t.Fatal("a token cap refused a Run at creation")
	}

	decision, err := subject.AdmitEffect(t.Context(), tenant(), run.Limits{Tokens: 200_000})
	if err != nil {
		t.Fatalf("admit effect: %v", err)
	}
	if decision.Allowed {
		t.Fatalf("a call that would exceed the hourly cap was allowed: %+v", decision)
	}
}

// Running without tools is useful; running without tokens is not. Which one a
// limit does is declared rather than inferred.
func TestADegradingLimitAllowsLessRatherThanRefusing(t *testing.T) {
	meter := &fakeMeter{usage: map[quota.Unit]int{quota.UnitToolCalls: 100}}
	subject := enforcer(t, meter, quota.Limit{
		Name: "hourly-tools", Scope: tenant(), Unit: quota.UnitToolCalls,
		Max: 100, Window: time.Hour, Degrade: true,
	})

	decision, err := subject.AdmitEffect(t.Context(), tenant(), run.Limits{ToolCalls: 1})
	if err != nil {
		t.Fatalf("admit effect: %v", err)
	}
	if !decision.Allowed || !decision.Degraded {
		t.Fatalf("decision=%+v want allowed and degraded", decision)
	}
	if decision.Limit.Name != "hourly-tools" {
		t.Errorf("the degradation does not name its cap: %+v", decision)
	}
}

// A refusal outranks a degradation: the strictest applicable answer is the one
// that holds, or a degrading limit would quietly launder a hard cap.
func TestARefusalOutranksADegradation(t *testing.T) {
	meter := &fakeMeter{usage: map[quota.Unit]int{
		quota.UnitToolCalls: 100, quota.UnitTokens: 1_000_000,
	}}
	subject := enforcer(
		t, meter,
		quota.Limit{
			Name: "a-tools", Scope: tenant(), Unit: quota.UnitToolCalls,
			Max: 100, Window: time.Hour, Degrade: true,
		},
		quota.Limit{
			Name: "b-tokens", Scope: tenant(), Unit: quota.UnitTokens,
			Max: 1_000_000, Window: time.Hour,
		},
	)

	decision, err := subject.AdmitEffect(t.Context(), tenant(), run.Limits{ToolCalls: 1, Tokens: 10})
	if err != nil {
		t.Fatalf("admit effect: %v", err)
	}
	if decision.Allowed {
		t.Fatalf("decision=%+v; the hard cap must win", decision)
	}
}

// The snapshot is pinned. A quota published while an effect is in flight
// applies to the next Run — revoking a reservation already being spent would
// leave the ledger describing a charge the enforcer says never happened.
func TestAPinnedSnapshotDoesNotChangeUnderAnInFlightRun(t *testing.T) {
	meter := &fakeMeter{usage: map[quota.Unit]int{quota.UnitTokens: 100}}
	pinned := enforcer(t, meter, quota.Limit{
		Name: "hourly-tokens", Scope: tenant(), Unit: quota.UnitTokens,
		Max: 1_000, Window: time.Hour,
	})

	before, err := pinned.AdmitEffect(t.Context(), tenant(), run.Limits{Tokens: 500})
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if !before.Allowed {
		t.Fatal("the effect was refused under its own snapshot")
	}

	// An operator publishes a far stricter quota. The in-flight Run holds its
	// own snapshot and does not see it.
	_ = enforcer(t, meter, quota.Limit{
		Name: "hourly-tokens", Scope: tenant(), Unit: quota.UnitTokens,
		Max: 10, Window: time.Hour,
	})

	after, err := pinned.AdmitEffect(t.Context(), tenant(), run.Limits{Tokens: 500})
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if after.Allowed != before.Allowed {
		t.Fatal("the pinned snapshot changed under an in-flight Run")
	}
	if pinned.Digest() != "quota-1" {
		t.Errorf("digest=%q; a decision must name the set it came from", pinned.Digest())
	}
}

// A meter that cannot answer fails closed. Treating an unreachable meter as
// "no usage" removes every cap at the moment the system is already unhealthy.
func TestAnUnreadableMeterRefuses(t *testing.T) {
	meter := &fakeMeter{err: errors.New("counter store down")}
	subject := enforcer(t, meter, quota.Limit{
		Name: "concurrency", Scope: tenant(), Unit: quota.UnitConcurrentRuns, Max: 10,
	})

	_, err := subject.AdmitRun(t.Context(), tenant())
	if run.KindOf(err) != run.ErrorDenied {
		t.Fatalf("kind=%s want=denied", run.KindOf(err))
	}
	if run.RetryOf(err) != run.RetryBackoff {
		t.Errorf("retry=%s; an unhealthy meter is worth retrying", run.RetryOf(err))
	}
}

func TestLimitsApplyOnlyToTheirScope(t *testing.T) {
	meter := &fakeMeter{usage: map[quota.Unit]int{quota.UnitConcurrentRuns: 100}}
	subject := enforcer(t, meter, quota.Limit{
		Name: "other-tenant", Scope: quota.Scope{Tenant: "other"},
		Unit: quota.UnitConcurrentRuns, Max: 1,
	})

	decision, err := subject.AdmitRun(t.Context(), tenant())
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if !decision.Allowed {
		t.Fatal("another tenant's cap refused this tenant's Run")
	}
	if len(meter.asked) != 0 {
		t.Errorf("an inapplicable limit was measured: %v", meter.asked)
	}
}

func TestAPrincipalLimitNarrowsWithinATenant(t *testing.T) {
	meter := &fakeMeter{usage: map[quota.Unit]int{quota.UnitConcurrentRuns: 5}}
	subject := enforcer(t, meter, quota.Limit{
		Name: "per-user", Scope: quota.Scope{Tenant: "acme", Principal: "alice"},
		Unit: quota.UnitConcurrentRuns, Max: 5,
	})

	forAlice, err := subject.AdmitRun(t.Context(), quota.Scope{Tenant: "acme", Principal: "alice"})
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if forAlice.Allowed {
		t.Fatal("alice was admitted past her own cap")
	}

	forBob, err := subject.AdmitRun(t.Context(), quota.Scope{Tenant: "acme", Principal: "bob"})
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if !forBob.Allowed {
		t.Fatal("alice's cap refused bob")
	}
}

func TestMalformedQuotaSetsAreRefusedAtConstruction(t *testing.T) {
	cases := map[string]quota.Limit{
		"unnamed":      {Unit: quota.UnitConcurrentRuns, Max: 1},
		"negative max": {Name: "a", Unit: quota.UnitConcurrentRuns, Max: -1},
		"unknown unit": {Name: "a", Unit: "guesses", Max: 1},
		"no window":    {Name: "a", Unit: quota.UnitTokens, Max: 1},
	}
	for name, limit := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := quota.NewEnforcer(quota.Snapshot{Limits: []quota.Limit{limit}}, &fakeMeter{}); err == nil {
				t.Fatal("a malformed quota was pinned")
			}
		})
	}
}

// Degrading means dropping the tool loop, so only a tool-call cap can ask for
// it. A token cap that asked would be silently ignored at the gateway, and an
// operator reading the published set would believe a ceiling exists that never
// stops anything.
func TestOnlyToolCallsMayAskToDegrade(t *testing.T) {
	limit := quota.Limit{
		Name: "hourly-tokens", Scope: tenant(), Unit: quota.UnitTokens,
		Max: 100, Window: time.Hour, Degrade: true,
	}

	_, err := quota.NewEnforcer(quota.Snapshot{Limits: []quota.Limit{limit}}, &fakeMeter{})
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) || runtimeError.Code != "undegradable_unit" {
		t.Fatalf("err=%v want undegradable_unit", err)
	}
	if runtimeError.Kind != run.ErrorInvalid || runtimeError.Retry != run.RetryNever {
		t.Errorf("kind=%v retry=%v", runtimeError.Kind, runtimeError.Retry)
	}
}

// The one unit that may: refusing this would take the only degrading limit the
// deployment publishes with it.
func TestAToolCallLimitMayAskToDegrade(t *testing.T) {
	limit := quota.Limit{
		Name: "hourly-tools", Scope: tenant(), Unit: quota.UnitToolCalls,
		Max: 100, Window: time.Hour, Degrade: true,
	}

	if _, err := quota.NewEnforcer(quota.Snapshot{Limits: []quota.Limit{limit}}, &fakeMeter{}); err != nil {
		t.Fatalf("a tool-call limit was refused for degrading: %v", err)
	}
}

func TestDuplicateQuotaNamesAreRefused(t *testing.T) {
	limit := quota.Limit{Name: "a", Scope: tenant(), Unit: quota.UnitConcurrentRuns, Max: 1}

	_, err := quota.NewEnforcer(quota.Snapshot{Limits: []quota.Limit{limit, limit}}, &fakeMeter{})
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) || runtimeError.Code != "duplicate_quota" {
		t.Fatalf("err=%v", err)
	}
}

func TestAMissingMeterIsRefused(t *testing.T) {
	if _, err := quota.NewEnforcer(quota.Snapshot{}, nil); err == nil {
		t.Fatal("an enforcer with nothing to measure was built")
	}
}

// Two applicable limits must always report the same one, or the same overload
// produces a different explanation on every worker.
func TestRefusalsAreDeterministic(t *testing.T) {
	meter := &fakeMeter{usage: map[quota.Unit]int{quota.UnitTokens: 1_000}}
	subject := enforcer(
		t, meter,
		quota.Limit{Name: "z-cap", Scope: tenant(), Unit: quota.UnitTokens, Max: 1, Window: time.Hour},
		quota.Limit{Name: "a-cap", Scope: tenant(), Unit: quota.UnitTokens, Max: 2, Window: time.Hour},
	)

	for range 20 {
		decision, err := subject.AdmitEffect(t.Context(), tenant(), run.Limits{Tokens: 10})
		if err != nil {
			t.Fatalf("admit: %v", err)
		}
		if decision.Limit.Name != "a-cap" {
			t.Fatalf("limit=%q; refusals must be reported in a fixed order", decision.Limit.Name)
		}
	}
}
