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
	// perScope answers for one named scope and takes precedence over usage.
	// Without it a fake cannot tell "measured across the tenant" from
	// "measured for this caller" — which is how the two were confused for as
	// long as they were.
	perScope map[quota.Scope]map[quota.Unit]int
	err      error
	// asked records what was measured, to prove a check actually happened
	// rather than being skipped.
	asked []quota.Unit
	// scopes records which scope each observation named.
	scopes []quota.Scope
}

func (m *fakeMeter) Observe(
	_ context.Context, scope quota.Scope, unit quota.Unit, _ time.Duration,
) (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	m.asked = append(m.asked, unit)
	m.scopes = append(m.scopes, scope)
	if at, ok := m.perScope[scope]; ok {
		return at[unit], nil
	}
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

// A want of zero skips the limit entirely, however far over the cap the scope
// already is.
//
// This is `check`'s "asking for none of a unit is not a request against it"
// rule, and on its own it is correct. It is pinned here because of what it
// costs elsewhere: the Runtime reserves `Tokens: request.MaxTokens`, that value
// comes from the published Definition, and no Definition in the host sets it —
// so every model call asks for zero tokens and every token limit is skipped
// before its usage is ever read.
//
// The consequence is that a token limit has TWO independent ways to do nothing:
// nobody records usage (so observed is zero), and nobody asks for tokens (so
// the limit is never reached). Fixing either alone changes nothing, which is
// the trap this test exists to keep visible. See TD-056.
func TestAskingForNoneOfAUnitSkipsItsLimit(t *testing.T) {
	meter := &fakeMeter{usage: map[quota.Unit]int{quota.UnitTokens: 10_000_000}}
	subject := enforcer(t, meter, quota.Limit{
		Name: "hourly-tokens", Scope: tenant(), Unit: quota.UnitTokens,
		Max: 1, Window: time.Hour,
	})

	// Ten million tokens spent against a cap of one, and this is allowed.
	decision, err := subject.AdmitEffect(t.Context(), tenant(), run.Limits{Tokens: 0, LLMCalls: 1})
	if err != nil {
		t.Fatalf("admit effect: %v", err)
	}
	if !decision.Allowed {
		t.Fatalf("a zero-token request was judged against a token cap: %+v", decision)
	}

	// The same call asking for one token is refused, which is what makes the
	// line above a statement about `want` and not about the meter.
	decision, err = subject.AdmitEffect(t.Context(), tenant(), run.Limits{Tokens: 1, LLMCalls: 1})
	if err != nil {
		t.Fatalf("admit effect: %v", err)
	}
	if decision.Allowed {
		t.Fatalf("want=1 should have been refused against Max=1 with usage spent: %+v", decision)
	}
}

// A cap declared for a tenant is measured across the tenant, not once per
// principal.
//
// The two readings differ by exactly the number of principals: measured at the
// caller's scope, a "tenant cap" of 100 hands 100 to everyone who asks. Nothing
// used to distinguish them, because the limits under test always had a scope
// equal to the caller's — and the fake ignored the scope anyway.
func TestATenantCapIsMeasuredAcrossTheTenant(t *testing.T) {
	alice := quota.Scope{Tenant: "acme", Principal: "alice"}
	meter := &fakeMeter{perScope: map[quota.Scope]map[quota.Unit]int{
		// Alice alone is well under; the tenant as a whole is at the line.
		alice:    {quota.UnitConcurrentRuns: 3},
		tenant(): {quota.UnitConcurrentRuns: 100},
	}}
	subject := enforcer(t, meter, quota.Limit{
		Name: "tenant-runs", Scope: tenant(),
		Unit: quota.UnitConcurrentRuns, Max: 100,
	})

	decision, err := subject.AdmitRun(t.Context(), alice)
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if decision.Allowed {
		t.Fatal("alice was admitted while the tenant was already at its cap")
	}
	if len(meter.scopes) != 1 || meter.scopes[0] != tenant() {
		t.Fatalf("measured %v, want the limit's own scope %v", meter.scopes, tenant())
	}
}

// The narrower form still narrows: a principal-scoped limit is measured for
// that principal, and does not see the tenant's other traffic.
func TestAPrincipalCapIsMeasuredForThatPrincipalAlone(t *testing.T) {
	alice := quota.Scope{Tenant: "acme", Principal: "alice"}
	meter := &fakeMeter{perScope: map[quota.Scope]map[quota.Unit]int{
		alice:    {quota.UnitConcurrentRuns: 1},
		tenant(): {quota.UnitConcurrentRuns: 100},
	}}
	subject := enforcer(t, meter, quota.Limit{
		Name: "alice-runs", Scope: alice,
		Unit: quota.UnitConcurrentRuns, Max: 5,
	})

	decision, err := subject.AdmitRun(t.Context(), alice)
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if !decision.Allowed {
		t.Fatal("a principal cap was refused on the tenant's usage")
	}
}

// A limit with no tenant cannot be measured, so it must not be publishable.
//
// Accepting it would produce a cap that fails closed the first time it applied
// to anything — the meter is asked for a named tenant and an unanswerable
// observation is a refusal.
func TestALimitWithNoTenantIsRefusedAtPublication(t *testing.T) {
	invalid := quota.Snapshot{Limits: []quota.Limit{{
		Name: "everyone", Unit: quota.UnitConcurrentRuns, Max: 1,
	}}}
	err := invalid.Validate()
	if run.CodeOf(err) != "unscoped_quota" {
		t.Fatalf("code = %q, want unscoped_quota (%v)", run.CodeOf(err), err)
	}
}

// PerPrincipal is the third reading: one published row, one allowance each.
//
// The distinction that matters is against TestATenantCapIsMeasuredAcrossTheTenant
// above — same tenant scope on the row, opposite measurement — because the two
// differ by exactly the number of principals, and getting it wrong silently
// either removes the tenant's ceiling or charges one user for everybody.
func TestAPerPrincipalCapIsMeasuredForTheCallerNotTheTenant(t *testing.T) {
	alice := quota.Scope{Tenant: "acme", Principal: "alice"}
	meter := &fakeMeter{perScope: map[quota.Scope]map[quota.Unit]int{
		// Alice has room; the tenant as a whole is far past this number.
		alice:    {quota.UnitTokens: 10},
		tenant(): {quota.UnitTokens: 9_000},
	}}
	subject := enforcer(t, meter, quota.Limit{
		Name: "user-tokens", Scope: tenant(), PerPrincipal: true,
		Unit: quota.UnitTokens, Max: 100, Window: time.Hour,
	})

	decision, err := subject.AdmitEffect(t.Context(), alice, run.Limits{Tokens: 1})
	if err != nil {
		t.Fatalf("admit effect: %v", err)
	}
	if !decision.Allowed {
		t.Fatalf("alice was refused on the tenant's spend rather than her own: %+v", decision)
	}
	if len(meter.scopes) != 1 || meter.scopes[0] != alice {
		t.Fatalf("measured %v, want the caller's scope %v", meter.scopes, alice)
	}
}

// And it still refuses: the allowance is per caller, not absent.
func TestAPerPrincipalCapRefusesTheCallerWhoSpentIt(t *testing.T) {
	alice := quota.Scope{Tenant: "acme", Principal: "alice"}
	meter := &fakeMeter{perScope: map[quota.Scope]map[quota.Unit]int{
		alice: {quota.UnitTokens: 100},
	}}
	subject := enforcer(t, meter, quota.Limit{
		Name: "user-tokens", Scope: tenant(), PerPrincipal: true,
		Unit: quota.UnitTokens, Max: 100, Window: time.Hour,
	})

	decision, err := subject.AdmitEffect(t.Context(), alice, run.Limits{Tokens: 1})
	if err != nil {
		t.Fatalf("admit effect: %v", err)
	}
	if decision.Allowed {
		t.Fatalf("alice was admitted after spending her whole allowance: %+v", decision)
	}
}

// A row that already names one principal cannot also be per-principal: the two
// readings differ and publication is the last moment anybody is reading.
func TestAPerPrincipalCapMayNotAlsoNameAPrincipal(t *testing.T) {
	invalid := quota.Snapshot{Limits: []quota.Limit{{
		Name:  "confused",
		Scope: quota.Scope{Tenant: "acme", Principal: "alice"}, PerPrincipal: true,
		Unit: quota.UnitTokens, Max: 100, Window: time.Hour,
	}}}
	if err := invalid.Validate(); run.CodeOf(err) != "overscoped_quota" {
		t.Fatalf("Validate() = %v, want overscoped_quota", err)
	}
}

// Admission units are counted live off the Run table, not summed per principal.
// Publishing one would measure the tenant while claiming to measure the caller.
func TestAnAdmissionUnitCannotBePerPrincipal(t *testing.T) {
	invalid := quota.Snapshot{Limits: []quota.Limit{{
		Name: "per-user-runs", Scope: tenant(), PerPrincipal: true,
		Unit: quota.UnitConcurrentRuns, Max: 4,
	}}}
	if err := invalid.Validate(); run.CodeOf(err) != "undividable_unit" {
		t.Fatalf("Validate() = %v, want undividable_unit", err)
	}
}

func TestAMediaOpsCapRefusesTheEffectThatWouldCrossIt(t *testing.T) {
	scope := tenant()
	meter := &fakeMeter{perScope: map[quota.Scope]map[quota.Unit]int{
		scope: {quota.UnitMediaOps: 7},
	}}
	enforcer, err := quota.NewEnforcer(quota.Snapshot{Limits: []quota.Limit{{
		Name:   "media-ops-per-hour",
		Scope:  scope,
		Unit:   quota.UnitMediaOps,
		Max:    10,
		Window: time.Hour,
	}}}, meter)
	if err != nil {
		t.Fatalf("NewEnforcer: %v", err)
	}

	// Three more fits exactly; four does not. The want is the real image
	// count, not a probe — unlike tokens, the number is known before the call.
	allowed, err := enforcer.AdmitEffect(context.Background(),
		scope, run.Limits{MediaOps: 3})
	if err != nil {
		t.Fatalf("AdmitEffect: %v", err)
	}
	if !allowed.Allowed {
		t.Fatal("refused an effect that fits exactly inside the cap")
	}

	refused, err := enforcer.AdmitEffect(context.Background(),
		scope, run.Limits{MediaOps: 4})
	if err != nil {
		t.Fatalf("AdmitEffect: %v", err)
	}
	if refused.Allowed {
		t.Fatal("allowed an effect that crosses the cap")
	}
	if refused.Limit.Name != "media-ops-per-hour" {
		t.Fatalf("wrong limit reported: %q", refused.Limit.Name)
	}
}

func TestAMediaOpsCapMayNotAskToDegrade(t *testing.T) {
	// Degrading means dropping the tool loop, which is the only reduced form
	// there is. Half an image is not a picture book.
	err := quota.Snapshot{Limits: []quota.Limit{{
		Name:    "media-ops-per-hour",
		Scope:   tenant(),
		Unit:    quota.UnitMediaOps,
		Max:     10,
		Window:  time.Hour,
		Degrade: true,
	}}}.Validate()
	if err == nil {
		t.Fatal("published a degrading media_ops cap")
	}
	if run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("wrong kind: %v", run.KindOf(err))
	}
}

func TestAMediaOpsCapNeedsAWindow(t *testing.T) {
	// A consumption cap with no window is a lifetime cap nothing resets.
	err := quota.Snapshot{Limits: []quota.Limit{{
		Name:  "media-ops-forever",
		Scope: tenant(),
		Unit:  quota.UnitMediaOps,
		Max:   10,
	}}}.Validate()
	if err == nil {
		t.Fatal("published an unwindowed media_ops cap")
	}
}
