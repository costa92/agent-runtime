package policy

import (
	"sort"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Snapshot is the active policy set a Run evaluates against.
//
// A Run takes one at the start and uses it end to end. A publish that lands
// mid-Run applies to the next Run: otherwise the same call could be allowed at
// step three and denied at step four, and no explanation would account for the
// difference.
type Snapshot struct {
	// Policies is the active set. Order does not matter; evaluation sorts.
	Policies []Policy
	// Digest identifies the snapshot, so an explanation can name which set of
	// rules produced it.
	Digest string
	// Default applies when nothing matches. It is configured explicitly
	// because the safe default differs by tool: deny for a high-risk one, allow
	// for a read-only side-effect-free one, and guessing either way is wrong
	// half the time.
	Default DefaultRule
}

// DefaultRule is the conclusion when no policy matches.
type DefaultRule struct {
	// HighRisk applies to a tool declared high risk or with a write side
	// effect. Zero value means DecisionDeny.
	HighRisk Decision
	// ReadOnly applies to a tool with no side effect beyond reading. Zero value
	// means DecisionAllow.
	ReadOnly Decision
}

func (d DefaultRule) decisionFor(facts CallFacts) Decision {
	if facts.ToolRiskLevel == RiskHigh || facts.ToolSideEffect == SideEffectWrite {
		if d.HighRisk == "" {
			return DecisionDeny
		}
		return d.HighRisk
	}
	if d.ReadOnly == "" {
		return DecisionAllow
	}
	return d.ReadOnly
}

// Match is one policy that fired, recorded for the explanation.
type Match struct {
	Name     string
	Scope    Scope
	Decision Decision
	// Shadow marks a policy that was evaluated but not enforced.
	Shadow bool
}

// Explanation is why a decision came out the way it did.
//
// Every evaluation produces one, including an allow. "Why was this allowed" is
// asked exactly as often as "why was this denied", usually after something has
// gone wrong, and a decision that only explains itself on refusal cannot answer
// it.
type Explanation struct {
	Decision Decision
	// Matched lists every policy that fired, in the order evaluation
	// considered them. Shadow entries are included and never affected the
	// outcome.
	Matched []Match
	// Deciding names the one policy that produced Decision. It is nil when no
	// policy did: nothing matched and the default applied. Reading it is the
	// only correct way to name the rule in force — inferring it from Matched by
	// comparing decisions picks a shadow entry whenever one happens to agree,
	// and cannot tell two enforced rules that agree apart at all.
	//
	// A Strategy that tightened the outcome is named by TightenedBy, not here:
	// Deciding stays the declarative rule the strategy narrowed, so an operator
	// reading the audit can still find the published policy involved.
	Deciding *Match
	// FromDefault marks a decision nothing matched.
	FromDefault bool
	// SnapshotDigest names the rule set this came from.
	SnapshotDigest string
	// TightenedBy names a Go Strategy that narrowed the declarative decision.
	TightenedBy string
}

// Strategy is the Go escape hatch for conditions the declarative facts cannot
// express.
//
// It can only tighten. A strategy that could loosen would be an undeclared
// override of published governance: an operator reading the active policy set
// would see a deny that a compiled-in function had quietly turned into an
// allow, with nothing in the resource model to point at.
type Strategy interface {
	Name() string
	// Tighten returns the decision it wants. Returning the input unchanged
	// means "no opinion".
	Tighten(decision Decision, facts CallFacts) Decision
}

// Evaluate is a pure function of the snapshot and the call facts. Same inputs,
// same decision, same explanation — no clock, no store, no network.
//
// Precedence is deny, then require_approval, then the rest, taken from the
// Decision type itself so it cannot be changed by reordering a loop here. Ties
// break on scope specificity, then on policy name, so two equally specific
// rules do not resolve by map order.
func Evaluate(snapshot Snapshot, facts CallFacts, strategies ...Strategy) (Explanation, error) {
	candidates := make([]Policy, 0, len(snapshot.Policies))
	for _, policy := range snapshot.Policies {
		if policy.Matches(facts) {
			candidates = append(candidates, policy)
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if left.Decision.Precedence() != right.Decision.Precedence() {
			return left.Decision.Precedence() < right.Decision.Precedence()
		}
		if left.Scope.Specificity() != right.Scope.Specificity() {
			// More specific first: a tool-scoped rule is a deliberate exception
			// to a tenant-wide one.
			return left.Scope.Specificity() > right.Scope.Specificity()
		}
		return left.Name < right.Name
	})

	explanation := Explanation{SnapshotDigest: snapshot.Digest}
	decided := false
	for _, policy := range candidates {
		explanation.Matched = append(explanation.Matched, Match{
			Name: policy.Name, Scope: policy.Scope, Decision: policy.Decision, Shadow: policy.Shadow,
		})
		if policy.Shadow || decided {
			// A shadow policy is evaluated and reported, never enforced. That
			// is what makes it safe to publish a rule and watch what it would
			// have done before it does it.
			continue
		}
		explanation.Decision = policy.Decision
		// A copy rather than a pointer into Matched: a later append can move
		// that backing array, and a Deciding that silently stopped tracking
		// would be worse than no field at all.
		deciding := explanation.Matched[len(explanation.Matched)-1]
		explanation.Deciding = &deciding
		decided = true
	}

	if !decided {
		explanation.Decision = snapshot.Default.decisionFor(facts)
		explanation.FromDefault = true
	}

	for _, strategy := range strategies {
		wanted := strategy.Tighten(explanation.Decision, facts)
		if wanted == explanation.Decision {
			continue
		}
		if wanted.Precedence() < 0 {
			return explanation, run.NewError("unknown_strategy_decision", run.ErrorInternal, run.RetryNever)
		}
		if wanted.Precedence() > explanation.Decision.Precedence() {
			// Looser. Refusing is the whole point: published governance must be
			// what an operator can read, and a compiled-in loosening is invisible
			// there.
			return explanation, run.NewError("strategy_loosened_decision", run.ErrorInternal, run.RetryNever)
		}
		explanation.Decision = wanted
		explanation.TightenedBy = strategy.Name()
	}

	return explanation, nil
}

// ShadowWouldTighten returns the strictest shadow policy that fired with a
// stricter decision than the enforced outcome, if there is one.
//
// That disagreement is the entire product of a dry run: it is the call enforce
// would have stopped and shadow did not. A shadow policy that agrees with the
// outcome changed nothing and predicts nothing, so it is not reported here —
// counting it would inflate the estimated impact of turning the rule on with
// calls that are already being stopped.
func (e Explanation) ShadowWouldTighten() (Match, bool) {
	var strictest Match
	found := false
	for _, match := range e.Matched {
		if !match.Shadow || match.Decision.Precedence() >= e.Decision.Precedence() {
			continue
		}
		if !found || match.Decision.Precedence() < strictest.Decision.Precedence() {
			strictest, found = match, true
		}
	}
	return strictest, found
}

