package resource_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/costa92/agent-runtime/policy"
	"github.com/costa92/agent-runtime/quota"
	"github.com/costa92/agent-runtime/resource"
	"github.com/costa92/agent-runtime/run"
)

// A Kind with no declared storage version has nowhere to be stored, and one
// with no published version cannot be published — either way the family it
// belongs to is not actually one family.
func TestEveryKindDeclaresAStorageAndPublishedVersion(t *testing.T) {
	for _, kind := range resource.Kinds() {
		storage, err := resource.StorageVersion(kind)
		if err != nil || storage == "" {
			t.Errorf("%s has no storage version: %v", kind, err)
		}
		published, err := resource.PublishedVersions(kind)
		if err != nil || len(published) == 0 {
			t.Errorf("%s declares no published apiVersion: %v", kind, err)
			continue
		}
		var storageIsPublished bool
		for _, version := range published {
			if version == storage {
				storageIsPublished = true
			}
		}
		if !storageIsPublished {
			t.Errorf("%s stores %q, which it does not accept on publish", kind, storage)
		}
	}
}

// Unparseable and unknown are both invalid, and deliberately give the same
// answer: guessing what "v1.0" meant would make the accepted set depend on the
// parser instead of on the declaration.
func TestUnknownOrUnparseableAPIVersionIsInvalid(t *testing.T) {
	for _, apiVersion := range []string{"", "1", "v0", "v1.0", "V1", "beta", "v99"} {
		err := resource.ValidateAPIVersion(resource.KindDefinition, apiVersion)
		if run.KindOf(err) != run.ErrorInvalid {
			t.Errorf("apiVersion %q error=%s want=invalid", apiVersion, run.KindOf(err))
		}
	}
	if err := resource.ValidateAPIVersion(resource.KindDefinition, "v1"); err != nil {
		t.Errorf("v1 rejected: %v", err)
	}
	if run.KindOf(resource.ValidateAPIVersion("Nonsense", "v1")) != run.ErrorInvalid {
		t.Error("an unknown Kind was accepted")
	}
}

// The parse node's job is "the payload is a JSON object", which is a narrower
// claim than "it unmarshals into a map" — encoding/json takes a literal null
// into any target without complaint. The cases are parameterised together
// because the hole that existed here survived by being the one value that is
// not a type mismatch, and picking cases one at a time is how it was missed.
func TestParseNodeAcceptsOnlyJSONObjects(t *testing.T) {
	for _, testCase := range []struct {
		payload string
		code    string // empty means the node must accept it
	}{
		{`{}`, ""},
		{`{"a":1}`, ""},
		{`null`, "null_payload"},
		{`[1,2]`, "unparseable_payload"},
		{`"x"`, "unparseable_payload"},
		{`42`, "unparseable_payload"},
		{`true`, "unparseable_payload"},
	} {
		err := resource.ParseNode.Admit(context.Background(), resource.AdmissionRequest{
			Kind:       resource.KindPolicy,
			Name:       "default",
			APIVersion: "v1",
			Payload:    json.RawMessage(testCase.payload),
		})
		if testCase.code == "" {
			if err != nil {
				t.Errorf("payload %s rejected: %v", testCase.payload, err)
			}
			continue
		}
		if run.CodeOf(err) != testCase.code {
			t.Errorf("payload %s code=%q want=%q", testCase.payload, run.CodeOf(err), testCase.code)
		}
		if run.KindOf(err) != run.ErrorInvalid {
			t.Errorf("payload %s kind=%s want=invalid", testCase.payload, run.KindOf(err))
		}
	}
}

// The digest identifies the resource, so it must depend on what the resource
// says and not on how the payload happened to be written. Go's encoder sorts
// object keys, which is what makes the round-trip canonical rather than tidy.
func TestDigestIsStableAcrossKeyOrderAndWhitespace(t *testing.T) {
	first := json.RawMessage(`{"a":1,"b":{"x":true,"y":[1,2]},"c":"z"}`)
	second := json.RawMessage("{\n  \"c\": \"z\",\n  \"b\": {\"y\": [1,2], \"x\": true},\n  \"a\": 1\n}")

	firstDigest, err := resource.Digest(first)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	secondDigest, err := resource.Digest(second)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("key order or whitespace changed the digest:\n%s\n%s", firstDigest, secondDigest)
	}

	changed, err := resource.Digest(json.RawMessage(`{"a":2,"b":{"x":true,"y":[1,2]},"c":"z"}`))
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if changed == firstDigest {
		t.Fatal("changing a value did not change the digest")
	}
}

// A large integer id re-encoded through float64 comes back rounded, which would
// change the digest of a payload nobody edited.
func TestDigestDoesNotRoundLargeIntegers(t *testing.T) {
	first, err := resource.Digest(json.RawMessage(`{"id":9007199254740993}`))
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	second, err := resource.Digest(json.RawMessage(`{"id":9007199254740992}`))
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	if first == second {
		t.Fatal("two ids one apart hashed the same; the encoder is rounding")
	}
}

// Conversion belongs to publish. If the read path could convert, what a Run
// executes would depend on which converter the loading deployment happened to
// have — and a Run that pinned a digest would still get different bytes back.
func TestConversionHappensOnlyAtPublish(t *testing.T) {
	if _, ok := any(resource.Reader(nil)).(interface{ Convert(any) any }); ok {
		t.Fatal("read path must not expose conversion")
	}
	if _, ok := any(resource.Reader(nil)).(resource.Converter); ok {
		t.Fatal("the Reader interface satisfies Converter; the read path can convert")
	}
}

func TestRefValidationRejectsMutableAndMalformedRefs(t *testing.T) {
	valid := resource.Ref{Kind: resource.KindPolicy, Name: "no-publish", APIVersion: "v1", Version: 3}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid ref rejected: %v", err)
	}

	cases := map[string]resource.Ref{
		"unknown kind":   {Kind: "Nope", Name: "x", APIVersion: "v1", Version: 1},
		"missing name":   {Kind: resource.KindPolicy, APIVersion: "v1", Version: 1},
		"bad apiVersion": {Kind: resource.KindPolicy, Name: "x", APIVersion: "v9", Version: 1},
		"version zero":   {Kind: resource.KindPolicy, Name: "x", APIVersion: "v1"},
	}
	for name, ref := range cases {
		if run.KindOf(ref.Validate()) != run.ErrorInvalid {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A condition that reaches into the Run's payload is the failure this closed
// fact set exists to prevent: there is no fallback that would evaluate it, so
// it is rejected by name rather than by a denylist someone has to maintain.
func TestPolicyRejectsConditionsOnRunPayload(t *testing.T) {
	for _, fact := range []policy.Fact{"input.title", "output.body", "payload.article.id", ""} {
		p := policy.Policy{
			Name: "p", Scope: policy.ScopeTool, Decision: policy.DecisionDeny,
			Conditions: []policy.Condition{{Fact: fact, Operator: policy.OpEquals, Values: []string{"x"}}},
		}
		if run.KindOf(p.Validate()) != run.ErrorInvalid {
			t.Errorf("fact %q was accepted", fact)
		}
	}

	allowed := policy.Policy{
		Name: "p", Scope: policy.ScopeTool, Decision: policy.DecisionRequireApproval,
		Conditions: []policy.Condition{
			{Fact: policy.FactToolRiskLevel, Operator: policy.OpIn, Values: []string{"high"}},
		},
	}
	if err := allowed.Validate(); err != nil {
		t.Fatalf("declared fact rejected: %v", err)
	}
}

func TestPolicyValidationRejectsUnknownScopeOperatorAndDecision(t *testing.T) {
	base := policy.Policy{Name: "p", Scope: policy.ScopeTool, Decision: policy.DecisionAllow}

	unknownScope := base
	unknownScope.Scope = "galaxy"
	if run.KindOf(unknownScope.Validate()) != run.ErrorInvalid {
		t.Error("unknown scope accepted")
	}

	unknownDecision := base
	unknownDecision.Decision = "maybe"
	if run.KindOf(unknownDecision.Validate()) != run.ErrorInvalid {
		t.Error("unknown decision accepted")
	}

	unknownOperator := base
	unknownOperator.Conditions = []policy.Condition{
		{Fact: policy.FactToolName, Operator: "matches_regex", Values: []string{"x"}},
	}
	if run.KindOf(unknownOperator.Validate()) != run.ErrorInvalid {
		t.Error("unknown operator accepted")
	}

	noValues := base
	noValues.Conditions = []policy.Condition{{Fact: policy.FactToolName, Operator: policy.OpEquals}}
	if run.KindOf(noValues.Validate()) != run.ErrorInvalid {
		t.Error("condition with no values accepted")
	}
}

// deny beats require_approval beats everything else. Encoded as a property of
// the decision so it cannot be changed by reordering a loop somewhere else.
func TestDecisionPrecedenceIsTotalAndDenyWins(t *testing.T) {
	decisions := policy.Decisions()
	if len(decisions) != 5 {
		t.Fatalf("decision enum has %d members; the evaluator switch would be incomplete", len(decisions))
	}
	if decisions[0] != policy.DecisionDeny {
		t.Fatalf("deny is not first in precedence: %v", decisions)
	}
	if policy.DecisionDeny.Precedence() >= policy.DecisionRequireApproval.Precedence() {
		t.Error("require_approval outranks deny")
	}
	if policy.DecisionRequireApproval.Precedence() >= policy.DecisionAllow.Precedence() {
		t.Error("allow outranks require_approval")
	}

	seen := map[int]bool{}
	for _, decision := range decisions {
		rank := decision.Precedence()
		if rank < 0 || seen[rank] {
			t.Errorf("decision %q has a duplicate or missing rank %d", decision, rank)
		}
		seen[rank] = true
	}
}

func TestScopeSpecificityOrdersBroadestFirst(t *testing.T) {
	if policy.ScopeTenant.Specificity() >= policy.ScopeTool.Specificity() {
		t.Error("tenant is not broader than tool")
	}
	if policy.Scope("nowhere").Valid() {
		t.Error("an unknown scope validated")
	}
}

func TestQuotaValidationRequiresAnExplicitBreachOutcome(t *testing.T) {
	valid := quota.Quota{Name: "q", Tenant: "t", OnExceeded: quota.OutcomeReject}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid quota rejected: %v", err)
	}

	missingOutcome := valid
	missingOutcome.OnExceeded = ""
	if run.KindOf(missingOutcome.Validate()) != run.ErrorInvalid {
		t.Error("a quota with no declared breach behaviour was accepted")
	}

	duplicateWindow := valid
	duplicateWindow.Usage = []quota.UsageLimit{
		{Window: quota.WindowDay, Tokens: 1}, {Window: quota.WindowDay, Tokens: 2},
	}
	if run.KindOf(duplicateWindow.Validate()) != run.ErrorInvalid {
		t.Error("two limits for one window were accepted")
	}

	negative := valid
	negative.MaxConcurrentRuns = -1
	if run.KindOf(negative.Validate()) != run.ErrorInvalid {
		t.Error("a negative limit was accepted")
	}
}
