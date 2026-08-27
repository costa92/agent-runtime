package definition

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

func writerDefinition() Definition {
	return Definition{
		Ref:            run.DefinitionRef{ID: "writer", Version: 1, Protocol: 1},
		Mode:           ModeSpecialist,
		Implementation: "content.writer",
	}
}

func TestNormalizeDefinitionProducesCanonicalDigest(t *testing.T) {
	normalized, digest, err := Normalize(writerDefinition())
	if err != nil || digest == "" || normalized.Ref.Version != 1 {
		t.Fatal(err, digest)
	}
}

// Two declarations that differ only in the order they listed their tools are
// the same definition. Without normalization every republish would look like a
// change, and a digest that changes for no reason is a digest nobody trusts.
func TestDigestIgnoresDeclarationOrderButNotContent(t *testing.T) {
	first := writerDefinition()
	first.Tools = []ToolRef{{Key: "publish"}, {Key: "search"}}
	first.Memories = []MemoryRef{
		{Key: "lessons", Namespace: "n", MaxRecords: 5, MaxTokens: 500},
		{Key: "facts", Namespace: "n", MaxRecords: 5, MaxTokens: 500},
	}

	second := writerDefinition()
	second.Tools = []ToolRef{{Key: "search"}, {Key: "publish"}}
	second.Memories = []MemoryRef{
		{Key: "facts", Namespace: "n", MaxRecords: 5, MaxTokens: 500},
		{Key: "lessons", Namespace: "n", MaxRecords: 5, MaxTokens: 500},
	}

	_, firstDigest, err := Normalize(first)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	_, secondDigest, err := Normalize(second)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("reordering the declaration changed the digest:\n%s\n%s", firstDigest, secondDigest)
	}
}

// The digest has to move when the thing it identifies moves, or a Run pinned to
// it can be handed different behaviour.
func TestDigestChangesWithPromptAndDeclaredRefs(t *testing.T) {
	base := writerDefinition()
	base.Tools = []ToolRef{{Key: "search"}}
	_, baseDigest, err := Normalize(base)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}

	changes := map[string]func(*Definition){
		"prompt":     func(d *Definition) { d.Prompt = "write carefully" },
		"tool added": func(d *Definition) { d.Tools = append(d.Tools, ToolRef{Key: "publish"}) },
		"tool required": func(d *Definition) {
			d.Tools = []ToolRef{{Key: "search", Required: true}}
		},
		"memory added": func(d *Definition) {
			d.Memories = []MemoryRef{{Key: "facts", Namespace: "n", MaxRecords: 1, MaxTokens: 1}}
		},
		"model profile": func(d *Definition) { d.Model = ModelPolicy{Profile: "fast"} },
		"budget":        func(d *Definition) { d.Budget = run.Limits{Tokens: 100} },
	}
	for name, mutate := range changes {
		changed := base
		changed.Tools = append([]ToolRef(nil), base.Tools...)
		mutate(&changed)
		_, digest, err := Normalize(changed)
		if err != nil {
			t.Fatalf("%s: normalize: %v", name, err)
		}
		if digest == baseDigest {
			t.Errorf("%s did not change the digest", name)
		}
	}
}

// Whitespace in a schema is not a change to the schema.
func TestDigestIgnoresSchemaFormatting(t *testing.T) {
	compact := writerDefinition()
	compact.InputSchema = json.RawMessage(`{"type":"object","properties":{"title":{"type":"string"}}}`)

	indented := writerDefinition()
	indented.InputSchema = json.RawMessage("{\n  \"type\": \"object\",\n  \"properties\": {\n    \"title\": {\"type\": \"string\"}\n  }\n}")

	_, compactDigest, err := Normalize(compact)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	_, indentedDigest, err := Normalize(indented)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if compactDigest != indentedDigest {
		t.Fatal("reformatting a schema changed the digest")
	}
}

func TestNormalizeRejectsMutableAndUnsupportedRefs(t *testing.T) {
	zeroVersion := writerDefinition()
	zeroVersion.Ref.Version = 0
	if run.KindOf(mustFail(t, zeroVersion)) != run.ErrorInvalid {
		t.Error("a version-zero ref was accepted")
	}

	zeroProtocol := writerDefinition()
	zeroProtocol.Ref.Protocol = 0
	if run.KindOf(mustFail(t, zeroProtocol)) != run.ErrorInvalid {
		t.Error("an unsupported protocol was accepted")
	}

	noID := writerDefinition()
	noID.Ref.ID = ""
	if run.KindOf(mustFail(t, noID)) != run.ErrorInvalid {
		t.Error("a definition with no id was accepted")
	}
}

func TestNormalizeRejectsLocallyInconsistentDeclarations(t *testing.T) {
	cases := map[string]func(*Definition){
		"no implementation": func(d *Definition) { d.Implementation = "  " },
		"unknown mode":      func(d *Definition) { d.Mode = "supervisor" },
		"duplicate tool": func(d *Definition) {
			d.Tools = []ToolRef{{Key: "search"}, {Key: "search"}}
		},
		"empty tool key": func(d *Definition) { d.Tools = []ToolRef{{Key: " "}} },
		"unbounded memory": func(d *Definition) {
			d.Memories = []MemoryRef{{Key: "facts", Namespace: "n"}}
		},
		"memory without namespace": func(d *Definition) {
			d.Memories = []MemoryRef{{Key: "facts", MaxRecords: 1, MaxTokens: 1}}
		},
		"duplicate memory": func(d *Definition) {
			d.Memories = []MemoryRef{
				{Key: "facts", Namespace: "n", MaxRecords: 1, MaxTokens: 1},
				{Key: "facts", Namespace: "n", MaxRecords: 2, MaxTokens: 2},
			}
		},
		"duplicate node": func(d *Definition) {
			d.Mode = ModeOrchestrator
			d.Graph = GraphSpec{Nodes: []NodeSpec{{Name: "a"}, {Name: "a"}}}
		},
		"negative budget": func(d *Definition) { d.Budget = run.Limits{Tokens: -1} },
		"invalid schema":  func(d *Definition) { d.InputSchema = json.RawMessage(`{not json`) },
	}
	for name, mutate := range cases {
		definition := writerDefinition()
		mutate(&definition)
		if run.KindOf(mustFail(t, definition)) != run.ErrorInvalid {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Task 7 owns the sole compiler. A second validator that resolved registry keys
// or walked the graph would be a second answer to "is this publishable", and
// the one that ran later would be the one nobody remembered to update.
func TestNormalizeDoesNotResolveRegistryKeys(t *testing.T) {
	definition := writerDefinition()
	definition.Implementation = "nothing.registered.under.this.key"
	definition.Tools = []ToolRef{{Key: "no.such.tool", Required: true}}
	definition.Memories = []MemoryRef{{Key: "no.such.memory", Namespace: "n", MaxRecords: 1, MaxTokens: 1}}

	if _, _, err := Normalize(definition); err != nil {
		t.Fatalf("Normalize resolved a registry key it does not own: %v", err)
	}
}

// Normalization returns a copy. A caller that kept its own declaration must not
// see it rewritten underneath, and the normalized value must not share backing
// arrays with it.
func TestNormalizeDoesNotMutateItsInput(t *testing.T) {
	definition := writerDefinition()
	definition.Tools = []ToolRef{{Key: "search"}, {Key: "publish"}}

	normalized, _, err := Normalize(definition)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if definition.Tools[0].Key != "search" {
		t.Fatalf("Normalize sorted the caller's slice in place: %+v", definition.Tools)
	}
	if normalized.Tools[0].Key != "publish" {
		t.Fatalf("the normalized copy is not sorted: %+v", normalized.Tools)
	}
}

func mustFail(t *testing.T, definition Definition) error {
	t.Helper()
	_, _, err := Normalize(definition)
	if err == nil {
		t.Error("expected a rejection, got none")
	}
	return err
}

// normalizeGraph rebuilds each NodeSpec field by field, so a field added to the
// struct and not added there is dropped before the compiler sees it — while the
// declaration still holds it, which is how a wired-up node behaves as if it
// were never wired.
//
// Checked by reflection rather than by listing the fields, because a test that
// lists them has the same gap as the code it guards.
func TestNormalizeKeepsEveryNodeSpecField(t *testing.T) {
	spec := NodeSpec{}
	value := reflect.ValueOf(&spec).Elem()
	for i := range value.NumField() {
		field := value.Field(i)
		switch field.Kind() {
		case reflect.String:
			field.SetString("v")
		case reflect.Bool:
			field.SetBool(true)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			field.SetInt(3)
		case reflect.Slice:
			field.Set(reflect.ValueOf([]string{"x"}))
		default:
			// A kind this test cannot populate is a kind it cannot prove
			// survives normalizeGraph's field-by-field rebuild. Failing is the
			// only honest answer: passing would report coverage it does not
			// have, which is exactly how a dropped field reaches production.
			t.Fatalf("NodeSpec.%s has kind %s, which this test cannot populate; "+
				"extend the switch and confirm normalizeGraph copies the field",
				value.Type().Field(i).Name, field.Kind())
		}
	}
	// Name and the reference fields must agree with each other for the node to
	// survive as declared: a node reads an upstream it depends on.
	spec.Name = "b"
	spec.Inputs = []string{"a"}
	spec.DependsOn = []string{"a"}

	normalized := normalizeGraph(GraphSpec{Nodes: []NodeSpec{{Name: "a", Agent: "draft"}, spec}}).Nodes[1]

	for i := range value.NumField() {
		name := value.Type().Field(i).Name
		want := value.Field(i).Interface()
		got := reflect.ValueOf(normalized).Field(i).Interface()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("NodeSpec.%s did not survive normalization: got %v, want %v", name, got, want)
		}
	}
}
