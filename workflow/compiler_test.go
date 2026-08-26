package workflow_test

import (
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/policy"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

type fakeKeys struct {
	keys   []string
	frozen bool
}

func (f fakeKeys) Frozen() bool   { return f.frozen }
func (f fakeKeys) Keys() []string { return f.keys }

type fakeTools struct {
	names  []string
	frozen bool
}

func (f fakeTools) Frozen() bool { return f.frozen }

func (f fakeTools) Specs() []tool.Spec {
	specs := make([]tool.Spec, 0, len(f.names))
	for _, name := range f.names {
		specs = append(specs, tool.Spec{
			Name: name, RiskLevel: policy.RiskLow, SideEffect: policy.SideEffectRead,
		})
	}
	return specs
}

func registries() workflow.Registries {
	return workflow.Registries{
		Agents:   fakeKeys{keys: []string{"answer", "draft", "review", "publish"}, frozen: true},
		Tools:    fakeTools{names: []string{"search"}, frozen: true},
		Memories: fakeKeys{keys: []string{"notes"}, frozen: true},
		Models:   fakeKeys{keys: []string{"fast"}, frozen: true},
	}
}

func base(d definition.Definition) definition.Definition {
	d.Ref = run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1}
	if d.Mode == "" {
		d.Mode = definition.ModeSpecialist
	}
	if d.Implementation == "" {
		d.Implementation = "answer"
	}
	return d
}

func compile(t *testing.T, d definition.Definition) *workflow.ExecutionGraph {
	t.Helper()
	graph, err := workflow.Compiler{Registries: registries()}.Compile(base(d))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return graph
}

// compileRef is compile for a Definition that already carries the Ref it wants.
// compile applies base(), which overwrites Ref, so a test asking whether the
// ref reaches the digest cannot use it.
func compileRef(t *testing.T, d definition.Definition) *workflow.ExecutionGraph {
	t.Helper()
	graph, err := workflow.Compiler{Registries: registries()}.Compile(d)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return graph
}

func compileError(t *testing.T, d definition.Definition) *run.Error {
	t.Helper()
	_, err := workflow.Compiler{Registries: registries()}.Compile(base(d))
	if err == nil {
		t.Fatal("compile succeeded, want refusal")
	}
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) {
		t.Fatalf("error is not a run.Error: %v", err)
	}
	return runtimeError
}

func dag(nodes ...definition.NodeSpec) definition.Definition {
	return definition.Definition{
		Mode:  definition.ModeOrchestrator,
		Graph: definition.GraphSpec{Nodes: nodes},
	}
}

// A single Agent is not a special case; it is a one-node graph. Two execution
// paths would be two sets of invariants, and the rarer one drifts.
func TestSingleAgentCompilesToOneNodeGraph(t *testing.T) {
	graph := compile(t, definition.Definition{Implementation: "answer"})

	if len(graph.Nodes) != 1 || graph.Nodes[0].Kind != workflow.NodeAgent {
		t.Fatalf("graph=%+v", graph.Nodes)
	}
	if graph.Nodes[0].Implementation != "answer" {
		t.Fatalf("implementation=%q", graph.Nodes[0].Implementation)
	}
	if graph.Nodes[0].Input.Source != workflow.SourceRunInput {
		t.Fatalf("input=%+v; a lone node reads the Run's input", graph.Nodes[0].Input)
	}
	if graph.Nodes[0].Failure != workflow.FailHard {
		t.Error("a node nobody marked optional must fail the Run")
	}
}

func TestGraphCompilesEveryDeclaredNode(t *testing.T) {
	graph := compile(t, dag(
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}, Inputs: []string{"a"}},
		definition.NodeSpec{Name: "a", Agent: "draft"},
	))

	if len(graph.Nodes) != 2 {
		t.Fatalf("nodes=%d want=2", len(graph.Nodes))
	}
	if graph.Nodes[0].ID != "a" || graph.Nodes[1].ID != "b" {
		t.Fatalf("nodes are not in canonical order: %+v", graph.Nodes)
	}
	if binding := graph.Nodes[1].Input; binding.Source != workflow.SourceNode ||
		len(binding.From) != 1 || binding.From[0] != "a" {
		t.Fatalf("binding=%+v", graph.Nodes[1].Input)
	}
}

// Only a leaf's output is the Run's output, so only a leaf carries the schema
// the Definition declared. Putting it on every node would mean an intermediate
// step is judged against a shape it was never meant to produce.
func TestOnlyLeavesCarryTheDeclaredOutputSchema(t *testing.T) {
	d := dag(
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}},
	)
	d.OutputSchema = json.RawMessage(`{"type":"object"}`)

	graph := compile(t, d)
	if len(graph.Nodes[0].OutputSchema) != 0 {
		t.Error("an intermediate node carries the Run's output schema")
	}
	if len(graph.Nodes[1].OutputSchema) == 0 {
		t.Error("the leaf does not carry the Run's output schema")
	}
}

func TestUnknownKeysAreRefusedAtPublish(t *testing.T) {
	cases := map[string]struct {
		definition definition.Definition
		code       string
	}{
		"agent": {
			definition.Definition{Implementation: "nobody"}, "unknown_agent_key",
		},
		"node agent": {
			dag(definition.NodeSpec{Name: "a", Agent: "nobody"}), "unknown_agent_key",
		},
		"tool": {
			definition.Definition{Tools: []definition.ToolRef{{Key: "nobody"}}}, "unknown_tool_key",
		},
		"memory": {
			definition.Definition{Memories: []definition.MemoryRef{
				{Key: "nobody", Namespace: "n", MaxRecords: 1, MaxTokens: 1},
			}}, "unknown_memory_key",
		},
		"model profile": {
			definition.Definition{Model: definition.ModelPolicy{Profile: "nobody"}}, "unknown_model_profile",
		},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			if code := compileError(t, testCase.definition).Code; code != testCase.code {
				t.Fatalf("code=%q want=%q", code, testCase.code)
			}
		})
	}
}

// An optional tool is one the Run may not call. It is not permission to name
// something nobody registered — that is still a broken Definition, and the
// Run would discover it halfway through.
func TestOptionalToolsMustStillResolve(t *testing.T) {
	code := compileError(t, definition.Definition{
		Tools: []definition.ToolRef{{Key: "nobody", Required: false}},
	}).Code
	if code != "unknown_tool_key" {
		t.Fatalf("code=%q", code)
	}
}

func TestCyclesAreRefused(t *testing.T) {
	err := compileError(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft", DependsOn: []string{"c"}},
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}},
		definition.NodeSpec{Name: "c", Agent: "publish", DependsOn: []string{"b"}},
	))
	if err.Code != "graph_cycle" {
		t.Fatalf("code=%q want=graph_cycle", err.Code)
	}
	// The refusal has to name the nodes; "there is a cycle somewhere" is not
	// something an operator can act on.
	if !strings.Contains(err.Error(), "a") || !strings.Contains(err.Error(), "c") {
		t.Errorf("the refusal does not name the cyclic nodes: %v", err)
	}
}

func TestSelfDependencyIsACycle(t *testing.T) {
	if code := compileError(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft", DependsOn: []string{"a"}},
	)).Code; code != "graph_cycle" {
		t.Fatalf("code=%q", code)
	}
}

func TestUnknownDependencyIsRefused(t *testing.T) {
	if code := compileError(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft", DependsOn: []string{"ghost"}},
	)).Code; code != "unknown_dependency" {
		t.Fatalf("code=%q", code)
	}
}

// Reading from a node you do not depend on is the one incompatibility provable
// here: without the edge, nothing orders the two and the value may not exist.
func TestBindingWithoutADependencyIsRefused(t *testing.T) {
	if code := compileError(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "review", Inputs: []string{"a"}},
	)).Code; code != "incompatible_binding" {
		t.Fatalf("code=%q", code)
	}
}

// A node that reads no upstream already receives the Run input and nothing
// else. Honouring the flag there would wrap that same value in a keyed object,
// so a declaration reading "also give me the Run input" would change the shape
// of what the node gets.
func TestWithRunInputWithoutAnUpstreamIsRefused(t *testing.T) {
	if code := compileError(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft", WithRunInput: true},
	)).Code; code != "redundant_binding" {
		t.Fatalf("code=%q, want redundant_binding", code)
	}
}

// The Run input's key in a keyed input object is not a namespace a node may
// enter: a node named run_input would shadow it, and which one won would depend
// on map iteration rather than on anything declared.
func TestANodeNamedAfterTheRunInputKeyIsRefused(t *testing.T) {
	if code := compileError(t, dag(
		definition.NodeSpec{Name: workflow.RunInputKey, Agent: "draft"},
	)).Code; code != "reserved_node_name" {
		t.Fatalf("code=%q, want reserved_node_name", code)
	}
}

func TestMemoryCeilingAboveTheBudgetIsRefused(t *testing.T) {
	if code := compileError(t, definition.Definition{
		Budget: run.Limits{Tokens: 100},
		Memories: []definition.MemoryRef{
			{Key: "notes", Namespace: "n", MaxRecords: 5, MaxTokens: 500},
		},
	}).Code; code != "memory_exceeds_budget" {
		t.Fatalf("code=%q", code)
	}
}

// Compiling against an open registry would make "this key exists" true at
// publish and false at execution, which is exactly what pinning removes.
func TestOpenRegistriesAreRefused(t *testing.T) {
	open := registries()
	open.Tools = fakeTools{names: []string{"search"}, frozen: false}

	_, err := workflow.Compiler{Registries: open}.Compile(base(definition.Definition{}))
	if err == nil {
		t.Fatal("compiled against an open registry")
	}
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) || runtimeError.Code != "registry_not_frozen" {
		t.Fatalf("err=%v", err)
	}
}

func TestMissingRegistryIsRefused(t *testing.T) {
	_, err := workflow.Compiler{}.Compile(base(definition.Definition{}))
	var runtimeError *run.Error
	if !errors.As(err, &runtimeError) || runtimeError.Code != "missing_registry" {
		t.Fatalf("err=%v", err)
	}
}

// The digest is what recovery reloads by, so it must not move when nothing
// meaningful changed and must move when anything did.
func TestDigestIsStableAcrossDeclarationOrder(t *testing.T) {
	first := compile(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}},
	))
	second := compile(t, dag(
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}},
		definition.NodeSpec{Name: "a", Agent: "draft"},
	))

	if first.Ref.Digest != second.Ref.Digest {
		t.Fatalf("reordering the declaration changed the digest:\n%s\n%s",
			first.Ref.Digest, second.Ref.Digest)
	}
	if first.Ref.Digest == "" {
		t.Fatal("the graph has no digest")
	}
}

func TestChangedGraphProducesADifferentDigestAndFailsRecovery(t *testing.T) {
	pinned := compile(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}},
	))
	// Same ref, different shape: the third node is the change.
	republished := compile(t, dag(
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}},
		definition.NodeSpec{Name: "c", Agent: "publish", DependsOn: []string{"b"}},
	))

	if pinned.Ref.Digest == republished.Ref.Digest {
		t.Fatal("a changed graph kept its digest; recovery could not tell them apart")
	}
	if err := republished.Verify(pinned.Ref); err == nil {
		t.Fatal("recovery accepted a graph the Run did not pin")
	} else if run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("kind=%s want=invalid", run.KindOf(err))
	}
	if err := pinned.Verify(pinned.Ref); err != nil {
		t.Fatalf("recovery refused the pinned graph: %v", err)
	}
}

func TestCompilationIsDeterministic(t *testing.T) {
	declaration := dag(
		definition.NodeSpec{Name: "c", Agent: "publish", DependsOn: []string{"a", "b"}},
		definition.NodeSpec{Name: "a", Agent: "draft"},
		definition.NodeSpec{Name: "b", Agent: "review", DependsOn: []string{"a"}, Inputs: []string{"a"}},
	)
	first := compile(t, declaration)
	for range 30 {
		again := compile(t, declaration)
		if again.Ref.Digest != first.Ref.Digest {
			t.Fatalf("digest is not deterministic: %s vs %s", first.Ref.Digest, again.Ref.Digest)
		}
		for i := range again.Nodes {
			if again.Nodes[i].ID != first.Nodes[i].ID {
				t.Fatalf("node order is not deterministic: %+v", again.Nodes)
			}
		}
	}
}

func TestSchemaProcessorRejectsAMalformedSchemaAtPublish(t *testing.T) {
	d := base(definition.Definition{})
	d.OutputSchema = json.RawMessage(`{"type":"nonsense"}`)

	compiler := workflow.Compiler{Registries: registries(), Schemas: refusingSchemas{}}
	if _, err := compiler.Compile(d); err == nil {
		t.Fatal("a schema the host rejects was published")
	}
}

// The Definition package must not grow a second compiler. Two answers to "is
// this publishable" is the failure this assertion exists to prevent, and it is
// checked structurally because a comment saying so is not enforcement.
func TestDefinitionExportsNoCompilerAndNeverImportsWorkflow(t *testing.T) {
	paths, err := filepath.Glob("../definition/*.go")
	if err != nil {
		t.Fatalf("list definition: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no definition sources found; the assertion would pass vacuously")
	}

	fileSet := token.NewFileSet()
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, imported := range file.Imports {
			if strings.Contains(imported.Path.Value, "agent-runtime/workflow") {
				t.Errorf("%s imports workflow", path)
			}
		}
		for _, declaration := range file.Decls {
			switch typed := declaration.(type) {
			case *ast.FuncDecl:
				if typed.Name.IsExported() && strings.Contains(typed.Name.Name, "Compile") {
					t.Errorf("%s exports %s", path, typed.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range typed.Specs {
					if typeSpec, ok := spec.(*ast.TypeSpec); ok && typeSpec.Name.Name == "Compiler" {
						t.Errorf("%s declares a Compiler", path)
					}
				}
			}
		}
	}
}

type refusingSchemas struct{}

func (refusingSchemas) ValidateSchema(json.RawMessage) error {
	return errors.New("unsupported type")
}

func (refusingSchemas) NormalizeValue(_, value json.RawMessage) (json.RawMessage, error) {
	return value, nil
}

// Tool authority is per node. Handing every node the Definition's whole tool
// list is how a planner ended up spending its entire turn budget answering a
// search tool it had no use for, and it is also how a node nobody granted a
// side-effecting tool could still call one.
func TestANodeIsGrantedOnlyTheToolsItDeclares(t *testing.T) {
	declared := dag(
		definition.NodeSpec{Name: "plan", Agent: "draft"},
		definition.NodeSpec{Name: "look", Agent: "review", DependsOn: []string{"plan"}, Tools: []string{"search"}},
	)
	declared.Tools = []definition.ToolRef{{Key: "search"}}

	graph := compile(t, declared)
	for _, node := range graph.Nodes {
		want := []string(nil)
		if node.ID == "look" {
			want = []string{"search"}
		}
		if len(node.Tools) != len(want) {
			t.Fatalf("node %q granted %v, want %v", node.ID, node.Tools, want)
		}
		for i, key := range want {
			if node.Tools[i] != key {
				t.Fatalf("node %q granted %v, want %v", node.ID, node.Tools, want)
			}
		}
	}
}

// A single-node Definition has no NodeSpec to narrow on and no sibling to
// narrow away from, so the Definition's tools are that node's tools.
func TestASingleNodeDefinitionKeepsItsDeclaredTools(t *testing.T) {
	graph := compile(t, definition.Definition{Tools: []definition.ToolRef{{Key: "search"}}})
	if len(graph.Nodes) != 1 || len(graph.Nodes[0].Tools) != 1 || graph.Nodes[0].Tools[0] != "search" {
		t.Fatalf("single node granted %v, want [search]", graph.Nodes[0].Tools)
	}
}

// A node grants a subset, never an addition: the audited list of what an agent
// can reach stays in the Definition rather than growing one node at a time.
func TestANodeCannotGrantAToolTheDefinitionDoesNotDeclare(t *testing.T) {
	declared := dag(definition.NodeSpec{Name: "look", Agent: "review", Tools: []string{"search"}})

	if code := compileError(t, declared).Code; code != "undeclared_node_tool" {
		t.Fatalf("code = %q, want undeclared_node_tool", code)
	}
}

// A node that works from several upstreams gets a distinct binding source, not
// SourceNode with a longer list. The payload shape is then read off the
// declaration instead of counted at run time, so a node whose upstream list
// grows from one to two cannot silently change what its consumer receives.
func TestSeveralUpstreamsCompileToTheirOwnBindingSource(t *testing.T) {
	graph := compile(t, dag(
		definition.NodeSpec{Name: "plan", Agent: "draft"},
		definition.NodeSpec{Name: "look", Agent: "review", DependsOn: []string{"plan"}, Inputs: []string{"plan"}},
		definition.NodeSpec{
			Name: "write", Agent: "publish",
			DependsOn: []string{"plan", "look"}, Inputs: []string{"look", "plan"},
		},
	))

	for _, node := range graph.Nodes {
		binding := node.Input
		switch node.ID {
		case "plan":
			if binding.Source != workflow.SourceRunInput {
				t.Fatalf("plan reads %+v, want the Run's own input", binding)
			}
		case "look":
			if binding.Source != workflow.SourceNode || len(binding.From) != 1 {
				t.Fatalf("look reads %+v, want one upstream", binding)
			}
		case "write":
			if binding.Source != workflow.SourceNodes || len(binding.From) != 2 {
				t.Fatalf("write reads %+v, want both upstreams", binding)
			}
			// Normalized order, so listing the same upstreams differently is
			// not a different definition — and not a different digest.
			if binding.From[0] != "look" || binding.From[1] != "plan" {
				t.Fatalf("upstreams = %v, want them sorted", binding.From)
			}
		}
	}
}

// Every upstream must be a declared dependency, not just the first one:
// without the edge nothing orders the two, so the value may not exist yet.
func TestEveryUpstreamMustBeADependency(t *testing.T) {
	declared := dag(
		definition.NodeSpec{Name: "plan", Agent: "draft"},
		definition.NodeSpec{Name: "look", Agent: "review"},
		definition.NodeSpec{
			Name: "write", Agent: "publish",
			DependsOn: []string{"plan"}, Inputs: []string{"plan", "look"},
		},
	)

	if code := compileError(t, declared).Code; code != "incompatible_binding" {
		t.Fatalf("code = %q, want incompatible_binding", code)
	}
}
