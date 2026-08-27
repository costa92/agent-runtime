// Package definition is what a Run executes: an immutable, versioned
// declaration of an agent's prompt, model policy, budget, tools, memories and
// graph.
//
// A Definition names capabilities by stable key. It does not hold
// implementations, and this package does not resolve those keys — binding keys
// to registered implementations, validating the DAG and producing the execution
// graph digest all belong to the single compiler in Task 7. Splitting
// validation across two compilers is how two answers to "is this publishable"
// come to exist.
package definition

import (
	"encoding/json"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// ExecutionMode distinguishes a Definition that executes as a single step from
// one that executes a multi-node graph. It once distinguished a worker from a
// router that could delegate to other Runs; delegation no longer exists, and
// the surviving meaning is structural — an orchestrator is a Definition whose
// GraphSpec has nodes, a specialist is one that runs its implementation
// directly.
//
// It stays declared rather than derived at compile time because the two facts
// are still different: a Definition published as a specialist is one that may
// not grow a graph without a new published version, which is what makes the
// mode reviewable. Hosts that build a Definition from stored rows may of course
// set it from the graph they read — see the host's runtimeDefinitionFrom.
type ExecutionMode string

const (
	ModeSpecialist   ExecutionMode = "specialist"
	ModeOrchestrator ExecutionMode = "orchestrator"
)

func (m ExecutionMode) Valid() bool {
	return m == ModeSpecialist || m == ModeOrchestrator
}

// ModelPolicy declares how a step chooses its engine. Profile is a stable key
// into a published ModelProfile rather than a provider and model string, so
// that swapping an engine is a publish and not a release.
type ModelPolicy struct {
	Profile     string  `json:"profile"`
	Temperature float64 `json:"temperature,omitzero"`
	MaxTokens   int     `json:"max_tokens,omitempty"`
}

// ToolRef names a tool by stable key.
type ToolRef struct {
	Key string `json:"key"`
	// Required means the Definition cannot run without it, so publish fails
	// when it is unregistered rather than the Run failing halfway through.
	Required bool `json:"required,omitempty"`
}

// MemoryRef names a memory provider by stable key, with the scope and budget
// the Definition is allowed to use it under.
type MemoryRef struct {
	Key       string `json:"key"`
	Namespace string `json:"namespace"`
	// MaxRecords and MaxTokens bound one retrieval. A memory read with no
	// ceiling is an unbounded prompt, which fails as a model error far away
	// from the retrieval that caused it.
	MaxRecords int  `json:"max_records"`
	MaxTokens  int  `json:"max_tokens"`
	Writable   bool `json:"writable,omitempty"`
}

// GraphSpec is the declared shape of a multi-step definition. It is carried and
// validated for local well-formedness here; cycle detection and compilation are
// the compiler's, in Task 7.
type GraphSpec struct {
	Nodes []NodeSpec `json:"nodes,omitempty"`
}

// NodeSpec is one step, what it waits for, and where its input comes from.
type NodeSpec struct {
	Name      string   `json:"name"`
	Agent     string   `json:"agent"`
	DependsOn []string `json:"depends_on,omitempty"`
	// Inputs names the nodes whose output feeds this one. Empty means the Run's
	// own input. It is declared rather than inferred from DependsOn because
	// "runs after" and "reads from" are different facts: a node can wait on
	// three predecessors and consume one of them.
	//
	// Naming several is what a step needs when it works from more than one
	// upstream, and it must be declared rather than worked around: a writer
	// restricted to one upstream sees only the research and never the plan it
	// was supposed to follow, so it reads the research as a finished draft and
	// comments on it instead of writing from it.
	Inputs []string `json:"inputs,omitempty"`
	// WithRunInput adds the Run's own input alongside those upstream outputs,
	// for a step that needs both. What the host puts in the Run input is not
	// always something an upstream carries forward: a writer working from a
	// plan still needs the reader profile the Run was started with, and it
	// reached the writer only as far as the planner happened to echo it.
	//
	// It is a separate field rather than a reserved name in Inputs, which holds
	// node names — a reserved name there would compete with a real node.
	WithRunInput bool `json:"with_run_input,omitempty"`
	// Tools is what this node may call, as a subset of the Definition's
	// declared tools. A node that names none gets none: authority is granted
	// per step rather than inherited by every step because one of them needed
	// it. Handing the whole declared set to each node made a planner spend its
	// turns answering a search tool it had no use for, and left a writer able
	// to publish because the researcher next to it was allowed to search.
	Tools []string `json:"tools,omitempty"`
	// Optional lets this node fail without failing the Run, which is what makes
	// a partial result a declared outcome instead of a judgement call made
	// after something broke.
	Optional bool `json:"optional,omitempty"`
}

// Definition is the whole declaration.
type Definition struct {
	Ref  run.DefinitionRef `json:"ref"`
	Mode ExecutionMode     `json:"mode"`
	// Implementation is the stable Agent registry key. Resolving it is the
	// compiler's job; this package only requires that one was named.
	Implementation string `json:"implementation"`

	InputSchema  json.RawMessage `json:"input_schema,omitempty"`
	OutputSchema json.RawMessage `json:"output_schema,omitempty"`

	Prompt   string      `json:"prompt,omitempty"`
	Model    ModelPolicy `json:"model,omitzero"`
	Budget   run.Limits  `json:"budget,omitzero"`
	Tools    []ToolRef   `json:"tools,omitempty"`
	Memories []MemoryRef `json:"memories,omitempty"`

	Graph GraphSpec `json:"graph,omitzero"`

	// RunLabels is the closed vocabulary of labels a caller may set on a Run of
	// this Definition. Empty means a Run of it carries no labels at all.
	//
	// Declared here because the alternative is free-text labels chosen per
	// request, and a governance fact anybody can invent is a governance fact
	// nobody reviews: policies would come to depend on strings that appear in
	// no published resource. Listing them on the Definition puts the vocabulary
	// through the same publish, version and digest as everything else it
	// declares, and makes "which facts can this agent be judged by" answerable
	// by reading the definition rather than by grepping callers.
	RunLabels []string `json:"run_labels,omitempty"`
}

// SchemaProcessor is the host's JSON Schema implementation.
//
// Validation and normalization are one operation so a boundary cannot approve
// one representation and persist or execute another. The Runtime does not ship
// an implementation; the host supplies the subset its contracts use.
type SchemaProcessor interface {
	ValidateSchema(schema json.RawMessage) error
	NormalizeValue(schema, value json.RawMessage) (json.RawMessage, error)
}
