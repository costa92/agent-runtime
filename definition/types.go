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

// ExecutionMode distinguishes an agent that does the work from one that routes
// to others. It is declared rather than inferred from whether a graph is
// present, because "it had no children this time" is not the same fact as "it
// is not allowed to have any".
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

// RoutingPolicy bounds an orchestrator's delegation. The limits live in the
// Definition rather than in the runtime configuration so that a published
// change to them is versioned, audited and attributable.
type RoutingPolicy struct {
	MaxDelegations int `json:"max_delegations,omitempty"`
	MaxConcurrency int `json:"max_concurrency,omitempty"`
	MaxDepth       int `json:"max_depth,omitempty"`
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

	Graph   GraphSpec     `json:"graph,omitzero"`
	Routing RoutingPolicy `json:"routing,omitzero"`
}

// SchemaValidator is the host's JSON Schema implementation.
//
// The Runtime does not ship one: schema validation is a large dependency with
// opinions, and a Runtime whose production graph is standard-library-only
// cannot carry it. The host supplies whichever it already uses.
type SchemaValidator interface {
	ValidateSchema(schema json.RawMessage) error
	ValidateValue(schema, value json.RawMessage) error
}
