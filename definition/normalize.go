package definition

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/kart-io/wechat-account/agent-runtime/resource"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// Normalize validates a Definition's own fields, returns an immutable
// normalized copy, and computes its canonical digest.
//
// "Its own fields" is the boundary and it is deliberate. Nothing here resolves
// a registry key, walks the graph for cycles, or checks that a tool exists —
// that is the compiler's, once, in Task 7. Two validators would eventually
// disagree about whether something is publishable, and the one that ran second
// would be the one nobody remembered to update.
//
// Normalization is what makes the digest meaningful: two declarations that
// differ only in the order they listed their tools are the same definition, and
// must hash the same, or every republish looks like a change.
func Normalize(definition Definition) (Definition, string, error) {
	if err := validate(definition); err != nil {
		return Definition{}, "", err
	}

	normalized := definition
	normalized.Implementation = strings.TrimSpace(definition.Implementation)
	normalized.Prompt = strings.TrimSpace(definition.Prompt)
	normalized.Tools = normalizeTools(definition.Tools)
	normalized.Memories = normalizeMemories(definition.Memories)
	normalized.Graph = normalizeGraph(definition.Graph)
	normalized.InputSchema = compactSchema(definition.InputSchema)
	normalized.OutputSchema = compactSchema(definition.OutputSchema)

	digest, err := resource.DigestOf(normalized)
	if err != nil {
		return Definition{}, "", err
	}
	return normalized, digest, nil
}

func validate(definition Definition) error {
	if definition.Ref.ID == "" {
		return run.NewError("missing_definition_id", run.ErrorInvalid, run.RetryNever)
	}
	if definition.Ref.Version == 0 {
		// A version-zero ref would let a Run pin something that can still
		// change under it, which is the one thing pinning exists to prevent.
		return run.NewError("mutable_version", run.ErrorInvalid, run.RetryNever)
	}
	if definition.Ref.Protocol == 0 {
		return run.NewError("unsupported_protocol", run.ErrorInvalid, run.RetryNever)
	}
	if !definition.Mode.Valid() {
		return run.NewError("unknown_execution_mode", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("mode %q", definition.Mode))
	}
	if strings.TrimSpace(definition.Implementation) == "" {
		return run.NewError("missing_implementation", run.ErrorInvalid, run.RetryNever)
	}
	if definition.Mode == ModeSpecialist && definition.Routing != (RoutingPolicy{}) {
		// A specialist that carries delegation limits is a declaration whose
		// two halves disagree, and the reader cannot tell which was meant.
		return run.NewError("routing_on_specialist", run.ErrorInvalid, run.RetryNever)
	}

	if err := validateSchemaShape("input_schema", definition.InputSchema); err != nil {
		return err
	}
	if err := validateSchemaShape("output_schema", definition.OutputSchema); err != nil {
		return err
	}

	seenTools := map[string]bool{}
	for _, tool := range definition.Tools {
		key := strings.TrimSpace(tool.Key)
		if key == "" {
			return run.NewError("empty_tool_key", run.ErrorInvalid, run.RetryNever)
		}
		if seenTools[key] {
			return run.NewError("duplicate_tool_key", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("tool %q declared twice", key))
		}
		seenTools[key] = true
	}

	seenMemories := map[string]bool{}
	for _, memory := range definition.Memories {
		key := strings.TrimSpace(memory.Key)
		if key == "" {
			return run.NewError("empty_memory_key", run.ErrorInvalid, run.RetryNever)
		}
		if strings.TrimSpace(memory.Namespace) == "" {
			// The namespace is the isolation boundary. Defaulting it would put
			// one tenant's retrieval in reach of another's records.
			return run.NewError("missing_memory_namespace", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("memory %q", key))
		}
		if memory.MaxRecords <= 0 || memory.MaxTokens <= 0 {
			return run.NewError("unbounded_memory_read", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("memory %q must bound both records and tokens", key))
		}
		scoped := key + "\x00" + memory.Namespace
		if seenMemories[scoped] {
			return run.NewError("duplicate_memory_ref", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("memory %q/%q declared twice", key, memory.Namespace))
		}
		seenMemories[scoped] = true
	}

	seenNodes := map[string]bool{}
	for _, node := range definition.Graph.Nodes {
		if strings.TrimSpace(node.Name) == "" {
			return run.NewError("empty_node_name", run.ErrorInvalid, run.RetryNever)
		}
		if seenNodes[node.Name] {
			return run.NewError("duplicate_node_name", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("node %q declared twice", node.Name))
		}
		seenNodes[node.Name] = true
	}

	if definition.Budget.LLMCalls < 0 || definition.Budget.Tokens < 0 || definition.Budget.ToolCalls < 0 {
		return run.NewError("negative_budget", run.ErrorInvalid, run.RetryNever)
	}
	return nil
}

// validateSchemaShape checks only that a schema is well-formed JSON object.
// Whether it is a valid JSON Schema is the host's SchemaValidator's answer —
// the Runtime does not ship a schema implementation.
func validateSchemaShape(field string, schema json.RawMessage) error {
	if len(schema) == 0 {
		return nil
	}
	var object map[string]any
	if err := json.Unmarshal(schema, &object); err != nil {
		return run.NewError("invalid_schema", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("%s: %w", field, err))
	}
	return nil
}

// normalizeTools sorts by key so that reordering a list is not a change.
func normalizeTools(tools []ToolRef) []ToolRef {
	if len(tools) == 0 {
		return nil
	}
	out := make([]ToolRef, len(tools))
	for i, tool := range tools {
		out[i] = ToolRef{Key: strings.TrimSpace(tool.Key), Required: tool.Required}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func normalizeMemories(memories []MemoryRef) []MemoryRef {
	if len(memories) == 0 {
		return nil
	}
	out := make([]MemoryRef, len(memories))
	for i, memory := range memories {
		out[i] = memory
		out[i].Key = strings.TrimSpace(memory.Key)
		out[i].Namespace = strings.TrimSpace(memory.Namespace)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].Namespace < out[j].Namespace
	})
	return out
}

// normalizeGraph sorts each node's dependencies but leaves node order alone:
// the compiler derives execution order from the edges, and reordering the
// declaration is not meant to be visible.
func normalizeGraph(graph GraphSpec) GraphSpec {
	if len(graph.Nodes) == 0 {
		return GraphSpec{}
	}
	nodes := make([]NodeSpec, len(graph.Nodes))
	for i, node := range graph.Nodes {
		nodes[i] = NodeSpec{Name: node.Name, Agent: node.Agent}
		if len(node.DependsOn) > 0 {
			depends := append([]string(nil), node.DependsOn...)
			sort.Strings(depends)
			nodes[i].DependsOn = depends
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return GraphSpec{Nodes: nodes}
}

// compactSchema removes insignificant whitespace so that reformatting a schema
// does not change the digest.
func compactSchema(schema json.RawMessage) json.RawMessage {
	if len(schema) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(schema, &value); err != nil {
		return schema
	}
	compacted, err := json.Marshal(value)
	if err != nil {
		return schema
	}
	return compacted
}
