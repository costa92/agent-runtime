package workflow

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/resource"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/tool"
)

// KeySet is a frozen registry seen as the only thing compilation needs from it:
// whether it is closed, and which keys it holds.
type KeySet interface {
	Frozen() bool
	Keys() []string
}

// ToolSet is the tool registry, which reports specs rather than bare keys.
type ToolSet interface {
	Frozen() bool
	Specs() []tool.Spec
}

// Registries are the frozen sets a Definition is compiled against.
//
// Every one is required. A nil registry would have to mean either "no keys of
// this type exist" or "do not check this type", and the second reading turns a
// publish-time refusal into a mid-Run failure on a Definition that validated.
type Registries struct {
	Agents   KeySet
	Tools    ToolSet
	Memories KeySet
	// Models holds published ModelProfile keys. The host supplies it: profiles
	// are published data, not registered code, so there is no Runtime registry
	// to read them from.
	Models KeySet
}

// Compiler is the only final compiler in the Runtime.
//
// Nothing else resolves a registry key, validates the DAG, or produces an
// ExecutionGraphRef. A second compiler would be a second answer to "is this
// publishable", and the Run would discover the disagreement halfway through.
type Compiler struct {
	Registries Registries
	// Schemas is the host's JSON Schema implementation, used to reject a
	// malformed schema at publish rather than at the first Run that binds it.
	// Optional: a host that ships no validator still gets every other check.
	Schemas definition.SchemaValidator
}

// Compile turns a normalized Definition into an immutable graph and its digest.
//
// It runs on publish and on migration, not on Start. Everything it proves —
// keys resolve, the graph is acyclic, bindings read from something that will
// have run — is proven once, for every Run of that version.
func (c Compiler) Compile(declared definition.Definition) (*ExecutionGraph, error) {
	normalized, _, err := definition.Normalize(declared)
	if err != nil {
		return nil, err
	}
	if err := c.checkFrozen(); err != nil {
		return nil, err
	}
	if err := c.checkReferences(normalized); err != nil {
		return nil, err
	}

	nodes, err := c.buildNodes(normalized)
	if err != nil {
		return nil, err
	}
	if err := checkAcyclic(nodes); err != nil {
		return nil, err
	}

	digest, err := resource.DigestOf(nodes)
	if err != nil {
		return nil, err
	}
	return &ExecutionGraph{
		Ref: run.ExecutionGraphRef{
			ID:       normalized.Ref.ID,
			Version:  normalized.Ref.Version,
			Protocol: normalized.Ref.Protocol,
			Digest:   digest,
		},
		Nodes: nodes,
	}, nil
}

// checkFrozen refuses to compile against a registry that can still change.
// A key that resolved at publish and disappears before the Run is exactly the
// failure pinning is supposed to remove.
func (c Compiler) checkFrozen() error {
	// Ordered rather than a map, so that a deployment missing two registries
	// reports the same one every time.
	sets := []struct {
		name string
		set  interface{ Frozen() bool }
	}{
		{"agents", c.Registries.Agents},
		{"tools", c.Registries.Tools},
		{"memories", c.Registries.Memories},
		{"models", c.Registries.Models},
	}
	for _, entry := range sets {
		name, set := entry.name, entry.set
		if set == nil {
			return run.NewError("missing_registry", run.ErrorInternal, run.RetryNever,
				fmt.Errorf("no %s registry", name))
		}
		if !set.Frozen() {
			return run.NewError("registry_not_frozen", run.ErrorInternal, run.RetryNever,
				fmt.Errorf("%s registry is still open", name))
		}
	}
	return nil
}

func (c Compiler) checkReferences(declared definition.Definition) error {
	agents := keySet(c.Registries.Agents.Keys())
	if !agents[declared.Implementation] {
		return run.NewError("unknown_agent_key", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("agent %q is not registered", declared.Implementation))
	}
	for _, node := range declared.Graph.Nodes {
		if !agents[node.Agent] {
			return run.NewError("unknown_agent_key", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("node %q names unregistered agent %q", node.Name, node.Agent))
		}
	}

	tools := map[string]bool{}
	for _, spec := range c.Registries.Tools.Specs() {
		tools[spec.Name] = true
	}
	declaredTools := map[string]bool{}
	for _, declaredTool := range declared.Tools {
		declaredTools[declaredTool.Key] = true
		if !tools[declaredTool.Key] {
			// Optional tools are refused too. "Optional" says the Run can
			// proceed without calling it, not that the Definition may name
			// something nobody registered.
			return run.NewError("unknown_tool_key", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("tool %q is not registered", declaredTool.Key))
		}
	}

	for _, node := range declared.Graph.Nodes {
		for _, key := range node.Tools {
			if !declaredTools[key] {
				// A node grants a subset, never an addition. Letting a node
				// name a tool the Definition did not declare would move the
				// audited list of what an agent can reach out of the
				// Definition and into its graph, one node at a time.
				return run.NewError("undeclared_node_tool", run.ErrorInvalid, run.RetryNever,
					fmt.Errorf("node %q grants tool %q, which the definition does not declare",
						node.Name, key))
			}
		}
	}

	memories := keySet(c.Registries.Memories.Keys())
	for _, declaredMemory := range declared.Memories {
		if !memories[declaredMemory.Key] {
			return run.NewError("unknown_memory_key", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("memory %q is not registered", declaredMemory.Key))
		}
		// A retrieval ceiling above the whole Run's token envelope cannot be
		// satisfied. Normalize cannot see this: it compares two independently
		// declared fields, which is the compiler's job.
		if declared.Budget.Tokens > 0 && declaredMemory.MaxTokens > declared.Budget.Tokens {
			return run.NewError("memory_exceeds_budget", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("memory %q reads up to %d tokens against a %d-token budget",
					declaredMemory.Key, declaredMemory.MaxTokens, declared.Budget.Tokens))
		}
	}

	if profile := declared.Model.Profile; profile != "" {
		if !keySet(c.Registries.Models.Keys())[profile] {
			return run.NewError("unknown_model_profile", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("model profile %q is not published", profile))
		}
	}

	if c.Schemas != nil {
		schemas := []struct {
			field  string
			schema json.RawMessage
		}{
			{"input_schema", declared.InputSchema},
			{"output_schema", declared.OutputSchema},
		}
		for _, entry := range schemas {
			if len(entry.schema) == 0 {
				continue
			}
			if err := c.Schemas.ValidateSchema(entry.schema); err != nil {
				return run.NewError("invalid_schema", run.ErrorInvalid, run.RetryNever,
					fmt.Errorf("%s: %w", entry.field, err))
			}
		}
	}
	return nil
}

// buildNodes produces the compiled nodes in canonical order.
//
// A Definition with no declared graph compiles to a single node. It is the same
// structure the scheduler walks for a DAG, so nothing downstream has to ask
// which shape it is looking at.
func (c Compiler) buildNodes(declared definition.Definition) ([]Node, error) {
	if len(declared.Graph.Nodes) == 0 {
		return []Node{{
			ID:             declared.Implementation,
			Kind:           NodeAgent,
			Implementation: declared.Implementation,
			// The one node is the whole Definition, so the Definition's tools
			// are its tools. There is no NodeSpec to narrow them on, and no
			// sibling to narrow them away from.
			Tools:        toolKeys(declared.Tools),
			Input:        Binding{Source: SourceRunInput},
			OutputSchema: declared.OutputSchema,
			Failure:      FailHard,
		}}, nil
	}

	declaredNodes := map[string]bool{}
	hasDependents := map[string]bool{}
	for _, spec := range declared.Graph.Nodes {
		declaredNodes[spec.Name] = true
	}
	for _, spec := range declared.Graph.Nodes {
		for _, dependency := range spec.DependsOn {
			if !declaredNodes[dependency] {
				return nil, run.NewError("unknown_dependency", run.ErrorInvalid, run.RetryNever,
					fmt.Errorf("node %q depends on undeclared %q", spec.Name, dependency))
			}
			hasDependents[dependency] = true
		}
	}

	nodes := make([]Node, 0, len(declared.Graph.Nodes))
	for _, spec := range declared.Graph.Nodes {
		binding, err := bindingFor(spec)
		if err != nil {
			return nil, err
		}
		node := Node{
			ID:             spec.Name,
			Kind:           NodeAgent,
			Implementation: spec.Agent,
			DependsOn:      append([]string(nil), spec.DependsOn...),
			Tools:          append([]string(nil), spec.Tools...),
			Input:          binding,
			Failure:        FailHard,
		}
		if spec.Optional {
			node.Failure = FailSoft
		}
		if !hasDependents[spec.Name] {
			// A leaf produces the Run's output, so it is the node whose result
			// the declared OutputSchema describes.
			node.OutputSchema = declared.OutputSchema
		}
		nodes = append(nodes, node)
	}

	// Canonical order, so that two publishes of the same declaration produce
	// the same digest regardless of how the nodes were listed.
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes, nil
}

func toolKeys(refs []definition.ToolRef) []string {
	if len(refs) == 0 {
		return nil
	}
	keys := make([]string, len(refs))
	for i, ref := range refs {
		keys[i] = ref.Key
	}
	return keys
}

// bindingFor checks that a node reads from something that is guaranteed to have
// produced a value by the time it runs.
//
// Reading from a node that is not a declared dependency is the incompatibility
// the compiler can actually prove: without the edge, nothing orders the two, so
// the value may not exist. Whether the produced value fits the consumer's shape
// cannot be checked here — nodes name Agents, and an Agent registry key carries
// no schema. That check happens against the declared OutputSchema when a result
// is applied, using the host's validator.
func bindingFor(spec definition.NodeSpec) (Binding, error) {
	if len(spec.Inputs) == 0 {
		return Binding{Source: SourceRunInput}, nil
	}
	depends := map[string]bool{}
	for _, dependency := range spec.DependsOn {
		depends[dependency] = true
	}
	seen := map[string]bool{}
	for _, from := range spec.Inputs {
		if !depends[from] {
			return Binding{}, run.NewError("incompatible_binding", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("node %q reads from %q without depending on it", spec.Name, from))
		}
		if seen[from] {
			return Binding{}, run.NewError("duplicate_binding", run.ErrorInvalid, run.RetryNever,
				fmt.Errorf("node %q reads from %q twice", spec.Name, from))
		}
		seen[from] = true
	}

	source := SourceNodes
	if len(spec.Inputs) == 1 {
		source = SourceNode
	}
	return Binding{Source: source, From: append([]string(nil), spec.Inputs...)}, nil
}

// checkAcyclic refuses a graph that can never finish, by Kahn's algorithm: if
// any node still has unmet dependencies once nothing more can be scheduled, the
// remainder is a cycle.
func checkAcyclic(nodes []Node) error {
	remaining := map[string]int{}
	dependents := map[string][]string{}
	for _, node := range nodes {
		remaining[node.ID] = len(node.DependsOn)
		for _, dependency := range node.DependsOn {
			dependents[dependency] = append(dependents[dependency], node.ID)
		}
	}

	var ready []string
	for id, count := range remaining {
		if count == 0 {
			ready = append(ready, id)
		}
	}

	settled := 0
	for len(ready) > 0 {
		id := ready[len(ready)-1]
		ready = ready[:len(ready)-1]
		settled++
		for _, dependent := range dependents[id] {
			remaining[dependent]--
			if remaining[dependent] == 0 {
				ready = append(ready, dependent)
			}
		}
	}
	if settled != len(nodes) {
		var cyclic []string
		for id, count := range remaining {
			if count > 0 {
				cyclic = append(cyclic, id)
			}
		}
		sort.Strings(cyclic)
		return run.NewError("graph_cycle", run.ErrorInvalid, run.RetryNever,
			fmt.Errorf("nodes %v can never become ready", cyclic))
	}
	return nil
}

func keySet(keys []string) map[string]bool {
	set := make(map[string]bool, len(keys))
	for _, key := range keys {
		set[key] = true
	}
	return set
}
