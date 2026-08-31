package agentruntime_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// observe.Observer has always declared Chunk, and nothing ever called it.
//
// The consequence was not a missing metric: a host that wanted to show output
// as it was produced had no way to see it, so the one that wanted to — SSE
// review and enhance — sliced the finished answer into ten-rune pieces and
// replayed them, which looks like streaming and is not. The port existed; only
// the call from the governed Model path to it was missing.
func TestAModelCallStreamsItsOutputToTheObserver(t *testing.T) {
	h := newHarness(
		t,
		scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
			if _, err := request.Ports.Model(ctx, llm.Request{}); err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}},
		withDefinition(definition.Definition{
			Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Mode:           definition.ModeSpecialist,
			Implementation: "answer",
			Prompt:         "be brief",
			Model:          definition.ModelPolicy{Profile: "fast", MaxTokens: 900},
		}),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Models = scriptedModels{chunks: []string{"账单", "从哪", "来"}}
		}),
	)

	if _, err := h.runtime.Advance(t.Context(), start(t, h).ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	if got := strings.Join(h.observer.chunks, ""); got != "账单从哪来" {
		t.Errorf("observed chunks = %q, want the provider's output in order", got)
	}
	if len(h.observer.chunks) != 3 {
		t.Errorf("chunk count = %d, want the provider's own 3; the Runtime must not "+
			"re-chunk what it forwards", len(h.observer.chunks))
	}
}

// A call that carries tool structure streams like any other.
//
// This assertion used to be its exact inverse, and the inversion is the point.
// The host client folded a streamed conversation into one user string, dropping
// every tool_call_id with it, so a structured round could not be streamed
// without corrupting it. The cost was not paid by tool calls but by readers:
// an Agent that offers a tool offers it on every round, so article review —
// the endpoint streaming exists for — streamed nothing, and the round after the
// tools resolve is exactly the round that produces the answer a person waits
// for. The host now streams a structured round through the tool-calling wire,
// which keeps the ids, so withholding chunks here would buy nothing.
func TestAToolCarryingCallStreams(t *testing.T) {
	h := newHarness(
		t,
		scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
			if _, err := request.Ports.Model(ctx, llm.Request{
				NoTools: true,
				Messages: []llm.Message{
					{Role: llm.RoleAssistant, ToolCalls: []llm.ToolCall{{ID: "c1", Name: "search"}}},
					{Role: llm.RoleTool, ToolCallID: "c1", Content: "hit"},
				},
			}); err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}},
		withDefinition(definition.Definition{
			Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Mode:           definition.ModeSpecialist,
			Implementation: "answer",
			Prompt:         "be brief",
			Model:          definition.ModelPolicy{Profile: "fast", MaxTokens: 900},
		}),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Models = scriptedModels{chunks: []string{"should", "not", "stream"}}
		}),
	)

	if _, err := h.runtime.Advance(t.Context(), start(t, h).ID); err != nil {
		t.Fatalf("advance: %v", err)
	}

	if got := strings.Join(h.observer.chunks, ""); got != "shouldnotstream" {
		t.Errorf("chunks = %q, want the provider's output: a structured round is "+
			"streamed through the tool-calling wire, which keeps its ids", got)
	}
}

// A model call still works when the engine cannot stream. Capabilities are
// declared per engine, and asking one that said no is how a working call turns
// into a provider error.
func TestANonStreamingEngineStillCompletes(t *testing.T) {
	var completed bool
	h := newHarness(
		t,
		scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
			if _, err := request.Ports.Model(ctx, llm.Request{}); err != nil {
				return agent.Response{}, err
			}
			completed = true
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}},
		withDefinition(definition.Definition{
			Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
			Mode:           definition.ModeSpecialist,
			Implementation: "answer",
			Prompt:         "be brief",
			Model:          definition.ModelPolicy{Profile: "fast", MaxTokens: 900},
		}),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Models = noStreamModels{chunks: []string{"ignored"}}
		}),
	)

	if _, err := h.runtime.Advance(t.Context(), start(t, h).ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if !completed {
		t.Fatal("the call did not complete against a non-streaming engine")
	}
	if len(h.observer.chunks) != 0 {
		t.Errorf("chunks = %v, want none from an engine that declares no streaming",
			h.observer.chunks)
	}
}

// noStreamModels declares Streaming false. Its Stream would fail the test if it
// were ever reached, which is the point: the guard is the declaration, not a
// hope that nobody calls it.
type noStreamModels struct{ chunks []string }

func (m noStreamModels) Resolve(context.Context, llm.ModelRef) (llm.Client, error) {
	return noStreamClient{}, nil
}

type noStreamClient struct{}

func (noStreamClient) Capabilities(context.Context, llm.ModelRef) (llm.Capabilities, error) {
	return llm.Capabilities{Tools: true, Streaming: false}, nil
}

func (noStreamClient) Complete(context.Context, llm.Request) (llm.Response, error) {
	return llm.Response{
		Message:  llm.Message{Role: llm.RoleAssistant, Content: "done"},
		Attempts: []llm.Attempt{{Usage: llm.Usage{InputTokens: 10, OutputTokens: 5}}},
	}, nil
}

func (noStreamClient) Stream(
	context.Context, llm.Request, func(llm.Chunk) error,
) (llm.Response, error) {
	return llm.Response{}, run.NewError(
		"test.stream_on_non_streaming_engine", run.ErrorInternal, run.RetryNever,
	)
}

// A request carrying images must not take the streaming path.
//
// Same reason a tool-carrying one must not: the host client folds a streamed
// request into a system and a user string, and an image content part has
// nowhere to go in a string. The model would be asked to describe a picture it
// was never sent — and would answer, confidently, about nothing.
func TestAnImageCarryingCallDoesNotStream(t *testing.T) {
	h := newHarness(
		t,
		scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
			if _, err := request.Ports.Model(ctx, llm.Request{
				Messages: []llm.Message{{
					Role:    llm.RoleUser,
					Content: "这张图里有什么？",
					Images:  []llm.ImageRef{{URL: "https://example.com/cat.png"}},
				}},
			}); err != nil {
				return agent.Response{}, err
			}
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}},
		withDefinition(visionDefinition()),
		withDeps(func(deps *agentruntime.Dependencies) {
			deps.Models = scriptedModels{chunks: []string{"should", "not", "stream"}, vision: true}
		}),
	)

	if _, err := h.runtime.Advance(t.Context(), start(t, h).ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if len(h.observer.chunks) != 0 {
		t.Errorf("chunks = %v, want none: an image request took the streaming path, "+
			"which folds the conversation into strings and drops the image", h.observer.chunks)
	}
}

// An engine that cannot see refuses the call rather than answering without the
// image.
//
// Deliberately unlike the tools degradation above it. Dropping tools leaves a
// question the model can still answer; dropping images leaves "describe this
// picture" with no picture, and a confident answer about nothing is worse than
// an error.
func TestAnImageCallOnASightlessEngineIsRefused(t *testing.T) {
	var modelErr error
	h := newHarness(
		t,
		scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
			_, modelErr = request.Ports.Model(ctx, llm.Request{
				Messages: []llm.Message{{
					Role:    llm.RoleUser,
					Content: "这张图里有什么？",
					Images:  []llm.ImageRef{{URL: "https://example.com/cat.png"}},
				}},
			})
			if modelErr != nil {
				return agent.Response{}, modelErr
			}
			return agent.Response{Output: json.RawMessage(`"ok"`)}, nil
		}},
		withDefinition(visionDefinition()),
		withDeps(func(deps *agentruntime.Dependencies) {
			// scriptedModels declares Tools and Streaming, never Vision.
			deps.Models = scriptedModels{}
		}),
	)

	if _, err := h.runtime.Advance(t.Context(), start(t, h).ID); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if !errors.Is(modelErr, llm.ErrCapabilityUnsupported) {
		t.Fatalf("error = %v, want ErrCapabilityUnsupported", modelErr)
	}
}

func visionDefinition() definition.Definition {
	return definition.Definition{
		Ref:            run.DefinitionRef{ID: "assistant", Version: 1, Protocol: 1},
		Mode:           definition.ModeSpecialist,
		Implementation: "answer",
		Prompt:         "look",
		Model:          definition.ModelPolicy{Profile: "fast", MaxTokens: 900},
	}
}
