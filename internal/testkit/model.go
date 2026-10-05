package testkit

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/costa92/agent-runtime/llm"
	"github.com/costa92/agent-runtime/run"
)

// ScriptedModel is a deterministic model client.
//
// Scripted rather than random: a conformance suite has to assert exact output,
// and an adapter checked against a client that varies would only be checked
// against whatever it happened to return.
type ScriptedModel struct {
	capabilities llm.Capabilities
	// chunks is the reply, pre-split. Concatenated it must equal what Complete
	// returns, which is the property the suite checks.
	chunks []string
	// failAfterPrompt makes the provider error once it has already consumed
	// the prompt — the case where usage must still be reported.
	failAfterPrompt bool
}

func NewScriptedModel() *ScriptedModel {
	return &ScriptedModel{
		capabilities: llm.Capabilities{Tools: true, Streaming: true, MaxInputTokens: 8000},
		chunks:       []string{"hello", " ", "world"},
	}
}

// NewToolLessModel returns a client for an engine without tool calling.
func NewToolLessModel() *ScriptedModel {
	model := NewScriptedModel()
	model.capabilities.Tools = false
	return model
}

// NewFailingModel returns a client whose provider errors after consuming the
// prompt.
func NewFailingModel() *ScriptedModel {
	model := NewScriptedModel()
	model.failAfterPrompt = true
	return model
}

var _ llm.Client = (*ScriptedModel)(nil)

func (m *ScriptedModel) Capabilities(context.Context, llm.ModelRef) (llm.Capabilities, error) {
	return m.capabilities, nil
}

func (m *ScriptedModel) Complete(ctx context.Context, request llm.Request) (llm.Response, error) {
	if err := ctx.Err(); err != nil {
		return llm.Response{}, llm.Cancelled(err)
	}
	if len(request.Tools) > 0 && !m.capabilities.Tools {
		return llm.Response{}, llm.ErrCapabilityUnsupported
	}

	usage := llm.Usage{InputTokens: promptTokens(request), OutputTokens: 3}
	if m.failAfterPrompt {
		// One attempt, with its usage. The Runtime owns retries; a client that
		// added its own would spend against an envelope it cannot see.
		return llm.Response{
			Attempts: []llm.Attempt{{
				Usage:  llm.Usage{InputTokens: usage.InputTokens},
				Failed: true,
				Detail: "scripted provider failure",
			}},
		}, run.NewError("model.provider_error", run.ErrorRetryable, run.RetryBackoff)
	}

	response := llm.Response{
		Message:      llm.Message{Role: llm.RoleAssistant, Content: strings.Join(m.chunks, "")},
		Usage:        usage,
		FinishReason: "stop",
		Attempts:     []llm.Attempt{{Usage: usage}},
	}
	if len(request.Tools) > 0 {
		response.ToolCalls = []llm.ToolCall{{
			ID:        "call-1",
			Name:      request.Tools[0].Name,
			Arguments: json.RawMessage(`{"query":"scripted"}`),
		}}
		response.FinishReason = "tool_calls"
	}
	return response, nil
}

func (m *ScriptedModel) Stream(ctx context.Context, request llm.Request, onChunk func(llm.Chunk) error) (llm.Response, error) {
	if err := ctx.Err(); err != nil {
		return llm.Response{}, llm.Cancelled(err)
	}
	if len(request.Tools) > 0 && !m.capabilities.Tools {
		return llm.Response{}, llm.ErrCapabilityUnsupported
	}

	for _, chunk := range m.chunks {
		if err := onChunk(llm.Chunk{Content: chunk}); err != nil {
			// The consumer's error is returned unwrapped so errors.Is finds it:
			// stopping a stream is a caller decision, not a model failure.
			return llm.Response{}, err
		}
	}
	return m.Complete(ctx, request)
}

func promptTokens(request llm.Request) int {
	var total int
	for _, message := range request.Messages {
		total += len(strings.Fields(message.Content))
	}
	if total == 0 {
		total = 1
	}
	return total
}
