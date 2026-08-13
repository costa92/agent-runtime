// Package llm is the provider-neutral model port.
//
// It holds no provider configuration, no API keys, no base URLs and no product
// types. A host adapts whichever provider it uses to this interface; the
// Runtime never learns which one that was. That is what lets a governance
// decision, a budget or an event mean the same thing across two deployments
// running different engines.
package llm

import (
	"encoding/json"

	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// ModelRef names an engine by stable key.
//
// It is a key into a published ModelProfile rather than a provider/model pair,
// so that swapping an engine is a publish and not a release. The Runtime never
// parses it.
type ModelRef struct {
	Profile string `json:"profile"`
}

// Capabilities is what an engine can actually do.
//
// Asked rather than assumed: a Runtime that tried tool calling and interpreted
// the failure would have to distinguish "this model has no tools" from "this
// call failed", and those need opposite responses — degrade in the first case,
// retry in the second.
type Capabilities struct {
	Tools     bool `json:"tools"`
	Streaming bool `json:"streaming"`
	// MaxInputTokens is the engine's context ceiling, zero when unknown.
	MaxInputTokens int `json:"max_input_tokens,omitempty"`
}

// Role is who produced a message.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	// RoleTool carries a tool's result back to the model, keyed by the call ID
	// the model issued.
	RoleTool Role = "tool"
)

// Message is one turn.
type Message struct {
	Role    Role   `json:"role"`
	Content string `json:"content"`
	// ToolCallID is set on a RoleTool message, tying the result to the call.
	ToolCallID string `json:"tool_call_id,omitempty"`
}

// ToolDef is a tool as the model sees it: a name, a description and a JSON
// Schema. The Runtime passes the schema through without validating it — the
// host owns schema validation, since the Runtime ships no schema library.
type ToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

// ToolCall is the model asking for a tool.
//
// ID is the model's own stable identifier and must survive round-tripping: it
// is how a result is matched to its call, and a client that regenerated it
// would silently mismatch results in a parallel call batch.
//
// Arguments stay raw. Decoding them here would mean guessing a shape the tool
// has not yet been consulted about, and re-encoding would change bytes the
// model produced.
type ToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// Usage is what one call consumed.
type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// Limits converts usage into the Runtime's metering units, so that budget and
// quota count model spend the same way they count everything else.
func (u Usage) Limits() run.Limits {
	return run.Limits{
		LLMCalls: 1,
		Tokens:   u.InputTokens + u.OutputTokens,
	}
}

// Attempt is one physical call to a provider.
//
// Every attempt reports its usage, including a failed one: a provider that
// consumed the prompt and then errored still charged for it, and a ledger that
// counted only successes would understate exactly the spend it exists to bound.
type Attempt struct {
	Usage Usage `json:"usage"`
	// Failed marks an attempt that did not produce a response. Kept alongside
	// the usage rather than replacing it, because the cost is real either way.
	Failed bool `json:"failed,omitempty"`
	// Detail is for operators. Nothing branches on it.
	Detail string `json:"detail,omitempty"`
}

// Request is one model call.
//
// It carries no attempt count and no retry policy. The Runtime owns attempt
// limits, because attempts are budget: a client that retried on its own would
// spend against an envelope it cannot see.
type Request struct {
	Model    ModelRef
	Messages []Message
	Tools    []ToolDef
	// Temperature and MaxTokens come from the published Definition.
	Temperature float64
	MaxTokens   int
}

// Chunk is one streamed fragment.
type Chunk struct {
	// Content is the incremental text. Chunks are delivered in order, and a
	// consumer concatenating them must get the same string as Complete would
	// have returned.
	Content string
	// ToolCall is set when the fragment completes a tool call.
	ToolCall *ToolCall
}

// Response is a finished call.
type Response struct {
	Message      Message
	ToolCalls    []ToolCall
	Usage        Usage
	FinishReason string
	// Attempts is every physical call made, in order. It is populated even
	// when the call ultimately failed, so the spend is attributable.
	Attempts []Attempt
}
