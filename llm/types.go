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
	// Vision reports whether the engine accepts Message.Images. Declared per
	// engine like the rest: an engine that cannot see answers an image request
	// with an ordinary-looking error about the payload, which is not something
	// a caller can act on.
	Vision bool `json:"vision"`
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
	// ToolCalls is set on the assistant message that asked for tools. It is the
	// pair of RoleTool's ToolCallID: a result without its call in the request
	// is rejected by providers, so the call must survive into the next round.
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	// Images are what the model should look at, alongside Content.
	//
	// A separate field rather than markup inside Content: the wire format for
	// an image is a structured content part, and a client that had to find
	// image references by parsing prose would be guessing. An engine that
	// cannot see them says so through Capabilities.Vision, and a request
	// carrying images never takes the streaming path — that path folds the
	// conversation into two strings and images do not survive it.
	Images []ImageRef `json:"images,omitempty"`
}

// ImageRef points at one image the model should look at.
//
// A URL, not bytes: the host already stores the image somewhere the provider
// can fetch, and carrying megabytes through the Runtime's budget accounting
// would mean charging a Run for transport it does not control. A data: URI is
// a URL, so a host that must inline small images still can.
type ImageRef struct {
	URL string `json:"url"`
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
	// NoTools says "offer the model nothing", as distinct from Tools being
	// empty because the caller had no opinion and wants the node's grant.
	//
	// The two are indistinguishable by length, and the Runtime fills the node's
	// tools whenever Tools is empty — so without this there is no way to take
	// tools away. A caller that has spent its tool budget needs exactly that:
	// leaving the definitions in front of a model that may no longer call them
	// teaches it to keep asking, and every ask costs a round.
	NoTools bool
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
	// Model echoes the profile this call was billed to, so a host can price
	// the usage without threading the request alongside the response. Filled
	// by the governed Model port; an agent never sets it.
	Model        ModelRef
	Message      Message
	ToolCalls    []ToolCall
	Usage        Usage
	FinishReason string
	// Attempts is every physical call made, in order. It is populated even
	// when the call ultimately failed, so the spend is attributable.
	Attempts []Attempt
}
