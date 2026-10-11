// Package model defines the provider-neutral model service used by Agent Loop.
// Provider credentials and transports belong to the host, not this module.
package model

import (
	"context"
	"encoding/json"

	"github.com/costa92/agent-runtime/session"
)

// Tool is one capability declaration shown to a model.
type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

// Capabilities belong to the prepared provider call, since routing may select
// a different model for each Step.
type Capabilities struct {
	Tools          bool `json:"tools"`
	Streaming      bool `json:"streaming"`
	Vision         bool `json:"vision"`
	MaxInputTokens int  `json:"max_input_tokens,omitempty"`
}

// Request contains the exact messages and tools proposed for one Step. The
// Agent Loop logs the admitted form before Call starts its first attempt.
type Request struct {
	Profile       string            `json:"profile"`
	Messages      []session.Message `json:"messages"`
	Tools         []Tool            `json:"tools,omitempty"`
	Deterministic bool              `json:"deterministic,omitempty"`
	NoReasoning   bool              `json:"no_reasoning,omitempty"`
	Reasoning     session.Reasoning `json:"reasoning"`
	Temperature   float64           `json:"temperature,omitempty"`
	MaxTokens     int               `json:"max_tokens,omitempty"`
}

// Usage records what a physical attempt spent, including failed attempts.
type Usage struct {
	InputTokens       int `json:"input_tokens"`
	OutputTokens      int `json:"output_tokens"`
	CachedInputTokens int `json:"cached_input_tokens,omitempty"`
}

// Chunk is process-local stream data. The settled response, not chunks, is
// written to the durable Session log.
type Chunk struct {
	Content  string                 `json:"content,omitempty"`
	ToolCall *session.ModelToolCall `json:"tool_call,omitempty"`
}

// Response is one physical provider attempt. Providers return any known usage
// even when Complete or Stream returns an error; Agent Loop owns retries.
type Response struct {
	Message   session.Message         `json:"message"`
	ToolCalls []session.ModelToolCall `json:"tool_calls,omitempty"`
	// Profile identifies the provider/model that served this physical attempt.
	Profile      string `json:"profile"`
	Usage        Usage  `json:"usage"`
	FinishReason string `json:"finish_reason,omitempty"`
}

// Call is bound to a provider, profile and its actual capabilities. It cannot
// be persisted or reused across a Step; a resumed Step prepares a new call.
type Call interface {
	Capabilities() Capabilities
	Complete(context.Context) (Response, error)
	Stream(context.Context, func(Chunk) error) (Response, error)
}

// Service prepares the provider call before the Agent Loop admits input into
// the durable log. Only the Agent Loop invokes Call, after its begin event is
// committed and budget reserved.
type Service interface {
	Prepare(context.Context, Request) (Call, error)
}
