// Package product defines deterministic product workflow decisions. A Product
// observes only committed Session facts; it cannot carry progress in memory.
package product

import (
	"context"
	"encoding/json"

	"github.com/costa92/agent-runtime/session"
)

type Kind string

const (
	Model   Kind = "model"
	Compute Kind = "compute"
	Tool    Kind = "tool"
)

// Plan is persisted in StepStart before any stage work. Messages are explicit
// model input for this stage, not hidden process-local prompt state. Payload
// is the product's versioned stage input, read back on crash recovery.
type Plan struct {
	Done         bool              `json:"done,omitempty"`
	FinalStatus  string            `json:"final_status,omitempty"`
	Key          string            `json:"key,omitempty"`
	Kind         Kind              `json:"kind,omitempty"`
	Payload      json.RawMessage   `json:"payload,omitempty"`
	Messages     []session.Message `json:"messages,omitempty"`
	AllowedTools []string          `json:"allowed_tools,omitempty"`
	Optional     bool              `json:"optional,omitempty"`
	Internal     bool              `json:"internal,omitempty"`
}

// Computed is a pure CPU stage result. Tool stages declare governed tool calls
// in Assistant.ToolCalls; Agent Loop performs and records each invocation.
type Computed struct {
	Assistant session.Message `json:"assistant"`
	Output    json.RawMessage `json:"output,omitempty"`
	Warnings  []string        `json:"warnings,omitempty"`
}

// Settlement is a deterministic quality gate after a model or tool stage.
// An optional stage may fail with warnings while the Turn continues.
type Settlement struct {
	Status   string          `json:"status"` // completed or failed
	Output   json.RawMessage `json:"output,omitempty"`
	Warnings []string        `json:"warnings,omitempty"`
}

// Product owns workflow order and stage-specific pure transformations. Model
// and external tool effects remain under Agent Loop and governed services.
type Product interface {
	Next(context.Context, session.Projection) (Plan, error)
	Compute(context.Context, session.Projection, Plan) (Computed, error)
	Settle(context.Context, session.Projection, Plan) (Settlement, error)
}
