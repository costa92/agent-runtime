// Package session defines the durable Session, Turn, and Step event protocol.
// Events are the only execution fact source; projections are rebuilt by Replay.
package session

import (
	"encoding/json"
	"time"
)

const ProtocolVersion uint32 = 2

type Kind string

const (
	SessionStarted   Kind = "session/start"
	SessionParked    Kind = "session/park"
	SessionResumed   Kind = "session/resume"
	SessionCancelled Kind = "session/cancel"
	HistorySeeded    Kind = "history/seed"
	TurnStarted      Kind = "turn/start"
	MessageAccepted  Kind = "message/accepted"
	StepStarted      Kind = "step/start"
	ModelRequested   Kind = "model/request"
	AttemptStarted   Kind = "model/attempt-start"
	AttemptFinished  Kind = "model/attempt-finish"
	ToolCalled       Kind = "tool/call"
	ToolSettled      Kind = "tool/result"
	ToolSkipped      Kind = "tool/skipped"
	ApprovalAsked    Kind = "approval/request"
	ApprovalResolved Kind = "approval/resolve"
	BudgetSettled    Kind = "budget/settle"
	StepEnded        Kind = "step/end"
	TurnEnded        Kind = "turn/end"
)

// Event is a versioned, contiguous fact in one Session's append-only log.
type Event struct {
	Version   uint32          `json:"version"`
	SessionID string          `json:"session_id"`
	Sequence  uint64          `json:"sequence"`
	Kind      Kind            `json:"kind"`
	Time      time.Time       `json:"time"`
	Payload   json.RawMessage `json:"payload"`
}

type OutboxRecord struct {
	Sequence uint64          `json:"sequence"`
	Key      string          `json:"key"`
	Payload  json.RawMessage `json:"payload"`
}

type SessionStart struct {
	TenantID       string `json:"tenant_id"`
	PrincipalID    string `json:"principal_id"`
	ProjectID      int64  `json:"project_id"`
	AgentVersionID int64  `json:"agent_version_id,omitempty"`
	ConfigDigest   string `json:"config_digest"`
	PluginDigest   string `json:"plugin_digest"`
}

type SessionPark struct {
	Reason string `json:"reason"`
}
type SessionResume struct {
	Reason string `json:"reason"`
}

// HistorySeed imports settled conversation context at cutover. It carries only
// model-visible messages and makes no claim about prior execution attempts.
type HistorySeed struct {
	Messages []Message `json:"messages"`
}

type TurnStart struct {
	ID string `json:"id"`
}

// Message is model-visible content. Tool calls and results are separate roles
// so replay can reconstruct their exact order without process-local state.
type Message struct {
	Role             string          `json:"role"`
	Content          string          `json:"content,omitempty"`
	ReasoningContent string          `json:"reasoning_content,omitempty"`
	Error            string          `json:"error,omitempty"`
	CallID           string          `json:"call_id,omitempty"`
	Images           []ImageRef      `json:"images,omitempty"`
	ToolCalls        []ModelToolCall `json:"tool_calls,omitempty"`
}

type ImageRef struct {
	URL string `json:"url"`
}

// ModelToolCall is a model-visible declaration, distinct from the durable
// ToolCall invocation that carries the service's idempotency key.
type ModelToolCall struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type AcceptedMessage struct {
	TurnID  string  `json:"turn_id"`
	StepID  string  `json:"step_id,omitempty"`
	Message Message `json:"message"`
}

type StepStart struct {
	TurnID       string          `json:"turn_id"`
	ID           string          `json:"id"`
	StageKey     string          `json:"stage_key"`
	Kind         StageKind       `json:"kind"`
	Input        json.RawMessage `json:"input"`
	AllowedTools []string        `json:"allowed_tools,omitempty"`
	Optional     bool            `json:"optional,omitempty"`
	Internal     bool            `json:"internal,omitempty"`
}

type StageKind string

const (
	StageModel   StageKind = "model"
	StageCompute StageKind = "compute"
	StageTool    StageKind = "tool"
)

// ModelRequest fixes the exact model-visible input before an attempt starts.
// Replay verifies History against prior committed facts.
type ModelRequest struct {
	TurnID        string         `json:"turn_id"`
	StepID        string         `json:"step_id"`
	ConfigDigest  string         `json:"config_digest"`
	Profile       string         `json:"profile"`
	Preamble      []Message      `json:"preamble,omitempty"`
	History       []Message      `json:"history"`
	StageMessages []Message      `json:"stage_messages,omitempty"`
	Tools         []ModelToolDef `json:"tools,omitempty"`
	NoTools       bool           `json:"no_tools,omitempty"`
	Deterministic bool           `json:"deterministic,omitempty"`
	Temperature   float64        `json:"temperature,omitempty"`
	MaxTokens     int            `json:"max_tokens,omitempty"`
	Reasoning     Reasoning      `json:"reasoning"`
}

// Messages is the exact input submitted to the model: committed conversation
// history followed by this stage's durable, request-specific prompt.
func (r ModelRequest) Messages() []Message {
	messages := make([]Message, 0, len(r.Preamble)+len(r.History)+len(r.StageMessages))
	messages = append(messages, r.Preamble...)
	messages = append(messages, r.History...)
	messages = append(messages, r.StageMessages...)
	return messages
}

type ModelToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type Reasoning struct {
	Enabled   bool   `json:"enabled"`
	Effort    string `json:"effort,omitempty"`
	MaxTokens int    `json:"max_tokens,omitempty"`
}

type AttemptStart struct {
	TurnID string `json:"turn_id"`
	StepID string `json:"step_id"`
	ID     string `json:"id"`
}

type AttemptFinish struct {
	TurnID  string     `json:"turn_id"`
	StepID  string     `json:"step_id"`
	ID      string     `json:"id"`
	Outcome string     `json:"outcome"` // succeeded or failed
	Message Message    `json:"message,omitempty"`
	Error   string     `json:"error,omitempty"`
	Profile string     `json:"profile,omitempty"`
	Usage   ModelUsage `json:"usage"`
}

type ModelUsage struct {
	InputTokens       int `json:"input_tokens"`
	OutputTokens      int `json:"output_tokens"`
	CachedInputTokens int `json:"cached_input_tokens,omitempty"`
}

// CostUsage is a measured non-token resource returned by a tool gateway.
type CostUsage struct {
	ResourceType string  `json:"resource_type"`
	Provider     string  `json:"provider"`
	Model        string  `json:"model"`
	Quantity     float64 `json:"quantity"`
	Unit         string  `json:"unit"`
}

type ToolCall struct {
	TurnID                string `json:"turn_id"`
	StepID                string `json:"step_id"`
	ID                    string `json:"id"`
	Name                  string `json:"name"`
	Arguments             string `json:"arguments"`
	IdempotencyKey        string `json:"idempotency_key"`
	ApprovalID            string `json:"approval_id,omitempty"`
	InvocationFingerprint string `json:"invocation_fingerprint,omitempty"`
}

type ToolResult struct {
	TurnID     string      `json:"turn_id"`
	StepID     string      `json:"step_id"`
	ID         string      `json:"id"`
	Outcome    string      `json:"outcome"` // succeeded, failed, or unknown
	Result     string      `json:"result"`
	Error      string      `json:"error,omitempty"`
	CostUsages []CostUsage `json:"cost_usages,omitempty"`
}

// ToolSkip closes a model-visible declaration that was never executed. It is
// distinct from ToolResult: no external invocation or measured usage exists.
type ToolSkip struct {
	TurnID string `json:"turn_id"`
	StepID string `json:"step_id"`
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

type ApprovalRequest struct {
	TurnID                string `json:"turn_id"`
	StepID                string `json:"step_id"`
	ID                    string `json:"id"`
	ToolCallID            string `json:"tool_call_id"`
	ToolName              string `json:"tool_name"`
	Arguments             string `json:"arguments"`
	IdempotencyKey        string `json:"idempotency_key"`
	InvocationFingerprint string `json:"invocation_fingerprint"`
}

type ApprovalDecision struct {
	TurnID   string `json:"turn_id"`
	StepID   string `json:"step_id"`
	ID       string `json:"id"`
	Decision string `json:"decision"` // approved or denied
}

type BudgetSettlement struct {
	TurnID   string         `json:"turn_id"`
	StepID   string         `json:"step_id"`
	Category BudgetCategory `json:"category"`
	Units    uint64         `json:"units"`
}

type BudgetCategory string

const (
	BudgetModelCalls   BudgetCategory = "model_calls"
	BudgetInputTokens  BudgetCategory = "input_tokens"
	BudgetOutputTokens BudgetCategory = "output_tokens"
	BudgetToolCalls    BudgetCategory = "tool_calls"
	BudgetMediaOps     BudgetCategory = "media_ops"
)

type StepEnd struct {
	TurnID   string          `json:"turn_id"`
	StepID   string          `json:"step_id"`
	Status   string          `json:"status"` // completed, failed, or cancelled
	Output   json.RawMessage `json:"output"`
	Warnings []string        `json:"warnings,omitempty"`
}

type TurnEnd struct {
	TurnID string `json:"turn_id"`
	Status string `json:"status"` // completed, failed, or cancelled
}

type ToolState struct {
	Call   ToolCall    `json:"call"`
	Result *ToolResult `json:"result,omitempty"`
}

type ApprovalState struct {
	Request  ApprovalRequest   `json:"request"`
	Decision *ApprovalDecision `json:"decision,omitempty"`
}

type Step struct {
	ID            string                    `json:"id"`
	StageKey      string                    `json:"stage_key"`
	Kind          StageKind                 `json:"kind"`
	Input         json.RawMessage           `json:"input"`
	AllowedTools  []string                  `json:"allowed_tools"`
	Optional      bool                      `json:"optional"`
	Internal      bool                      `json:"internal,omitempty"`
	Status        string                    `json:"status"`
	Output        json.RawMessage           `json:"output,omitempty"`
	Warnings      []string                  `json:"warnings"`
	Request       *ModelRequest             `json:"request,omitempty"`
	Attempts      []AttemptFinish           `json:"attempts"`
	ActiveAttempt string                    `json:"active_attempt,omitempty"`
	DeclaredTools []ModelToolCall           `json:"declared_tools"`
	Tools         map[string]ToolState      `json:"tools"`
	SkippedTools  map[string]ToolSkip       `json:"skipped_tools,omitempty"`
	Approvals     map[string]ApprovalState  `json:"approvals"`
	Budget        map[BudgetCategory]uint64 `json:"budget"`
	Deterministic bool                      `json:"deterministic"`
}

type Turn struct {
	ID       string    `json:"id"`
	Status   string    `json:"status"`
	Steps    []Step    `json:"steps"`
	Messages []Message `json:"messages"`
}

type Projection struct {
	ID              string    `json:"id"`
	Sequence        uint64    `json:"sequence"`
	TenantID        string    `json:"tenant_id"`
	PrincipalID     string    `json:"principal_id"`
	ProjectID       int64     `json:"project_id"`
	AgentVersionID  int64     `json:"agent_version_id,omitempty"`
	Status          string    `json:"status"`
	ConfigDigest    string    `json:"config_digest"`
	PluginDigest    string    `json:"plugin_digest"`
	ParkedReason    string    `json:"parked_reason,omitempty"`
	CancelRequested bool      `json:"cancel_requested,omitempty"`
	Turns           []Turn    `json:"turns"`
	History         []Message `json:"history"`
}
