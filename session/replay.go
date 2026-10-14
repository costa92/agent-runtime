package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
)

var ErrInvalidLog = errors.New("invalid session event log")

// Replay validates a complete event log and derives its state. It never uses
// snapshots or plugin state, so interrupted work is visible after a restart.
func Replay(events []Event) (Projection, error) {
	var p Projection
	if len(events) == 0 {
		return p, fmt.Errorf("%w: empty", ErrInvalidLog)
	}
	turnIDs := map[string]bool{}
	stepIDs := map[string]bool{}
	callIDs := map[string]bool{}
	idempotencyKeys := map[string]bool{}
	approvalIDs := map[string]bool{}
	attemptIDs := map[string]bool{}
	seeded := false
	for i, e := range events {
		if e.Version != ProtocolVersion || e.SessionID == "" || e.Sequence != uint64(i+1) || len(e.Payload) == 0 || !json.Valid(e.Payload) {
			return Projection{}, invalid(i, "envelope or sequence")
		}
		if i == 0 && e.Kind != SessionStarted {
			return Projection{}, invalid(i, "first event must start session")
		}
		if i > 0 && e.SessionID != p.ID {
			return Projection{}, invalid(i, "session ID changed")
		}
		var t *Turn
		var s *Step
		if len(p.Turns) > 0 {
			t = &p.Turns[len(p.Turns)-1]
			if len(t.Steps) > 0 {
				s = &t.Steps[len(t.Steps)-1]
			}
		}
		fail := func(reason string) (Projection, error) { return Projection{}, invalid(i, reason) }
		if p.ParkedReason != "" && e.Kind != SessionResumed && e.Kind != SessionCancelled {
			return fail("session is parked")
		}
		if p.Status == "cancelled" && e.Kind != ToolSettled {
			return fail("session is cancelled")
		}
		switch e.Kind {
		case SessionStarted:
			if i != 0 {
				return fail("duplicate session start")
			}
			var x SessionStart
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if x.TenantID == "" || x.PrincipalID == "" || x.ConfigDigest == "" || x.PluginDigest == "" {
				return fail("missing session pin or identity")
			}
			if x.ProjectID < 0 || x.AgentVersionID < 0 {
				return fail("invalid session attribution")
			}
			p.ID, p.TenantID, p.PrincipalID, p.ConfigDigest, p.PluginDigest = e.SessionID, x.TenantID, x.PrincipalID, x.ConfigDigest, x.PluginDigest
			p.ProjectID, p.AgentVersionID, p.Status = x.ProjectID, x.AgentVersionID, "active"
		case SessionParked:
			var x SessionPark
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if x.Reason == "" {
				return fail("park reason required")
			}
			p.ParkedReason = x.Reason
		case SessionResumed:
			var x SessionResume
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if p.ParkedReason == "" || x.Reason == "" {
				return fail("resume requires parked session and reason")
			}
			p.ParkedReason = ""
		case SessionCancelled:
			if len(e.Payload) != 2 || string(e.Payload) != "{}" {
				return fail("cancel payload must be empty")
			}
			p.Status = "cancelled"
			p.ParkedReason = ""
			if t != nil && t.Status == "active" {
				if s != nil && s.Status == "active" && hasUnresolvedTool(s) {
					// Preserve the active tool fact so an outcome that races with
					// cancellation can still be reconciled after the Session ends.
				} else {
					t.Status = "cancelled"
					if s != nil && s.Status == "active" {
						s.Status = "cancelled"
					}
				}
			}
		case HistorySeeded:
			var x HistorySeed
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if seeded || len(p.Turns) > 0 || len(x.Messages) == 0 {
				return fail("history seed must precede turns and occur once")
			}
			if !validHistorySeed(x.Messages) {
				return fail("invalid seeded message or tool transcript")
			}
			p.History = append(p.History, x.Messages...)
			seeded = true
		case TurnStarted:
			var x TurnStart
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if x.ID == "" || turnIDs[x.ID] || (t != nil && t.Status == "active") {
				return fail("turn already active or ID reused")
			}
			p.Turns = append(p.Turns, Turn{ID: x.ID, Status: "active"})
			turnIDs[x.ID] = true
		case MessageAccepted:
			var x AcceptedMessage
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if t == nil || t.Status != "active" || x.TurnID != t.ID || !validMessage(x.Message) {
				return fail("message outside active turn")
			}
			if x.Message.Role == "assistant" {
				if s == nil || s.Status != "active" || x.StepID != s.ID || s.Request != nil || s.Kind == StageModel {
					return fail("deterministic assistant message outside step")
				}
				s.Deterministic = true
				s.DeclaredTools = append(s.DeclaredTools, x.Message.ToolCalls...)
			} else if (x.Message.Role != "user" && x.Message.Role != "system") || x.StepID != "" || (s != nil && s.Status == "active") {
				return fail("input message after step began")
			}
			if x.Message.CallID != "" {
				return fail("accepted message has tool identity")
			}
			t.Messages = append(t.Messages, x.Message)
			p.History = append(p.History, x.Message)
		case StepStarted:
			var x StepStart
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if t == nil || t.Status != "active" || x.TurnID != t.ID || x.ID == "" || x.StageKey == "" || !validStageKind(x.Kind) || !canonicalJSON(x.Input) || !validToolNames(x.AllowedTools) || (x.Kind == StageCompute && len(x.AllowedTools) > 0) || (x.Internal && (x.Kind != StageModel || len(x.AllowedTools) > 0)) || stepIDs[x.ID] || len(t.Messages) == 0 || (s != nil && s.Status == "active") {
				return fail("step cannot start")
			}
			t.Steps = append(t.Steps, Step{ID: x.ID, StageKey: x.StageKey, Kind: x.Kind, Input: append(json.RawMessage(nil), x.Input...), AllowedTools: append([]string(nil), x.AllowedTools...), Optional: x.Optional, Internal: x.Internal, Status: "active", Tools: map[string]ToolState{}, SkippedTools: map[string]ToolSkip{}, Approvals: map[string]ApprovalState{}, Budget: map[BudgetCategory]uint64{}})
			stepIDs[x.ID] = true
		case ModelRequested:
			var x ModelRequest
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if !active(t, s, x.TurnID, x.StepID) || s.Kind != StageModel || s.Request != nil || s.Deterministic || x.ConfigDigest != p.ConfigDigest || x.HistoryThrough != p.Sequence || !validModelRequest(x) || !modelToolsAllowed(x.Tools, s.AllowedTools) {
				return fail("model request differs from committed history or pins")
			}
			// Historical messages are immutable. Bound the request's view to this
			// prefix while allowing later History appends to share its backing array.
			x.History = p.History[:len(p.History):len(p.History)]
			s.Request = &x
		case AttemptStarted:
			var x AttemptStart
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if !active(t, s, x.TurnID, x.StepID) || s.Request == nil || s.ActiveAttempt != "" || x.ID == "" || attemptIDs[x.ID] || successful(s) {
				return fail("model attempt cannot start")
			}
			s.ActiveAttempt = x.ID
			attemptIDs[x.ID] = true
		case AttemptFinished:
			var x AttemptFinish
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if !active(t, s, x.TurnID, x.StepID) || s.ActiveAttempt == "" || x.ID != s.ActiveAttempt || (x.Outcome != "succeeded" && x.Outcome != "failed") {
				return fail("model attempt not active")
			}
			if x.Usage.InputTokens < 0 || x.Usage.OutputTokens < 0 || x.Usage.CachedInputTokens < 0 || x.Usage.CachedInputTokens > x.Usage.InputTokens || ((x.Usage.InputTokens > 0 || x.Usage.OutputTokens > 0) && x.Profile == "") {
				return fail("invalid model usage")
			}
			if x.Outcome == "succeeded" {
				if x.Error != "" || (x.Message.Role != "assistant" && x.Message.Role != "") || (hasContent(x.Message) && x.Message.Role != "assistant") || x.Message.CallID != "" || !validMessage(x.Message) && hasContent(x.Message) {
					return fail("invalid model completion")
				}
				if hasContent(x.Message) {
					for _, call := range x.Message.ToolCalls {
						if !modelToolDeclared(s.Request, call.Name) {
							return fail("model used a tool absent from request")
						}
					}
					if !s.Internal {
						p.History = append(p.History, x.Message)
						t.Messages = append(t.Messages, x.Message)
					}
					s.DeclaredTools = append(s.DeclaredTools, x.Message.ToolCalls...)
				}
			} else if hasContent(x.Message) || x.Error == "" {
				return fail("invalid model failure")
			}
			s.Attempts = append(s.Attempts, x)
			s.ActiveAttempt = ""
		case ToolCalled:
			var x ToolCall
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if !active(t, s, x.TurnID, x.StepID) || (s.Kind != StageTool && !(s.Kind == StageModel && successful(s))) || !toolAllowed(s.AllowedTools, x.Name) || x.ID == "" || x.Name == "" || x.IdempotencyKey == "" || callIDs[x.ID] || idempotencyKeys[x.IdempotencyKey] || !approvalsGranted(s) || (s.Kind == StageModel && !declaredCall(s, x)) || !boundApproval(s, x) {
				return fail("tool call cannot start")
			}
			s.Tools[x.ID] = ToolState{Call: x}
			callIDs[x.ID] = true
			idempotencyKeys[x.IdempotencyKey] = true
		case ToolSettled:
			var x ToolResult
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if !active(t, s, x.TurnID, x.StepID) || (p.Status == "cancelled" && x.Outcome == "unknown") {
				return fail("tool result outside step")
			}
			state, ok := s.Tools[x.ID]
			if !ok || (state.Result != nil && state.Result.Outcome != "unknown") || (state.Result != nil && x.Outcome == "unknown") || (x.Outcome != "succeeded" && x.Outcome != "failed" && x.Outcome != "unknown") {
				return fail("unknown or duplicate tool settlement")
			}
			if (x.Outcome == "succeeded" && x.Error != "") || (x.Outcome == "failed" && x.Error == "") || (x.Outcome == "unknown" && x.Result != "") {
				return fail("invalid tool result outcome")
			}
			if x.Outcome == "unknown" && len(x.CostUsages) > 0 {
				return fail("unknown tool usage")
			}
			for _, usage := range x.CostUsages {
				if usage.ResourceType == "" || usage.Provider == "" || usage.Model == "" || usage.Quantity <= 0 || math.IsNaN(usage.Quantity) || math.IsInf(usage.Quantity, 0) || (usage.Unit != "request" && usage.Unit != "second") {
					return fail("invalid tool usage")
				}
			}
			state.Result = &x
			s.Tools[x.ID] = state
			if x.Outcome != "unknown" {
				p.History = append(p.History, Message{Role: "tool", CallID: x.ID, Content: x.Result, Error: x.Error})
			}
		case ToolSkipped:
			var x ToolSkip
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if !active(t, s, x.TurnID, x.StepID) || x.ID == "" || x.Reason == "" || callIDs[x.ID] {
				return fail("tool skip outside active declaration or call already recorded")
			}
			declared := false
			for _, call := range s.DeclaredTools {
				if call.ID == x.ID {
					declared = true
					break
				}
			}
			if !declared {
				return fail("skipped tool was not declared")
			}
			s.SkippedTools[x.ID] = x
			callIDs[x.ID] = true
			p.History = append(p.History, Message{Role: "tool", CallID: x.ID, Content: "Tool was not executed: " + x.Reason, Error: x.Reason})
		case ApprovalAsked:
			var x ApprovalRequest
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if !active(t, s, x.TurnID, x.StepID) || s.Kind == StageCompute || !toolAllowed(s.AllowedTools, x.ToolName) || x.ID == "" || x.ToolCallID == "" || x.ToolName == "" || x.IdempotencyKey == "" || x.InvocationFingerprint == "" || approvalIDs[x.ID] || (s.Kind == StageModel && !declaredCall(s, ToolCall{ID: x.ToolCallID, Name: x.ToolName, Arguments: x.Arguments})) {
				return fail("approval cannot be requested")
			}
			s.Approvals[x.ID] = ApprovalState{Request: x}
			approvalIDs[x.ID] = true
		case ApprovalResolved:
			var x ApprovalDecision
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if !active(t, s, x.TurnID, x.StepID) || (x.Decision != "approved" && x.Decision != "denied") {
				return fail("invalid approval decision")
			}
			state, ok := s.Approvals[x.ID]
			if !ok || state.Decision != nil {
				return fail("unknown or duplicate approval decision")
			}
			state.Decision = &x
			s.Approvals[x.ID] = state
		case BudgetSettled:
			var x BudgetSettlement
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if !active(t, s, x.TurnID, x.StepID) || x.Units == 0 || !validBudgetCategory(x.Category) || ^uint64(0)-s.Budget[x.Category] < x.Units {
				return fail("invalid budget settlement")
			}
			s.Budget[x.Category] += x.Units
		case StepEnded:
			var x StepEnd
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if !active(t, s, x.TurnID, x.StepID) || !terminal(x.Status) || !canonicalJSON(x.Output) || !validWarnings(x.Warnings) || s.ActiveAttempt != "" {
				return fail("step cannot end")
			}
			for _, v := range s.Tools {
				if v.Result == nil || v.Result.Outcome == "unknown" {
					return fail("unsettled tool call")
				}
			}
			if !allDeclaredCallsRegistered(s) {
				return fail("tool declaration lacks an execution result or skip")
			}
			if x.Status == "completed" && len(s.SkippedTools) > 0 {
				return fail("completed step contains unexecuted tool declarations")
			}
			for _, v := range s.Approvals {
				if v.Decision == nil {
					return fail("unresolved approval")
				}
			}
			if x.Status == "completed" && ((s.Kind == StageModel && !successful(s)) || (s.Kind == StageTool && len(s.Tools) == 0)) {
				return fail("completed step lacks result")
			}
			if x.Status == "completed" && s.Kind == StageModel && !modelProducedContent(s) && len(s.Tools) == 0 {
				return fail("successful model step produced no content or tool call")
			}
			if s.Kind == StageCompute && (s.Request != nil || len(s.Attempts) > 0 || len(s.Tools) > 0 || len(s.Approvals) > 0) {
				return fail("compute stage had external effect")
			}
			if s.Kind == StageTool && (s.Request != nil || len(s.Attempts) > 0) {
				return fail("tool stage called model")
			}
			s.Status = x.Status
			s.Output = append(json.RawMessage(nil), x.Output...)
			s.Warnings = append([]string(nil), x.Warnings...)
		case TurnEnded:
			var x TurnEnd
			if err := decode(e.Payload, &x); err != nil {
				return fail(err.Error())
			}
			if t == nil || t.Status != "active" || x.TurnID != t.ID || !terminal(x.Status) || len(t.Messages) == 0 || (s != nil && s.Status == "active") {
				return fail("turn cannot end")
			}
			t.Status = x.Status
		default:
			return fail("unknown event kind")
		}
		p.Sequence = e.Sequence
	}
	return p, nil
}

// ModelHistory returns the exact model-visible history at a valid log prefix.
func ModelHistory(events []Event) ([]Message, error) {
	p, err := Replay(events)
	if err != nil {
		return nil, err
	}
	return append([]Message(nil), p.History...), nil
}

func active(t *Turn, s *Step, turnID, stepID string) bool {
	return t != nil && s != nil && t.Status == "active" && s.Status == "active" && t.ID == turnID && s.ID == stepID
}
func successful(s *Step) bool {
	for _, a := range s.Attempts {
		if a.Outcome == "succeeded" {
			return true
		}
	}
	return false
}

func hasUnresolvedTool(s *Step) bool {
	for _, tool := range s.Tools {
		if tool.Result == nil || tool.Result.Outcome == "unknown" {
			return true
		}
	}
	return false
}
func modelProducedContent(s *Step) bool {
	for _, a := range s.Attempts {
		if a.Outcome == "succeeded" && (a.Message.Content != "" || a.Message.ReasoningContent != "") {
			return true
		}
	}
	return false
}
func hasContent(m Message) bool {
	return m.Content != "" || m.ReasoningContent != "" || len(m.Images) > 0 || len(m.ToolCalls) > 0
}

// validHistorySeed accepts only a complete model-visible transcript. A tool
// result must settle a declaration in the immediately preceding assistant
// message; imported history has no durable invocation facts to repair gaps.
func validHistorySeed(messages []Message) bool {
	pending := map[string]bool{}
	seen := map[string]bool{}
	for _, m := range messages {
		if m.Role == "tool" {
			if m.CallID == "" || !pending[m.CallID] || m.ReasoningContent != "" || len(m.Images) != 0 || len(m.ToolCalls) != 0 || (m.Content == "" && m.Error == "") {
				return false
			}
			delete(pending, m.CallID)
			continue
		}
		if len(pending) != 0 || (m.Role != "user" && m.Role != "assistant" && m.Role != "system") || !validMessage(m) || m.CallID != "" {
			return false
		}
		for _, call := range m.ToolCalls {
			if seen[call.ID] {
				return false
			}
			seen[call.ID] = true
			pending[call.ID] = true
		}
	}
	return len(pending) == 0
}

func validMessage(m Message) bool {
	if !hasContent(m) {
		return false
	}
	if m.Role != "user" && m.Role != "system" && m.Role != "assistant" && m.Role != "tool" {
		return false
	}
	if m.Error != "" && m.Role != "tool" {
		return false
	}
	if m.ReasoningContent != "" && m.Role != "assistant" {
		return false
	}
	if m.Role != "assistant" && len(m.ToolCalls) > 0 {
		return false
	}
	if m.Role != "user" && len(m.Images) > 0 {
		return false
	}
	for _, image := range m.Images {
		if image.URL == "" {
			return false
		}
	}
	ids := map[string]bool{}
	for _, call := range m.ToolCalls {
		if call.ID == "" || call.Name == "" || len(call.Arguments) == 0 || !json.Valid(call.Arguments) || ids[call.ID] {
			return false
		}
		ids[call.ID] = true
	}
	return true
}
func validModelRequest(r ModelRequest) bool {
	if r.Profile == "" || r.MaxTokens < 0 || math.IsNaN(r.Temperature) || math.IsInf(r.Temperature, 0) || r.Temperature < 0 || r.Temperature > 2 || r.Reasoning.MaxTokens < 0 {
		return false
	}
	if !r.Reasoning.Enabled && (r.Reasoning.Effort != "" || r.Reasoning.MaxTokens != 0) {
		return false
	}
	if r.NoTools && len(r.Tools) > 0 {
		return false
	}
	for _, message := range r.Preamble {
		if message.Role != "system" || !validMessage(message) || message.CallID != "" || len(message.ToolCalls) > 0 {
			return false
		}
	}
	names := map[string]bool{}
	for _, tool := range r.Tools {
		if tool.Name == "" || len(tool.Parameters) == 0 || !json.Valid(tool.Parameters) || names[tool.Name] {
			return false
		}
		names[tool.Name] = true
	}
	for _, message := range r.StageMessages {
		if (message.Role != "system" && message.Role != "user" && message.Role != "assistant") || !validMessage(message) || message.CallID != "" || len(message.ToolCalls) > 0 {
			return false
		}
	}
	return true
}
func validStageKind(kind StageKind) bool {
	return kind == StageModel || kind == StageCompute || kind == StageTool
}
func validToolNames(names []string) bool {
	seen := map[string]bool{}
	for _, name := range names {
		if name == "" || seen[name] {
			return false
		}
		seen[name] = true
	}
	return true
}
func toolAllowed(names []string, name string) bool {
	for _, item := range names {
		if item == name {
			return true
		}
	}
	return false
}
func modelToolsAllowed(tools []ModelToolDef, allowed []string) bool {
	for _, tool := range tools {
		if !toolAllowed(allowed, tool.Name) {
			return false
		}
	}
	return true
}
func modelToolDeclared(request *ModelRequest, name string) bool {
	if request == nil {
		return false
	}
	for _, tool := range request.Tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}
func canonicalJSON(raw json.RawMessage) bool {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) || !json.Valid(raw) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	encoded, err := json.Marshal(value)
	return err == nil && bytes.Equal(raw, encoded)
}
func validWarnings(warnings []string) bool {
	for _, warning := range warnings {
		if warning == "" {
			return false
		}
	}
	return true
}
func boundApproval(s *Step, c ToolCall) bool {
	if c.ApprovalID == "" {
		if c.InvocationFingerprint != "" {
			return false
		}
		for _, a := range s.Approvals {
			if a.Request.ToolCallID == c.ID {
				return false
			}
		}
		return true
	}
	a, ok := s.Approvals[c.ApprovalID]
	return ok && a.Decision != nil && a.Decision.Decision == "approved" && a.Request.ToolCallID == c.ID && a.Request.ToolName == c.Name && a.Request.Arguments == c.Arguments && a.Request.IdempotencyKey == c.IdempotencyKey && a.Request.InvocationFingerprint == c.InvocationFingerprint
}
func declaredCall(s *Step, c ToolCall) bool {
	for _, d := range s.DeclaredTools {
		if d.ID == c.ID && d.Name == c.Name && bytes.Equal(d.Arguments, []byte(c.Arguments)) {
			return true
		}
	}
	return false
}
func allDeclaredCallsRegistered(s *Step) bool {
	for _, d := range s.DeclaredTools {
		if _, ok := s.Tools[d.ID]; !ok {
			if _, skipped := s.SkippedTools[d.ID]; !skipped {
				return false
			}
		}
	}
	return true
}
func approvalsGranted(s *Step) bool {
	for _, a := range s.Approvals {
		if a.Decision == nil || a.Decision.Decision != "approved" {
			return false
		}
	}
	return true
}
func terminal(s string) bool { return s == "completed" || s == "failed" || s == "cancelled" }
func validBudgetCategory(c BudgetCategory) bool {
	return c == BudgetModelCalls || c == BudgetInputTokens || c == BudgetOutputTokens || c == BudgetToolCalls || c == BudgetMediaOps
}
func invalid(i int, reason string) error {
	return fmt.Errorf("%w at event %d: %s", ErrInvalidLog, i+1, reason)
}
func decode(data json.RawMessage, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		if err == nil {
			return errors.New("trailing payload")
		}
		return err
	}
	return nil
}
