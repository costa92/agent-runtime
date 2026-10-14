// Package agentloop drives durable Sessions through Turns and Steps. A lease
// fences every transition, and a model attempt is recorded before provider IO.
package agentloop

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/costa92/agent-runtime/capability"
	"github.com/costa92/agent-runtime/model"
	"github.com/costa92/agent-runtime/product"
	"github.com/costa92/agent-runtime/profile"
	"github.com/costa92/agent-runtime/session"
)

var ErrParked = errors.New("session parked for an external decision or reconciliation")

// Config is pinned by the Session's published profile. A missing digest is a
// deployment error, never a reason to execute with the current profile.
type Config struct {
	Profile        string
	Digest         string
	MaxSteps       int
	MaxAttempts    int
	MaxTokens      int
	Temperature    float64
	Tools          []model.Tool
	Reasoning      session.Reasoning
	BudgetPolicy   session.BudgetPolicy
	AgentVersionID int64
	Scope          profile.Scope
	SystemPrompt   string
}

type Loop struct {
	Store          session.Store
	Model          model.Service
	Tools          *capability.Service
	Product        product.Product
	Config         Config
	ContextSources []ContextSource
	Now            func() time.Time
	NewID          func() (string, error)
}

// New constructs a complete execution loop. Products own all workflow choices;
// there is no implicit conversation workflow when a Product is missing.
func New(store session.Store, models model.Service, tools *capability.Service, workflow product.Product, config Config, sources ...ContextSource) (*Loop, error) {
	l := &Loop{Store: store, Model: models, Tools: tools, Product: workflow, Config: config, ContextSources: append([]ContextSource(nil), sources...)}
	if err := l.validate(); err != nil {
		return nil, err
	}
	return l, nil
}

// ContextSource snapshots one host-owned input before the first model request
// of a Turn. Its output is persisted in ModelRequest.Preamble; later steps and
// recovery use that fact rather than reading mutable host state again.
type ContextSource interface {
	Snapshot(context.Context, session.Projection) ([]session.Message, error)
}

// Input is the public inbox command for a conversational Turn. Other command
// types, including approval and reconciliation, have separate typed paths.
type Input struct {
	Message session.Message `json:"message"`
}

// CreateSession starts a pinned Session. The resolved plugin digest comes from
// the same published manifest used to build this Loop.
func (l *Loop) CreateSession(ctx context.Context, id, tenantID, principalID string, projectID int64, pluginDigest string) (session.Projection, error) {
	if err := l.validate(); err != nil {
		return session.Projection{}, err
	}
	if id == "" || tenantID == "" || principalID == "" || projectID < 0 || pluginDigest == "" {
		return session.Projection{}, session.ErrInvalidInput
	}
	if (l.Config.Scope == profile.ProjectScope && projectID == 0) || (l.Config.Scope == profile.PrincipalScope && projectID != 0) {
		return session.Projection{}, session.ErrInvalidInput
	}
	start, err := l.event(id, 1, session.SessionStarted, session.SessionStart{TenantID: tenantID, PrincipalID: principalID, ProjectID: projectID, AgentVersionID: l.Config.AgentVersionID, ConfigDigest: l.Config.Digest, PluginDigest: pluginDigest})
	if err != nil {
		return session.Projection{}, err
	}
	return l.Store.Create(ctx, start)
}

func (l *Loop) CancelSession(ctx context.Context, lease session.Lease) (session.Projection, error) {
	p, err := l.Store.Inspect(ctx, lease.SessionID)
	if err != nil {
		return session.Projection{}, err
	}
	if p.Status == "cancelled" {
		return p, nil
	}
	return l.commit(ctx, lease, p, fact{session.SessionCancelled, struct{}{}})
}

// RequestCancellation persists a cancellation request before attempting to
// finalize it. A live worker keeps its lease and receives the pending
// projection; after the lease is available, this method commits the terminal
// cancellation event without requiring the Session's pinned execution Profile.
func (l *Loop) RequestCancellation(ctx context.Context, sessionID, workerID string, ttl time.Duration) (session.Projection, error) {
	if l == nil || l.Store == nil || sessionID == "" || workerID == "" || ttl <= 0 {
		return session.Projection{}, session.ErrInvalidInput
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	p, err := l.Store.Inspect(settleCtx, sessionID)
	if err != nil {
		return session.Projection{}, err
	}
	if p.Status == "cancelled" {
		return p, nil
	}
	if err := l.Store.RequestCancel(settleCtx, sessionID); err != nil {
		if !errors.Is(err, session.ErrConflict) {
			return session.Projection{}, err
		}
		p, inspectErr := l.Store.Inspect(settleCtx, sessionID)
		if inspectErr != nil {
			return session.Projection{}, inspectErr
		}
		if p.Status == "cancelled" {
			return p, nil
		}
	}
	lease, err := l.Store.ClaimSession(settleCtx, sessionID, workerID, ttl)
	if errors.Is(err, session.ErrLease) {
		return l.Store.Inspect(settleCtx, sessionID)
	}
	if err != nil {
		return session.Projection{}, err
	}
	defer func() {
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer releaseCancel()
		_ = l.Store.Release(releaseCtx, lease)
	}()
	return l.CancelSession(settleCtx, lease)
}

// Submit validates a model-visible user message before making it durable in
// the Session inbox. The idempotency key is unique within that Session.
func (l *Loop) Submit(ctx context.Context, id, idempotencyKey string, message session.Message) (session.InboxItem, bool, error) {
	if err := l.validate(); err != nil {
		return session.InboxItem{}, false, err
	}
	if id == "" || idempotencyKey == "" || message.Role != "user" || strings.TrimSpace(message.Content) == "" && len(message.Images) == 0 || message.CallID != "" || message.Error != "" || message.ReasoningContent != "" || len(message.ToolCalls) > 0 {
		return session.InboxItem{}, false, session.ErrInvalidInput
	}
	for _, image := range message.Images {
		if image.URL == "" {
			return session.InboxItem{}, false, session.ErrInvalidInput
		}
	}
	payload, err := json.Marshal(Input{Message: message})
	if err != nil {
		return session.InboxItem{}, false, err
	}
	return l.Store.Enqueue(ctx, id, session.Input{IdempotencyKey: idempotencyKey, Payload: payload})
}

func (l *Loop) validate() error {
	if l == nil || l.Store == nil || l.Model == nil || l.Product == nil || l.Config.Profile == "" || l.Config.Digest == "" || l.Config.MaxSteps <= 0 || l.Config.MaxAttempts <= 0 || (l.Config.Scope != profile.ProjectScope && l.Config.Scope != profile.PrincipalScope) {
		return errors.New("agentloop: incomplete services or profile")
	}
	return nil
}

func (l *Loop) now() time.Time {
	if l.Now != nil {
		return l.Now().UTC()
	}
	return time.Now().UTC()
}

func (l *Loop) id() (string, error) {
	if l.NewID != nil {
		return l.NewID()
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (l *Loop) event(id string, seq uint64, kind session.Kind, payload any) (session.Event, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return session.Event{}, err
	}
	return session.Event{Version: session.ProtocolVersion, SessionID: id, Sequence: seq, Kind: kind, Time: l.now(), Payload: b}, nil
}

type fact struct {
	Kind    session.Kind
	Payload any
}

func (l *Loop) commit(ctx context.Context, lease session.Lease, p session.Projection, facts ...fact) (session.Projection, error) {
	events := make([]session.Event, len(facts))
	outbox := make([]session.OutboxRecord, len(facts))
	for i, fact := range facts {
		e, err := l.event(p.ID, p.Sequence+uint64(i)+1, fact.Kind, fact.Payload)
		if err != nil {
			return session.Projection{}, err
		}
		events[i] = e
		payload, err := json.Marshal(e)
		if err != nil {
			return session.Projection{}, err
		}
		outbox[i] = session.OutboxRecord{Key: fmt.Sprintf("%s:%d", p.ID, e.Sequence), Payload: payload}
	}
	return l.Store.Append(ctx, session.AppendCommand{Lease: lease, ExpectedSequence: p.Sequence, Events: events, Outbox: outbox, Budget: l.Config.BudgetPolicy})
}

// Drive advances one leased Session until it finishes the current Turn or
// reaches a durable park point. The worker owns lease renewal while Drive runs.
func (l *Loop) Drive(ctx context.Context, lease session.Lease) error {
	if err := l.validate(); err != nil {
		return err
	}
	if lease.SessionID == "" || lease.WorkerID == "" || lease.Fence == 0 {
		return session.ErrLease
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p, err := l.Store.Inspect(ctx, lease.SessionID)
		if err != nil {
			return err
		}
		if p.ConfigDigest != l.Config.Digest {
			return fmt.Errorf("agentloop: pinned profile %s is unavailable", p.ConfigDigest)
		}
		if len(p.Turns) == 0 || p.Turns[len(p.Turns)-1].Status != "active" {
			return l.openTurn(ctx, lease, p)
		}
		t := p.Turns[len(p.Turns)-1]
		if len(t.Steps) == 0 || t.Steps[len(t.Steps)-1].Status != "active" {
			if err := l.openStep(ctx, lease, p, t); err != nil {
				return err
			}
			continue
		}
		s := t.Steps[len(t.Steps)-1]
		if s.Kind != session.StageModel {
			if err := l.advanceDeterministic(ctx, lease, p, t, s); err != nil {
				return err
			}
			continue
		}
		if s.Request == nil {
			if err := l.startModelStep(ctx, lease, p, t.ID, s.ID); err != nil {
				return err
			}
			continue
		}
		if s.ActiveAttempt != "" {
			// Claim fenced the previous worker. A provider attempt has no
			// externally committed write to reconcile, so record the lost
			// response and let the configured attempt limit decide a retry.
			_, err = l.commit(ctx, lease, p, fact{session.AttemptFinished, session.AttemptFinish{TurnID: t.ID, StepID: s.ID, ID: s.ActiveAttempt, Outcome: "failed", Error: "worker interrupted before durable completion"}})
			if err != nil {
				return err
			}
			continue
		}
		if len(s.Attempts) == 0 || s.Attempts[len(s.Attempts)-1].Outcome == "failed" {
			if len(s.Attempts) >= l.Config.MaxAttempts {
				_, err = l.commit(ctx, lease, p, fact{session.StepEnded, session.StepEnd{TurnID: t.ID, StepID: s.ID, Status: "failed", Output: json.RawMessage(`{}`), Warnings: []string{"model attempts exhausted"}}})
				if err != nil {
					return err
				}
				continue
			}
			if err := l.attempt(ctx, lease, p, t.ID, s.ID); err != nil {
				return err
			}
			continue
		}
		if len(s.DeclaredTools) > 0 {
			advanced, err := l.advanceTool(ctx, lease, p, t, s)
			if err != nil {
				return err
			}
			if advanced {
				continue
			}
		}
		settlement, err := l.settle(ctx, p, s)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, product.ErrUnavailable) {
				return err
			}
			if err := l.failStep(ctx, lease, p, t.ID, s, err.Error()); err != nil {
				return err
			}
			continue
		}
		if _, err = l.commit(ctx, lease, p, fact{session.StepEnded, session.StepEnd{TurnID: t.ID, StepID: s.ID, Status: settlement.Status, Output: settlement.Output, Warnings: settlement.Warnings}}); err != nil {
			return err
		}
	}
}

func (l *Loop) openTurn(ctx context.Context, lease session.Lease, p session.Projection) error {
	inbox, err := l.Store.Inbox(ctx, p.ID)
	if err != nil {
		return err
	}
	if len(inbox) == 0 {
		return nil
	}
	var input Input
	if err := json.Unmarshal(inbox[0].Payload, &input); err != nil {
		return fmt.Errorf("agentloop: invalid inbox item: %w", err)
	}
	if input.Message.Role != "user" || input.Message.Content == "" && len(input.Message.Images) == 0 {
		return errors.New("agentloop: inbox must carry a user message")
	}
	turnID, err := l.id()
	if err != nil {
		return err
	}
	first, err := l.event(p.ID, p.Sequence+1, session.TurnStarted, session.TurnStart{ID: turnID})
	if err != nil {
		return err
	}
	second, err := l.event(p.ID, p.Sequence+2, session.MessageAccepted, session.AcceptedMessage{TurnID: turnID, Message: input.Message})
	if err != nil {
		return err
	}
	firstPayload, err := json.Marshal(first)
	if err != nil {
		return err
	}
	secondPayload, err := json.Marshal(second)
	if err != nil {
		return err
	}
	_, err = l.Store.Append(ctx, session.AppendCommand{Lease: lease, ExpectedSequence: p.Sequence, Events: []session.Event{first, second}, Outbox: []session.OutboxRecord{{Key: fmt.Sprintf("%s:%d", p.ID, first.Sequence), Payload: firstPayload}, {Key: fmt.Sprintf("%s:%d", p.ID, second.Sequence), Payload: secondPayload}}, AckInboxID: inbox[0].ID, Budget: l.Config.BudgetPolicy})
	return err
}

func (l *Loop) request(preamble, history []session.Message, plan product.Plan) model.Request {
	messages := make([]session.Message, 0, len(preamble)+len(history)+len(plan.Messages))
	messages = append(messages, preamble...)
	messages = append(messages, history...)
	messages = append(messages, plan.Messages...)
	tools := toolsForStage(l.Config.Tools, plan.AllowedTools)
	return model.Request{Profile: l.Config.Profile, Messages: messages, Tools: tools, NoReasoning: !l.Config.Reasoning.Enabled, Reasoning: l.Config.Reasoning, Temperature: l.Config.Temperature, MaxTokens: l.Config.MaxTokens}
}

func (l *Loop) snapshotPreamble(ctx context.Context, p session.Projection) ([]session.Message, error) {
	turn := p.Turns[len(p.Turns)-1]
	for _, prior := range turn.Steps {
		if prior.Request != nil {
			return append([]session.Message(nil), prior.Request.Preamble...), nil
		}
	}
	preamble := make([]session.Message, 0, 1+len(l.ContextSources))
	if l.Config.SystemPrompt != "" {
		preamble = append(preamble, session.Message{Role: "system", Content: l.Config.SystemPrompt})
	}
	for _, source := range l.ContextSources {
		if source == nil {
			return nil, errors.New("agentloop: nil context source")
		}
		messages, err := source.Snapshot(ctx, p)
		if err != nil {
			return nil, err
		}
		for _, message := range messages {
			if message.Role != "system" || strings.TrimSpace(message.Content) == "" || len(message.Images) > 0 || len(message.ToolCalls) > 0 {
				return nil, session.ErrInvalidInput
			}
			preamble = append(preamble, message)
		}
	}
	return preamble, nil
}

func requestFromEvent(recorded session.ModelRequest) model.Request {
	tools := make([]model.Tool, len(recorded.Tools))
	for i, item := range recorded.Tools {
		tools[i] = model.Tool{Name: item.Name, Description: item.Description, Parameters: item.Parameters}
	}
	return model.Request{Profile: recorded.Profile, Messages: recorded.Messages(), Tools: tools, NoReasoning: !recorded.Reasoning.Enabled, Reasoning: recorded.Reasoning, Deterministic: recorded.Deterministic, Temperature: recorded.Temperature, MaxTokens: recorded.MaxTokens}
}

func (l *Loop) startModelStep(ctx context.Context, lease session.Lease, p session.Projection, turnID, stepID string) error {
	step := p.Turns[len(p.Turns)-1].Steps[len(p.Turns[len(p.Turns)-1].Steps)-1]
	plan, err := planFromStep(step)
	if err != nil {
		return err
	}
	if err := l.authorizeModel(ctx, p, plan); err != nil {
		if ctx.Err() != nil || errors.Is(err, product.ErrUnavailable) {
			return err
		}
		return l.failStep(ctx, lease, p, turnID, step, "model authorization failed: "+err.Error())
	}
	preamble, err := l.snapshotPreamble(ctx, p)
	if err != nil {
		_, appendErr := l.commit(ctx, lease, p, fact{session.StepEnded, session.StepEnd{TurnID: turnID, StepID: stepID, Status: "failed", Output: json.RawMessage(`{}`), Warnings: []string{err.Error()}}})
		return appendErr
	}
	req := l.request(preamble, p.History, plan)
	call, err := l.Model.Prepare(ctx, req)
	if err != nil {
		_, appendErr := l.commit(ctx, lease, p, fact{session.StepEnded, session.StepEnd{TurnID: turnID, StepID: stepID, Status: "failed", Output: json.RawMessage(`{}`), Warnings: []string{err.Error()}}})
		return appendErr
	}
	if !call.Capabilities().Tools && len(req.Tools) > 0 {
		_, appendErr := l.commit(ctx, lease, p, fact{session.StepEnded, session.StepEnd{TurnID: turnID, StepID: stepID, Status: "failed", Output: json.RawMessage(`{}`), Warnings: []string{"provider does not support stage tools"}}})
		return appendErr
	}
	toolDefs := make([]session.ModelToolDef, len(req.Tools))
	for i, tool := range req.Tools {
		toolDefs[i] = session.ModelToolDef{Name: tool.Name, Description: tool.Description, Parameters: tool.Parameters}
	}
	_, err = l.commit(ctx, lease, p, fact{session.ModelRequested, session.ModelRequest{TurnID: turnID, StepID: stepID, ConfigDigest: p.ConfigDigest, Profile: req.Profile, Preamble: preamble, HistoryThrough: p.Sequence, StageMessages: plan.Messages, Tools: toolDefs, NoTools: len(toolDefs) == 0, Temperature: req.Temperature, MaxTokens: req.MaxTokens, Reasoning: l.Config.Reasoning}})
	return err
}

func (l *Loop) attempt(ctx context.Context, lease session.Lease, p session.Projection, turnID, stepID string) error {
	id, err := l.id()
	if err != nil {
		return err
	}
	turn := p.Turns[len(p.Turns)-1]
	step := turn.Steps[len(turn.Steps)-1]
	if step.Request == nil || step.ID != stepID {
		return session.ErrConflict
	}
	plan, err := planFromStep(step)
	if err != nil {
		return err
	}
	if err := l.authorizeModel(ctx, p, plan); err != nil {
		if ctx.Err() != nil || errors.Is(err, product.ErrUnavailable) {
			return err
		}
		return l.failStep(ctx, lease, p, turnID, step, "model authorization failed: "+err.Error())
	}
	request := requestFromEvent(*step.Request)
	call, err := l.Model.Prepare(ctx, request)
	if err != nil {
		_, appendErr := l.commit(ctx, lease, p, fact{session.StepEnded, session.StepEnd{TurnID: turnID, StepID: stepID, Status: "failed", Output: json.RawMessage(`{}`), Warnings: []string{err.Error()}}})
		return appendErr
	}
	p, err = l.commit(ctx, lease, p, fact{session.AttemptStarted, session.AttemptStart{TurnID: turnID, StepID: stepID, ID: id}}, fact{session.BudgetSettled, session.BudgetSettlement{TurnID: turnID, StepID: stepID, Category: session.BudgetModelCalls, Units: 1}})
	if err != nil {
		if errors.Is(err, session.ErrBudgetExceeded) {
			current, inspectErr := l.Store.Inspect(ctx, lease.SessionID)
			if inspectErr != nil {
				return inspectErr
			}
			return l.failStep(ctx, lease, current, turnID, step, "model budget exceeded")
		}
		return err
	}
	response, callErr := call.Complete(ctx)
	fatalModel := false
	if callErr == nil {
		if len(response.ToolCalls) > 0 && len(response.Message.ToolCalls) > 0 && !reflect.DeepEqual(response.ToolCalls, response.Message.ToolCalls) {
			callErr = errors.New("model returned conflicting tool declarations")
			fatalModel = true
		} else if response.Usage.InputTokens < 0 || response.Usage.OutputTokens < 0 || response.Usage.CachedInputTokens < 0 || response.Usage.CachedInputTokens > response.Usage.InputTokens {
			callErr = errors.New("model returned negative usage")
		}
	}
	finish := session.AttemptFinish{TurnID: turnID, StepID: stepID, ID: id, Outcome: "succeeded", Message: response.Message, Profile: response.Profile, Usage: session.ModelUsage{InputTokens: response.Usage.InputTokens, OutputTokens: response.Usage.OutputTokens, CachedInputTokens: response.Usage.CachedInputTokens}}
	if len(response.ToolCalls) > 0 {
		finish.Message.ToolCalls = response.ToolCalls
	}
	if finish.Message.Role == "" {
		finish.Message.Role = "assistant"
	}
	if callErr == nil {
		for _, declared := range finish.Message.ToolCalls {
			allowed := false
			for _, advertised := range step.Request.Tools {
				if advertised.Name == declared.Name {
					allowed = true
					break
				}
			}
			if !allowed {
				callErr = fmt.Errorf("model declared unavailable tool %s", declared.Name)
				fatalModel = true
				break
			}
		}
	}
	if callErr != nil {
		finish.Outcome = "failed"
		finish.Error = callErr.Error()
		finish.Message = session.Message{}
	} else if err := validateModelMessage(finish.Message); err != nil {
		finish.Outcome = "failed"
		finish.Error = err.Error()
		finish.Message = session.Message{}
	}
	facts := []fact{{session.AttemptFinished, finish}}
	if response.Usage.InputTokens > 0 {
		facts = append(facts, fact{session.BudgetSettled, session.BudgetSettlement{TurnID: turnID, StepID: stepID, Category: session.BudgetInputTokens, Units: uint64(response.Usage.InputTokens)}})
	}
	if response.Usage.OutputTokens > 0 {
		facts = append(facts, fact{session.BudgetSettled, session.BudgetSettlement{TurnID: turnID, StepID: stepID, Category: session.BudgetOutputTokens, Units: uint64(response.Usage.OutputTokens)}})
	}
	if fatalModel {
		facts = append(facts, fact{session.StepEnded, session.StepEnd{TurnID: turnID, StepID: stepID, Status: "failed", Output: json.RawMessage(`{}`), Warnings: []string{finish.Error}}})
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err = l.commit(settleCtx, lease, p, facts...)
	return err
}

func (l *Loop) authorizeModel(ctx context.Context, p session.Projection, plan product.Plan) error {
	if authorizer, ok := l.Product.(product.ModelAuthorizer); ok {
		return authorizer.AuthorizeModel(ctx, p, plan)
	}
	return nil
}

func validateModelMessage(m session.Message) error {
	if m.Role != "assistant" || m.CallID != "" || m.Error != "" || len(m.Images) != 0 || m.Content == "" && m.ReasoningContent == "" && len(m.ToolCalls) == 0 {
		return errors.New("model returned an invalid assistant response")
	}
	ids := make(map[string]bool, len(m.ToolCalls))
	for _, call := range m.ToolCalls {
		if call.ID == "" || call.Name == "" || !json.Valid(call.Arguments) || ids[call.ID] {
			return errors.New("model returned an invalid tool declaration")
		}
		ids[call.ID] = true
	}
	return nil
}
