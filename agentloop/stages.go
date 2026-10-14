package agentloop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/costa92/agent-runtime/model"
	"github.com/costa92/agent-runtime/product"
	"github.com/costa92/agent-runtime/session"
)

func (l *Loop) openStep(ctx context.Context, lease session.Lease, p session.Projection, turn session.Turn) error {
	if len(turn.Steps) > 0 {
		last := turn.Steps[len(turn.Steps)-1]
		if last.Status != "completed" && !last.Optional {
			_, err := l.commit(ctx, lease, p, fact{session.TurnEnded, session.TurnEnd{TurnID: turn.ID, Status: last.Status}})
			return err
		}
	}
	plan, err := l.Product.Next(ctx, p)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, product.ErrUnavailable) {
			return err
		}
		return l.failPlanning(ctx, lease, p, turn, err)
	}
	if plan.Done {
		status := plan.FinalStatus
		if status == "" {
			status = "completed"
		}
		if status != "completed" && status != "failed" && status != "cancelled" {
			return l.failPlanning(ctx, lease, p, turn, errors.New("agentloop: invalid product final status"))
		}
		_, err := l.commit(ctx, lease, p, fact{session.TurnEnded, session.TurnEnd{TurnID: turn.ID, Status: status}})
		return err
	}
	if len(turn.Steps) >= l.Config.MaxSteps {
		_, err := l.commit(ctx, lease, p, fact{session.TurnEnded, session.TurnEnd{TurnID: turn.ID, Status: "failed"}})
		return err
	}
	if plan.Key == "" || plan.Kind != product.Model && plan.Kind != product.Compute && plan.Kind != product.Tool {
		return l.failPlanning(ctx, lease, p, turn, errors.New("agentloop: product returned an invalid stage"))
	}
	if plan.Internal && (plan.Kind != product.Model || len(plan.AllowedTools) != 0) {
		return l.failPlanning(ctx, lease, p, turn, errors.New("agentloop: internal stage must be a model stage without tools"))
	}
	for _, name := range plan.AllowedTools {
		found := false
		for _, configured := range l.Config.Tools {
			if configured.Name == name {
				found = true
				break
			}
		}
		if !found {
			return l.failPlanning(ctx, lease, p, turn, fmt.Errorf("agentloop: stage %s requests unavailable tool %s", plan.Key, name))
		}
	}
	input, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	var canonical any
	if err := json.Unmarshal(input, &canonical); err != nil {
		return err
	}
	input, err = json.Marshal(canonical)
	if err != nil {
		return err
	}
	stepID, err := l.id()
	if err != nil {
		return err
	}
	_, err = l.commit(ctx, lease, p, fact{session.StepStarted, session.StepStart{
		TurnID: turn.ID, ID: stepID, StageKey: plan.Key,
		Kind: session.StageKind(plan.Kind), Input: input, Optional: plan.Optional, Internal: plan.Internal,
		AllowedTools: append([]string(nil), plan.AllowedTools...),
	}})
	return err
}

// A planning rejection precedes a business Step. Preserve its reason in a
// failed diagnostic Step without inventing a model attempt or external effect.
func (l *Loop) failPlanning(ctx context.Context, lease session.Lease, p session.Projection, turn session.Turn, cause error) error {
	id, err := l.id()
	if err != nil {
		return err
	}
	_, err = l.commit(ctx, lease, p,
		fact{session.StepStarted, session.StepStart{TurnID: turn.ID, ID: id, StageKey: "runtime/planning", Kind: session.StageCompute, Input: json.RawMessage(`{}`)}},
		fact{session.StepEnded, session.StepEnd{TurnID: turn.ID, StepID: id, Status: "failed", Output: json.RawMessage(`{}`), Warnings: []string{cause.Error()}}},
		fact{session.TurnEnded, session.TurnEnd{TurnID: turn.ID, Status: "failed"}},
	)
	return err
}

func planFromStep(s session.Step) (product.Plan, error) {
	var plan product.Plan
	if err := json.Unmarshal(s.Input, &plan); err != nil {
		return product.Plan{}, err
	}
	if plan.Done || plan.Key != s.StageKey || plan.Kind != product.Kind(s.Kind) || plan.Optional != s.Optional {
		return product.Plan{}, errors.New("agentloop: stored product plan differs from step facts")
	}
	plan.AllowedTools = append([]string(nil), s.AllowedTools...)
	return plan, nil
}

func (l *Loop) advanceDeterministic(ctx context.Context, lease session.Lease, p session.Projection, turn session.Turn, step session.Step) error {
	plan, err := planFromStep(step)
	if err != nil {
		return err
	}
	if !step.Deterministic {
		computed, err := l.Product.Compute(ctx, p, plan)
		if err != nil {
			_, appendErr := l.commit(ctx, lease, p, fact{session.StepEnded, session.StepEnd{TurnID: turn.ID, StepID: step.ID, Status: "failed", Output: json.RawMessage(`{}`), Warnings: []string{err.Error()}}})
			return appendErr
		}
		if computed.Assistant.Role == "" {
			computed.Assistant.Role = "assistant"
		}
		if err := validateModelMessage(computed.Assistant); err != nil {
			return err
		}
		accepted := fact{session.MessageAccepted, session.AcceptedMessage{TurnID: turn.ID, StepID: step.ID, Message: computed.Assistant}}
		if step.Kind == session.StageCompute {
			output := computed.Output
			if len(output) == 0 {
				output = json.RawMessage(`{}`)
			}
			_, err = l.commit(ctx, lease, p, accepted, fact{session.StepEnded, session.StepEnd{TurnID: turn.ID, StepID: step.ID, Status: "completed", Output: output, Warnings: computed.Warnings}})
			return err
		}
		if len(computed.Assistant.ToolCalls) == 0 {
			return errors.New("agentloop: tool stage declared no calls")
		}
		_, err = l.commit(ctx, lease, p, accepted)
		return err
	}
	if step.Kind != session.StageTool {
		return session.ErrConflict
	}
	advanced, err := l.advanceTool(ctx, lease, p, turn, step)
	if err != nil {
		return err
	}
	if advanced {
		return nil
	}
	settlement, err := l.settle(ctx, p, step)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, product.ErrUnavailable) {
			return err
		}
		return l.failStep(ctx, lease, p, turn.ID, step, err.Error())
	}
	_, err = l.commit(ctx, lease, p, fact{session.StepEnded, session.StepEnd{TurnID: turn.ID, StepID: step.ID, Status: settlement.Status, Output: settlement.Output, Warnings: settlement.Warnings}})
	return err
}

func (l *Loop) settle(ctx context.Context, p session.Projection, step session.Step) (product.Settlement, error) {
	plan, err := planFromStep(step)
	if err != nil {
		return product.Settlement{}, err
	}
	settlement, err := l.Product.Settle(ctx, p, plan)
	if err != nil {
		return product.Settlement{}, err
	}
	if settlement.Status != "completed" && settlement.Status != "failed" {
		return product.Settlement{}, errors.New("agentloop: invalid product settlement")
	}
	if len(settlement.Output) == 0 {
		settlement.Output = json.RawMessage(`{}`)
	}
	if !json.Valid(settlement.Output) {
		return product.Settlement{}, errors.New("agentloop: product output is not JSON")
	}
	return settlement, nil
}

func toolsForStage(all []model.Tool, allowed []string) []model.Tool {
	if len(allowed) == 0 {
		return nil
	}
	selected := make([]model.Tool, 0, len(allowed))
	for _, name := range allowed {
		for _, tool := range all {
			if tool.Name == name {
				selected = append(selected, tool)
				break
			}
		}
	}
	return selected
}
