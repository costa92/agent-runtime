package agentruntime

import (
	"context"
	"fmt"

	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/memory"
	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
	"github.com/kart-io/wechat-account/agent-runtime/workflow"
)

// memoryScope builds the isolation boundary from the Definition, never from the
// caller.
//
// Tenant comes from the Run's principal and namespace from the declaration. An
// agent that could pass its own scope would be able to read another tenant's
// records while looking perfectly authorized, because the scope is what the
// provider isolates on.
func (s *session) memoryScope(declared definition.MemoryRef, node workflow.Node) memory.Scope {
	return memory.Scope{
		Tenant:    s.state().Principal.Tenant,
		Namespace: declared.Namespace,
		Principal: s.state().Principal,
		AgentKey:  node.Implementation,
		RunID:     s.state().ID,
	}
}

// declaredMemory resolves a key against what the Definition declared.
//
// A key the Definition never named is refused even when the provider exists:
// the Definition is what the Run was published to do, and the registry is only
// what the deployment happens to have installed.
func declaredMemory(s *session, key string) (definition.MemoryRef, error) {
	for _, declared := range s.declared.Memories {
		if declared.Key == key {
			return declared, nil
		}
	}
	return definition.MemoryRef{}, run.NewError(run.CodeUndeclaredMemoryKey, run.ErrorDenied, run.RetryNever,
		fmt.Errorf("definition %s does not declare memory %q", s.state().Definition.ID, key))
}

// commitMemory records a committed write atomically with its settlement.
//
// The provider does not commit it. A provider that wrote its own record would
// be a second writer of Run-adjacent state, outside the fence — and the write
// and its budget settlement could then land separately.
func (s *session) commitMemory(ctx context.Context, id run.ID, key, namespace string, fact memory.ResultFact, node string) error {
	reserved := s.state().Invocations[id].Reserved
	charged := run.Limits{ToolCalls: 1}

	transition := run.Transition{Next: s.state()}
	transition.Next.Revision = s.state().Revision + 1
	transition.Next.Budget = s.state().Budget.Settle(reserved, charged, fact.Outcome != run.OutcomeUnknown)
	// Same rule as complete(): the settled outcome must reach the snapshot.
	// Left in_flight it reads to parkUnclassifiedEffects as a worker that died
	// mid-effect, and an approval-resumed Run parks itself in waiting_resolution
	// for the memory write it already finished.
	existing := transition.Next.Invocations[id]
	existing.Outcome = fact.Outcome
	transition.Next.Invocations[id] = existing

	settlement := store.BudgetSettlement{
		ReservationID: id, Charged: run.Limits{ToolCalls: 1}, Release: true,
	}
	if fact.Outcome == run.OutcomeUnknown {
		settlement.Release = false
		parked, err := run.Reduce(s.state(), run.Command{
			Kind: run.CommandRecordUnknown, InvocationID: id,
		})
		if err != nil {
			return err
		}
		parked.Next.Budget = transition.Next.Budget
		transition = parked
	}
	stampNode(transition.Events, node)

	committed, err := s.runtime.deps.Store.CommitMemoryMutation(ctx, store.CommitMemoryMutationCommand{
		Fence:      s.fence(),
		Invocation: store.InvocationResult{ID: id, Outcome: fact.Outcome},
		Mutation: store.MemoryMutationFact{
			Key: key, Namespace: namespace, Ref: fact.Mutation.Ref,
		},
		Usage:  run.Limits{ToolCalls: 1},
		Budget: settlement,
		Commit: store.CommitContext{Transition: transition, Events: transition.Events},
	})
	if err != nil {
		return err
	}
	s.setSnapshot(committed)
	return nil
}

// remainingPercent is the budget headroom a policy condition can read.
//
// A percentage rather than absolute numbers because a policy that said "deny
// writes below 50,000 tokens" would mean something different for every
// envelope, and the rule an operator wants to write is "when this Run is nearly
// out".
func remainingPercent(budget run.Budget) int {
	if budget.Envelope.Tokens <= 0 {
		return 100
	}
	remaining := budget.Envelope.Tokens - budget.Committed().Tokens
	if remaining <= 0 {
		return 0
	}
	return remaining * 100 / budget.Envelope.Tokens
}
