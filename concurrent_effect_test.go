package agentruntime_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

func TestConcurrentModelCallsCommitIndependentInvocations(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		var group sync.WaitGroup
		group.Add(2)
		errors := make(chan error, 2)
		for range 2 {
			go func() {
				defer group.Done()
				_, err := request.Ports.Model(ctx, llm.Request{NoTools: true})
				errors <- err
			}()
		}
		<-entered
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			close(release)
			group.Wait()
			t.Fatal("second independent call did not reach the model")
		}
		close(release)
		group.Wait()
		close(errors)
		for err := range errors {
			if err != nil {
				t.Errorf("concurrent model call: %v", err)
			}
		}
		return agent.Response{Output: json.RawMessage(`{"done":true}`)}, nil
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = scriptedModels{onCall: func() {
			entered <- struct{}{}
			<-release
		}}
	}))
	started := start(t, h)
	result, err := h.runtime.Advance(t.Context(), started.ID)
	if err != nil || result.Run.State != run.StateSucceeded {
		t.Fatalf("advance: state=%s err=%v", result.Run.State, err)
	}
	if got := len(result.Run.Invocations); got != 2 {
		t.Fatalf("invocations=%d want=2", got)
	}
}
