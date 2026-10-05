package agentruntime_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	agentruntime "github.com/costa92/agent-runtime"
	"github.com/costa92/agent-runtime/agent"
	"github.com/costa92/agent-runtime/llm"
)

func TestModelInvocationRecordsFinalRequestDigest(t *testing.T) {
	var sent llm.Request
	h := newHarness(t, scriptedAgent{execute: func(ctx context.Context, request agent.Request) (agent.Response, error) {
		_, err := request.Ports.Model(ctx, llm.Request{Messages: []llm.Message{{Role: llm.RoleUser, Content: "private question"}}})
		return agent.Response{Output: json.RawMessage(`"ok"`)}, err
	}}, withDeps(func(deps *agentruntime.Dependencies) {
		deps.Models = capturingModels{onRequest: func(request llm.Request) { sent = request }}
	}))
	started := start(t, h)
	if _, err := h.runtime.Advance(t.Context(), started.ID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := h.store.Get(t.Context(), started.ID)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(sent)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(encoded)
	want := "sha256:v1:" + hex.EncodeToString(sum[:])
	for _, invocation := range snapshot.Invocations {
		if invocation.RequestDigest != want {
			t.Fatalf("request digest = %q, want %q", invocation.RequestDigest, want)
		}
		if strings.Contains(invocation.RequestDigest, "private question") {
			t.Fatal("request content leaked into invocation ledger")
		}
		return
	}
	t.Fatal("no model invocation recorded")
}
