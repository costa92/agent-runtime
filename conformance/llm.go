package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/costa92/agent-runtime/llm"
	"github.com/costa92/agent-runtime/run"
)

// LLMHarness is what a model adapter supplies to be checked.
type LLMHarness struct {
	Client llm.Client
	// ToolLess is a client for an engine without tool calling, so the suite can
	// check the degradation path rather than assume it.
	ToolLess llm.Client
	// Failing is a client whose provider errors after consuming the prompt —
	// the case where usage must still be reported.
	Failing llm.Client
}

// Conformance is the reusable suite every model adapter must pass.
//
// It checks behaviour the Runtime depends on and cannot verify per adapter:
// that capabilities are answered rather than discovered by failing, that
// cancellation is not retried, that spend is reported even when the call
// failed, and that a tool call survives the round trip intact.
func LLM(t *testing.T, newHarness func(t *testing.T) LLMHarness) {
	t.Helper()

	t.Run("UnsupportedToolCallingIsAStableCode", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		capabilities, err := harness.ToolLess.Capabilities(ctx, llm.ModelRef{Profile: "toolless"})
		if err != nil {
			t.Fatalf("capabilities: %v", err)
		}
		if capabilities.Tools {
			t.Fatal("the tool-less client claims tool support")
		}

		_, err = harness.ToolLess.Complete(ctx, llm.Request{
			Model:    llm.ModelRef{Profile: "toolless"},
			Messages: []llm.Message{{Role: llm.RoleUser, Content: "hello"}},
			Tools:    []llm.ToolDef{{Name: "search", Parameters: json.RawMessage(`{}`)}},
		})
		var runtimeError *run.Error
		if !errors.As(err, &runtimeError) {
			t.Fatalf("error is not a Runtime error: %v", err)
		}
		// The Runtime degrades on this — drops the tool loop and asks again —
		// so it cannot be decided by matching a message.
		if runtimeError.Code != llm.CodeCapabilityUnsupported {
			t.Fatalf("code=%q want=%q", runtimeError.Code, llm.CodeCapabilityUnsupported)
		}
	})

	t.Run("CancellationIsNeverRetried", func(t *testing.T) {
		harness := newHarness(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := harness.Client.Complete(ctx, llm.Request{
			Model:    llm.ModelRef{Profile: "default"},
			Messages: []llm.Message{{Role: llm.RoleUser, Content: "hello"}},
		})
		if err == nil {
			t.Fatal("a cancelled call succeeded")
		}
		if run.KindOf(err) != run.ErrorInterrupted {
			t.Errorf("kind=%s want=interrupted", run.KindOf(err))
		}
		// Retrying would override a decision somebody made.
		if run.RetryOf(err) != run.RetryNever {
			t.Errorf("retry=%s want=never", run.RetryOf(err))
		}
	})

	t.Run("EveryPaidAttemptReportsUsageEvenOnFailure", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		response, err := harness.Failing.Complete(ctx, llm.Request{
			Model:    llm.ModelRef{Profile: "failing"},
			Messages: []llm.Message{{Role: llm.RoleUser, Content: "hello"}},
		})
		if err == nil {
			t.Fatal("the failing client succeeded")
		}
		if len(response.Attempts) == 0 {
			t.Fatal("a failed call reported no attempts; its spend is unattributable")
		}

		var reported int
		for _, attempt := range response.Attempts {
			reported += attempt.Usage.InputTokens + attempt.Usage.OutputTokens
		}
		if reported == 0 {
			// The provider consumed the prompt and then errored. It charged for
			// it, and a ledger counting only successes understates exactly the
			// spend it exists to bound.
			t.Fatal("a failed attempt reported no usage")
		}
	})

	t.Run("StreamChunksPreserveOrderAndMatchComplete", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()
		request := llm.Request{
			Model:    llm.ModelRef{Profile: "default"},
			Messages: []llm.Message{{Role: llm.RoleUser, Content: "hello"}},
		}

		var streamed strings.Builder
		response, err := harness.Client.Stream(ctx, request, func(chunk llm.Chunk) error {
			streamed.WriteString(chunk.Content)
			return nil
		})
		if err != nil {
			t.Fatalf("stream: %v", err)
		}
		if streamed.String() != response.Message.Content {
			t.Fatalf("concatenated chunks differ from the response:\n%q\n%q",
				streamed.String(), response.Message.Content)
		}

		complete, err := harness.Client.Complete(ctx, request)
		if err != nil {
			t.Fatalf("complete: %v", err)
		}
		if complete.Message.Content != response.Message.Content {
			t.Fatalf("streaming and non-streaming disagree:\n%q\n%q",
				complete.Message.Content, response.Message.Content)
		}
	})

	t.Run("StreamStopsWhenTheConsumerErrors", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		stop := errors.New("enough")
		var delivered int
		_, err := harness.Client.Stream(ctx, llm.Request{
			Model:    llm.ModelRef{Profile: "default"},
			Messages: []llm.Message{{Role: llm.RoleUser, Content: "hello"}},
		}, func(llm.Chunk) error {
			delivered++
			return stop
		})
		if !errors.Is(err, stop) {
			t.Fatalf("the consumer's error was swallowed: %v", err)
		}
		if delivered != 1 {
			t.Fatalf("delivered %d chunks after the consumer stopped", delivered)
		}
	})

	t.Run("ToolCallsKeepTheirIDAndRawArguments", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		response, err := harness.Client.Complete(ctx, llm.Request{
			Model:    llm.ModelRef{Profile: "tools"},
			Messages: []llm.Message{{Role: llm.RoleUser, Content: "search"}},
			Tools:    []llm.ToolDef{{Name: "search", Parameters: json.RawMessage(`{"type":"object"}`)}},
		})
		if err != nil {
			t.Fatalf("complete: %v", err)
		}
		if len(response.ToolCalls) == 0 {
			t.Fatal("no tool call was returned")
		}

		call := response.ToolCalls[0]
		if call.ID == "" {
			// The ID is how a result is matched to its call; a regenerated one
			// silently mismatches results in a parallel batch.
			t.Fatal("the tool call has no stable ID")
		}
		var decoded map[string]any
		if err := json.Unmarshal(call.Arguments, &decoded); err != nil {
			t.Fatalf("arguments are not raw JSON: %v", err)
		}
		if _, ok := decoded["query"]; !ok {
			t.Fatalf("arguments were rewritten in transit: %s", call.Arguments)
		}
	})

	t.Run("ClientExecutesOnlyTheAttemptItWasGiven", func(t *testing.T) {
		harness := newHarness(t)
		ctx := context.Background()

		// The Runtime owns attempt limits because attempts are budget. A client
		// that retried internally would spend against an envelope it cannot see,
		// and the overrun would look like a single expensive call.
		response, err := harness.Failing.Complete(ctx, llm.Request{
			Model:    llm.ModelRef{Profile: "failing"},
			Messages: []llm.Message{{Role: llm.RoleUser, Content: "hello"}},
		})
		if err == nil {
			t.Fatal("the failing client succeeded")
		}
		if len(response.Attempts) != 1 {
			t.Fatalf("attempts=%d want=1; the client retried on its own", len(response.Attempts))
		}
	})
}
