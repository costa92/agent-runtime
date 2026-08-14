package store_test

import (
	"encoding/json"
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/run"
	"github.com/kart-io/wechat-account/agent-runtime/store"
)

// Every Kind has a payload type.
//
// Totality is the property that matters: a Kind added without one would be
// enqueued as a raw blob by whoever needed it first, and the shape would be
// settled by that call site rather than by this file.
func TestEveryProjectionKindHasAPayloadType(t *testing.T) {
	payloads := []store.ProjectionPayload{
		store.UserTurnPayload{},
		store.AssistantMessagePayload{},
		store.PendingApprovalPayload{},
		store.ProgressPayload{},
		store.TerminalResultPayload{},
	}

	covered := map[store.ProjectionKind]bool{}
	for _, payload := range payloads {
		kind := payload.ProjectionKind()
		if !kind.Valid() {
			t.Errorf("%T claims kind %q, which is not one the Store accepts", payload, kind)
		}
		if covered[kind] {
			t.Errorf("two payload types claim %q; the projector could not tell them apart", kind)
		}
		covered[kind] = true
	}

	for _, kind := range []store.ProjectionKind{
		store.ProjectionUserTurn, store.ProjectionAssistantMessage,
		store.ProjectionPendingApproval, store.ProjectionProgress,
		store.ProjectionTerminalResult,
	} {
		if !covered[kind] {
			t.Errorf("%q has no payload type", kind)
		}
	}
}

// A fact's Kind comes from its payload, so the two cannot disagree.
func TestTheKindComesFromThePayload(t *testing.T) {
	fact, err := store.NewProjectionFact("run-1",
		store.AssistantMessagePayload{
			Output: json.RawMessage(`{"answer":"hi"}`), AgentKey: "writer", NodeID: "draft",
		})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if fact.Kind != store.ProjectionAssistantMessage {
		t.Fatalf("kind = %q", fact.Kind)
	}

	decoded, err := store.DecodeProjection[store.AssistantMessagePayload](fact)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(decoded.Output) != `{"answer":"hi"}` ||
		decoded.AgentKey != "writer" || decoded.NodeID != "draft" {
		t.Fatalf("round trip = %+v", decoded)
	}
}

// Decoding into the wrong payload type must fail rather than succeed empty.
//
// This is the failure the type parameter exists to prevent: JSON into a
// mismatched struct leaves every field at its zero value and returns no error,
// so the host writes a blank transcript line and nothing anywhere reports a
// problem.
func TestDecodingTheWrongPayloadTypeIsRefused(t *testing.T) {
	fact, err := store.NewProjectionFact("run-1", store.UserTurnPayload{Text: "question"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if _, err := store.DecodeProjection[store.ProgressPayload](fact); run.KindOf(err) != run.ErrorInvalid {
		t.Fatalf("a user turn decoded as progress: %v", err)
	}
}

// The constructor applies the identity checks at the point the fact is made
// rather than at the point it is committed. The sequence is not among them: it
// is the Store's to assign.
func TestAnUnidentifiableFactIsRefusedAtConstruction(t *testing.T) {
	if _, err := store.NewProjectionFact("", store.ProgressPayload{}); err == nil {
		t.Error("a fact with no Run was constructed")
	}
}

// The wire form is the payload itself, with no envelope.
//
// Asserted because the envelope is the fact's own columns — Kind, RunID,
// Sequence. Repeating any of them inside the payload would create a second
// place for them to be right, and the projector would have to decide which one
// to believe.
func TestThePayloadCarriesNoEnvelope(t *testing.T) {
	fact, err := store.NewProjectionFact("run-1",
		store.TerminalResultPayload{State: "failed"})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	var wire map[string]json.RawMessage
	if err := json.Unmarshal(fact.Payload, &wire); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, forbidden := range []string{"kind", "run_id", "sequence"} {
		if _, present := wire[forbidden]; present {
			t.Errorf("the payload repeats %q, which is already a column of the fact", forbidden)
		}
	}
}
