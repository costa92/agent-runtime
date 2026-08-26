package workflow_test

import (
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/definition"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// The digest has to move when execution changes, not only when the node shape
// does.
//
// It used to hash the compiled nodes alone. Prompt, model policy and memory
// scopes are not in a node — they are read live off the Definition on every
// resume (engine.go's system message, tool_loop.go's model policy,
// memory_loop.go's scopes) — so a Definition whose prompt changed kept the same
// digest and an in-flight Run adopted the new one at its next step with nothing
// to notice.
//
// Builtins are the case that makes this matter. Their Version is a hardcoded 1
// forever, so unlike a published version they get no new identity when they
// change: the digest is the only thing that can say they did.
func TestChangingThePromptMovesTheDigest(t *testing.T) {
	before := compile(t, definition.Definition{Prompt: "original"})
	after := compile(t, definition.Definition{Prompt: "rewritten"})

	if before.Ref.Digest == after.Ref.Digest {
		t.Error("a changed prompt left the digest identical; an in-flight Run would silently adopt it")
	}
}

func TestChangingTheModelPolicyMovesTheDigest(t *testing.T) {
	before := compile(t, definition.Definition{Prompt: "p"})
	after := compile(t, definition.Definition{
		Prompt: "p", Model: definition.ModelPolicy{Profile: "fast"},
	})

	if before.Ref.Digest == after.Ref.Digest {
		t.Error("a changed model policy left the digest identical")
	}
}

func TestChangingAMemoryScopeMovesTheDigest(t *testing.T) {
	before := compile(t, definition.Definition{Prompt: "p"})
	after := compile(t, definition.Definition{
		Prompt:   "p",
		Memories: []definition.MemoryRef{{Key: "notes", Namespace: "ns", MaxRecords: 3, MaxTokens: 100}},
	})

	if before.Ref.Digest == after.Ref.Digest {
		t.Error("a changed memory scope left the digest identical")
	}
}

// The same Definition must still digest the same, or nothing could ever resume.
func TestTheSameDefinitionDigestsTheSame(t *testing.T) {
	first := compile(t, definition.Definition{Prompt: "p"})
	second := compile(t, definition.Definition{Prompt: "p"})

	if first.Ref.Digest != second.Ref.Digest {
		t.Error("compiling the same Definition twice produced two digests")
	}
}

// The ref itself must not be an input: a Run pins {ID, Version, Protocol}
// separately, and folding them into the digest would make the digest restate an
// identity the ref already carries.
func TestTheRefIsNotPartOfTheDigest(t *testing.T) {
	first := compile(t, definition.Definition{Prompt: "p"})

	other := base(definition.Definition{Prompt: "p"})
	other.Ref = run.DefinitionRef{ID: "assistant", Version: 9, Protocol: 1}
	second := compileRef(t, other)

	if first.Ref.Digest != second.Ref.Digest {
		t.Error("the same behavior under a different version produced a different digest")
	}
}
