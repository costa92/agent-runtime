package run

import "testing"

func TestMergeModelUsageAccumulatesPerProfile(t *testing.T) {
	var usage []ModelUsage
	usage = MergeModelUsage(usage, "chat", 10, 20)
	usage = MergeModelUsage(usage, "chat", 5, 7)
	usage = MergeModelUsage(usage, "synthesizer", 1, 2)

	if len(usage) != 2 {
		t.Fatalf("len = %d, want 2", len(usage))
	}
	if usage[0].Profile != "chat" || usage[0].InputTokens != 15 || usage[0].OutputTokens != 27 {
		t.Fatalf("chat line = %+v, want input 15 output 27", usage[0])
	}
	if usage[1].Profile != "synthesizer" || usage[1].InputTokens != 1 || usage[1].OutputTokens != 2 {
		t.Fatalf("synthesizer line = %+v", usage[1])
	}
}

func TestMergeModelUsagePreservesOrder(t *testing.T) {
	var usage []ModelUsage
	usage = MergeModelUsage(usage, "b", 1, 1)
	usage = MergeModelUsage(usage, "a", 2, 2)
	usage = MergeModelUsage(usage, "b", 3, 3)

	if len(usage) != 2 || usage[0].Profile != "b" || usage[1].Profile != "a" {
		t.Fatalf("usage = %+v, want [b a]", usage)
	}
}
