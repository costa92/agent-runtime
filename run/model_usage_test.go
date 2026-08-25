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

// The ceiling a per-call timeout is set against is the worst single call, so
// the maximum must survive being merged with faster ones.
func TestCallTimingKeepsTheWorstCallNotTheLatest(t *testing.T) {
	var usage []ModelUsage
	usage = MergeCallTiming(usage, "deepseek/v4", 12_000)
	usage = MergeCallTiming(usage, "deepseek/v4", 900)

	if len(usage) != 1 {
		t.Fatalf("lines = %d; one profile is one line", len(usage))
	}
	if usage[0].Calls != 2 {
		t.Fatalf("calls = %d, want 2", usage[0].Calls)
	}
	if usage[0].MaxCallMS != 12_000 {
		t.Fatalf("max = %d; a later fast call must not lower the worst one", usage[0].MaxCallMS)
	}
}

// Timing and tokens are reported by different parties — the Runtime measures
// the round trip, the agent reads the response — so a profile may arrive on one
// path before the other.
func TestCallTimingAndTokensAccumulateOnTheSameLine(t *testing.T) {
	var usage []ModelUsage
	usage = MergeCallTiming(usage, "openai/gpt-4o", 500)
	usage = MergeModelUsage(usage, "openai/gpt-4o", 100, 50)

	if len(usage) != 1 {
		t.Fatalf("lines = %d; timing and tokens for one profile are one line", len(usage))
	}
	if usage[0].InputTokens != 100 || usage[0].OutputTokens != 50 {
		t.Fatalf("tokens = %d/%d; merging timing first must not lose them", usage[0].InputTokens, usage[0].OutputTokens)
	}
	if usage[0].Calls != 1 || usage[0].MaxCallMS != 500 {
		t.Fatalf("timing = %d calls / %d ms; merging tokens after must not clear it", usage[0].Calls, usage[0].MaxCallMS)
	}
}
