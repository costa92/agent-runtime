package run

// ModelUsage is one model profile's consumption within a node.
//
// It breaks a node's Used.Tokens down per model profile so a host can price
// the spend. The Runtime itself never parses a profile — it is an opaque key
// into the host's published model catalogue — so pricing stays the host's job.
//
// InputTokens and OutputTokens are kept apart because they are two different
// priceable resources.
type ModelUsage struct {
	Profile string `json:"profile"`
	// InputTokens counts prompt tokens billed to this profile, including
	// cached reads: the provider bills them all and the price distinguishes
	// them, not this record.
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`

	// Calls counts the physical model calls this node made against the
	// profile, and MaxCallMS is the longest single one of them.
	//
	// They are here rather than derived from timestamps elsewhere because
	// nothing else records how long ONE call took: the event stream times
	// reservations, not provider round trips, so a node that spent six minutes
	// inside a single call and a node that made four fast ones are
	// indistinguishable after the fact. That distinction is exactly what sizing
	// a per-call timeout needs — and a per-call timeout is what stops a call
	// from outliving the host's wait for the whole Run.
	//
	// The maximum rather than the total: a ceiling is set against the worst
	// single call, and a total divided by Calls hides it.
	Calls     int `json:"calls,omitempty"`
	MaxCallMS int `json:"max_call_ms,omitempty"`
}

// MergeModelUsage folds one usage line into the per-profile total, keyed by
// profile. Two layers of one node sharing a model (a router and a synthesizer,
// say) produce one line, because the host prices per profile and two lines
// would double-count the same model.
func MergeModelUsage(existing []ModelUsage, profile string, input, output int) []ModelUsage {
	for i := range existing {
		if existing[i].Profile == profile {
			existing[i].InputTokens += input
			existing[i].OutputTokens += output
			return existing
		}
	}
	return append(existing, ModelUsage{Profile: profile, InputTokens: input, OutputTokens: output})
}

// MergeCallTiming folds one call's duration into the per-profile line.
//
// Separate from MergeModelUsage because the two are measured by different
// parties: tokens are reported by the agent from the response it received,
// while duration is measured by the Runtime around the provider round trip —
// the agent never sees a clock. Keeping them apart also means an agent that
// reports no usage at all still gets its calls timed.
func MergeCallTiming(existing []ModelUsage, profile string, durationMS int) []ModelUsage {
	if durationMS < 0 {
		durationMS = 0
	}
	for i := range existing {
		if existing[i].Profile == profile {
			existing[i].Calls++
			if durationMS > existing[i].MaxCallMS {
				existing[i].MaxCallMS = durationMS
			}
			return existing
		}
	}
	return append(existing, ModelUsage{Profile: profile, Calls: 1, MaxCallMS: durationMS})
}
