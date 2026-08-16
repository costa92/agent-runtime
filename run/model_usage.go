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
