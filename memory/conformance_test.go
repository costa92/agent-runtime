package memory_test

import (
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/memory"
)

func TestMemoryProviderConformance(t *testing.T) {
	memory.ProviderConformance(t, func(t *testing.T) memory.ProviderHarness {
		provider := testkit.NewMemoryProvider()
		return memory.ProviderHarness{
			Provider: provider,
			Seed:     provider.Seed,
			Writes:   provider.Writes,
		}
	})
}
