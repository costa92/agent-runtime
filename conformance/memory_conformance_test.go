package conformance_test

import (
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/conformance"
	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
)

func TestMemoryProviderConformance(t *testing.T) {
	conformance.MemoryProvider(t, func(t *testing.T) conformance.MemoryHarness {
		provider := testkit.NewMemoryProvider()
		return conformance.MemoryHarness{
			Provider: provider,
			Seed:     provider.Seed,
			Writes:   provider.Writes,
		}
	})
}
