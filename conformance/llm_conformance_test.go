package conformance_test

import (
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/conformance"
	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
)

func TestScriptedModelConformance(t *testing.T) {
	conformance.LLM(t, func(t *testing.T) conformance.LLMHarness {
		return conformance.LLMHarness{
			Client:   testkit.NewScriptedModel(),
			ToolLess: testkit.NewToolLessModel(),
			Failing:  testkit.NewFailingModel(),
		}
	})
}
