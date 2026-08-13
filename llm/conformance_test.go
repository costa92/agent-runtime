package llm_test

import (
	"testing"

	"github.com/kart-io/wechat-account/agent-runtime/internal/testkit"
	"github.com/kart-io/wechat-account/agent-runtime/llm"
)

func TestScriptedModelConformance(t *testing.T) {
	llm.Conformance(t, func(t *testing.T) llm.Harness {
		return llm.Harness{
			Client:   testkit.NewScriptedModel(),
			ToolLess: testkit.NewToolLessModel(),
			Failing:  testkit.NewFailingModel(),
		}
	})
}
