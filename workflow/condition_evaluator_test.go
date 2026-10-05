package workflow_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/costa92/agent-runtime/workflow"
)

func TestConditionEvaluator(t *testing.T) {
	evaluator := workflow.NewConditionEvaluator()
	ctx := context.Background()

	tests := []struct {
		name      string
		condition string
		input     string
		want      bool
		wantErr   bool
	}{
		{
			name:      "numeric less than match",
			condition: "node.quality_score < 80",
			input:     `{"quality_score": 75}`,
			want:      true,
		},
		{
			name:      "numeric less than mismatch",
			condition: "node.quality_score < 80",
			input:     `{"quality_score": 85}`,
			want:      false,
		},
		{
			name:      "numeric less equal match",
			condition: "node.quality_score <= 80",
			input:     `{"quality_score": 80}`,
			want:      true,
		},
		{
			name:      "boolean comparison",
			condition: "node.is_valid == false",
			input:     `{"is_valid": false}`,
			want:      true,
		},
		{
			name:      "nested field path",
			condition: "node.review.score > 90",
			input:     `{"review": {"score": 95}}`,
			want:      true,
		},
		{
			name:      "string equality",
			condition: "node.status == 'needs_fix'",
			input:     `{"status": "needs_fix"}`,
			want:      true,
		},
		{
			name:      "missing field returns false without error",
			condition: "node.missing_score < 80",
			input:     `{"quality_score": 75}`,
			want:      false,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evaluator.Evaluate(ctx, tt.condition, json.RawMessage(tt.input))
			if (err != nil) != tt.wantErr {
				t.Fatalf("Evaluate() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}
