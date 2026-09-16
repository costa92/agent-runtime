package agentruntime_test

import (
	"context"
	"testing"

	agentruntime "github.com/kart-io/wechat-account/agent-runtime"
	"github.com/kart-io/wechat-account/agent-runtime/agent"
	"github.com/kart-io/wechat-account/agent-runtime/run"
)

// levelRecordingLogger records which level each message was written at.
type levelRecordingLogger struct {
	levels map[string]string
}

func newLevelRecordingLogger() *levelRecordingLogger {
	return &levelRecordingLogger{levels: map[string]string{}}
}

func (l *levelRecordingLogger) record(level, msg string) { l.levels[msg] = level }

func (l *levelRecordingLogger) Debug(_ context.Context, msg string, _ ...any) {
	l.record("debug", msg)
}
func (l *levelRecordingLogger) Info(_ context.Context, msg string, _ ...any) {
	l.record("info", msg)
}
func (l *levelRecordingLogger) Warn(_ context.Context, msg string, _ ...any) {
	l.record("warn", msg)
}
func (l *levelRecordingLogger) Error(_ context.Context, msg string, _ ...any) {
	l.record("error", msg)
}

func failingWith(err error) agent.Agent {
	return scriptedAgent{execute: func(context.Context, agent.Request) (agent.Response, error) {
		return agent.Response{}, err
	}}
}

// A node that fails because the caller asked for something this deployment
// cannot do — a model without vision, a malformed command — is a routine
// outcome the caller is told about. Logging it at Error buries the faults that
// actually need an operator, so only kinds that mean "the Runtime or an
// adapter is broken" keep that level.
func TestNodeFailureLogLevelFollowsErrorKind(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		level string
	}{
		{
			name:  "caller error stays out of the error log",
			err:   run.NewError("model.capability_unsupported", run.ErrorInvalid, run.RetryNever, nil),
			level: "warn",
		},
		{
			name:  "refusal stays out of the error log",
			err:   run.NewError("quota.exhausted", run.ErrorDenied, run.RetryNever, nil),
			level: "warn",
		},
		{
			name:  "adapter fault still reaches the error log",
			err:   run.NewError("provider.broken", run.ErrorInternal, run.RetryNever, nil),
			level: "error",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			logger := newLevelRecordingLogger()
			h := newHarness(t, failingWith(testCase.err), withDeps(func(deps *agentruntime.Dependencies) {
				deps.Logger = logger
			}))
			snapshot := start(t, h)

			for i := 0; i < 8; i++ {
				if _, err := h.runtime.Advance(t.Context(), snapshot.ID); err != nil {
					break
				}
				if _, ok := logger.levels["agent node failed"]; ok {
					break
				}
			}

			got, ok := logger.levels["agent node failed"]
			if !ok {
				t.Fatalf("node failure was never logged; levels = %v", logger.levels)
			}
			if got != testCase.level {
				t.Fatalf("logged at %q, want %q", got, testCase.level)
			}
		})
	}
}
