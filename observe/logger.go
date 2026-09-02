package observe

import (
	"context"
	"log/slog"
)

// Logger is the Runtime's diagnostic log port.
//
// Every method takes a context so the host's implementation can pick up
// whatever the context carries — a trace id, a request id — and stamp it the
// same way the host stamps its own lines. The Runtime writing through a
// package-level logger would have produced lines with no trace id in the
// middle of a traced request, which is the one property a log line in a
// runtime must not lose.
//
// It is modelled on gorm's logger.Interface: a small leveled interface the
// library ships a plain default for, and the host replaces at assembly.
type Logger interface {
	Debug(ctx context.Context, msg string, kv ...any)
	Info(ctx context.Context, msg string, kv ...any)
	Warn(ctx context.Context, msg string, kv ...any)
	Error(ctx context.Context, msg string, kv ...any)
}

// StdLogger writes through the process-wide log/slog default. It is the
// default so that a host which wires nothing still sees Runtime failures.
type StdLogger struct{}

func (StdLogger) Debug(ctx context.Context, msg string, kv ...any) {
	slog.Default().DebugContext(ctx, msg, kv...)
}

func (StdLogger) Info(ctx context.Context, msg string, kv ...any) {
	slog.Default().InfoContext(ctx, msg, kv...)
}

func (StdLogger) Warn(ctx context.Context, msg string, kv ...any) {
	slog.Default().WarnContext(ctx, msg, kv...)
}

func (StdLogger) Error(ctx context.Context, msg string, kv ...any) {
	slog.Default().ErrorContext(ctx, msg, kv...)
}

// NopLogger discards everything, for tests that assert on nothing else.
type NopLogger struct{}

func (NopLogger) Debug(context.Context, string, ...any) {}
func (NopLogger) Info(context.Context, string, ...any)  {}
func (NopLogger) Warn(context.Context, string, ...any)  {}
func (NopLogger) Error(context.Context, string, ...any) {}
