package observe

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

type ctxKey struct{}

// ctxHandler is what a host's handler does: read the context and stamp the
// record. The port must hand the context through for that to work.
type ctxHandler struct{ slog.Handler }

func (h ctxHandler) Handle(ctx context.Context, r slog.Record) error {
	if v, ok := ctx.Value(ctxKey{}).(string); ok {
		r.AddAttrs(slog.String("trace_id", v))
	}
	return h.Handler.Handle(ctx, r)
}

func TestStdLoggerCarriesTheContextToTheHandler(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	t.Cleanup(func() { slog.SetDefault(old) })
	slog.SetDefault(slog.New(ctxHandler{slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})}))

	ctx := context.WithValue(context.Background(), ctxKey{}, "trace-42")
	var l Logger = StdLogger{}
	l.Debug(ctx, "d", "k", 1)
	l.Info(ctx, "i")
	l.Warn(ctx, "w")
	l.Error(ctx, "e")

	out := buf.String()
	if strings.Count(out, `"trace_id":"trace-42"`) != 4 {
		t.Fatalf("every level must reach the handler with its context:\n%s", out)
	}
	if !strings.Contains(out, `"k":1`) {
		t.Fatalf("key/value pairs dropped:\n%s", out)
	}
}
