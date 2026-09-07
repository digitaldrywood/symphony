package hubserver

import (
	"context"
	"log/slog"
)

type hostedLogHandler struct {
	output slog.Handler
}

func (h hostedLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.output.Enabled(ctx, level)
}

func (h hostedLogHandler) Handle(ctx context.Context, record slog.Record) error {
	return h.output.Handle(ctx, slog.NewRecord(record.Time, record.Level, "hosted service event", 0))
}

func (h hostedLogHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h hostedLogHandler) WithGroup(string) slog.Handler {
	return h
}
