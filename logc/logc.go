package logc

import (
	"context"
	"errors"
	"go.uber.org/zap/zapcore"

	"go.uber.org/zap"
)

type CtxLogger string

const (
	CTX_LOGGER = "logger"
)

func ConfigureCtx(logger *zap.Logger, ctx context.Context) context.Context {
	return context.WithValue(ctx, CTX_LOGGER, logger) //nolint:staticcheck
}

func With(ctx context.Context, fields ...zapcore.Field) context.Context {
	if len(fields) == 0 {
		return ctx
	}
	return ConfigureCtx(GetLogger(ctx).With(fields...), ctx)
}

func Check(ctx context.Context, lvl zapcore.Level, msg string) *zapcore.CheckedEntry {
	return GetLogger(ctx).WithOptions(skip...).Check(lvl, msg)
}

func Debug(ctx context.Context, message string, fields ...zap.Field) {
	if ignoreContextCanceled && ContextCanceled(fields...) {
		return
	}
	GetLogger(ctx).WithOptions(skip...).Debug(message, fields...)
}

func Info(ctx context.Context, message string, fields ...zap.Field) {
	if ignoreContextCanceled && ContextCanceled(fields...) {
		return
	}
	GetLogger(ctx).WithOptions(skip...).Info(message, fields...)
}

func Warn(ctx context.Context, message string, fields ...zap.Field) {
	if ignoreContextCanceled && ContextCanceled(fields...) {
		return
	}
	GetLogger(ctx).WithOptions(skip...).Warn(message, fields...)
}

func Error(ctx context.Context, message string, fields ...zap.Field) {
	if ignoreContextCanceled && ContextCanceled(fields...) {
		return
	}
	GetLogger(ctx).WithOptions(skip...).Error(message, fields...)
}

func Fatal(ctx context.Context, message string, fields ...zap.Field) {
	GetLogger(ctx).WithOptions(skip...).Fatal(message, fields...)
}

func Panic(ctx context.Context, msg string, fields ...zapcore.Field) {
	GetLogger(ctx).WithOptions(skip...).Panic(msg, fields...)
}
func WithOptions(ctx context.Context, opts ...zap.Option) context.Context {
	return ConfigureCtx(GetLogger(ctx).WithOptions(opts...), ctx)
}

func ContextCanceled(fields ...zap.Field) bool {
	for _, field := range fields {
		if field.Type != zapcore.ErrorType {
			continue
		}
		if field.Interface == nil {
			continue
		}
		err := field.Interface.(error)
		if errors.Is(err, context.Canceled) {
			return true
		}
	}
	return false
}
