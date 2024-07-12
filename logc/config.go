package logc

import (
	"context"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var ignoreContextCanceled = false

var globalLogger *zap.Logger
var skip = []zap.Option{zap.AddCallerSkip(1)}

func Flags() *pflag.FlagSet {
	fs := pflag.NewFlagSet("ctx_logger", pflag.ExitOnError)
	fs.String("log-level", "debug", "")
	fs.BoolP("is-prod", "p", false, "")
	fs.BoolP("ignore-context-canceled", "c", false, "")
	return fs
}

func NewLoggerFromFlags() (*zap.Logger, error) {
	logger, err := NewLogger(viper.GetBool("is-prod"), viper.GetString("log-level"), viper.GetBool("ignore-context-canceled"))
	if err != nil {
		return nil, err
	}
	globalLogger = logger
	return logger, nil
}

func NewLogger(production bool, level string, icc bool) (*zap.Logger, error) {
	var conf zap.Config
	if production {
		conf = zap.NewProductionConfig()
	} else {
		conf = zap.NewDevelopmentConfig()
	}

	if err := conf.Level.UnmarshalText([]byte(level)); err != nil {
		return nil, err
	}
	ignoreContextCanceled = icc
	return conf.Build()
}

func GetLogger(ctx context.Context) *zap.Logger {
	if ctx == nil {
		if globalLogger != nil {
			return globalLogger
		}
		return zap.L()
	}
	logger := ctx.Value(CTX_LOGGER)
	if logger == nil {
		if globalLogger != nil {
			return globalLogger
		}
		return zap.L()
	}
	return logger.(*zap.Logger)
}
