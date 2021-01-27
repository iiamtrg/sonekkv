package logutil

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)


var DefaultZapLoggerConfig = zap.Config{
	Level: zap.NewAtomicLevelAt(zap.DebugLevel),

	Development: false,
	Sampling: &zap.SamplingConfig{
		Initial:    100,
		Thereafter: 100,
	},

	Encoding: "console",

	EncoderConfig: zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	},

	OutputPaths: []string{"stdout"},
	ErrorOutputPaths: []string{"stdout"},
}
