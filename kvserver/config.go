package kvserver

import (
	"go.uber.org/zap"
)

type ServerConfig struct {
	Name string
	Host	string
	Port	string
	DataDir		string
	MaxRequestBytes uint
	Logger *zap.Logger
}
