package client

import (
	"context"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"time"
)

type Config struct {
	Endpoint string `json:"endpoint"`

	DialTimeout time.Duration `json:"dial-timeout"`

	DialKeepAliveTime time.Duration `json:"dial-keep-alive-time"`

	DialKeepAliveTimeout time.Duration `json:"dial-keep-alive-timeout"`

	PermitWithoutStream bool

	MaxCallSendMsgSize int

	MaxCallRecvMsgSize int

	DialOptions []grpc.DialOption

	Context context.Context

	LogConfig *zap.Config

}
