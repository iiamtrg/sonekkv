package client

import (
	"google.golang.org/grpc"
	"math"
)

var (
	defaultMaxCallSendMsgSize = grpc.MaxCallSendMsgSize(2 * 1024 * 1024)
	defaultMaxCallRecvMsgSize = grpc.MaxCallRecvMsgSize(math.MaxInt32)
)

var defaultCallOpts = []grpc.CallOption{defaultMaxCallSendMsgSize, defaultMaxCallRecvMsgSize}
