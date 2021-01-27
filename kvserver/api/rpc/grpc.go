package rpc

import (
	"google.golang.org/grpc"
	"math"
	"sonekKV/kvserver"
	pb "sonekKV/kvserver/kvserverpb"
)

const (
	grpcOverheadBytes = 512 * 1024
	maxSendBytes      = math.MaxInt32
)

func Server(s *kvserver.SonekKVServer, gopts ...grpc.ServerOption)  *grpc.Server {
	var opts []grpc.ServerOption
	opts = append(opts)
	opts = append(opts, grpc.MaxRecvMsgSize(int(s.Cfg.MaxRequestBytes+grpcOverheadBytes)))
	opts = append(opts, grpc.MaxSendMsgSize(maxSendBytes))
	opts = append(opts, grpc.MaxSendMsgSize(maxSendBytes))

	grpcServer := grpc.NewServer(append(opts, gopts...)...)
	pb.RegisterKVServer(grpcServer, NewKVServer(s))
	pb.RegisterWatchServer(grpcServer, NewWatchServer(s))


	return grpcServer
}


