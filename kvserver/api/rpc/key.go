package rpc

import (
	"context"
	"errors"
	"sonekKV/kvserver"
	pb "sonekKV/kvserver/kvserverpb"
)

var (
	ErrGRPCEmptyRequest = errors.New("request is required")
	ErrGRPCEmptyKey     = errors.New("key is required")
	ErrGRPCEmptyValue   = errors.New("value is required")
)

type KV interface {
	Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error)
	Get(ctx context.Context, r *pb.GetRequest) (*pb.GetResponse, error)
	Delete(ctx context.Context, r *pb.DeleteRequest) (*pb.DeleteResponse, error)
}

type kvServer struct {
	kv KV
}

func (k kvServer) Get(ctx context.Context, request *pb.GetRequest) (*pb.GetResponse, error) {
	if err := checkGetRequest(request); err != nil {
		return nil, err
	}
	return k.kv.Get(ctx, request)
}

func (k kvServer) Put(ctx context.Context, request *pb.PutRequest) (*pb.PutResponse, error) {
	if err := checkPutRequest(request); err != nil {
		return nil, err
	}
	return k.kv.Put(ctx, request)
}

func (k kvServer) Delete(ctx context.Context, request *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := checkDeleteRequest(request); err != nil {
		return nil, err
	}
	return k.kv.Delete(ctx, request)
}

func NewKVServer(s *kvserver.SonekKVServer) pb.KVServer {
	return &kvServer{kv: s}
}

func checkGetRequest(r *pb.GetRequest) error {
	if r == nil {
		return ErrGRPCEmptyRequest
	}
	if len(r.Key) == 0 {
		return ErrGRPCEmptyKey
	}
	return nil
}

func checkPutRequest(r *pb.PutRequest) error {
	if r == nil {
		return ErrGRPCEmptyRequest
	}
	if len(r.Key) == 0 {
		return ErrGRPCEmptyKey
	}
	if len(r.Value) == 0 {
		return ErrGRPCEmptyValue
	}
	return nil
}

func checkDeleteRequest(r *pb.DeleteRequest) error {
	if r == nil {
		return ErrGRPCEmptyRequest
	}
	if len(r.Key) == 0 {
		return ErrGRPCEmptyKey
	}
	return nil
}
