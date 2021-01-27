package client

import (
	"context"
	"google.golang.org/grpc"
	pb "sonekKV/kvserver/kvserverpb"
)

type (
	PutResponse     *pb.PutResponse
	GetResponse     *pb.GetResponse
	DeleteResponse  *pb.DeleteResponse
)
type KV interface {
	Put (ctx context.Context, key, val string) (PutResponse, error)
	Get(ctx context.Context, key string) (GetResponse, error)
	Delete(ctx context.Context, key string) (DeleteResponse, error)
}

type kv struct {
	remote   pb.KVClient
	callOpts []grpc.CallOption
}

func (k *kv) Put(ctx context.Context, key, val string) (PutResponse, error) {
	req := &pb.PutRequest{Key: []byte(key), Value: []byte(val)}
	return k.remote.Put(ctx, req, k.callOpts...)
}

func (k *kv) Get(ctx context.Context, key string) (GetResponse, error) {
	req := &pb.GetRequest{Key: []byte(key)}
	return k.remote.Get(ctx, req, k.callOpts...)
}

func (k *kv) Delete(ctx context.Context, key string) (DeleteResponse, error) {
	req := &pb.DeleteRequest{Key: []byte(key)}
	return k.remote.Delete(ctx, req, k.callOpts...)
}


func NewKV(c *Client) KV {
	api := &kv{remote: NewKVClient(c)}
	if c != nil {
		api.callOpts = c.callOpts
	}
	return api
}
