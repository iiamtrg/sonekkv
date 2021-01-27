package client

import (
	"context"
	"google.golang.org/grpc"
	pb "sonekKV/kvserver/kvserverpb"
)

type kvClient struct {
	remote   pb.KVClient
	callOpts []grpc.CallOption
}

func (kvc *kvClient) Get(ctx context.Context, in *pb.GetRequest, opts ...grpc.CallOption) (*pb.GetResponse, error) {
	return kvc.remote.Get(ctx, in, opts...)
}

func (kvc *kvClient) Put(ctx context.Context, in *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutResponse, error) {
	return kvc.remote.Put(ctx, in, opts...)
}

func (kvc *kvClient) Delete(ctx context.Context, in *pb.DeleteRequest, opts ...grpc.CallOption) (*pb.DeleteResponse, error) {
	return kvc.remote.Delete(ctx, in, opts...)
}

func NewKVClient(c *Client) pb.KVClient {
	return &kvClient{
		remote: pb.NewKVClient(c.conn),
	}
}
