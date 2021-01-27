package kvserver

import (
	"context"
	"sonekKV/kvserver/api/service"
	pb "sonekKV/kvserver/kvserverpb"
	"sync"
)

type SonekKVServer struct {
	Cfg     ServerConfig
	mu *sync.RWMutex
	kv service.WatchableKV

}

func NewServer(cfg ServerConfig) (srv *SonekKVServer, err error) {
	st, err := service.NewStore(service.StoreConfig{
		DataDir: cfg.DataDir,
	})
	if err != nil {
		return nil, err
	}
	srv = &SonekKVServer{
		Cfg: cfg,
		mu: new(sync.RWMutex),
	}
	srv.kv = service.NewWatchableStore(st)
	return
}

func (s *SonekKVServer) KV() service.WatchableKV { return s.kv }


func (s *SonekKVServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	var (
		err error
		resp = &pb.GetResponse{}
	)
	r, err := s.kv.Get(req.Key)
	if err != nil {
		return nil, err
	}
	resp.Kv = r
	return resp, nil
}

func (s *SonekKVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	var (
		err error
		resp = &pb.PutResponse{}
	)
	err = s.kv.Put(req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	defer s.kv.End()

	resp.Kv = &pb.KeyValue{
		Key: req.Key,
		Value: req.Value,
	}
	return resp, nil
}

func (s *SonekKVServer) Delete(ctx context.Context, r *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	var (
		err error
		resp = &pb.DeleteResponse{}
	)
	err = s.kv.Delete(r.Key)
	if err != nil {
		return nil, err
	}
	defer s.kv.End()
	resp.Kv = &pb.KeyValue{
		Key: r.Key,
		Value: nil,
	}
	return resp, nil
}

// Watchable returns a watchable interface attached to the etcdserver.
func (s *SonekKVServer) Watchable() service.WatchableKV { return s.KV() }


func (srv *SonekKVServer) Close() {
	if srv.kv != nil {
		srv.kv.Close()
	}
}


