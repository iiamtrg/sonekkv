package rpc

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"io"
	"sonekKV/kvserver"
	"sonekKV/kvserver/api/service"
	pb "sonekKV/kvserver/kvserverpb"
	"sync"
)

const ctrlStreamBufLen = 16
var (
	ErrGRPCContextCanceled = errors.New("GRPC context canceled")
)

type watchServer struct {
	lg *zap.Logger

	maxRequestBytes int

	watchable service.WatchableKV
}

// NewWatchServer returns a new watch server.
func NewWatchServer(s *kvserver.SonekKVServer) pb.WatchServer {
	srv := &watchServer{
		lg: s.Cfg.Logger,
		maxRequestBytes: int(s.Cfg.MaxRequestBytes + grpcOverheadBytes),
		watchable: s.Watchable(),
	}
	return srv
}

func (s *watchServer) KV() service.WatchableKV {
	return s.watchable
}

type serverWatchStream struct {
	lg *zap.Logger

	clusterID int64
	memberID  int64

	maxRequestBytes int

	watchable service.Watchable

	gRPCStream  pb.Watch_WatchServer
	watchStream service.WatchStream
	ctrlStream  chan *pb.WatchResponse

	// mu protects progress, prevKV, fragment
	mu sync.RWMutex

	// closec indicates the stream is closed.
	closec chan struct{}

	// wg waits for the send loop to complete
	wg sync.WaitGroup
}

func (ws *watchServer) Watch(stream pb.Watch_WatchServer) (err error) {
	sws := serverWatchStream{
		lg: ws.lg,

		maxRequestBytes: ws.maxRequestBytes,

		watchable: ws.watchable,

		gRPCStream:  stream,
		watchStream: ws.watchable.NewWatchStream(),

		ctrlStream: make(chan *pb.WatchResponse, ctrlStreamBufLen),

		closec: make(chan struct{}),
	}

	sws.wg.Add(1)
	go func() {
		sws.sendLoop()
		sws.wg.Done()
	}()

	errc := make(chan error, 1)

	go func() {
		if rerr := sws.recvLoop(); rerr != nil {
			sws.lg.Debug("failed to receive watch request from gRPC stream", zap.Error(rerr))
			errc <- rerr
		}
	}()

	select {
	case err = <-errc:
		close(sws.ctrlStream)
	case <-stream.Context().Done():
		err = stream.Context().Err()

		if err == context.Canceled {
			err = ErrGRPCContextCanceled
		}
	}
	sws.close()
	return err
}

func (sws *serverWatchStream) recvLoop() error {
	for {
		req, err := sws.gRPCStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch uv := req.RequestUnion.(type) {
		case *pb.WatchRequest_CreateRequest:
			if uv.CreateRequest == nil {
				break
			}

			creq := uv.CreateRequest
			if len(creq.Key) == 0 {
				// \x00 is the smallest key
				creq.Key = []byte{0}
			}

			id, err := sws.watchStream.Watch(service.WatchID(creq.WatchId), creq.Key)
			wr := &pb.WatchResponse{
				WatchId:  int64(id),
				Created:  true,
				Canceled: err != nil,
			}
			if err != nil {
				wr.CancelReason = err.Error()
			}
			select {
			case sws.ctrlStream <- wr:
			case <-sws.closec:
				return nil
			}
		case *pb.WatchRequest_CancelRequest:
			if uv.CancelRequest != nil {
				id := uv.CancelRequest.WatchId
				err := sws.watchStream.Cancel(service.WatchID(id))
				if err == nil {
					sws.ctrlStream <- &pb.WatchResponse{
						WatchId:  id,
						Canceled: true,
					}
				}
			}
		default:
			continue
		}
	}
}


func (sws *serverWatchStream) sendLoop() {
	// watch ids that are currently active
	ids := make(map[service.WatchID]struct{})
	// watch responses pending on a watch id creation message
	pending := make(map[service.WatchID][]*pb.WatchResponse)

	for {
		select {
		case wresp, ok := <-sws.watchStream.Chan():
			if !ok {
				return
			}
			evt := wresp.Event
			wr := &pb.WatchResponse{
				WatchId:         int64(wresp.WatchID),
				Event:           &evt,
				Canceled:        false,
			}

			if _, okID := ids[wresp.WatchID]; !okID {
				// buffer if id not yet announced
				wrs := append(pending[wresp.WatchID], wr)
				pending[wresp.WatchID] = wrs
				continue
			}

			serr := sws.gRPCStream.Send(wr)
			if serr != nil {
				sws.lg.Debug("failed to send watch response to gRPC stream", zap.Error(serr))
			}

		case c, ok := <-sws.ctrlStream:
			if !ok {
				return
			}

			if err := sws.gRPCStream.Send(c); err != nil {
				sws.lg.Debug("failed to send watch response to gRPC stream", zap.Error(err))
				return
			}

			// track id creation
			wid := service.WatchID(c.WatchId)
			if c.Canceled {
				delete(ids, wid)
				continue
			}
			if c.Created {
				// flush buffered events
				ids[wid] = struct{}{}
				for _, v := range pending[wid] {
					if err := sws.gRPCStream.Send(v); err != nil {
						sws.lg.Debug("failed to send pending watch response to gRPC stream", zap.Error(err))
						return
					}
				}
				delete(pending, wid)
			}
		case <-sws.closec:
			return
		}
	}
}

func (sws *serverWatchStream) close() {
	sws.watchStream.Close()
	close(sws.closec)
	sws.wg.Wait()
}
