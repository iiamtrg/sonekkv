package client

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	pb "sonekKV/kvserver/kvserverpb"
	"sync"
	"time"
)

const (
	closeSendErrTimeout = 250 * time.Millisecond
)
type WatchChan <- chan WatchResponse

type Watcher interface {
	Watch(ctx context.Context, target string) WatchChan
	Close() error
}
// watchStreamRequest is a union of the supported watch request operation types
type watchStreamRequest interface {
	toPB() *pb.WatchRequest
}

type WatchResponse struct {
	WatchID int64
	Event *pb.Event
	Canceled bool
	Created bool

	closeErr error
}

type watcher struct {
	remote pb.WatchClient

	callOpts []grpc.CallOption

	mu sync.RWMutex

	streams map[string]*watchGrpcStream
	lg      *zap.Logger

}

// watchGrpcStream tracks all watch resources attached to a single grpc stream.
type watchGrpcStream struct {
	owner    *watcher
	remote   pb.WatchClient
	callOpts []grpc.CallOption

	ctx context.Context

	ctxKey string

	cancel context.CancelFunc

	// substreams holds all active watchers on this grpc stream
	substreams map[int64]*watcherStream
	// resuming holds all resuming watchers on this grpc stream
	resuming []*watcherStream

	// reqc sends a watch request from Watch() to the main goroutine
	reqc chan watchStreamRequest
	// respc receives data from the watch client
	respc chan *pb.WatchResponse
	// donec closes to broadcast shutdown
	donec chan struct{}
	// errc transmits errors from grpc Recv to the watch stream reconnect logic
	errc chan error
	// closingc gets the watcherStream of closing watchers
	closingc chan *watcherStream
	// wg is Done when all substream goroutines have exited
	wg sync.WaitGroup

	// resumec closes to signal that all substreams should begin resuming
	resumec chan struct{}
	// closeErr is the error that closed the watch stream
	closeErr error

	lg *zap.Logger
}

func (w *watcher) Close() (err error) {
	w.mu.Lock()
	streams := w.streams
	w.streams = nil
	w.mu.Unlock()
	for _, wgs := range streams {
		if werr := wgs.close(); werr != nil {
			err = werr
		}
	}
	// Consider context.Canceled as a successful close
	if err == context.Canceled {
		err = nil
	}
	return err
}


func (w *watchGrpcStream) close() (err error) {
	w.cancel()
	<-w.donec
	select {
	case err = <-w.errc:
	default:
	}
	return err
}


func (w *watcher) closeStream(wgs *watchGrpcStream) {
	w.mu.Lock()
	close(wgs.donec)
	wgs.cancel()
	if w.streams != nil {
		delete(w.streams, wgs.ctxKey)
	}
	w.mu.Unlock()
}
func (w *watchGrpcStream) addSubstream(resp *pb.WatchResponse, ws *watcherStream) {
	if resp.WatchId == -1{
		w.closeErr = ErrNoAvailableEndpoint
		close(ws.recvc)
		return
	}
	ws.id = resp.WatchId
	w.substreams[ws.id] = ws
}

// nextResume chooses the next resuming to register with the grpc stream. Abandoned
// streams are marked as nil in the queue since the head must wait for its inflight registration.
func (w *watchGrpcStream) nextResume() *watcherStream {
	for len(w.resuming) != 0 {
		if w.resuming[0] != nil {
			return w.resuming[0]
		}
		w.resuming = w.resuming[1:len(w.resuming)]
	}
	return nil
}

// dispatchEvent sends a WatchResponse to the appropriate watcher stream
func (w *watchGrpcStream) dispatchEvent(pbresp *pb.WatchResponse) bool {
	// TODO: return watch ID?
	wr := &WatchResponse{
		Event: pbresp.Event,
		Canceled: pbresp.Canceled,
		Created: pbresp.Created,
	}

	return w.unicastResponse(wr, pbresp.WatchId)
}

// IsProgressNotify returns true if the WatchResponse is progress notification.
func (wr *WatchResponse) IsProgressNotify() bool {
	return wr.Event == nil && !wr.Canceled && !wr.Created
}

// unicastResponse sends a watch response to a specific watch substream.
func (w *watchGrpcStream) unicastResponse(wr *WatchResponse, watchId int64) bool {
	ws, ok := w.substreams[watchId]
	if !ok {
		return false
	}
	select {
	case ws.recvc <- wr:
	case <-ws.donec:
		return false
	}
	return true
}


func (w *watcher) Watch(ctx context.Context, target string) WatchChan {
	wr := &watchRequest{
		ctx: ctx,
		target: target,
		retc:           make(chan chan WatchResponse, 1),
	}
	ok := false
	ctxKey := streamKeyFromCtx(ctx)
	w.mu.Lock()
	if w.streams == nil {
		w.mu.Unlock()
		ch := make(chan WatchResponse)
		close(ch)
		return ch
	}
	wgs := w.streams[ctxKey]
	if wgs == nil {
		wgs = w.newWatcherGrpcStream(ctx)
		w.streams[ctxKey] = wgs
	}
	donec := wgs.donec
	reqc := wgs.reqc
	w.mu.Unlock()

	closeCh := make(chan WatchResponse, 1)

	closeCh <- WatchResponse{}

	select {
	case reqc <- wr:
		ok = true
	case <-wr.ctx.Done():
	case <-donec:
		if wgs.closeErr != nil {
			closeCh <- WatchResponse{Canceled: true, closeErr: wgs.closeErr}
			break
		}
		return w.Watch(ctx, target)
	}
	// receive channel
	if ok {
		select {
		case ret := <-wr.retc:
			return ret
		case <-ctx.Done():
		case <-donec:
			if wgs.closeErr != nil {
				closeCh <- WatchResponse{Canceled: true, closeErr: wgs.closeErr}
				break
			}
			// retry
			return w.Watch(ctx, target)
		}
	}

	close(closeCh)
	return closeCh
}

func (w *watcher) newWatcherGrpcStream(inctx context.Context) *watchGrpcStream {
	ctx, cancel := context.WithCancel(inctx)
	wgs := &watchGrpcStream{
		owner:      w,
		remote:     w.remote,
		callOpts:   w.callOpts,
		ctx:        ctx,
		ctxKey: streamKeyFromCtx(ctx),
		cancel:     cancel,
		substreams: make(map[int64]*watcherStream),
		respc:      make(chan *pb.WatchResponse),
		reqc:       make(chan watchStreamRequest),
		donec:      make(chan struct{}),
		errc:       make(chan error, 1),
		closingc:   make(chan *watcherStream),
		resumec:    make(chan struct{}),
		lg:         w.lg,
	}
	go wgs.run()
	return wgs
}


// run is the root of the goroutines for managing a watcher client
func (w *watchGrpcStream) run() {
	var wc pb.Watch_WatchClient
	var closeErr error

	// substreams marked to close but goroutine still running; needed for
	// avoiding double-closing recvc on grpc stream teardown
	closing := make(map[*watcherStream]struct{})
	defer func() {
		w.closeErr = closeErr
		// shutdown substreams and resuming substreams
		for _, ws := range w.substreams {
			if _, ok := closing[ws]; !ok {
				close(ws.recvc)
				closing[ws] = struct{}{}
			}
		}
		for _, ws := range w.resuming {
			if _, ok := closing[ws]; ws != nil && !ok {
				close(ws.recvc)
				closing[ws] = struct{}{}
			}
		}
		w.joinSubstreams()
		for range closing {
			w.closeSubstream(<-w.closingc)
		}
		w.wg.Wait()
		w.owner.closeStream(w)
	}()

	// start a stream with the etcd grpc server
	if wc, closeErr = w.newWatchClient(); closeErr != nil {
		return
	}

	cancelSet := make(map[int64]struct{})

	var cur *pb.WatchResponse
	for {
		select {
		// Watch() requested
		case req := <-w.reqc:
			switch wreq := req.(type) {
			case *watchRequest:
				outc := make(chan WatchResponse, 1)
				// TODO: pass custom watch ID?
				ws := &watcherStream{
					initReq: *wreq,
					id:      -1,
					outc:    outc,
					// unbuffered so resumes won't cause repeat events
					recvc: make(chan *WatchResponse),
				}
				ws.donec = make(chan struct{})
				w.wg.Add(1)
				go w.serveSubstream(ws, w.resumec)
				// queue up for watcher creation/resume
				w.resuming = append(w.resuming, ws)
				if len(w.resuming) == 1 {
					// head of resume queue, can register a new watcher
					if err := wc.Send(ws.initReq.toPB()); err != nil {
						if w.lg != nil {
							w.lg.Debug("error when sending request", zap.Error(err))
						}
					}
				}
			}

		// new events from the watch client
		case pbresp := <-w.respc:
			if cur == nil || pbresp.Created || pbresp.Canceled {
				cur = pbresp
			} else if cur != nil && cur.WatchId == pbresp.WatchId {
				// merge new events
				cur.Event = pbresp.Event
			}
			switch {
			case pbresp.Created:
				// response to head of queue creation
				if ws := w.resuming[0]; ws != nil {
					w.addSubstream(pbresp, ws)
					w.dispatchEvent(pbresp)
					w.resuming[0] = nil
				}
				if ws := w.nextResume(); ws != nil {
					if err := wc.Send(ws.initReq.toPB()); err != nil {
						if w.lg != nil {
							w.lg.Debug("error when sending request", zap.Error(err))
						}
					}
				}
				// reset for next iteration
				cur = nil
			default:
				// dispatch to appropriate watch stream
				ok := w.dispatchEvent(cur)

				// reset for next iteration
				cur = nil

				if ok {
					break
				}

				// watch response on unexpected watch id; cancel id
				if _, ok := cancelSet[pbresp.WatchId]; ok {
					break
				}

				cancelSet[pbresp.WatchId] = struct{}{}
				cr := &pb.WatchRequest_CancelRequest{
					CancelRequest: &pb.WatchCancelRequest{
						WatchId: pbresp.WatchId,
					},
				}
				req := &pb.WatchRequest{RequestUnion: cr}
				if w.lg != nil {
					w.lg.Debug("sending watch cancel request for failed dispatch", zap.Int64("watch-id", pbresp.WatchId))
				}
				if err := wc.Send(req); err != nil {
					if w.lg != nil {
						w.lg.Debug("failed to send watch cancel request", zap.Int64("watch-id", pbresp.WatchId), zap.Error(err))
					}
				}
			}
		// watch client failed on Recv; spawn another if possible
		case err := <-w.errc:
			if isHaltErr(w.ctx, err) {
				closeErr = err
				return
			}
			if wc, closeErr = w.newWatchClient(); closeErr != nil {
				return
			}
			if ws := w.nextResume(); ws != nil {
				if err := wc.Send(ws.initReq.toPB()); err != nil {
					if w.lg != nil {
						w.lg.Debug("error when sending request", zap.Error(err))
					}
				}
			}
			cancelSet = make(map[int64]struct{})

		case <-w.ctx.Done():
			return

		case ws := <-w.closingc:
			if ws.id != -1 {
				// client is closing an established watch; close it on the server proactively instead of waiting
				// to close when the next message arrives
				cancelSet[ws.id] = struct{}{}
				cr := &pb.WatchRequest_CancelRequest{
					CancelRequest: &pb.WatchCancelRequest{
						WatchId: ws.id,
					},
				}
				req := &pb.WatchRequest{RequestUnion: cr}
				if w.lg != nil {
					w.lg.Debug("sending watch cancel request for closed watcher", zap.Int64("watch-id", ws.id))
				}
				if err := wc.Send(req); err != nil {
					if w.lg != nil {
						w.lg.Debug("failed to send watch cancel request", zap.Int64("watch-id", ws.id), zap.Error(err))
					}
				}
			}
			w.closeSubstream(ws)
			delete(closing, ws)
			// no more watchers on this stream, shutdown
			if len(w.substreams)+len(w.resuming) == 0 {
				return
			}
		}
	}
}


// serveWatchClient forwards messages from the grpc stream to run()
func (w *watchGrpcStream) serveWatchClient(wc pb.Watch_WatchClient) {
	for {
		resp, err := wc.Recv()
		if err != nil {
			select {
			case w.errc <- err:
			case <-w.donec:
			}
			return
		}
		select {
		case w.respc <- resp:
		case <-w.donec:
			return
		}
	}
}

// serveSubstream forwards watch responses from run() to the subscriber
func (w *watchGrpcStream) serveSubstream(ws *watcherStream, resumec chan struct{}) {
	if ws.closing {
		panic("created substream goroutine but substream is closing")
	}
	// nextRev is the minimum expected next revision
	resuming := false
	defer func() {
		if !resuming {
			ws.closing = true
		}
		close(ws.donec)
		if !resuming {
			w.closingc <- ws
		}
		w.wg.Done()
	}()

	emptyWr := &WatchResponse{}
	for {
		curWr := emptyWr
		outc := ws.outc

		if len(ws.buf) > 0 {
			curWr = ws.buf[0]
		} else {
			outc = nil
		}
		select {
		case outc <- *curWr:
			ws.buf[0] = nil
			ws.buf = ws.buf[1:]
		case wr, ok := <-ws.recvc:
			if !ok {
				// shutdown from closeSubstream
				return
			}
			if wr.Created {
				if ws.initReq.retc != nil {
					ws.initReq.retc <- ws.outc
					// to prevent next write from taking the slot in buffered channel
					// and posting duplicate create events
					ws.initReq.retc = nil
				}
			}
			// TODO pause channel if buffer gets too large
			ws.buf = append(ws.buf, wr)
		case <-w.ctx.Done():
			return
		case <-ws.initReq.ctx.Done():
			return
		case <-resumec:
			resuming = true
			return
		}
	}
}

func (w *watchGrpcStream) newWatchClient() (pb.Watch_WatchClient, error) {
	// mark all substreams as resuming
	close(w.resumec)
	w.resumec = make(chan struct{})
	w.joinSubstreams()
	for _, ws := range w.substreams {
		ws.id = -1
		w.resuming = append(w.resuming, ws)
	}
	// strip out nils, if any
	var resuming []*watcherStream
	for _, ws := range w.resuming {
		if ws != nil {
			resuming = append(resuming, ws)
		}
	}
	w.resuming = resuming
	w.substreams = make(map[int64]*watcherStream)

	// connect to grpc stream while accepting watcher cancelation
	stopc := make(chan struct{})
	donec := w.waitCancelSubstreams(stopc)
	wc, err := w.openWatchClient()
	close(stopc)
	<-donec
	// serve all non-closing streams, even if there's a client error
	// so that the teardown path can shutdown the streams as expected.
	for _, ws := range w.resuming {
		if ws.closing {
			continue
		}
		ws.donec = make(chan struct{})
		w.wg.Add(1)
		go w.serveSubstream(ws, w.resumec)
	}

	if err != nil {
		return nil, err
	}

	// receive data from new grpc stream
	go w.serveWatchClient(wc)
	return wc, nil
}

func (w *watchGrpcStream) waitCancelSubstreams(stopc <-chan struct{}) <-chan struct{} {
	var wg sync.WaitGroup
	wg.Add(len(w.resuming))
	donec := make(chan struct{})
	for i := range w.resuming {
		go func(ws *watcherStream) {
			defer wg.Done()
			if ws.closing {
				if ws.initReq.ctx.Err() != nil && ws.outc != nil {
					close(ws.outc)
					ws.outc = nil
				}
				return
			}
			select {
			case <-ws.initReq.ctx.Done():
				// closed ws will be removed from resuming
				ws.closing = true
				close(ws.outc)
				ws.outc = nil
				w.wg.Add(1)
				go func() {
					defer w.wg.Done()
					w.closingc <- ws
				}()
			case <-stopc:
			}
		}(w.resuming[i])
	}
	go func() {
		defer close(donec)
		wg.Wait()
	}()
	return donec
}

// joinSubstreams waits for all substream goroutines to complete.
func (w *watchGrpcStream) joinSubstreams() {
	for _, ws := range w.substreams {
		<-ws.donec
	}
	for _, ws := range w.resuming {
		if ws != nil {
			<-ws.donec
		}
	}
}

func (w *watchGrpcStream) closeSubstream(ws *watcherStream) {
	// send channel response in case stream was never established
	select {
	case ws.initReq.retc <- ws.outc:
	default:
	}
	// close subscriber's channel
	if closeErr := w.closeErr; closeErr != nil && ws.initReq.ctx.Err() == nil {
		go w.sendCloseSubstream(ws, &WatchResponse{Canceled: true, closeErr: w.closeErr})
	} else if ws.outc != nil {
		close(ws.outc)
	}
	if ws.id != -1 {
		delete(w.substreams, ws.id)
		return
	}
	for i := range w.resuming {
		if w.resuming[i] == ws {
			w.resuming[i] = nil
			return
		}
	}
}


func (w *watchGrpcStream) sendCloseSubstream(ws *watcherStream, resp *WatchResponse) {
	select {
	case ws.outc <- *resp:
	case <-ws.initReq.ctx.Done():
	}
	close(ws.outc)
}



// watchRequest is issued by the subscriber to start a new watcher
type watchRequest struct {
	ctx context.Context
	target string
	// retc receives a chan WatchResponse once the watcher is established
	retc chan chan WatchResponse
}

var maxBackoff = 100 * time.Millisecond

func (w *watchGrpcStream) openWatchClient() (ws pb.Watch_WatchClient, err error) {
	backoff := time.Millisecond
	for {
		select {
		case <-w.ctx.Done():
			if err == nil {
				return nil, w.ctx.Err()
			}
			return nil, err
		default:
		}
		if ws, err = w.remote.Watch(w.ctx, w.callOpts...); ws != nil && err == nil {
			break
		}
		if isHaltErr(w.ctx, err) {
			return nil, err
		}
		if isUnavailableErr(w.ctx, err) {
			// retry, but backoff
			if backoff < maxBackoff {
				// 25% backoff factor
				backoff = backoff + backoff/4
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			time.Sleep(backoff)
		}
	}
	return ws, nil
}

// toPB converts an internal watch request structure to its protobuf WatchRequest structure.
func (wr *watchRequest) toPB() *pb.WatchRequest {
	req := &pb.WatchCreateRequest{
		Key:            []byte(wr.target),
	}
	cr := &pb.WatchRequest_CreateRequest{CreateRequest: req}
	return &pb.WatchRequest{RequestUnion: cr}
}

// watcherStream represents a registered watcher
type watcherStream struct {
	// initReq is the request that initiated this request
	initReq watchRequest

	// outc publishes watch responses to subscriber
	outc chan WatchResponse
	// recvc buffers watch responses before publishing
	recvc chan *WatchResponse
	// donec closes when the watcherStream goroutine stops.
	donec chan struct{}
	// closing is set to true when stream should be scheduled to shutdown.
	closing bool
	// id is the registered watch id on the grpc stream
	id int64

	// buf holds all events received from etcd but not yet consumed by the client
	buf []*WatchResponse
}

func NewWatcher(c *Client) Watcher {
	return NewWatchFromWatchClient(pb.NewWatchClient(c.conn), c)
}
func NewWatchFromWatchClient(wc pb.WatchClient, c *Client) Watcher {
	w := &watcher{
		remote:  wc,
		streams: make(map[string]*watchGrpcStream),
	}
	if c != nil {
		w.callOpts = c.callOpts
		w.lg = c.lg
	}
	return w
}


func streamKeyFromCtx(ctx context.Context) string {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		return fmt.Sprintf("%+v", md)
	}
	return ""
}