package service

import (
	"errors"
	pb "sonekKV/kvserver/kvserverpb"
	"sync"
)

var (
	ErrWatcherNotExist    = errors.New("kv_store: watcher does not exist")
	ErrEmptyWatcher       = errors.New("kv_store: watcher range is empty")
	ErrWatcherDuplicateID = errors.New("kv_store: duplicate watch ID provided on the WatchStream")
	chanBufLen            = 128

)


type WatchID int64
const AutoWatchID WatchID = 0

type WatchStream interface {
	Watch(id WatchID, target []byte) (WatchID, error)
	Chan() <-chan WatchResponse
	Cancel(id WatchID) error
	Close()
}
type WatchResponse struct {
	WatchID WatchID
	Event   pb.Event
}

// watchStream contains a collection of watchers that share
// one streaming chan to send out watched events and other control events.
type watchStream struct {
	watchable watchable

	ch        chan WatchResponse
	mu sync.Mutex
	// nextID is the ID pre-allocated for next new watcher in this stream
	nextID   WatchID
	closed   bool
	cancels  map[WatchID]cancelFunc
	watchers map[WatchID]*watcher
}



// Watch creates a new watcher in the stream and returns its WatchID.
func (ws *watchStream) Watch(id WatchID, target []byte) (WatchID, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.closed {
		return -1, ErrEmptyWatcher
	}
	if id == AutoWatchID {
		for ws.watchers[ws.nextID] != nil {
			ws.nextID++
		}
		id = ws.nextID
		ws.nextID++
	} else if _, ok := ws.watchers[id]; ok {
		return -1, ErrWatcherDuplicateID
	}

	w, c := ws.watchable.watch(target, id, ws.ch)
	ws.cancels[id] = c
	ws.watchers[id] = w
	return id, nil
}

func (ws *watchStream) Chan() <-chan WatchResponse {
	return ws.ch
}
func (ws *watchStream) Cancel(id WatchID) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	cancel, ok := ws.cancels[id]
	w := ws.watchers[id]
	ok = ok && !ws.closed
	if !ok {
		return ErrWatcherNotExist
	}
	cancel()

	if ww := ws.watchers[id]; ww == w {
		delete(ws.cancels, id)
		delete(ws.watchers, id)
	}
	ws.mu.Unlock()

	return nil
}

func (ws *watchStream) Close() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	for _, cancel := range ws.cancels {
		cancel()
	}
	ws.closed = true
	close(ws.ch)
}
