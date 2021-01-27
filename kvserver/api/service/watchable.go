package service

import (
	pb "sonekKV/kvserver/kvserverpb"
	"sync"
	"time"
)

type watchable interface {
	watch(target []byte, id WatchID, ch chan<- WatchResponse) (*watcher, cancelFunc)
}

type watchableStore struct {
	*store
	keyWatchers watcherSetByKey
	mu          *sync.RWMutex
	wg          sync.WaitGroup
}

func NewWatchableStore(s *store) *watchableStore {
	ws := &watchableStore{
		store: s,
		keyWatchers: make(watcherSetByKey),
		mu: new(sync.RWMutex),
	}
	ws.TxnRead = ws.store.Read()
	ws.TxnWrite = ws.store.Write()
	return ws
}

func (was *watchableStore) NewWatchStream() WatchStream {
	return &watchStream{
		watchable: was,
		ch:        make(chan WatchResponse, chanBufLen),
		watchers:  make(map[WatchID]*watcher),
		cancels:  make(map[WatchID]cancelFunc),
	}
}
type cancelFunc func()

func (ws *watchableStore) watch(target []byte, id WatchID, ch chan <- WatchResponse) (*watcher, cancelFunc) {
	wa := &watcher{
		target: target,
		id: id,
		ch: ch,
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.keyWatchers.add(wa)
	return wa, func() { ws.cancelWatcher(wa) }
}

// cancelWatcher removes references of the watcher from the watchableStore
func (s *watchableStore) cancelWatcher(wa *watcher) {
	for {
		s.mu.Lock()
		if s.keyWatchers.delete(wa) {
			break
		} else if wa.ch == nil {
			break
		}
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
	}

	wa.ch = nil
	s.mu.Unlock()
}

func (was *watchableStore) End() {
	change := was.Change()
	if change == nil {
		return
	}

	evt := new(pb.Event)
	evt.Kv = change
	if change.NumberOfMod == 0 {
		evt.Type = pb.Event_DELETE
	} else {
		evt.Type = pb.Event_PUT
	}
	was.mu.Lock()
	was.notify(evt)
	was.mu.Unlock()
}


func (s *watchableStore) notify(evt *pb.Event) {
	for w, eb := range newWatcherBatch(s.keyWatchers, evt) {
		w.send(WatchResponse{WatchID: w.id, Event: *eb})
	}
}
func newWatcherBatch(wk watcherSetByKey, ev *pb.Event) watcherBatch {
	wb := make(watcherBatch)
	for w := range wk[string(ev.Kv.Key)] {
		wb.add(w, ev)
	}
	return wb
}

type watcherSetByKey map[string]watcherSet
type watcherSet map[*watcher]struct{}

func (w watcherSetByKey) add(wa *watcher) {
	set := w[string(wa.target)]
	if set == nil {
		set = make(watcherSet)
		w[string(wa.target)] = set
	}
	set.add(wa)
}

func (w watcherSetByKey) delete(wa *watcher) bool {
	k := string(wa.target)
	if v, ok := w[k]; ok {
		if _, ok := v[wa]; ok {
			delete(v, wa)
			if len(v) == 0 {
				delete(w, k)
			}
			return true
		}
	}
	return false
}

func (w watcherSet) add(wa *watcher) {
	if _, ok := w[wa]; !ok {
		w[wa] = struct{}{}
	}
}

type watcherBatch map[*watcher]*pb.Event

func (wb watcherBatch) add(w *watcher, ev *pb.Event) {
	eb := wb[w]
	if eb == nil {
		wb[w] = ev
	}
}
