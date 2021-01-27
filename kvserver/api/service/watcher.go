package service

type watcher struct {
	// the watcher key
	target []byte
	id     WatchID
	ch     chan <- WatchResponse
}

func (w *watcher) send(wr WatchResponse) bool {
	select {
	case w.ch <- wr:
		return true
	default:
		return false
	}
}
