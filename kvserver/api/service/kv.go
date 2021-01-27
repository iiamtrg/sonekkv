package service

import pb "sonekKV/kvserver/kvserverpb"

type TxnRead interface {
	Get(key []byte) (r *pb.KeyValue, err error)
}

type TxnWrite interface {
	Put(key, value []byte) error
	Delete(key []byte) error
	Change() (r *pb.KeyValue)
}

type WatchableKV interface {
	KV
	Watchable
	End()
}

type KV interface {
	TxnRead
	TxnWrite

	Read()	TxnRead
	Write() TxnWrite
	Close()
}
type Watchable interface {
	NewWatchStream() WatchStream
}