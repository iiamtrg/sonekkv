package service

import (
	"errors"
	"github.com/tecbot/gorocksdb"
	pb "sonekKV/kvserver/kvserverpb"
)

var (
	ErrNoAvailableDataDir = errors.New("data-dir is required")
)


type store struct {

	TxnRead
	TxnWrite
	cfg StoreConfig

	db *gorocksdb.DB

	opts *gorocksdb.Options
	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
	to *gorocksdb.TransactionOptions

}
func (s *store) Close() {
	s.db.Close()
}

func NewStore(cfg StoreConfig) (*store, error) {
	if cfg.DataDir == "" {
		return nil, ErrNoAvailableDataDir
	}
	s := new(store)

	if cfg.opts == nil {
		opts := gorocksdb.NewDefaultOptions()
		rateLimiter := gorocksdb.NewRateLimiter(DefaultRateBytesPerSec, DefaultRefillPeriodUs, DefaultFairness)
		opts.SetRateLimiter(rateLimiter)
		opts.SetCreateIfMissing(true)
		cfg.opts = opts
	}
	s.opts = cfg.opts
	if cfg.applyOpts != nil {
		cfg.applyOpts(s.opts)
	}
	if s.ro == nil {
		s.ro = gorocksdb.NewDefaultReadOptions()
	}
	if s.wo == nil {
		s.wo = gorocksdb.NewDefaultWriteOptions()
	}

	db, err := gorocksdb.OpenDb(s.opts, cfg.DataDir)
	if err != nil {
		return nil, err
	}
	s.db = db
	return s, nil
}


type storeTxnRead struct {
	s *store
}

func (tr *storeTxnRead) Get(key []byte) (r *pb.KeyValue, err error) {
	return tr.get(key)
}

func (tr *storeTxnRead) get(key []byte) (kv *pb.KeyValue, err error) {
	val, err := tr.s.db.Get(tr.s.ro, key)
	if err != nil {
		return nil, err
	}
	defer val.Free()
	if val.Exists() {
		kv = new(pb.KeyValue)
		err = kv.Unmarshal(val.Data())
		if err != nil {
			return nil, err
		}
		return kv, nil
	}
	return nil, nil
}

func (s *store) Read() TxnRead {
	return &storeTxnRead{
		s,
	}
}

type storeTxnWrite struct {
	s *store
	change *pb.KeyValue
}

func (tw *storeTxnWrite) Put(key, value []byte) error {
	return tw.put(key, value)
}

func (tw *storeTxnWrite) put(key, value []byte) error {
	// if the key exists before
	val, err := tw.s.db.Get(tw.s.ro, key)
	if err != nil {
		return err
	}
	defer val.Free()
	kv := new(pb.KeyValue)
	if val.Exists(){
		err = kv.Unmarshal(val.Data())
		if err != nil {
			return err
		}
		kv.Value = value
		kv.NumberOfMod++
	} else {
		kv.Key = key
		kv.Value = value
		kv.NumberOfMod = 1
	}
	d, err := kv.Marshal()
	if err != nil {
		return err
	}
	err = tw.s.db.Put(tw.s.wo, key, d)
	if err != nil {
		return err
	}
	tw.change = kv
	return nil
}

func (tw *storeTxnWrite) Delete(key []byte) error {
	return tw.delete(key)
}

func (tw *storeTxnWrite) delete(key []byte) error {
	// if the key exists before
	val, err := tw.s.db.Get(tw.s.ro, key)
	if err != nil {
		return err
	}
	defer val.Free()
	kv := new(pb.KeyValue)
	if val.Exists(){
		err = kv.Unmarshal(val.Data())
		if err != nil {
			return err
		}
		err = tw.s.db.Delete(tw.s.wo, key)
		if err != nil {
			return err
		}
		kv.NumberOfMod = 0
		tw.change = kv
	}
	return nil
}

func (tw *storeTxnWrite) Change() (r *pb.KeyValue) {
	return tw.change
}

func (s *store) Write() TxnWrite {
	return &storeTxnWrite{
		s: s,
		change: nil,
	}
}



