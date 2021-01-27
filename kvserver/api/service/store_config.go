package service

import (
	"github.com/tecbot/gorocksdb"
)

const (
	DefaultRateBytesPerSec = 1024
	DefaultRefillPeriodUs = 100 * 1000
	DefaultFairness = 10
)

type StoreConfig struct {
	DataDir   string

	opts      *gorocksdb.Options

	ro *gorocksdb.ReadOptions
	wo *gorocksdb.WriteOptions
	to *gorocksdb.TransactionOptions

	applyOpts func(opts *gorocksdb.Options)

}
