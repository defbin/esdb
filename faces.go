package esdb

import "time"

type EventStore interface {
	Write(ns string, data []byte) (uint64, error)
	Iterator(ns string, start uint64) Iterator
}

type Iterator interface {
	Next() bool
	Value() (uint64, time.Time, []byte)
	Release()
	Error() error
}

type StateView interface {
	Query(key string) (interface{}, error)
}
