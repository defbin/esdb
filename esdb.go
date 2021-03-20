package esdb

import "time"

type Offset uint32

type EventStore interface {
	Put(key, data []byte) (Offset, error)
	NewIterator(key []byte, start Offset) Iterator
	Close() error
}

type Iterator interface {
	Next() bool
	Key() ([]byte, Offset, time.Time)
	Value() []byte
	Release()
	Error() error
}
