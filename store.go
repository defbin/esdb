package esdb

import (
	"fmt"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

type eventStoreImpl struct {
	db *leveldb.DB
	m  sync.Mutex
}

func NewEventStore(path string) (EventStore, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create event store: %v", err)
	}

	return &eventStoreImpl{db: db}, nil
}

func (s *eventStoreImpl) Put(ns, data []byte) (Offset, error) {
	s.m.Lock()
	defer s.m.Unlock()

	nextOffset, err := s.nextOffset(ns)
	if err != nil {
		return 0, err
	}

	now := time.Now()
	key := formatKey(ns, nextOffset, now)

	if err := s.db.Put(key, data, nil); err != nil {
		return 0, err
	}

	return nextOffset, nil
}

func (s *eventStoreImpl) NewIterator(ns []byte, offset Offset) Iterator {
	return newIterator(s.db.NewIterator(sinceRange(ns, offset), nil))
}

func (s *eventStoreImpl) Close() error {
	return s.db.Close()
}

func (s *eventStoreImpl) nextOffset(ns []byte) (Offset, error) {
	it := s.db.NewIterator(sinceRange(ns, 0), nil)
	if !it.Last() {
		if err := it.Error(); err != nil {
			return 0, it.Error()
		}

		return 0, nil
	}

	_, offset, _ := parseKey(it.Key())
	return offset + 1, nil
}
