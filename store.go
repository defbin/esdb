package esdb

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var ErrNoOffset = errors.New("no offset")

type eventStoreImpl struct {
	db *leveldb.DB
	m  sync.Mutex
}

func NewEventStore(db *leveldb.DB, namespace string) EventStore {
	return &eventStoreImpl{db: db}
}

func (s *eventStoreImpl) Write(ns string, data []byte) (uint64, error) {
	s.m.Lock()
	defer s.m.Unlock()

	offset, err := s.nextOffset(ns)
	if err != nil {
		return 0, err
	}

	now := time.Now()
	batch := &leveldb.Batch{}
	batch.Put(formatKey(ns, offset, now), data)
	batch.Put(formatOffsetKey(ns, now), binaryFromUint64(offset))
	if err := s.db.Write(batch, nil); err != nil {
		return 0, err
	}

	return offset, nil
}

func (s *eventStoreImpl) Iterator(ns string, offset uint64) Iterator {
	slice := &util.Range{
		Start: formatKey(ns, offset, time.Time{}),
		Limit: formatKey(ns, math.MaxUint64, time.Now()),
	}
	return newIterator(s.db, slice)
}

func (s *eventStoreImpl) nextOffset(ns string) (uint64, error) {
	it := s.db.NewIterator(util.BytesPrefix(formatOffsetKey(ns, time.Time{})), nil)
	if !it.Last() {
		if err := it.Error(); err != nil {
			return 0, it.Error()
		}

		return 0, nil
	}

	return uint64FromBinary(it.Value()) + 1, nil
}
