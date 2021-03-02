package esdb

import (
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type iteratorImpl struct {
	iter iterator.Iterator
}

func newIterator(db *leveldb.DB, slice *util.Range) Iterator {
	return &iteratorImpl{db.NewIterator(slice, nil)}
}

func (it *iteratorImpl) Next() bool {
	return it.iter.Next()
}

func (it *iteratorImpl) Value() (uint64, time.Time, []byte) {
	_, offset, timestamp := parseKey(it.iter.Key())
	return offset, timestamp, it.iter.Value()
}

func (it *iteratorImpl) Release() {
	it.iter.Release()
}

func (it *iteratorImpl) Error() error {
	return it.iter.Error()
}
