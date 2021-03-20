package esdb

import (
	"time"

	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type iteratorImpl struct {
	iter iterator.Iterator
}

func newIterator(it iterator.Iterator) Iterator {
	return &iteratorImpl{it}
}

func (it *iteratorImpl) Next() bool {
	return it.iter.Next()
}

func (it *iteratorImpl) Key() ([]byte, Offset, time.Time) {
	return parseKey(it.iter.Key())
}

func (it *iteratorImpl) Value() []byte {
	// underlying byte slice can be reused and changed when Value() is called
	value := it.iter.Value()
	// make copy to keep original data
	return append([]byte{}, value...)
}

func (it *iteratorImpl) Release() {
	it.iter.Release()
}

func (it *iteratorImpl) Error() error {
	return it.iter.Error()
}
