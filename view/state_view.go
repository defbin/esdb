package view

import (
	"strconv"
	"sync"

	"github.com/defbin/esdb"
)

type stateViewImpl struct {
	db DB
	it NewIterator
	ap ApplyFunc
	m  sync.Mutex
}

func NewStateView(cfg *StateViewConfig) StateView {
	return &stateViewImpl{
		db: cfg.Store,
		it: cfg.NewIterator,
		ap: cfg.Apply,
	}
}

func (s *stateViewImpl) Get(key []byte) ([]byte, error) {
	if err := s.sync(); err != nil {
		return nil, err
	}

	return s.db.Get(key)
}

func (s *stateViewImpl) sync() error {
	s.m.Lock()
	defer s.m.Unlock()

	offset, err := s.getOffset()
	if err != nil && err != ErrDBNotFound {
		return err
	}

	it := s.it(offset)
	defer it.Release()

	for it.Next() {
		ctx := newApplyContext(s.db)
		err := s.ap(ctx, it.Value())
		if err != nil {
			return err
		}
	}

	if err := it.Error(); err != nil {
		return err
	}

	return nil
}

func (s *stateViewImpl) getOffset() (esdb.Offset, error) {
	raw, err := s.db.Get([]byte("offset"))
	if err != nil {
		return 0, err
	}
	offset, err := strconv.ParseUint(string(raw), 10, 32)
	if err != nil {
		return 0, err
	}

	return esdb.Offset(offset), nil
}

func (s *stateViewImpl) setOffset(offset esdb.Offset) error {
	raw := strconv.FormatUint(uint64(offset), 10)
	return s.db.Set([]byte("offset"), []byte(raw))
}
