package view

import (
	"encoding/binary"
	"sync"

	"github.com/defbin/esdb"
)

type StateView interface {
	Sync() error
	Get(key interface{}) (interface{}, error)
}

type ApplyContext interface {
	Get(key interface{}) (interface{}, error)
	Set(key, value interface{}) error
	Delete(key interface{}) error
}

type DB interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
}

type StateViewConfig struct {
	Store       DB
	NewIterator func(esdb.Offset) esdb.Iterator
	Apply       func(ApplyContext, []byte) error
	KeyCodec    Codec
	ValueCodec  Codec
}

type stateViewImpl struct {
	cfg *StateViewConfig
	m   sync.Mutex
}

func NewStateView(cfg StateViewConfig) StateView {
	return &stateViewImpl{cfg: &cfg}
}

func (s *stateViewImpl) Get(key interface{}) (interface{}, error) {
	if err := s.Sync(); err != nil {
		return nil, err
	}

	rawKey, err := s.cfg.KeyCodec.Encode(key)
	if err != nil {
		return nil, err
	}

	rawValue, err := s.cfg.Store.Get(rawKey)
	if err != nil {
		return nil, err
	}

	value, err := s.cfg.ValueCodec.Decode(rawValue)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (s *stateViewImpl) Sync() error {
	s.m.Lock()
	defer s.m.Unlock()

	offset, err := s.getOffset()
	if err != nil {
		return err
	}

	it := s.cfg.NewIterator(offset)
	defer it.Release()

	for it.Next() {
		// todo apply context
		err := s.cfg.Apply(nil, it.Value())
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
	raw, err := s.cfg.Store.Get([]byte("offset"))
	if err != nil {
		return 0, err
	}

	return esdb.Offset(uint32FromBinary(raw)), nil
}

func (s *stateViewImpl) setOffset(offset esdb.Offset) error {
	raw := binaryFromUint32(uint32(offset))
	return s.cfg.Store.Set([]byte("offset"), raw)
}

// todo(binaryFromUint32) move out
func binaryFromUint32(offset uint32) []byte {
	var arr [4]byte
	binary.BigEndian.PutUint32(arr[:], offset)
	return arr[:]
}

// todo(uint32FromBinary) move out
func uint32FromBinary(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}
