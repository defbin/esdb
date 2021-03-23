package view

import (
	"errors"

	"github.com/defbin/esdb"
)

var ErrDBNotFound = errors.New("esdb: view: not found")

type NewIterator func(esdb.Offset) esdb.Iterator

type ApplyFunc func(ApplyContext, []byte) error

type StateViewConfig struct {
	Store       DB
	NewIterator NewIterator
	Apply       ApplyFunc
}

type StateView interface {
	Get(key []byte) ([]byte, error)
}

type ApplyContext interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
}

type DB interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
}
