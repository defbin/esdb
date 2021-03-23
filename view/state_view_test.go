package view

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/defbin/esdb"
)

type testEvent struct {
	Id  string
	Str string
	Num int
}

type testEntity struct {
	Id  string
	Str string
	Num int
}

type stringCodec struct{}

func (stringCodec) Encode(value interface{}) (data []byte, err error) {
	return []byte(value.(string)), nil
}

func (stringCodec) Decode(data []byte) (value interface{}, err error) {
	return string(data), nil
}

type memo struct {
	s map[string][]byte
}

func newMemo() *memo {
	return &memo{make(map[string][]byte)}
}

func (m *memo) Get(key []byte) ([]byte, error) {
	data, ok := m.s[string(key)]
	if !ok {
		return nil, ErrDBNotFound
	}

	return data, nil
}

func (m *memo) Set(key, value []byte) error {
	m.s[string(key)] = value
	return nil
}

func (m *memo) Delete(key []byte) error {
	delete(m.s, string(key))
	return nil
}

func TestCreateEntity(t *testing.T) {
	ns := []byte("a")

	keyCodec := NewMustCodec(stringCodec{})
	eventCodec := NewJSONCodec(&testEvent{})
	entityCodec := NewJSONCodec(&testEntity{})

	es := newTestEventStore(t)
	sv := NewStateView(&StateViewConfig{
		Store: newMemo(),
		NewIterator: func(o esdb.Offset) esdb.Iterator {
			return es.NewIterator(ns, o)
		},
		Apply: applyTestEvent(eventCodec, entityCodec),
	})

	event := &testEvent{
		Id:  "id01",
		Str: "str02",
		Num: 3,
	}

	val, err := sv.Get(keyCodec.MustEncode(event.Id))
	if err != nil && err != ErrDBNotFound {
		t.Fatal(err)
	}

	if val != nil {
		t.Fatalf("want: nil, got: %#v", val)
	}

	d, err := eventCodec.Encode(event)
	if err != nil {
		t.Fatal(err)
	}
	_, err = es.Put(ns, d)
	if err != nil {
		t.Fatal(err)
	}

	d, err = sv.Get(keyCodec.MustEncode(event.Id))
	if err != nil && err != ErrDBNotFound {
		t.Fatal(err)
	}

	got, err := entityCodec.Decode(d)
	if err != nil {
		t.Fatal(err)
	}

	want := &testEntity{
		Id:  event.Id,
		Str: event.Str,
		Num: event.Num,
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want: %#v, got: %#v", want, got)
	}
}

func applyTestEvent(eventCodec, entityCodec Codec) func(ApplyContext, []byte) error {
	return func(ctx ApplyContext, data []byte) error {
		val, err := eventCodec.Decode(data)
		if err != nil {
			return err
		}

		event := val.(*testEvent)
		entity := &testEntity{
			Id:  event.Id,
			Str: event.Str,
			Num: event.Num,
		}
		d, err := entityCodec.Encode(entity)
		if err != nil {
			return err
		}

		return ctx.Set([]byte(event.Id), d)
	}
}

func newTestEventStore(t *testing.T) esdb.EventStore {
	t.Helper()

	path, err := ioutil.TempDir("", "test_*")
	if err != nil {
		t.Fatalf("unable to create tmp dir: %v", err)
	}

	t.Cleanup(func() {
		if err := os.RemoveAll(path); err != nil {
			t.Error(err)
		}
	})

	es, err := esdb.NewEventStore(path)
	if err != nil {
		t.Fatal(err)
	}

	return es
}
