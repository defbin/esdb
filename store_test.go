package esdb

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"testing/quick"
	"time"
)

const (
	testRecordCount = 10
	storeCount      = 5
	sinceOffset     = 2
)

func TestOffset(t *testing.T) {
	t.Parallel()

	const triesCount = testRecordCount
	es := newTestEventStore(t)
	defer es.Close()

	for i := Offset(0); i != triesCount; i++ {
		offset, err := es.Put(nil, nil)
		if err != nil {
			t.Fatalf("unable to put: %v", err)
		}

		if offset != i {
			t.Fatalf("offset: want %v, got %v", i, offset)
		}
	}
}

func TestWriteToSingleNamespace(t *testing.T) {
	t.Parallel()

	es := newTestEventStore(t)
	defer es.Close()

	sid := 0
	want := seedRandomData(t, es, 1)[0]
	got := fetchEventStoreData(t, es, strconv.Itoa(sid), 0)

	checkTestResult(t, testRecordCount, want, got)
}

func TestWriteToMultipleNamespaces(t *testing.T) {
	t.Parallel()

	es := newTestEventStore(t)
	defer es.Close()

	records := seedRandomData(t, es, storeCount)

	for n, want := range records {
		got := fetchEventStoreData(t, es, strconv.Itoa(n), 0)

		checkTestResult(t, testRecordCount, want, got)
	}
}

func TestIteratorSinceSingleNamespace(t *testing.T) {
	t.Parallel()

	es := newTestEventStore(t)
	defer es.Close()

	sid := 0
	want := seedRandomData(t, es, 1)[sid][sinceOffset:]
	got := fetchEventStoreData(t, es, strconv.Itoa(sid), sinceOffset)

	checkTestResult(t, testRecordCount-sinceOffset, want, got)
}

func TestIteratorSinceMultipleNamespaces(t *testing.T) {
	t.Parallel()

	es := newTestEventStore(t)
	defer es.Close()

	records := seedRandomData(t, es, storeCount)

	for sid := range records {
		want := records[sid][sinceOffset:]
		got := fetchEventStoreData(t, es, strconv.Itoa(sid), sinceOffset)

		checkTestResult(t, testRecordCount-sinceOffset, want, got)
	}
}

func fetchEventStoreData(t *testing.T, es EventStore, ns string, offset Offset) []testRecord {
	t.Helper()

	records := make([]testRecord, 0, testRecordCount-offset)

	it := es.NewIterator([]byte(ns), offset)
	defer it.Release()

	for i := 0; it.Next(); i++ {
		_, offset, _ := it.Key()
		data := it.Value()
		records = append(records, testRecord{offset: offset, data: data})
	}

	if err := it.Error(); err != nil {
		t.Fatalf("iteration failed: %v", err)
	}

	return records
}

func checkTestResult(t *testing.T, recCount int, want, got []testRecord) {
	t.Helper()

	if len(want) != recCount {
		t.Fatalf("not enough number of records: want %v, got %v", recCount, len(want))
	}

	if len(want) != len(got) {
		t.Fatalf("wrong number of records: want %v, got %v", len(want), len(got))
	}

	for i := 0; i != recCount; i++ {
		checkTestRecord(t, &want[i], &got[i])
	}

	return
}

func checkTestRecord(t *testing.T, want, got *testRecord) {
	t.Helper()

	if want.offset != got.offset {
		t.Errorf("offset: want %#v, got %#v", want.offset, got.offset)
	}
	if !bytes.Equal(want.data, got.data) {
		t.Errorf("data: want %#v, got %#v", want.data, got.data)
	}
}

func newTestEventStore(t *testing.T) EventStore {
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

	es, err := NewEventStore(path)
	if err != nil {
		t.Fatal(err)
	}

	return es
}

type testRecord struct {
	data   []byte
	offset Offset
}

func bytesGen() func() []byte {
	t := reflect.TypeOf([]byte{})
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	return func() []byte {
		value, _ := quick.Value(t, rnd)
		return value.Bytes()
	}
}

func seedRandomData(t *testing.T, es EventStore, storeCount int) [][]testRecord {
	t.Helper()

	gen := bytesGen()

	records := make([][]testRecord, storeCount)
	for i := range records {
		records[i] = make([]testRecord, testRecordCount)
	}

	for i := 0; i < testRecordCount; i++ {
		for sid, store := range records {
			data := gen()

			offset, err := es.Put([]byte(strconv.Itoa(sid)), data)
			if err != nil {
				t.Fatalf("unable to write data: %v", err)
			}

			store[i] = testRecord{data: data, offset: offset}
		}
	}

	return records
}
