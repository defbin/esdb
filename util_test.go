package esdb

import (
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb/comparer"
)

var compare = comparer.DefaultComparer.Compare

func TestNumberKeyOrder(t *testing.T) {
	now := time.Time{}
	want := -1
	for i := uint64(0); i < 20; i++ {
		k1 := formatKey("", i, now)
		k2 := formatKey("", i+1, now)
		got := compare(k1, k2)
		if got != want {
			t.Errorf("want: %v, got: %v", want, got)
		}
	}
}

func TestStringLengthKeyOrder(t *testing.T) {
	want := -1
	got := compare([]byte(""), []byte("\x00"))
	if got != want {
		t.Errorf("want: %v, got: %v", want, got)
	}
}
