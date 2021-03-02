package esdb

import (
	"bytes"
	"testing"
	"testing/quick"
	"time"

	"github.com/syndtr/goleveldb/leveldb/comparer"
)

func TestSimpleStringLengthOrder(t *testing.T) {
	t.Parallel()

	k1 := []byte("")
	k2 := []byte(" ")
	got := comparer.DefaultComparer.Compare(k1, k2)
	if got != -1 {
		t.Errorf("want: -1, got: %v", got)
	}
}

func TestKeyOrder(t *testing.T) {
	t.Parallel()

	check := func(a, b Offset, x, y int64) bool {
		if a > b {
			a, b = b, a
		}

		t1, t2 := time.Unix(0, x), time.Unix(0, y)
		if a == b && t1.After(t2) {
			t1, t2 = t2, t1
		}

		k1 := formatKey([]byte("a"), a, t1)
		k2 := formatKey([]byte("a"), b, t2)
		got := comparer.DefaultComparer.Compare(k1, k2)
		if got == 1 {
			t.Errorf("not (%#v < %#v)", k1, k2)
			return false
		}

		return true
	}

	if err := quick.Check(check, nil); err != nil {
		t.Error(err)
	}
}

func TestEventKeyZeroDuality(t *testing.T) {
	t.Parallel()

	key, offset, nSec := []byte{}, Offset(0), int64(0)
	timestamp := time.Unix(0, nSec)
	result := formatKey(key, offset, timestamp)
	key0, offset0, timestamp0 := parseKey(result)

	if !bytes.Equal(key0, key) {
		t.Errorf("want %v, got %v", key, key0)
	}
	if offset0 != offset {
		t.Errorf("want %v, got %v", offset, key0)
	}
	if !timestamp0.Equal(timestamp) {
		t.Errorf("want %v, got %v", timestamp.UnixNano(), timestamp0.UnixNano())
	}
}

func TestEventKeyDuality(t *testing.T) {
	t.Parallel()

	check := func(key string, offset Offset, nSec uint32) bool {
		timestamp := time.Unix(0, int64(nSec))
		result := formatKey([]byte(key), offset, timestamp)
		key0, offset0, timestamp0 := parseKey(result)

		if !bytes.Equal(key0, []byte(key)) {
			t.Errorf("want %v, got %v", key, key0)
			return false
		}
		if offset0 != offset {
			t.Errorf("want %v, got %v", offset, key0)
			return false
		}
		if !timestamp0.Equal(timestamp) {
			t.Errorf("want %v, got %v", timestamp.UnixNano(), timestamp0.UnixNano())
			return false
		}

		return true
	}

	if err := quick.Check(check, nil); err != nil {
		t.Fatal(err)
	}
}
