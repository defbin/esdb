package esdb

import (
	"encoding/binary"
	"math"
	"time"

	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	offsetSize = 4
	timeSize   = 8
)

const eventKeyPrefix = "esdb~event/"

func formatKey(key []byte, offset Offset, t time.Time) []byte {
	buf := make([]byte, len(eventKeyPrefix)+len(key)+offsetSize+timeSize)
	n := copy(buf, eventKeyPrefix)
	n += copy(buf[n:], key)
	n += copy(buf[n:], binaryFromUint32(uint32(offset)))
	copy(buf[n:], binaryFromTime(t))
	return buf
}

func parseKey(data []byte) (key []byte, offset Offset, timestamp time.Time) {
	length := len(data)
	key = data[len(eventKeyPrefix) : length-(offsetSize+timeSize)]
	offset = Offset(uint32FromBinary(data[length-(offsetSize+timeSize) : length-timeSize]))
	timestamp = timeFromBinary(data[length-timeSize:])
	return
}

func sinceRange(key []byte, offset Offset) *util.Range {
	start := formatKey(key, offset, time.Time{})
	limit := formatKey(key, math.MaxUint32, time.Unix(0, math.MaxInt64))

	return &util.Range{Start: start, Limit: limit}
}

func binaryFromUint32(offset uint32) []byte {
	var arr [offsetSize]byte
	binary.BigEndian.PutUint32(arr[:], offset)
	return arr[:]
}

func uint32FromBinary(data []byte) uint32 {
	return binary.BigEndian.Uint32(data)
}

func binaryFromTime(t time.Time) []byte {
	return binaryFromUint32(uint32(t.UnixNano()))
}

func timeFromBinary(data []byte) time.Time {
	d := uint32FromBinary(data)
	t := time.Unix(0, int64(d))
	return t
}
