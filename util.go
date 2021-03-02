package esdb

import (
	"bytes"
	"encoding/binary"
	"time"
)

const (
	offsetSize = 8
	timeSize   = 8
)

const (
	eventKeyPrefix  = "esdb~event\x00"
	offsetKeyPrefix = "esdb~offset\x00"
)

func formatKey(key string, offset uint64, t time.Time) []byte {
	buf := bytes.NewBufferString(eventKeyPrefix)
	buf.WriteString(key)
	buf.Write(binaryFromUint64(offset))
	buf.Write(binaryFromTime(t))
	return buf.Bytes()
}

func parseKey(data []byte) (entity string, offset uint64, timestamp time.Time) {
	key := data[len(eventKeyPrefix):]
	entity = string(key[:len(key)-(offsetSize+timeSize)])
	offset = uint64FromBinary(key[len(entity) : offsetSize+timeSize])
	timestamp = timeFromBinary(key[len(entity)+offsetSize:])
	return
}

func formatOffsetKey(key string, t time.Time) []byte {
	buf := bytes.NewBufferString(offsetKeyPrefix)
	buf.WriteString(key)
	buf.Write(binaryFromTime(t))
	return buf.Bytes()
}

func parseOffsetKey(data []byte) (entity string, timestamp time.Time) {
	key := data[len(offsetKeyPrefix):]
	entity = string(key[:len(key)-timeSize])
	timestamp = timeFromBinary(key[len(entity):])
	return
}

func binaryFromUint64(offset uint64) []byte {
	var arr [offsetSize]byte
	binary.BigEndian.PutUint64(arr[:], offset)
	return arr[:]
}

func uint64FromBinary(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func binaryFromTime(t time.Time) []byte {
	return binaryFromUint64(uint64(t.UnixNano()))
}

func timeFromBinary(data []byte) time.Time {
	return time.Unix(0, int64(uint64FromBinary(data)))
}
