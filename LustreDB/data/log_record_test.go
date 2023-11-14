package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	record := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("World"),
		Type:  LogRecordNormal,
	}
	logRecord, i := EncodeLogRecord(record)
	assert.Greater(t, i, int64(5))
	t.Log(logRecord, i)
}

func TestGetLogRecordCrc(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("World"),
		Type:  LogRecordNormal,
	}

	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc := GetLogRecordCrc(rec1, headerBuf1[crc32.Size:])
	t.Log(crc)
}

func TestDecodeLogRecordHeader(t *testing.T) {
	record := &LogRecord{
		Key:   []byte("hello"),
		Value: []byte("World"),
		Type:  LogRecordNormal,
	}
	logRecord, _ := EncodeLogRecord(record)
	header, _ := DecodeLogRecordHeader(logRecord)
	t.Log(header)
}
