package data

import (
	"github.com/stretchr/testify/assert"
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
