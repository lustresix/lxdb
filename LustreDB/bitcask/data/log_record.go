package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// crc = 4  type = 1 keySize = 5 valueSize = 5 total = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type logRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
// Data memory index, describing the location of data on disk
type LogRecordPos struct {

	// 文件 id，表示数据存储到哪个文件中
	Fid uint32

	// 偏移量，表示数据存储到了数据文件的哪个位置
	Offset int64
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
func EncodeLogRecord(LogRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCrc(log *LogRecord, header []byte) uint32 {
	return 0
}
