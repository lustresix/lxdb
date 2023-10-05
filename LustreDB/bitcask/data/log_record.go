package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
	LogRecordFinish
)

// crc = 4  type = 1 keySize = 5 valueSize = 5 total = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord 写入到数据文件的记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordHeader struct {
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

type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
func EncodeLogRecord(LogRecord *LogRecord) ([]byte, int64) {
	bytes := make([]byte, maxLogRecordHeaderSize)

	// 第四位的数是类型
	bytes[4] = LogRecord.Type
	var index = 5

	// keySize 和 valueSize 为变长 以此来节省空间
	// 写入 key 的长度
	index += binary.PutVarint(bytes[index:], int64(len(LogRecord.Key)))

	// 写入 value 的长度
	index += binary.PutVarint(bytes[index:], int64(len(LogRecord.Value)))

	// 总长度
	var size = index + len(LogRecord.Key) + len(LogRecord.Value)

	// 返回的数据
	finalByte := make([]byte, size)

	// 将数据复制到最终数据里
	copy(finalByte[:index], bytes[:index])
	copy(finalByte[index:], LogRecord.Key)
	copy(finalByte[index+len(LogRecord.Key):], LogRecord.Value)

	// 编码
	crc := crc32.ChecksumIEEE(finalByte[4:])
	binary.LittleEndian.PutUint32(finalByte[:4], crc)

	return finalByte, int64(size)
}

// DecodeLogRecordHeader 解码 返回值 头部信息 头部长度
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	// 如果比 crc + type 还小那么就有问题
	if len(buf) < 5 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var index = 5

	kSize, n := binary.Varint(buf[index:])
	header.keySize = uint32(kSize)
	index += n

	vSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(vSize)
	index += n

	return header, int64(index)
}

// GetLogRecordCrc 校验 crc
func GetLogRecordCrc(log *LogRecord, header []byte) uint32 {
	if log == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, log.Key)
	crc = crc32.Update(crc, crc32.IEEETable, log.Value)

	return crc
}
