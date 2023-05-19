package data

import (
	"LustreDB/bitcask/io"
	"fmt"
	io2 "io"
	"path/filepath"
)

const DataFileNameSuffix = ".lx"

// DataFile 数据文件
type DataFile struct {

	// 文件id
	FileId uint32

	// 偏移地址
	WriteOff int64

	// io 读写管理
	IOManager io.IOManager
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// 地址/fileId.lx
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	// 初始化 IOManager 管理器接口
	manager, err := io.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IOManager: manager,
	}, nil
}

// 根据 offset 从文件中读取数据 LogRecord
func (df *DataFile) Read(offset int64) (*LogRecord, int64, error) {

	// 读取 Hear 部分的数据
	b, err := df.readNBytes(maxLogRecordHeaderSize, offset)
	if err != nil {
		return nil, 0, err
	}

	// 解码
	header, h := decodeLogRecordHeader(b)
	if header == nil {
		return nil, 0, io2.EOF
	}
	// 这里是如果读到了文件的末尾那么返回一个 EOF 错误
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, err
	}

	// 获取 key 和 value 的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var headerSize = keySize + valueSize + h

	record := &LogRecord{
		Type: header.recordType,
	}

	// 如果 key 或者 value 存在值，那么解码获取实际值
	if keySize > 0 || valueSize > 0 {
		bytes, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		record.Key = bytes[:keySize]
		record.Value = bytes[keySize:]

		// 校验 crc 是否正确
	}

	return nil, 0, nil
}

func (df *DataFile) Write(buf []byte) error {

	return nil
}

// Sync 持久化数据到磁盘
func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

// 指定读多少个字节，从而调用 ioManager 来读取数据
func (df *DataFile) readNBytes(n, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	if err != nil {
		return nil, err
	}
	return
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}
