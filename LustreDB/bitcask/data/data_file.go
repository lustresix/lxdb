package data

import "LustreDB/bitcask/io"

const DataFileNameSuffix = ".data"

// DataFile 数据文件
type DataFile struct {

	// 文件id
	FileId uint32

	// 偏移地址
	WriteOff int64

	// io 读写管理
	ManagerIO io.ManagerIO
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Read(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

// Sync 持久化数据到磁盘
func (df *DataFile) Sync() error {
	return nil
}
