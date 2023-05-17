package io

// DataFilePerm 用户具有读写权限，组用户和其它用户具有只读权限
const DataFilePerm = 0644

// ManagerIO 抽象 IO 管理接口
type ManagerIO interface {
	// Read 从给定的位置读取数据
	Read([]byte, int64)

	// Write 写入字节数组到文件中去
	Write([]byte)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error
}
