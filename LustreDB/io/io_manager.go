package io

// DataFilePerm 用户具有读写权限，组用户和其它用户具有只读权限
const DataFilePerm = 0644

// IOManager 磁盘设计，抽象 IO 管理接口
type IOManager interface {
	// Read 从给定的位置读取数据
	Read([]byte, int64) (int, error)

	// Write 写入字节数组到文件中去
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取文件的大小
	Size() (int64, error)
}

func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
