package bitcask

import (
	"LustreDB/bitcask/index"
	"os"
)

// Options 用户可选的配置项
type Options struct {
	// 数据库数据目录
	DirPath string

	// 数据文件的大小
	DataFileSize int64

	// 是否决定持久化
	SyncWrites bool

	// 索引类型
	IndexType index.IndexerType
}

type IteratorOptions struct {
	// 遍历前缀为指定的key，默认为空
	Prefix []byte

	// 是否反向遍历，false为正常遍历
	Reverse bool
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint
	// 提交后是否要持久化
	SyncWrite bool
}

type IndexerType int8

const (
	BTree IndexerType = iota + 1
)

var DefaultOptions = Options{
	DirPath: os.TempDir(),
	// 256MB
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    index.Btree,
}

var DefaultIteratorOption = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
