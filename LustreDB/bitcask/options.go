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

var DefaultOptions = Options{
	DirPath: os.TempDir(),
	// 256MB
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    index.Btree,
}
