package bitcask

import (
	"LustreDB/bitcask/data"
	"sync"
)

// WriteBatch 原子性
type WriteBatch struct {
	lo *sync.Mutex
	db *DB

	pendingWrites map[string]*data.LogRecord
}
