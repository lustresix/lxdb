package bitcask

import (
	"LustreDB/bitcask/index"
	"bytes"
)

type Iterator struct {
	indexIter index.Iterator

	db *DB

	options IteratorOptions
}

func (db *DB) NewIterator(opt IteratorOptions) *Iterator {
	iterator := db.index.Iterator(opt.Reverse)
	return &Iterator{
		indexIter: iterator,
		db:        db,
		options:   opt,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (bti *Iterator) Rewind() {
	bti.indexIter.Rewind()
	bti.skipToNext()
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (bti *Iterator) Seek(key []byte) {
	bti.indexIter.Seek(key)
	bti.skipToNext()
}

// Next 跳转到下一个 key
func (bti *Iterator) Next() {
	bti.indexIter.Next()
	bti.skipToNext()
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (bti *Iterator) Valid() bool {
	return bti.indexIter.Valid()
}

// Key 当前遍历位置的 Key 数据
func (bti *Iterator) Key() []byte {
	return bti.indexIter.Key()
}

// Value 当前遍历位置的 Value 数据
func (bti *Iterator) Value() ([]byte, error) {
	value := bti.indexIter.Value()
	bti.db.lo.RLock()
	defer bti.db.lo.RUnlock()
	return bti.db.getValue(value)
}

// Close 关闭迭代器，释放相应资源
func (bti *Iterator) Close() {
	bti.indexIter.Close()
}

func (bti *Iterator) skipToNext() {
	i := len(bti.options.Prefix)

	// 如果i的长度为0那么用户就没有设置prefix
	if i == 0 {
		return
	}

	for ; bti.indexIter.Valid(); bti.indexIter.Next() {
		key := bti.indexIter.Key()
		if i <= len(key) && bytes.Compare(bti.options.Prefix, key[:i]) == 0 {
			break
		}
	}
}
