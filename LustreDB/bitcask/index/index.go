package index

import (
	"LustreDB/bitcask/data"
	"bytes"
	"github.com/google/btree"
)

type IndexerType = int8

const (
	// Btree 索引
	Btree IndexerType = iota + 1

	// ART 自适应基数树
	ART
)

// Indexer 内存设计，抽象索引接口，包括 PUT,GET,DELETE方法
// Indexer Abstract index interface, including PUT, GET, and DELETE methods
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置的信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据 key 值取出对应的索引信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 值删除对应的索引
	Delete(key []byte) bool
}

func NewIndexer(indexType IndexerType) Indexer {
	switch indexType {
	case Btree:
		return NewBtree()
	case ART:
		// TODO:ART
		return nil
	default:
		panic("unsupported index type")
	}

}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less tests whether the current item is less than the given argument.
// If !a.Less(b) && !b.Less(a), we treat this to mean a == b
func (it *Item) Less(bi btree.Item) bool {
	return bytes.Compare(it.key, bi.(*Item).key) == -1
}
