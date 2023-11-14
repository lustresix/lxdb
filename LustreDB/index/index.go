package index

import (
	"LustreDB/data"
	"bytes"
	"github.com/google/btree"
)

type IndexerType = int8

const (
	// Btree 索引
	Btree IndexerType = iota + 1

	// ART 自适应基数树
	ART

	BPtree
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

	// Iterator 返回迭代器
	Iterator(reverse bool) Iterator

	// Close 关闭迭代器
	Close() error

	Size() int
}

func NewIndexer(indexType IndexerType, dir string, syncWrite bool) Indexer {
	switch indexType {
	case Btree:
		return NewBtree()
	case ART:
		return NewArt()
	case BPtree:
		return NewBPTree(dir, syncWrite)
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

// Iterator 索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相应资源
	Close()
}
