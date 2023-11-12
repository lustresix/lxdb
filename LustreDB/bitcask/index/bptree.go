package index

import (
	"LustreDB/bitcask/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// B+ 树索引保存到磁盘中

type BPTree struct {
	tree *bbolt.DB
}

func (bpt *BPTree) Close() error {
	return bpt.tree.Close()
}

func NewBPTree(dir string, syncWrites bool) *BPTree {
	options := bbolt.DefaultOptions
	options.NoSync = syncWrites
	open, err := bbolt.Open(filepath.Join(dir, bptreeIndexFileName), 0644, options)
	if err != nil {
		panic("failed to open bPlusTree")
	}

	if err := open.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to creat bucket in bptree")
	}

	return &BPTree{
		tree: open,
	}

}

// Put 向索引中存储 key 对应的数据位置的信息
func (bpt *BPTree) Put(key []byte, pos *data.LogRecordPos) bool {
	err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		err := bucket.Put(key, data.EncodeLogRecordPos(pos))
		return err
	})
	return err == nil
}

// Get 根据 key 值取出对应的索引信息
func (bpt *BPTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	_ = bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		get := bucket.Get(key)
		if len(get) > 0 {
			pos = data.DecodeLogRecordPos(get)
		}
		return nil
	})
	return pos

}

// Delete 根据 key 值删除对应的索引
func (bpt *BPTree) Delete(key []byte) bool {
	err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		err := bucket.Delete(key)
		return err
	})
	return err == nil
}

// Size 返回大小
func (bpt *BPTree) Size() int {
	var size int
	err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	})
	if err != nil {
		return 0
	}
	return size
}

// Iterator 返回迭代器
func (bpt *BPTree) Iterator(reverse bool) Iterator {
	return nil
}

// bptIterator 索引迭代器
type bptIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBPTreeIterator(tree *bbolt.DB, reverse bool) *bptIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpti := &bptIterator{
		tx:      tx,
		cursor:  tx.Cursor(),
		reverse: reverse,
	}
	bpti.Rewind()
	return bpti
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (bpti *bptIterator) Rewind() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Last()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.First()
	}
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (bpti *bptIterator) Seek(key []byte) {
	bpti.currKey, bpti.currValue = bpti.cursor.Seek(key)
}

// Next 跳转到下一个 key
func (bpti *bptIterator) Next() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Prev()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.Next()
	}
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (bpti *bptIterator) Valid() bool {
	return len(bpti.currKey) != 0
}

// Key 当前遍历位置的 Key 数据
func (bpti *bptIterator) Key() []byte {
	return bpti.currKey
}

// Value 当前遍历位置的 Value 数据
func (bpti *bptIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpti.currValue)
}

// Close 关闭迭代器，释放相应资源
func (bpti *bptIterator) Close() {
	_ = bpti.tx.Rollback()
}
