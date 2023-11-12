package index

import (
	"LustreDB/bitcask/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

// BTree 来自 google 的 btree https://github.com/google/btree
// BTree from Google's BTree https://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	// " Write operations are not safe for concurrent mutation by multiple
	// goroutines, but Read operations are." So we need add a lock to protect it
	lock *sync.RWMutex
}

func (bt *BTree) Close() error {
	return nil
}

// NewBtree 初始化 BTree 索引
// initializes BTree index
func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

// Put 向索引中存储 key 对应的数据位置的信息
// stores information about the data location corresponding to the key in the index
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{
		key: key,
		pos: pos,
	}
	// Lock before storage
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(it)
	return true
}

// Get 根据 key 值取出对应的索引信息
// retrieves the corresponding index information based on the key value
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{
		key: key,
	}
	get := bt.tree.Get(it)
	if get == nil {
		return nil
	}
	return get.(*Item).pos
}

// Delete 根据 key 值删除对应的索引
// deletes the corresponding index based on the key value
func (bt *BTree) Delete(key []byte) bool {
	it := Item{
		key: key,
	}
	bt.lock.Lock()
	del := bt.tree.Delete(&it)
	bt.lock.Unlock()
	if del != nil {
		return true
	}
	return false
}

func (bt *BTree) Size() int {
	size := bt.tree.Len()
	return size
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// BTree 索引迭代器
type btreeIterator struct {
	currIndex int
	reverse   bool
	values    []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 返回false就终止排序
	saveValues := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}
	// 如果数据reverse就是从小到大开始排序
	if reverse {
		tree.Descend(saveValues)
	}
	tree.Ascend(saveValues)

	return &btreeIterator{
		values:    values,
		reverse:   reverse,
		currIndex: 0,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		// 二分查找
		sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		// 相反
		sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (bti *btreeIterator) Next() {
	bti.currIndex++
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// Key 当前遍历位置的 Key 数据
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

// Close 关闭迭代器，释放相应资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}
