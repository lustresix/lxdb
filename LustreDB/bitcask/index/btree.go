package index

import (
	"LustreDB/bitcask/data"
	"github.com/google/btree"
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
	it := Item{
		key: key,
		pos: pos,
	}
	// Lock before storage
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(&it)
	return true
}

// Get 根据 key 值取出对应的索引信息
// retrieves the corresponding index information based on the key value
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := Item{
		key: key,
	}
	get := bt.tree.Get(&it)
	if get != nil {
		return get.(*Item).pos
	}
	return nil
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
