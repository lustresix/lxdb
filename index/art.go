package index

import (
	"bytes"
	"github.com/lustresix/lxdb/data"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree 封装https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// NewArt 初始化索引
func NewArt() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

// Put 向索引中存储 key 对应的数据位置的信息
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	art.tree.Insert(key, pos)
	return true
}

// Get 根据 key 值取出对应的索引信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

// Delete 根据 key 值删除对应的索引
func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	_, deleted := art.tree.Delete(key)
	return deleted
}

// Size 返回大小
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	size := art.tree.Size()
	return size
}

// Iterator 返回迭代器
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree, reverse)
}

// ArtIterator 索引迭代器
type ArtIterator struct {
	currIndex int
	reverse   bool
	values    []*Item
}

func newArtIterator(tree goart.Tree, reverse bool) *ArtIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)
	return &ArtIterator{
		values:    values,
		reverse:   reverse,
		currIndex: 0,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (arti *ArtIterator) Rewind() {
	arti.currIndex = 0
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (arti *ArtIterator) Seek(key []byte) {
	if arti.reverse {
		// 二分查找
		sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0
		})
	} else {
		// 相反
		sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (arti *ArtIterator) Next() {
	arti.currIndex++
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (arti *ArtIterator) Valid() bool {
	return arti.currIndex < len(arti.values)
}

// Key 当前遍历位置的 Key 数据
func (arti *ArtIterator) Key() []byte {
	return arti.values[arti.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (arti *ArtIterator) Value() *data.LogRecordPos {
	return arti.values[arti.currIndex].pos
}

// Close 关闭迭代器，释放相应资源
func (arti *ArtIterator) Close() {
	arti.values = nil
}
