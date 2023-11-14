package index

import (
	"LustreDB/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestNewBPTree(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	_ = NewBPTree(path, false)

}

func TestBPTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPTree(path, false)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	tree.Put([]byte("key2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	tree.Put([]byte("key3"), &data.LogRecordPos{Fid: 1, Offset: 12})
}

func TestBPTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPTree(path, false)

	get := tree.Get([]byte("key0"))
	assert.Nil(t, get)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	tree.Put([]byte("key2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	tree.Put([]byte("key3"), &data.LogRecordPos{Fid: 1, Offset: 12})

	get = tree.Get([]byte("key1"))
	assert.NotNil(t, get)
}

func TestBPTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	_ = os.MkdirAll(path, os.ModePerm)
	t.Log(path)
	defer func() {
		_ = os.Remove(path)
	}()
	tree := NewBPTree(path, false)

	tree.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 12})

	get := tree.Get([]byte("key1"))
	assert.NotNil(t, get)

	tree.Delete([]byte("key1"))
	get = tree.Get([]byte("key1"))
	assert.Nil(t, get)
}
