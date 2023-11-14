package index

import (
	"LustreDB/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewArt()
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key4"), &data.LogRecordPos{Fid: 1, Offset: 12})
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewArt()
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	get := art.Get([]byte("key1"))
	assert.NotNil(t, get)

	get = art.Get([]byte("key2"))
	assert.Nil(t, get)

	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 10, Offset: 120})
	get = art.Get([]byte("key1"))
	t.Log(get)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewArt()
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	b := art.Delete([]byte("key1"))
	assert.True(t, b)

	b = art.Delete([]byte("key2"))
	assert.False(t, b)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewArt()
	size := art.Size()
	assert.Equal(t, size, 0)
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key4"), &data.LogRecordPos{Fid: 1, Offset: 12})
	size = art.Size()
	assert.Equal(t, size, 4)
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewArt()
	art.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key4"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iterator := art.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		assert.NotNil(t, iterator.Key())
		assert.NotNil(t, string(iterator.Key()))
	}
	iterator = art.Iterator(true)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		assert.NotNil(t, string(iterator.Key()))
	}

}
