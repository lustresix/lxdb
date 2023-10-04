package index

import (
	"LustreDB/bitcask/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBtree()

	put := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, put)

	put1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    3,
		Offset: 4,
	})
	assert.True(t, put1)

}

func TestBTree_Get(t *testing.T) {
	bt := NewBtree()

	put := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, put)

	get := bt.Get(nil)
	assert.Equal(t, int64(100), get.Offset)
	assert.Equal(t, uint32(1), get.Fid)

	put1 := bt.Put([]byte("lex"), &data.LogRecordPos{
		Fid:    3,
		Offset: 4,
	})
	assert.True(t, put1)
	get1 := bt.Get([]byte("lex"))
	t.Log(get1)
}

func TestBtreeIterator_Close(t *testing.T) {
	bt := NewBtree()
	_ = bt.Put([]byte("code"), &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})

	iterator := bt.Iterator(false)
	assert.True(t, iterator.Valid())
}
