package bitcask

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-p")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	open, err := Open(opts)
	defer DestroyDB(open)
	assert.Nil(t, err)
	assert.NotNil(t, open)

	iterator := open.NewIterator(DefaultIteratorOption)
	assert.NotNil(t, iterator)
	t.Log(iterator.Valid())
}
