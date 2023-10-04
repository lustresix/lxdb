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
	defer destroyDB(open)
	assert.Nil(t, err)
	assert.NotNil(t, open)

	iterator := open.NewIterator(DefaultIteratorOption)
	assert.NotNil(t, iterator)
	t.Log(iterator.Valid())
}

func destroyDB(db *DB) {
	if db != nil {
		if db.activeFiles != nil {
			_ = db.activeFiles.Close() //todo After implementing the Close method, use the Close method instead
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}
