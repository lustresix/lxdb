package LustreDB

import (
	"github.com/lustresix/lxdb/utils"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_Close(t *testing.T) {
	options := DefaultOptions
	options.DirPath = "tmp"
	lx, err := Open(options)
	assert.Nil(t, err)
	err = lx.Put(utils.GetTestKey(1), utils.RandomValue(12))
	assert.Nil(t, err)
	lx2, err := Open(options)
	get, err := lx2.Get(utils.GetTestKey(1))
	t.Log(string(get))
	assert.Nil(t, err)
}
