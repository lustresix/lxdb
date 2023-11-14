package LustreDB

import (
	utils2 "LustreDB/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewWriteBatch(t *testing.T) {
	// 初始化数据库
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer DestroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据不提交
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)

	err = wb.Put(utils2.GetTestKey(1), utils2.RandomValue(10))
	assert.Nil(t, err)

	err = wb.Delete(utils2.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils2.GetTestKey(1))
	assert.Equal(t, utils2.ErrKeyNotFound, err)

	// 正常提交
	err = wb.Commit()
	assert.Nil(t, err)

	bytes, err := db.Get(utils2.GetTestKey(1))
	assert.NotNil(t, bytes)
	assert.Nil(t, err)

	// 删除有效数据
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils2.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	bytes1, err := db.Get(utils2.GetTestKey(1))
	t.Log(bytes1)
	t.Log(err)
}

func TestDB_NewWriteBatch1(t *testing.T) {
	// 初始化数据库
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer DestroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils2.GetTestKey(1), utils2.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils2.GetTestKey(2), utils2.RandomValue(10))
	assert.Nil(t, err)

	err = wb.Delete(utils2.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db, err = Open(opts)
	assert.Nil(t, err)

	get, err := db.Get(utils2.GetTestKey(1))
	t.Log(get)
	t.Log(err)
}
