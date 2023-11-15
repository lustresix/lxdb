package redis

import (
	LustreDB "github.com/lustresix/lxdb"
	"github.com/lustresix/lxdb/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := LustreDB.DefaultOptions
	temp, _ := os.MkdirTemp("", "bitcask-go-redis")
	opts.DirPath = temp

	redis, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	defer LustreDB.DestroyDB(redis.db)

	err = redis.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = redis.Set(utils.GetTestKey(2), time.Second+5, utils.RandomValue(100))
	assert.Nil(t, err)

	get, err := redis.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, get)

	time.Sleep(time.Second + 6)
	get, err = redis.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Nil(t, get)
}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := LustreDB.DefaultOptions
	temp, _ := os.MkdirTemp("", "bitcask-go-redis")
	opts.DirPath = temp

	redis, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	defer LustreDB.DestroyDB(redis.db)

	hset, err := redis.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, hset)

	hset, err = redis.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.False(t, hset)

	get, err := redis.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.NotNil(t, get)
}
