package benchmark

import (
	LustreDB "github.com/lustresix/lxdb"
	"github.com/lustresix/lxdb/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var db *LustreDB.DB

func init() {
	// 初始化存储引擎
	option := LustreDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-bench")
	option.DirPath = dir

	var err error
	db, err = LustreDB.Open(option)
	if err != nil {
		return
	}
}

func Benchmark_Put(b *testing.B) {
	// 时间
	b.ResetTimer()
	// 内存分配情况
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}
func Benchmark_Get(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
	// 时间
	b.ResetTimer()
	// 内存分配情况
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(i))
		assert.Nil(b, err)
	}
}
