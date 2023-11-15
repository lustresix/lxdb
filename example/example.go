package main

import (
	"fmt"
	LustreDB "github.com/lustresix/lxdb"
)

func main() {
	options := LustreDB.DefaultOptions
	options.DirPath = "tmp"
	open, err := LustreDB.Open(options)
	if err != nil {
		panic(err)
	}

	err = open.Put([]byte("hello"), []byte("world1"))
	if err != nil {
		panic(err)
	}

	get, err := open.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(get))

	err = open.Delete([]byte("hello"))
	if err != nil {
		panic(err)
	}
	batchOptions := LustreDB.DefaultWriteBatchOptions
	wb := open.NewWriteBatch(batchOptions)
	_ = wb.Put([]byte("hello1"), []byte("world1"))
	_ = wb.Delete([]byte("hello1"))
	err = wb.Commit()
	if err != nil {
		return
	}
}
