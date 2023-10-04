package main

import (
	"LustreDB/bitcask"
	"fmt"
)

func main() {
	options := bitcask.DefaultOptions
	options.DirPath = "tmp"
	open, err := bitcask.Open(options)
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
	fmt.Print("val = ", string(get))

	err = open.Delete([]byte("hello"))
	if err != nil {
		panic(err)
	}
}
