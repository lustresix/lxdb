package main

import (
	"LustreDB/bitcask"
	"fmt"
	"github.com/gin-gonic/gin"
	"os"
)

var db *bitcask.DB

func init() {
	var err error
	options := bitcask.DefaultOptions
	dir, err := os.MkdirTemp("", "bitcask-go-http")
	options.DirPath = dir
	db, err = bitcask.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}

}

func main() {
	router := gin.Default()

	InitializeRoutes(router)

	err := router.Run(":8080")
	if err != nil {
		return
	}
}
