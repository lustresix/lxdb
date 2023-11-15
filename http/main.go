package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	LustreDB "github.com/lustresix/lxdb"
	"os"
)

var db *LustreDB.DB

func init() {
	var err error
	options := LustreDB.DefaultOptions
	dir, err := os.MkdirTemp("", "bitcask-go-http")
	options.DirPath = dir
	db, err = LustreDB.Open(options)
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
