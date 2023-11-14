package main

import (
	"LustreDB"
	lxrd "LustreDB/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

// D:\phpstudy_pro\Extensions\redis3.0.504

type BitcaskSever struct {
	dbs   map[int]*lxrd.RedisDataStructure
	sever *redcon.Server
	lo    sync.RWMutex
}

func main() {
	// 打开Redis 数据服务
	redisDataStructure, err := lxrd.NewRedisDataStructure(LustreDB.DefaultOptions)
	if err != nil {
		panic(err)
	}
	// 初始化 server
	bitcaskServer := &BitcaskSever{
		dbs: make(map[int]*lxrd.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	bitcaskServer.sever = redcon.NewServer(addr, handler, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()
}

func (svr *BitcaskSever) listen() {
	log.Println("bitcask server running, ready to accept connection")
	_ = svr.sever.ListenAndServe()
}

func (svr *BitcaskSever) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.lo.Lock()
	defer svr.lo.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskSever) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.sever.Close()
	log.Println("good bye!")
}
