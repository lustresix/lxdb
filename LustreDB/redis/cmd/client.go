package main

import (
	lxrd "LustreDB/redis"
	"LustreDB/utils"
	"errors"
	"fmt"
	"github.com/tidwall/redcon"
	"strconv"
	"strings"
	"time"
)

func wrongNumOfCmd(cmd string) error {
	return fmt.Errorf("ERR wrong number of segments for %s command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCmd = map[string]cmdHandler{
	"quit":   nil,
	"ping":   nil,
	"set":    set,
	"get":    get,
	"del":    del,
	"ttl":    ttl,
	"append": appends,
}

type BitcaskClient struct {
	server *BitcaskSever
	db     *lxrd.RedisDataStructure
}

func handler(conn redcon.Conn, cmd redcon.Command) {
	lower := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCmd[lower]
	if !ok {
		conn.WriteError("Err unsupported this command:'" + lower + "'")
		return
	}

	client, _ := conn.Context().(*BitcaskClient)
	switch lower {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("pong")
	default:
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if errors.Is(err, utils.ErrKeyNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 && len(args) != 4 {
		fmt.Println(len(args))
		return nil, wrongNumOfCmd("set")
	}

	var expire time.Duration
	if len(args) == 4 {
		tim, err := strconv.Atoi(string(args[3]))
		if err != nil {
			return nil, wrongNumOfCmd("set")
		}
		switch string(args[2]) {
		case "ex":
			expire = time.Duration(tim) * time.Second
		case "xp":
			expire = time.Duration(tim) * time.Nanosecond
		default:
			return nil, wrongNumOfCmd("set")
		}
	}
	value, key := args[1], args[0]
	err := cli.db.Set(key, expire, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleString("ok"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, wrongNumOfCmd("get")
	}

	key := args[0]
	value, err := cli.db.Get(key)
	if err != nil || value == nil {
		return nil, err
	}
	return redcon.SimpleString(value), nil
}

func del(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) == 0 {
		return nil, wrongNumOfCmd("del")
	}
	cal := 0
	for _, key := range args {
		err := cli.db.Del(key)
		if err == nil {
			cal++
		}
	}
	return redcon.SimpleString(strconv.Itoa(cal)), nil
}
func appends(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, wrongNumOfCmd("append")
	}

	key := args[0]
	value, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}

	expire, err := cli.db.Ttl(key)
	if err != nil {
		return nil, err
	}
	value = append(value, args[1]...)
	if expire == -1 {
		err := cli.db.Set(key, 0, value)
		if err != nil {
			return nil, err
		}
	} else if expire != -2 {
		err := cli.db.Set(key, time.Duration(expire), value)
		if err != nil {
			return nil, err
		}
	}
	return redcon.SimpleString(strconv.Itoa(len(value))), nil
}

func ttl(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, wrongNumOfCmd("ttl")
	}
	key := args[0]
	expire, err := cli.db.Ttl(key)
	if err != nil {
		return nil, err
	}
	if expire == -1 || expire == -2 {
		return redcon.SimpleString(strconv.Itoa(int(expire))), nil
	}
	remainingSeconds := int(expire / int64(time.Second))
	return redcon.SimpleString(strconv.Itoa(remainingSeconds)), nil
}

// TODO: HSET HGET HGETALL
// TODO: LPUSH LPOP RPUSH RPOP
// TODO: SADD SREM SMEMBERS
// TODO: ZADD ZRANGE
