package redis

import (
	"encoding/binary"
	"errors"
	LustreDB "github.com/lustresix/lxdb"
	"github.com/lustresix/lxdb/utils"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("wrong type operation against a key holding the wrong kind of value")
)

// RedisDataStructure Redis 数据结构的数据
type RedisDataStructure struct {
	db *LustreDB.DB
}

type RedisDataType = byte

const (
	String RedisDataType = iota + 1
	Hash
	List
	Set
	ZSet
)

func NewRedisDataStructure(option LustreDB.Options) (*RedisDataStructure, error) {
	open, err := LustreDB.Open(option)
	if err != nil {
		return nil, err
	}

	return &RedisDataStructure{
		db: open,
	}, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

// -------------------- String --------------------

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// value = type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	// 如果用户设置过期时间那么就是 now + ttl
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// 调用存储接口写入数据

	err := rds.db.Put(key, encValue)
	return err
}

// TODO:整合一下 ttl 和 get 代码部分可以重复使用

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	get, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 先解码
	dataType := get[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	var idx = 1
	expire, n := binary.Varint(get[idx:])
	idx += n

	// key 是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return get[idx:], nil
}

func (rds *RedisDataStructure) Ttl(key []byte) (int64, error) {
	get, err := rds.db.Get(key)
	if err != nil {
		return -2, err
	}

	// 先解码
	dataType := get[0]
	if dataType != String {
		return -2, ErrWrongTypeOperation
	}

	var idx = 1
	expire, _ := binary.Varint(get[idx:])
	if expire == 0 {
		return -1, nil
	}

	// key 是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return -2, nil
	}

	return expire - time.Now().UnixNano(), nil
}

// -------------------- Hash --------------------

func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造 hash 数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	// 数据是否存在
	var exist = true
	_, err = rds.db.Get(encKey)
	if errors.Is(err, utils.ErrKeyNotFound) {
		exist = false
	}

	wb := rds.db.NewWriteBatch(LustreDB.DefaultWriteBatchOptions)
	//不存在则更新
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, value)
	err = wb.Commit()
	if err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encode := hk.encode()

	// 是否存在
	var exist = true
	_, err = rds.db.Get(encode)
	if errors.Is(err, utils.ErrKeyNotFound) {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(LustreDB.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encode)
		err := wb.Commit()
		if err != nil {
			return false, err
		}
	}

	return !exist, nil
}

// -------------------- List --------------------

func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// 数据部分
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	// TODO: 如果只从一边push数据导致数据量过大
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	wb := rds.db.NewWriteBatch(LustreDB.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), nil)
	err = wb.Commit()
	if err != nil {
		return 0, err
	}
	return meta.size, nil
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.PopInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.PopInner(key, false)
}

func (rds *RedisDataStructure) PopInner(key []byte, isLeft bool) ([]byte, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	// 数据部分
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	err = rds.db.Put(key, meta.encode())
	if err != nil {
		return nil, err
	}
	return element, nil
}

// -------------------- Set --------------------

func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	_, err = rds.db.Get(sk.encode())
	if errors.Is(err, utils.ErrKeyNotFound) {
		wb := rds.db.NewWriteBatch(LustreDB.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		err := wb.Commit()
		if err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && !errors.Is(err, utils.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, utils.ErrKeyNotFound) {
		return false, nil
	}
	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && errors.Is(err, utils.ErrKeyNotFound) {
		return false, nil
	}
	wb := rds.db.NewWriteBatch(LustreDB.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	err = wb.Commit()
	if err != nil {
		return false, err
	}
	return true, nil
}

// -------------------- ZSet --------------------

func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
		score:   score,
	}

	var exist = true
	val, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && !errors.Is(err, utils.ErrKeyNotFound) {
		return false, nil
	}

	// 是否存在
	if errors.Is(err, utils.ErrKeyNotFound) {
		exist = false
	}

	// 是否重复
	if exist {
		if score == BtoF(val) {
			return false, nil
		}
	}

	// 如果存在相同的member且不重复，就删除原先的就数据
	wb := rds.db.NewWriteBatch(LustreDB.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	} else {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   BtoF(val),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), FtoB(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	err = wb.Commit()
	if err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}
	return BtoF(value), nil
}
