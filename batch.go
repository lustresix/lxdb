package LustreDB

import (
	"encoding/binary"
	"github.com/lustresix/lxdb/data"
	"github.com/lustresix/lxdb/utils"
	"sync"
	"sync/atomic"
)

// 表示非事务的序号
const nonTransactionSeq uint64 = 0

// 表示事务执行完成
var txnFinKey = []byte("txn-fin")

// WriteBatch 原子性
type WriteBatch struct {
	options       WriteBatchOptions
	lo            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(opt WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPtree && !db.seqNoFileExists && !db.isInitial {
		panic("cannot use write batch, seq no file not exists")
	}
	return &WriteBatch{
		options:       opt,
		lo:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return utils.ErrKeyIsEmpty
	}
	wb.lo.Lock()
	defer wb.lo.Unlock()

	record := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = record
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return utils.ErrKeyIsEmpty
	}
	wb.lo.Lock()
	defer wb.lo.Unlock()

	get := wb.db.index.Get(key)
	if get != nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
	}

	record := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDelete,
	}
	wb.pendingWrites[string(key)] = record
	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.lo.Lock()
	defer wb.lo.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	} else if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return utils.ErrorOverMaxNumber
	}

	wb.db.lo.Lock()
	defer wb.db.lo.Unlock()

	// 获取当前事物的序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 将写单条数据先暂存起来，直到全部运行完之后再进行更新
	position := make(map[string]*data.LogRecordPos)
	for _, recode := range wb.pendingWrites {
		seq := logRecordKeyWithSeq(recode.Key, seqNo)
		record, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   seq,
			Type:  recode.Type,
			Value: recode.Value,
		})
		if err != nil {
			return err
		}

		position[string(recode.Key)] = record
	}
	// 事物完成最后再加一条
	d := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordFinish,
	}

	// 把事务完成的标识加入db中
	_, err := wb.db.appendLogRecord(d)
	if err != nil {
		return err
	}

	// 如果事务内的record全部完成，就根据配置进行持久化
	if wb.options.SyncWrite && wb.db.activeFiles != nil {
		err := wb.db.activeFiles.Sync()
		if err != nil {
			return err
		}
	}

	// 更新索引
	for _, record := range wb.pendingWrites {
		pos := position[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		} else if record.Type == data.LogRecordDelete {
			wb.db.index.Delete(record.Key)
		}
	}

	// 清空暂存的数据
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// 编码key和seq
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	// 可变长度整数（以下简称为varint）压缩算法是将整数压缩成比通常需要的更小空间的一种方法。
	//一个varint算法以用一个字节表示10，而用4个字节来表示8亿。
	varint := binary.PutUvarint(seq[:], seqNo)

	// 编码之后的key值为前半部分为原来的key，拼接上压缩之后的seqNo
	bytes := make([]byte, varint+len(key))
	copy(bytes[:varint], seq[:varint])
	copy(bytes[varint:], key)

	return bytes
}

// 解析logRecord的key，获取实际的key和序列号
func parseLogRecord(key []byte) ([]byte, uint64) {
	seq, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seq
}
