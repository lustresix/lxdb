package LustreDB

import (
	data2 "LustreDB/data"
	"LustreDB/index"
	"LustreDB/utils"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const seqNoKey = "seq.no"

// DB bitcask 存储引擎实例
type DB struct {
	// 配置项
	options Options

	// 读写锁
	lo *sync.RWMutex

	// 文件 id，只能在加载索引的时候使用，不能在其他地方更新或使用
	fileIds []int

	// 活跃文件
	activeFiles *data2.DataFile

	// 旧的数据文件
	olderFiles map[uint32]*data2.DataFile

	// 内存索引
	index index.Indexer

	// 事物序列号
	seqNo uint64

	// 是否在merge
	merged bool

	// 是否存在seqNo文件
	seqNoFileExists bool

	// 是否初始化
	isInitial bool
}

func Open(options Options) (*DB, error) {
	// 对用户传入的配置项的校验
	err := checkOptions(options)
	if err != nil {
		return nil, err
	}

	var isInitial bool

	// 目录是否存在，如果目录不存在则创建
	_, err = os.Stat(options.DirPath)
	if os.IsNotExist(err) {
		isInitial = true
		err := os.MkdirAll(options.DirPath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	dir, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(dir) == 0 {
		isInitial = true
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		lo:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data2.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
	}

	err = db.loadMergeFiles()
	if err != nil {
		return nil, err
	}

	// 加载数据文件
	err = db.loadDataFiles()
	if err != nil {
		return nil, err
	}

	if options.IndexType != BPtree {
		// 是否有索引文件，如果有从索引文件中加载
		err = db.loadIndexFromHintFile()
		if err != nil {
			return nil, err
		}

		// 从数据文件中加载索引
		err = db.loadIndexFromDataFiles()
		if err != nil {
			return nil, err
		}
	}

	if options.IndexType == BPtree {
		err := db.loadSeqNo()
		if err != nil {
			return nil, err
		}
		if db.activeFiles != nil {
			size, err := db.activeFiles.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFiles.WriteOff = size
		}
	}

	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFiles == nil {
		return nil
	}
	db.lo.Lock()
	defer db.lo.Unlock()
	err := db.index.Close()
	if err != nil {
		return err
	}

	// 保存当前事务序列号
	file, err := data2.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data2.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	logRecord, _ := data2.EncodeLogRecord(record)

	err = file.Write(logRecord)
	if err != nil {
		return err
	}

	err = file.Sync()
	if err != nil {
		return err
	}

	err = db.activeFiles.Close()
	if err != nil {
		return err
	}

	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFiles == nil {
		return nil
	}
	db.lo.Lock()
	defer db.lo.Unlock()
	return db.activeFiles.Sync()
}

// Delete 根据 key 来删除对应的数据
func (db *DB) Delete(key []byte) error {
	// 判断 key 的有效性
	if len(key) == 0 {
		return utils.ErrKeyIsEmpty
	}

	// 检查 key 是否存在
	get := db.index.Get(key)
	if get == nil {
		return utils.ErrKeyNotFound
	}

	// 构造 LogRecord 文件 标记为这个是被删除的数据
	logRecord := &data2.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeq),
		Type: data2.LogRecordDelete,
	}

	// 把数据追加写入到文档中
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 在内存索引中删除 key
	b := db.index.Delete(key)
	if !b {
		return utils.ErrKeyNotFound
	}

	return nil

}

// Put 写入 Key/Value 数据， Key 不为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return utils.ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	record := &data2.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeq),
		Value: value,
		Type:  data2.LogRecordNormal,
	}
	// 追加写入到当前活跃的数据库中
	logRecord, err := db.appendLogRecordWithLock(record)
	if err != nil {
		return err
	}

	// 更新索引
	put := db.index.Put(key, logRecord)
	if !put {
		return utils.ErrIndexUpdateFailed
	}

	return nil
}

// Get 根据 key 来读取数据，key 不能为空
func (db *DB) Get(key []byte) ([]byte, error) {
	db.lo.RLock()
	defer db.lo.RUnlock()

	// 判断 key 是否有效
	if len(key) == 0 {
		return nil, utils.ErrKeyIsEmpty
	}

	// 从内存的数据结构中取出 key 对应索引的位置信息
	get := db.index.Get(key)
	// 如果找不到说明 key 不存在
	if get == nil {
		return nil, utils.ErrKeyNotFound
	}
	value, err := db.getValue(get)
	return value, err
}

// ListKeys 获取数据库中所有的key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有的数据，并执行用户指定的操作，函数返回 false 时停止遍历
func (db *DB) Fold(fn func(key, value []byte) bool) error {
	db.lo.RLock()
	defer db.lo.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValue(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) getValue(get *data2.LogRecordPos) ([]byte, error) {
	// 根据文件的 id 找到数据文件,如果活跃文件里没有，就从旧的数据文件里面找
	var dataFile *data2.DataFile
	if db.activeFiles.FileId == get.Fid {
		dataFile = db.activeFiles
	} else {
		dataFile = db.olderFiles[get.Fid]
	}

	if dataFile == nil {
		return nil, utils.ErrDataFileNotFound
	}

	// 根据偏移量读取数据
	read, _, err := dataFile.Read(get.Offset)
	if err != nil {
		return nil, err
	}

	// 判断此条数据是否有被删除
	if read.Type == data2.LogRecordDelete {
		return nil, utils.ErrDataFileNotFound
	}
	return read.Value, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data2.LogRecord) (*data2.LogRecordPos, error) {
	db.lo.Lock()
	defer db.lo.Unlock()

	return db.appendLogRecord(logRecord)
}

// 追加写入到当前活跃的文件中
func (db *DB) appendLogRecord(logRecord *data2.LogRecord) (*data2.LogRecordPos, error) {
	// 判断当前活跃数据文件是否存在
	// 如果为空则初始化文件
	if db.activeFiles == nil {
		err := db.setActiveData()
		if err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	record, size := data2.EncodeLogRecord(logRecord)

	// 如果这个数据满了那么将当前的转换为旧的数据文件，创建新的数据文件
	if db.activeFiles.WriteOff+size > db.options.DataFileSize {
		// 先将数据持久化
		err := db.activeFiles.Sync()
		if err != nil {
			return nil, err
		}
		// 将活跃文件转化为旧文件
		db.olderFiles[db.activeFiles.FileId] = db.activeFiles

		// 打开新的数据文件
		err = db.setActiveData()
		if err != nil {
			return nil, err
		}
	}

	// 数据的偏移地址
	off := db.activeFiles.WriteOff

	// 写入数据
	err := db.activeFiles.Write(record)

	if err != nil {
		return nil, err
	}

	// 根据用户所选是否需要持久化
	if db.options.SyncWrites {
		err := db.activeFiles.Sync()
		if err != nil {
			return nil, err
		}
	}

	pos := &data2.LogRecordPos{
		Fid:    db.activeFiles.FileId,
		Offset: off,
	}

	return pos, nil
}

func DestroyDB(db *DB) {
	_ = db.Close()
	_ = os.RemoveAll(db.options.DirPath)
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data2.SeqNoName)
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return err
	}

	seqNoFile, err := data2.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}

	read, _, err := seqNoFile.Read(0)
	if err != nil {
		return err
	}

	atoi, err := strconv.ParseUint(string(read.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNo = atoi
	db.seqNoFileExists = true

	return os.Remove(fileName)
}
