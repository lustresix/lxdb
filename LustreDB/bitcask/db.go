package bitcask

import (
	"LustreDB/bitcask/data"
	"LustreDB/bitcask/index"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	// 配置项
	options Options

	// 锁
	lo *sync.RWMutex

	// 活跃文件
	activeFiles *data.DataFile

	// 旧的数据文件
	olderFiles map[uint32]*data.DataFile

	// 内存索引
	index index.Indexer
}

func (db *DB) Put(key []byte, value []byte) error {
	// 判断 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体
	record := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	// 追加写入到当前活跃的数据库中
	logRecord, err := db.appendLogRecord(&record)
	if err != nil {
		return err
	}

	// 更新索引
	put := db.index.Put(key, logRecord)
	if put {
		return ErrIndexUpdateFailed
	}

	return nil
}

// 追加写入到当前活跃的数据库中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.lo.Lock()
	defer db.lo.Unlock()

	// 判断当前活跃数据文件是否存在
	// 如果为空则初始化文件
	if db.activeFiles == nil {
		err := db.setActiveData()
		if err != nil {
			return nil, err
		}
	}

	// 写入数据编码
	record, size := data.EncodeLogRecord(logRecord)

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
	// 数据的偏移地址
	off := db.activeFiles.WriteOff

	pos := data.LogRecordPos{
		Fid:    db.activeFiles.FileId,
		Offset: off,
	}
	return &pos, nil
}

// setActiveData 初始化活跃的文件
// 在访问此方法必须要持有互斥锁
func (db *DB) setActiveData() error {
	var initialFileId uint32 = 0
	if db.activeFiles != nil {
		// 如果当前活跃文件id不为空，那么新的文件的id就是原来的+1
		initialFileId = db.activeFiles.FileId + 1
	}

	// 打开新的数据文件
	file, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFiles = file
	return nil
}
