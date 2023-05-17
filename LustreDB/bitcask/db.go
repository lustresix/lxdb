package bitcask

import (
	"LustreDB/bitcask/data"
	"LustreDB/bitcask/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	// 配置项
	options Options

	// 锁
	lo *sync.RWMutex

	// 文件 id，只能在加载索引的时候使用，不能在其他地方更新或使用
	fileIds []int

	// 活跃文件
	activeFiles *data.DataFile

	// 旧的数据文件
	olderFiles map[uint32]*data.DataFile

	// 内存索引
	index index.Indexer
}

func Open(options Options) (*DB, error) {
	// 对用户传入的配置项的校验
	err := checkOptions(options)
	if err != nil {
		return nil, err
	}

	// 目录是否存在，如果目录不存在则创建
	_, err = os.Stat(options.DirPath)
	if os.IsNotExist(err) {
		err := os.Mkdir(options.DirPath, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		lo:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载数据文件
	err = db.loadDataFiles()
	if err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	err = db.loadIndexFromDataFiles()
	if err != nil {
		return nil, err
	}

	return db, nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {

	dir, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	// 遍历目录中的文件，找到所有以 .data 结尾的文件
	for _, entry := range dir {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 自动生成的数据是比如说 0001.data
			// 切割后取 0001 为自动生成的文件名
			// 如果这个已经不是数字了，那么就判定为文件被损坏
			split := strings.Split(entry.Name(), ".")
			atoi, err := strconv.Atoi(split[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, atoi)
		}
	}

	// 对文件 id 进行排序，从小到大依次加载
	sort.Ints(fileIds)

	// 遍历每个文件 id， 打开对应的数据文件
	for i, fid := range fileIds {
		file, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		// 如果这是最后一个文件，代表他是活跃的文件
		if i == len(fileIds)-1 {
			db.activeFiles = file
		} else {
			db.olderFiles[uint32(fid)] = file
		}
	}

	return nil

}

// 从数据文件中加载索引
// 遍历文件中的记录，并更新到内部索引
func (db *DB) loadIndexFromDataFiles() error {
	// 如果是空的数据库就返回
	if len(db.fileIds) == 0 {
		return nil
	}

	// 遍历所有文件id，处理文件中的记录
	for _, fileId := range db.fileIds {
		var id = uint32(fileId)
		var dataFile *data.DataFile
		if id == db.activeFiles.FileId {
			dataFile = db.activeFiles
		} else {
			dataFile = db.olderFiles[id]
		}

		var offset int64 = 0
		for {
			read, size, err := dataFile.Read(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			pos := data.LogRecordPos{Fid: id, Offset: offset}

			if read.Type == data.LogRecordDelete {
				db.index.Delete(read.Key)
			} else {
				db.index.Put(read.Key, &pos)
			}

			offset += size
		}
	}
	return nil
}

// Put 写入 Key/Value 数据， Key 不为空
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

// Get 根据 key 来读取数据，key 不能为空
func (db *DB) Get(key []byte) ([]byte, error) {
	db.lo.RLock()
	defer db.lo.RUnlock()

	// 判断 key 是否有效
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存的数据结构中取出 key 对应索引的位置信息
	get := db.index.Get(key)
	// 如果找不到说明 key 不存在
	if get == nil {
		return nil, ErrKeyNotFound
	}

	// 根据文件的 id 找到数据文件,如果活跃文件里没有，就从旧的数据文件里面找
	var dataFile *data.DataFile
	if db.activeFiles.FileId == get.Fid {
		dataFile = db.activeFiles
	} else {
		dataFile = db.olderFiles[get.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量读取数据
	read, _, err := dataFile.Read(get.Offset)
	if err != nil {
		return nil, err
	}

	// 判断此条数据是否有被删除
	if read.Type == data.LogRecordDelete {
		return nil, ErrDataFileNotFound
	}
	return read.Value, nil
}

// 追加写入到当前活跃的文件中
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

func checkOptions(options Options) error {
	return nil
}
