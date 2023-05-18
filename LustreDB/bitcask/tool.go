package bitcask

import (
	"LustreDB/bitcask/data"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

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
	for i, fileId := range db.fileIds {
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

			// 如果是删除的类型就从索引当中删除
			if read.Type == data.LogRecordDelete {
				db.index.Delete(read.Key)
			} else {
				db.index.Put(read.Key, &pos)
			}
			// 读取之后偏移数据量的长度
			offset += size
		}

		// 如果是当前活跃文件，更新这个文件的 offset
		if i == len(db.fileIds)-1 {
			db.activeFiles.WriteOff = offset
		}
	}
	return nil
}

// 检查输入是否正确
func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path can not empty")
	}
	if options.DataFileSize < 0 {
		return errors.New("database should greater than 0")
	}
	return nil
}
