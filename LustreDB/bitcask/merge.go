package bitcask

import (
	"LustreDB/bitcask/data"
	"LustreDB/bitcask/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDir    = "-merge"
	mergeFinish = "-merge_finish"
)

// Merge 清理无效数据，生成hint文件
func (db *DB) Merge() error {
	// 活跃文件为空，那么直接返回
	if db.activeFiles == nil {
		return nil
	}

	db.lo.Lock()
	if db.merged {
		return utils.ErrorMergeIsProgress
	}

	db.merged = true
	defer func() {
		db.merged = false
	}()

	// 持久化当前活跃文件
	err := db.activeFiles.Sync()
	if err != nil {
		db.lo.Unlock()
		return err
	}
	// 将现在的活跃文件变为旧文件，然后在开一个新的活跃文件
	db.olderFiles[db.activeFiles.FileId] = db.activeFiles
	err = db.setActiveData()
	if err != nil {
		db.lo.Unlock()
		return err
	}

	noMergedFile := db.activeFiles.FileId

	// 取出所以需要的 merge 文件
	var mergeFile []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFile = append(mergeFile, file)
	}
	db.lo.Unlock()

	// 从小到大依次进行merge
	sort.Slice(mergeFile, func(i, j int) bool {
		return mergeFile[i].FileId < mergeFile[j].FileId
	})
	mergePath := db.getMergePath()
	// 如果目录存在，说明已经merge过了，应该把这个目录删掉
	_, err = os.Stat(mergePath)
	if os.IsNotExist(err) {
		err := os.RemoveAll(mergePath)
		if err != nil {
			return err
		}
	}

	// 新建应该对应的目录
	err = os.MkdirAll(mergePath, os.ModePerm)
	if err != nil {
		return err
	}

	// 打开一个临时的实例
	mergeOption := db.options
	mergeOption.DirPath = mergePath
	mergeOption.SyncWrites = false
	mergeDB, err := Open(mergeOption)
	if err != nil {
		return err
	}

	file, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历处理每个数据文件
	for _, dataFile := range mergeFile {
		var offset int64 = 0
		for {
			read, i, err := dataFile.Read(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			record, _ := parseLogRecord(read.Key)
			get := db.index.Get(record)

			// 和索引内存中的进行比较
			if get != nil && get.Fid == dataFile.FileId && get.Offset == offset {
				read.Key = logRecordKeyWithSeq(record, nonTransactionSeq)
				pos, err := mergeDB.appendLogRecord(read)
				if err != nil {
					return err
				}
				err = file.WriteHintRecord(record, pos)
				if err != nil {
					return err
				}
			}
			offset += i
		}
	}

	// 持久化
	err = file.Sync()
	if err != nil {
		return err
	}
	err = mergeDB.activeFiles.Sync()
	if err != nil {
		return err
	}

	openMergeFile, err := data.OpenMergeFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinish),
		Value: []byte(strconv.Itoa(int(noMergedFile))),
	}
	record, _ := data.EncodeLogRecord(mergeFinRecord)
	err = openMergeFile.Write(record)
	if err != nil {
		return err
	}

	err = openMergeFile.Sync()
	if err != nil {
		return err
	}

	return nil
}

// 得到merge的路径
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDir)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()

	_, err := os.Stat(mergePath)
	if os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dir, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找表示看一下是否完成，找标识的数据文件
	var mergeFinished bool
	var mergeFileName []string
	for _, i := range dir {
		if i.Name() == data.MergeFileName {
			mergeFinished = true
		}
		if i.Name() == data.SeqNoName {
			continue
		}
		mergeFileName = append(mergeFileName, i.Name())
	}

	if !mergeFinished {
		return nil
	}

	// 看看merge是否完成没有完成直接返回
	fid, err := db.NoMergeFinishedFid(mergePath)
	if err != nil {
		return nil
	}

	// 删除旧的数据文件，就是id小于没有merge的
	var fileId uint32 = 0
	for fileId < fid {
		name := data.GetDataFileName(db.options.DirPath, fileId)
		_, err := os.Stat(name)
		if err == nil {
			err = os.Remove(name)
			if err != nil {
				return err
			}
		}

		fileId++
	}
	// 新的数据文件移动到数据目录中
	for _, fileName := range mergeFileName {
		srcPath := filepath.Join(mergePath, fileName)
		desPath := filepath.Join(db.options.DirPath, fileName)
		err := os.Rename(srcPath, desPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) NoMergeFinishedFid(pathDir string) (uint32, error) {
	file, err := data.OpenMergeFile(pathDir)
	if err != nil {
		return 0, err
	}
	read, _, err := file.Read(0)
	if err != nil {
		return 0, err
	}
	atoi, err := strconv.Atoi(string(read.Value))
	if err != nil {
		return 0, err
	}
	return uint32(atoi), nil
}

func (db *DB) loadIndexFromHintFile() error {
	join := filepath.Join(db.options.DirPath, data.HintFileName)
	_, err := os.Stat(join)
	if os.IsNotExist(err) {
		return nil
	}

	file, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		read, i, err := file.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		offset += i

		pos := data.DecodeLogRecordPos(read.Value)
		db.index.Put(read.Key, pos)
	}
	return nil
}
