package io

import "os"

type FileIO struct {
	// 系统文件的描述符
	fo *os.File
}

// NewFileIOManager 初始化标准文件
func NewFileIOManager(fileName string) (*FileIO, error) {
	file, err := os.OpenFile(
		fileName,
		// 如果不存在就创建，并且赋予读写权限，只允许追加写入
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePerm,
	)
	if err != nil {
		return nil, err
	}

	return &FileIO{fo: file}, nil
}

// 封装系统 io 方便后续调用不同的 io 类型
// 如接入 mmap 等

func (f *FileIO) Read(b []byte, offset int64) (int, error) {
	return f.fo.ReadAt(b, offset)
}

func (f *FileIO) Write(b []byte) (int, error) {
	return f.fo.Write(b)
}

func (f *FileIO) Sync() error {
	return f.fo.Sync()
}

func (f *FileIO) Close() error {
	return f.fo.Close()
}

func (f *FileIO) Size() (int64, error) {
	stat, err := f.fo.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
