package utils

import "errors"

var (
	ErrKeyIsEmpty = errors.New("key is empty")

	ErrIndexUpdateFailed = errors.New("fail to update the index")

	ErrKeyNotFound = errors.New("key no found in database")

	ErrDataFileNotFound = errors.New("cannot found data file")

	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")

	ErrorIncorrectCrc = errors.New("CRC does not pass")

	ErrorOverMaxNumber = errors.New("the context is over the max number")
)
