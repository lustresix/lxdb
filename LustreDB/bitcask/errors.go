package bitcask

import "errors"

var (
	ErrKeyIsEmpty = errors.New("key is empty")

	ErrIndexUpdateFailed = errors.New("fail to update the index")
)
