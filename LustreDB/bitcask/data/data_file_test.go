package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	file1, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, file1)

	file3, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file3)

	t.Log(os.TempDir())
}

func TestDataFile_Write(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("aaa"))
	assert.Nil(t, err)

}

func TestDataFile_Close(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("aaa"))
	assert.Nil(t, err)
	err = file.Close()
	assert.Nil(t, err)
}

func TestDataFile_Read(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	err = file.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = file.Sync()
	assert.Nil(t, err)
}
