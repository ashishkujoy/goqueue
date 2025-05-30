package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendAndReadSingleEntry(t *testing.T) {
	filePath := fmt.Sprintf("%s/%s", os.TempDir(), "TestAppendAndReadSingleEntry")
	defer os.Remove(filePath)
	store, err := NewStore(filePath)

	assert.NoError(t, err)

	offset, err := store.Append([]byte("Hello World"))
	assert.NoError(t, err)
	assert.Equal(t, 0, offset)

	data, err := store.Read(offset)
	assert.NoError(t, err)
	assert.Equal(t, data, []byte("Hello World"))
}

func TestAppendAndReadMultipleEntry(t *testing.T) {
	filePath := fmt.Sprintf("%s/%s", os.TempDir(), "TestAppendAndReadMultipleEntry")
	defer os.Remove(filePath)
	store, err := NewStore(filePath)
	defer store.Close()

	assert.NoError(t, err)

	offset1, _ := store.Append([]byte("Hello World"))
	offset2, _ := store.Append([]byte("Another Hello World"))
	data1, err := store.Read(offset1)
	assert.NoError(t, err)
	data2, err := store.Read(offset2)
	assert.NoError(t, err)
	assert.Equal(t, data1, []byte("Hello World"))
	assert.Equal(t, data2, []byte("Another Hello World"))
}

func TestReadFromANewStoreAfterClose(t *testing.T) {
	filePath := fmt.Sprintf("%s/%s", os.TempDir(), "TestReadFromANewStoreAfterClose")
	defer os.Remove(filePath)
	store, err := NewStore(filePath)
	defer store.Close()

	assert.NoError(t, err)

	offset1, _ := store.Append([]byte("Hello World"))
	offset2, _ := store.Append([]byte("Another Hello World"))
	err = store.Close()
	assert.NoError(t, err)

	store, err = NewStore(filePath)
	assert.NoError(t, err)

	data1, err := store.Read(offset1)
	assert.NoError(t, err)
	data2, err := store.Read(offset2)
	assert.NoError(t, err)
	assert.Equal(t, data1, []byte("Hello World"))
	assert.Equal(t, data2, []byte("Another Hello World"))
}

func TestRestoreStore(t *testing.T) {
	filePath := fmt.Sprintf("%s/%s", os.TempDir(), "TestReadFromANewStoreAfterClose")
	defer os.Remove(filePath)
	store, err := NewStore(filePath)
	defer store.Close()

	assert.NoError(t, err)

	offset1, _ := store.Append([]byte("Hello World"))
	offset2, _ := store.Append([]byte("Another Hello World"))
	err = store.Close()
	assert.NoError(t, err)

	restoreStore, err := RestoreStore(filePath)
	assert.Nil(t, restoreStore.writer)
	assert.NoError(t, err)

	data1, err := restoreStore.Read(offset1)
	assert.NoError(t, err)
	assert.Equal(t, data1, []byte("Hello World"))
	data2, err := restoreStore.Read(offset2)
	assert.NoError(t, err)
	assert.Equal(t, data2, []byte("Another Hello World"))
	assert.Equal(t, data1, []byte("Hello World"))
}
