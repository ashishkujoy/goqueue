package consumer

import (
	"ashishkujoy/queue/internal/config"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func CreateMetadataDir(prefix string) (string, error) {
	metadataDir := os.TempDir() + "/metadata" + prefix
	err := os.MkdirAll(metadataDir, 0755)
	if err != nil {
		return "", err
	}
	return metadataDir, nil
}

func TestReadAndWriteIndex(t *testing.T) {
	metadataDir, err := CreateMetadataDir("1")
	assert.NoError(t, err)
	defer os.RemoveAll(metadataDir)

	cfg := config.NewConfig("/tmp", metadataDir, 1234, time.Second*100)
	index, err := NewConsumerIndex(cfg)
	assert.NoError(t, err)

	index.WriteIndex(1, 10)
	index.WriteIndex(2, 20)
	index.WriteIndex(3, 30)

	assert.Equal(t, 10, index.ReadIndex(1))
	assert.Equal(t, 20, index.ReadIndex(2))
	assert.Equal(t, 30, index.ReadIndex(3))
	assert.Equal(t, -1, index.ReadIndex(4))
}

func TestReadFromARestoredIndex(t *testing.T) {
	metadataDir, err := CreateMetadataDir("2")
	assert.NoError(t, err)
	defer os.RemoveAll(metadataDir)

	cfg := config.NewConfig("/tmp", metadataDir, 1234, time.Second*100)
	index, err := NewConsumerIndex(cfg)
	assert.NoError(t, err)

	index.WriteIndex(11, 10)
	index.WriteIndex(12, 20)
	index.WriteIndex(13, 30)

	assert.NoError(t, index.Close())

	restoredIndex, err := RestoreConsumerIndex(cfg)
	assert.NoError(t, err)

	assert.Equal(t, 10, restoredIndex.ReadIndex(11))
	assert.Equal(t, 20, restoredIndex.ReadIndex(12))
	assert.Equal(t, 30, restoredIndex.ReadIndex(13))
	assert.Equal(t, -1, restoredIndex.ReadIndex(14))
}

func TestRestoreIndexUsesTheLatestSnapshot(t *testing.T) {
	metadataDir, err := CreateMetadataDir("2")
	assert.NoError(t, err)
	defer os.RemoveAll(metadataDir)

	cfg := config.NewConfig("/tmp", metadataDir, 1234, time.Second*100)

	index1, err := NewConsumerIndex(cfg)
	assert.NoError(t, err)

	index1.WriteIndex(11, 10)
	index1.WriteIndex(12, 20)
	index1.WriteIndex(13, 30)

	assert.NoError(t, index1.Close())

	index2, err := RestoreConsumerIndex(cfg)
	assert.NoError(t, err)

	index2.WriteIndex(12, 200)
	index2.WriteIndex(14, 1300)

	assert.NoError(t, index2.Close())

	restoredIndex, err := RestoreConsumerIndex(cfg)
	assert.NoError(t, err)

	assert.Equal(t, 10, restoredIndex.ReadIndex(11))
	assert.Equal(t, 200, restoredIndex.ReadIndex(12))
	assert.Equal(t, 30, restoredIndex.ReadIndex(13))
	assert.Equal(t, 1300, restoredIndex.ReadIndex(14))
}
