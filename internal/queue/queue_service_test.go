package queueinternal

import (
	"ashishkujoy/queue/internal/config"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnqueue(t *testing.T) {
	segmentPath := createTempDir("testEnqueue/segments")
	defer os.RemoveAll(segmentPath)
	metaDataPath := createTempDir("testEnqueue/metadata")
	defer os.RemoveAll(metaDataPath)
	config := config.NewConfig(segmentPath, metaDataPath, 1024, time.Second)

	queueService, err := NewQueueService(config)
	assert.NoError(t, err)

	queueService.Enqueue([]byte("Hello World"))
	queueService.Enqueue([]byte("Hello World 1"))
	queueService.Enqueue([]byte("Hello World 2"))
	queueService.Enqueue([]byte("Hello World 3"))

	data, _ := queueService.Dequeue(1)
	assert.Equal(t, []byte("Hello World"), data)

	data, _ = queueService.Dequeue(1)
	assert.Equal(t, []byte("Hello World 1"), data)

	data, _ = queueService.Dequeue(1)
	assert.Equal(t, []byte("Hello World 2"), data)

	data, _ = queueService.Dequeue(2)
	assert.Equal(t, []byte("Hello World"), data)

	data, _ = queueService.Dequeue(1)
	assert.Equal(t, []byte("Hello World 3"), data)
}
