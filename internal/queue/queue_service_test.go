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
	metaDataPath := createTempDir("testEnqueue/metadata")
	defer os.RemoveAll(segmentPath)
	defer os.RemoveAll(metaDataPath)
	cfg := config.NewConfig(segmentPath, metaDataPath, 1024, time.Second)

	queueService, err := NewQueueService(cfg)
	assert.NoError(t, err)

	assert.NoError(t, queueService.Enqueue([]byte("Hello World")))
	assert.NoError(t, queueService.Enqueue([]byte("Hello World 1")))
	assert.NoError(t, queueService.Enqueue([]byte("Hello World 2")))
	assert.NoError(t, queueService.Enqueue([]byte("Hello World 3")))

	_, err = queueService.Dequeue(1)
	assert.NoError(t, err)
	//assert.Equal(t, []byte("Hello World"), data)
	//
	//data, _ = queueService.Dequeue(1)
	//assert.Equal(t, []byte("Hello World 1"), data)
	//
	//data, _ = queueService.Dequeue(1)
	//assert.Equal(t, []byte("Hello World 2"), data)
	//
	//data, _ = queueService.Dequeue(2)
	//assert.Equal(t, []byte("Hello World"), data)
	//
	//data, _ = queueService.Dequeue(1)
	//assert.Equal(t, []byte("Hello World 3"), data)
}
