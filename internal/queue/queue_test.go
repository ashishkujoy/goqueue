package queueinternal

import (
	"ashishkujoy/queue/internal/config"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func createTempDir(suffix string) string {
	dir := os.TempDir() + "/" + suffix
	os.MkdirAll(dir, 0755)
	return dir
}

func removeTempDir(suffix string) {
	dir := os.TempDir() + "/" + suffix
	os.RemoveAll(dir)
}

func TestEnqueueSingleElement(t *testing.T) {
	cfg := config.NewConfig(
		createTempDir("TestEnqueueSingleElement"),
		createTempDir("metadata"),
		1000,
		time.Second)
	defer removeTempDir("TestEnqueueSingleElement")
	defer removeTempDir("metadata")
	queue, err := NewQueue(cfg)
	assert.NoError(t, err)
	defer queue.Close()

	id, err := queue.Enqueue([]byte("First Message"))
	assert.NoError(t, err)

	data, err := queue.Dequeue(id)
	assert.NoError(t, err)
	assert.Equal(t, data, []byte("First Message"))
}
